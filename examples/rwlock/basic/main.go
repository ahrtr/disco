// Command basic demonstrates disco's read/write lock end-to-end: multiple
// readers running concurrently without blocking each other, a writer
// waiting for every one of them to release, and the same zombie-fencing
// safety guarantee the lock/election examples show — a writer whose lease
// has expired can no longer safely act as the writer, even though it once
// held the lock.
//
// Scenario:
//  1. Writer A locks (exclusive) and writes to the resource — accepted.
//  2. Reader B tries to RLock — it blocks, since A currently holds the
//     write lock.
//  3. Writer A gets stuck — keepalives stop, etcd expires its lease,
//     which releases A's write lock and unblocks B.
//  4. Reader B acquires the read lock. Reader C then also acquires the
//     read lock concurrently — proving readers never block each other.
//  5. Writer D tries to Lock — it blocks, since B and C both still hold
//     read locks.
//  6. B and C release. D's Lock unblocks: D is now the writer, and it
//     writes with its newer token — accepted.
//  7. Writer A wakes up and retries its write with its now-stale token —
//     rejected by the resource with 409 Conflict.
//
// Prerequisites:
//
//   - A running etcd cluster reachable at localhost:2379.
//
//     go run ./examples/rwlock/basic
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/ahrtr/disco/fencing"
	"github.com/ahrtr/disco/fencing/guard"
	etcdprovider "github.com/ahrtr/disco/provider/etcd"
	"github.com/ahrtr/disco/rwlock"
)

const rwlockKey = "/rwlocks/my-service"

func main() {
	// ── A real HTTP resource server, guarded by fencing/guard's HTTP
	// middleware, exactly like the lock/election examples.
	g := guard.New()
	resource := httptest.NewServer(g.HTTPMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "write accepted; high-water=%d\n", g.HighWater())
	})))
	defer resource.Close()
	log.Printf("Resource: real HTTP server listening at %s", resource.URL)

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("create etcd client: %v", err)
	}
	defer cli.Close()

	// Writer A uses a short TTL and a cancellable context.
	// Cancelling the context stops keepalives; etcd expires the lease after
	// the TTL — simulating a process freeze (GC pause, network partition, etc.).
	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()

	writerA, err := etcdprovider.NewRWLock(cli, rwlockKey,
		etcdprovider.WithContext(ctxA),
		etcdprovider.WithDefaultTTL(5*time.Second),
	)
	if err != nil {
		log.Fatalf("create writer A: %v", err)
	}
	defer writerA.Close()

	readerB, err := etcdprovider.NewRWLock(cli, rwlockKey)
	if err != nil {
		log.Fatalf("create reader B: %v", err)
	}
	defer readerB.Close()

	readerC, err := etcdprovider.NewRWLock(cli, rwlockKey)
	if err != nil {
		log.Fatalf("create reader C: %v", err)
	}
	defer readerC.Close()

	writerD, err := etcdprovider.NewRWLock(cli, rwlockKey)
	if err != nil {
		log.Fatalf("create writer D: %v", err)
	}
	defer writerD.Close()

	ctx := context.Background()

	// ── Step 1: Writer A locks and writes
	log.Println("Writer A: acquiring write lock …")
	grantA, err := writerA.Lock(ctx)
	if err != nil {
		log.Fatalf("writer A lock: %v", err)
	}
	log.Printf("Writer A: write lock acquired  fencing_token=%d  TTL=5s", grantA.FencingToken)
	doWrite(resource.URL, "Writer A", grantA.Token())

	// ── Step 2: Reader B tries to read and blocks (A holds the write lock)
	bLocked := make(chan struct{})
	go func() {
		log.Println("Reader B: waiting for the read lock (blocked by Writer A) …")
		if _, err := readerB.RLock(ctx); err != nil {
			log.Fatalf("reader B rlock: %v", err)
		}
		close(bLocked)
	}()

	// ── Step 3: Writer A gets stuck
	// cancelA stops the keepalive goroutine. etcd expires the lease after the
	// TTL, which releases A's write lock and unblocks Reader B.
	log.Println("Writer A: got stuck (keepalives stopped — lease expires in 5s) …")
	cancelA()

	// ── Step 4: Reader B acquires the read lock, then Reader C joins
	// concurrently — proving readers never block each other.
	<-bLocked
	log.Println("Reader B: read lock acquired")
	log.Println("Reader C: acquiring the read lock too (must not block on Reader B) …")
	if _, err := readerC.RLock(ctx); err != nil {
		log.Fatalf("reader C rlock: %v", err)
	}
	log.Println("Reader C: read lock acquired concurrently with Reader B")

	// ── Step 5: Writer D tries to lock and blocks (B and C both hold read locks)
	type lockResult struct {
		grant *rwlock.Grant
		err   error
	}
	dLocked := make(chan lockResult, 1)
	go func() {
		log.Println("Writer D: waiting for the write lock (blocked by B and C) …")
		grant, err := writerD.Lock(ctx)
		dLocked <- lockResult{grant, err}
	}()

	select {
	case <-dLocked:
		log.Fatal("BUG: Writer D acquired the write lock while readers still held it")
	case <-time.After(300 * time.Millisecond):
	}

	// ── Step 6: B and C release; D's write lock unblocks and it writes
	log.Println("Reader B: releasing …")
	if err := readerB.RUnlock(ctx); err != nil {
		log.Fatalf("reader B runlock: %v", err)
	}
	log.Println("Reader C: releasing …")
	if err := readerC.RUnlock(ctx); err != nil {
		log.Fatalf("reader C runlock: %v", err)
	}
	res := <-dLocked
	if res.err != nil {
		log.Fatalf("writer D lock: %v", res.err)
	}
	log.Printf("Writer D: write lock acquired  fencing_token=%d", res.grant.FencingToken)
	doWrite(resource.URL, "Writer D", res.grant.Token())

	// ── Step 7: Writer A wakes up and retries with its now-stale token
	log.Println("Writer A: woke up — retrying its write with its (now stale) token …")
	doWrite(resource.URL, "Writer A (stale)", grantA.Token())
}

// doWrite sends a real HTTP POST to the resource server with the given
// fencing token attached via the X-Fencing-Token header.
func doWrite(url, name string, token fencing.Token) {
	req, err := http.NewRequest(http.MethodPost, url, nil)
	if err != nil {
		log.Fatalf("build request: %v", err)
	}
	fencing.InjectHTTP(req, token)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatalf("%s: do request: %v", name, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	log.Printf("%s: %s — %s", name, resp.Status, strings.TrimSpace(string(body)))
}
