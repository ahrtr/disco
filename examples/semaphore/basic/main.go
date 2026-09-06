// Command basic demonstrates disco's counting semaphore end-to-end: a fixed
// pool of permits shared by more workers than the pool can hold at once,
// and the same zombie-fencing safety guarantee the lock/rwlock/election
// examples show — a worker whose lease has expired can no longer safely
// act as a permit holder, even though it once acquired one.
//
// Scenario:
//  1. Workers A and B each acquire one of the pool's 2 permits and write to
//     the resource — both accepted.
//  2. Worker C tries to Acquire — it blocks, since both permits are held.
//  3. Worker A gets stuck — keepalives stop, etcd expires its lease, which
//     releases A's permit and unblocks C.
//  4. Worker C acquires the freed permit and writes — accepted, with a
//     newer fencing token than A's.
//  5. Worker A wakes up and retries its write with its now-stale token —
//     rejected by the resource with 409 Conflict.
//
// Prerequisites:
//
//   - A running etcd cluster reachable at localhost:2379.
//
//     go run ./examples/semaphore/basic
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
	"github.com/ahrtr/disco/semaphore"
)

const (
	semaphoreKey   = "/semaphores/my-pool"
	semaphoreLimit = 2
)

func main() {
	// ── A real HTTP resource server, guarded by fencing/guard's HTTP
	// middleware, exactly like the lock/rwlock/election examples.
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

	// Worker A uses a short TTL and a cancellable context.
	// Cancelling the context stops keepalives; etcd expires the lease after
	// the TTL — simulating a process freeze (GC pause, network partition, etc.).
	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()

	workerA, err := etcdprovider.NewSemaphore(cli, semaphoreKey, semaphoreLimit,
		etcdprovider.WithContext(ctxA),
		etcdprovider.WithDefaultTTL(5*time.Second),
	)
	if err != nil {
		log.Fatalf("create worker A: %v", err)
	}
	defer workerA.Close()

	workerB, err := etcdprovider.NewSemaphore(cli, semaphoreKey, semaphoreLimit)
	if err != nil {
		log.Fatalf("create worker B: %v", err)
	}
	defer workerB.Close()

	workerC, err := etcdprovider.NewSemaphore(cli, semaphoreKey, semaphoreLimit)
	if err != nil {
		log.Fatalf("create worker C: %v", err)
	}
	defer workerC.Close()

	ctx := context.Background()

	// ── Step 1: A and B each take one of the 2 permits and write
	log.Println("Worker A: acquiring a permit …")
	grantA, err := workerA.Acquire(ctx)
	if err != nil {
		log.Fatalf("worker A acquire: %v", err)
	}
	log.Printf("Worker A: permit acquired  fencing_token=%d  TTL=5s", grantA.FencingToken)
	doWrite(resource.URL, "Worker A", grantA.Token())

	log.Println("Worker B: acquiring a permit …")
	grantB, err := workerB.Acquire(ctx)
	if err != nil {
		log.Fatalf("worker B acquire: %v", err)
	}
	log.Printf("Worker B: permit acquired  fencing_token=%d", grantB.FencingToken)
	doWrite(resource.URL, "Worker B", grantB.Token())

	if stat, err := workerA.Stat(ctx); err == nil {
		log.Printf("Pool status: %d/%d permits held", stat.Held(), stat.Limit)
	}

	// ── Step 2: Worker C tries to acquire and blocks (both permits held)
	type acquireResult struct {
		grant *semaphore.Grant
		err   error
	}
	cAcquired := make(chan acquireResult, 1)
	go func() {
		log.Println("Worker C: waiting for a permit (blocked — pool is full) …")
		grant, err := workerC.Acquire(ctx)
		cAcquired <- acquireResult{grant, err}
	}()

	select {
	case <-cAcquired:
		log.Fatal("BUG: Worker C acquired a permit while the pool was full")
	case <-time.After(300 * time.Millisecond):
	}

	// ── Step 3: Worker A gets stuck
	// cancelA stops the keepalive goroutine. etcd expires the lease after the
	// TTL, which releases A's permit and unblocks Worker C.
	log.Println("Worker A: got stuck (keepalives stopped — lease expires in 5s) …")
	cancelA()

	// ── Step 4: Worker C acquires the freed permit and writes
	res := <-cAcquired
	if res.err != nil {
		log.Fatalf("worker C acquire: %v", res.err)
	}
	grantC := res.grant
	log.Printf("Worker C: permit acquired  fencing_token=%d", grantC.FencingToken)
	doWrite(resource.URL, "Worker C", grantC.Token())

	// ── Step 5: Worker A wakes up and retries with its now-stale token
	log.Println("Worker A: woke up — retrying its write with its (now stale) token …")
	doWrite(resource.URL, "Worker A (stale)", grantA.Token())
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
