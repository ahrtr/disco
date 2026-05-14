// Command client demonstrates the end-to-end disco flow over HTTP, including
// the core safety guarantee: a zombie client whose lease has expired is
// rejected by the resource even though it once held the lock.
//
// Scenario:
//  1. Client A acquires the lock with a 5s TTL.
//  2. Client A gets stuck — keepalives stop, and etcd expires the lease after 5s.
//  3. Client B is waiting in a goroutine; it acquires the lock once A's lease expires.
//  4. Client B writes to the resource, advancing its high-water mark to token T'.
//  5. Client A wakes up and tries to write with the old token T < T'.
//  6. The resource rejects A's request with 409 Conflict.
//
// Prerequisites:
//   - A running etcd cluster reachable at localhost:2379.
//   - The HTTP resource server running in a separate terminal:
//     go run ./examples/http/resource
//
//	go run ./examples/http/client
package main

import (
	"context"
	"io"
	"log"
	"net/http"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/ahrtr/disco/lock"
	etcdprovider "github.com/ahrtr/disco/provider/etcd"
)

const (
	lockKey          = "/locks/my-resource"
	resourceWriteURL = "http://localhost:8080/write"
)

func main() {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("create etcd client: %v", err)
	}
	defer cli.Close()

	// Provider A uses a short TTL and a cancellable context.
	// Cancelling the context stops the keepalive goroutine; etcd then expires
	// the lease after the TTL — exactly what happens when a real process freezes
	// (long GC pause, network partition, OOM kill, etc.).
	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()

	providerA, err := etcdprovider.NewLock(cli, lockKey,
		etcdprovider.WithContext(ctxA),
		etcdprovider.WithDefaultTTL(5*time.Second),
	)
	if err != nil {
		log.Fatalf("create provider A: %v", err)
	}
	defer providerA.Close()

	providerB, err := etcdprovider.NewLock(cli, lockKey)
	if err != nil {
		log.Fatalf("create provider B: %v", err)
	}
	defer providerB.Close()

	ctx := context.Background()

	// ── Step 1: Client A acquires the lock ────────────────────────────────────
	log.Println("Client A: acquiring lock …")
	sessionA, err := providerA.Lock(ctx)
	if err != nil {
		log.Fatalf("client A lock: %v", err)
	}
	log.Printf("Client A: lock acquired  fencing_token=%d  TTL=5s", sessionA.FencingToken())

	// ── Step 2: Client B waits for the lock in a separate goroutine ───────────
	bWritten := make(chan struct{})
	go func() {
		log.Println("Client B: waiting for the lock …")
		sessionB, err := providerB.Lock(ctx)
		if err != nil {
			log.Fatalf("client B lock: %v", err)
		}
		log.Printf("Client B: lock acquired  fencing_token=%d", sessionB.FencingToken())

		log.Println("Client B: writing to resource …")
		doWrite("Client B", sessionB)
		close(bWritten)

		if err := sessionB.Unlock(ctx); err != nil {
			log.Printf("client B unlock: %v", err)
		} else {
			log.Println("Client B: lock released")
		}
	}()

	// ── Step 3: Client A gets stuck ───────────────────────────────────────────
	// cancelA stops the session's keepalive goroutine. With no renewals coming
	// in, etcd expires the lease after the 5s TTL and automatically releases
	// the lock, which unblocks Client B.
	log.Println("Client A: got stuck (keepalives stopped — lease expires in 5s) …")
	cancelA()

	// ── Step 4: Wait for Client B to write ────────────────────────────────────
	// Blocks until B has acquired the lock (after A's lease expires ~5s from
	// now) and written to the resource, advancing the high-water mark to T'.
	<-bWritten

	// ── Step 5: Client A wakes up and tries to write ──────────────────────────
	// A's grant still holds the old fencing token T in memory, but the resource
	// server's high-water mark is now T'. Since T < T', the write is rejected.
	log.Println("Client A: woke up — attempting write with stale token …")
	doWrite("Client A", sessionA)
}

// doWrite sends a POST /write request to the resource server with the
// session's fencing token attached via the X-Fencing-Token header.
func doWrite(name string, session *lock.Session) {
	req, err := http.NewRequest(http.MethodPost, resourceWriteURL, nil)
	if err != nil {
		log.Fatalf("build request: %v", err)
	}
	session.InjectHTTP(req)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatalf("%s: do request: %v", name, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	log.Printf("%s: %s — %s", name, resp.Status, body)
}
