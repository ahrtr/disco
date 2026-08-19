// Command basic demonstrates leader election end-to-end, including the same
// core safety guarantee the lock examples show: a zombie leader whose lease
// has expired can no longer safely act as leader, even though it once won
// the election.
//
// Scenario:
//  1. A real HTTP resource server, guarded by fencing/guard, starts up.
//  2. Candidate A campaigns and wins immediately (it's the only candidate),
//     then writes to the resource — accepted.
//  3. A dedicated watcher (a passive observer, not a candidate) starts
//     watching leadership via Observe() and reports every change it sees.
//  4. Candidate B campaigns — it blocks, since A is currently leader.
//  5. Candidate A gets stuck — keepalives stop, etcd expires its lease.
//  6. Candidate B's Campaign unblocks: it is now leader, and it writes to
//     the resource with its newer token — accepted.
//  7. The watcher observes the handover.
//  8. Candidate A wakes up and retries its write with its now-stale token —
//     rejected by the resource with 409 Conflict.
//
// Prerequisites:
//
//   - A running etcd cluster reachable at localhost:2379.
//
//     go run ./examples/election/basic
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

	"github.com/ahrtr/disco/election"
	"github.com/ahrtr/disco/fencing"
	"github.com/ahrtr/disco/fencing/guard"
	etcdprovider "github.com/ahrtr/disco/provider/etcd"
)

const electionKey = "/elections/my-service"

func main() {
	// ── Step 1: A real HTTP resource server, started up front
	// Guarded by fencing/guard's HTTP middleware, the same one
	// examples/lock/http/resource uses — this is a real net/http server
	// accepting real requests, just started in-process for convenience.
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

	// Candidate A uses a short TTL and a cancellable context.
	// Cancelling the context stops keepalives; etcd expires the lease after
	// the TTL — simulating a process freeze (GC pause, network partition, etc.).
	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()

	candidateA, err := etcdprovider.NewElection(cli, electionKey,
		etcdprovider.WithContext(ctxA),
		etcdprovider.WithDefaultTTL(5*time.Second),
	)
	if err != nil {
		log.Fatalf("create candidate A: %v", err)
	}
	defer candidateA.Close()

	candidateB, err := etcdprovider.NewElection(cli, electionKey)
	if err != nil {
		log.Fatalf("create candidate B: %v", err)
	}
	defer candidateB.Close()

	// The watcher never campaigns; it only observes who the current leader
	// is, the way a fleet of read replicas would locate the active primary.
	watcher, err := etcdprovider.NewElection(cli, electionKey)
	if err != nil {
		log.Fatalf("create watcher: %v", err)
	}
	defer watcher.Close()

	ctx := context.Background()

	// ── Step 2: Candidate A campaigns and wins immediately
	log.Println("Candidate A: campaigning …")
	if err := candidateA.Campaign(ctx, "node-a"); err != nil {
		log.Fatalf("candidate A campaign: %v", err)
	}
	leaderA, err := candidateA.Leader(ctx)
	if err != nil {
		log.Fatalf("candidate A leader: %v", err)
	}
	log.Printf("Candidate A: elected leader  value=%q  token=%d  TTL=5s", leaderA.Value, leaderA.Token())
	doWrite(resource.URL, "Candidate A", leaderA.Token())

	// ── Step 3: The watcher observes leadership changes in the background
	observed := make(chan *election.Leadership, 2)
	go func() {
		ch := watcher.Observe(ctx)
		for i := 0; i < 2; i++ {
			l, ok := <-ch
			if !ok {
				return
			}
			log.Printf("Watcher: observed leader  value=%q  token=%d", l.Value, l.Token())
			observed <- l
		}
		close(observed)
	}()

	// ── Step 4: Candidate B campaigns and blocks (A is currently leader)
	bWon := make(chan struct{})
	go func() {
		log.Println("Candidate B: campaigning (will block until A steps down) …")
		if err := candidateB.Campaign(ctx, "node-b"); err != nil {
			log.Fatalf("candidate B campaign: %v", err)
		}
		close(bWon)
	}()

	// ── Step 5: Candidate A gets stuck
	// cancelA stops the keepalive goroutine. etcd expires the lease after the
	// TTL, which deletes A's candidate key and unblocks Candidate B.
	log.Println("Candidate A: got stuck (keepalives stopped — lease expires in 5s) …")
	cancelA()

	// ── Step 6: Wait for Candidate B to win, then write with its new token
	<-bWon
	leaderB, err := candidateB.Leader(ctx)
	if err != nil {
		log.Fatalf("candidate B leader: %v", err)
	}
	log.Printf("Candidate B: elected leader  value=%q  token=%d", leaderB.Value, leaderB.Token())
	doWrite(resource.URL, "Candidate B", leaderB.Token())

	// ── Step 7: Wait for the watcher to observe both leadership terms
	for range observed {
	}

	// ── Step 8: Candidate A wakes up and retries with its now-stale token
	log.Println("Candidate A: woke up — retrying its write with its (now stale) token …")
	doWrite(resource.URL, "Candidate A (stale)", leaderA.Token())
}

// doWrite sends a real HTTP POST to the resource server with the leader's
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
