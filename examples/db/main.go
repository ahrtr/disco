// Command db demonstrates how to protect a database resource using fencing
// tokens. Unlike the HTTP/gRPC examples where a guard middleware sits in front
// of the resource, database protection requires storing the fencing token
// inside the database itself and verifying it atomically within the same
// transaction as the write.
//
// This example uses an in-memory database to remain self-contained. In
// production replace the in-memory implementation with a real SQL database;
// the comments in Database.Write show the equivalent SQL pattern.
//
// Scenario:
//  1. Client A acquires the lock with a 5s TTL.
//  2. Client A gets stuck — keepalives stop, etcd expires the lease after 5s.
//  3. Client B acquires the lock with a strictly higher token and writes to the DB.
//  4. Client A wakes up and tries to write with its stale token — rejected by the DB.
//
// Prerequisites:
//   - A running etcd cluster reachable at localhost:2379.
//
//	go run ./examples/db
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/ahrtr/disco/lock/fencing"
	etcdprovider "github.com/ahrtr/disco/provider/etcd"
)

// Database is an in-memory store that enforces fencing token protection.
//
// In production this would be backed by a SQL database. The fencingToken
// field maps to a column in a resource_locks table, and Write would execute
// inside a transaction:
//
//	BEGIN;
//	UPDATE resource_locks
//	   SET fencing_token = $1
//	 WHERE resource_key = 'x' AND fencing_token < $1;
//	-- if 0 rows affected: token is stale → ROLLBACK and reject
//	INSERT INTO data_table (data) VALUES ($2);
//	COMMIT;
//
// The database transaction guarantees that the token check and the data write
// are atomic — no concurrent writer can slip through between the two steps.
type Database struct {
	mu           sync.Mutex
	fencingToken int64
	data         string
}

// Write atomically checks the fencing token and updates the stored data.
// It accepts token >= current high-water mark and advances the mark when
// token > current mark. Returns fencing.ErrTokenStale if the token is lower.
func (db *Database) Write(token fencing.Token, data string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	t := int64(token)
	if t < db.fencingToken {
		return fmt.Errorf("%w (got %d, db high-water=%d)",
			fencing.ErrTokenStale, token, db.fencingToken)
	}
	if t > db.fencingToken {
		db.fencingToken = t
	}
	db.data = data
	return nil
}

// State returns the current data and fencing token for diagnostics.
func (db *Database) State() (data string, token int64) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.data, db.fencingToken
}

const lockKey = "/locks/my-db-resource"

func main() {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("create etcd client: %v", err)
	}
	defer cli.Close()

	db := &Database{}

	// Provider A uses a short TTL and a cancellable context.
	// Cancelling the context stops keepalives; etcd expires the lease after
	// the TTL — simulating a process freeze (GC pause, network partition, etc.).
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

		if err := db.Write(sessionB.FencingToken(), "hello from client B"); err != nil {
			log.Printf("Client B: write rejected: %v", err)
		} else {
			data, tok := db.State()
			log.Printf("Client B: write accepted — data=%q  db_token=%d", data, tok)
		}
		close(bWritten)

		if err := sessionB.Unlock(ctx); err != nil {
			log.Printf("client B unlock: %v", err)
		} else {
			log.Println("Client B: lock released")
		}
	}()

	// ── Step 3: Client A gets stuck ───────────────────────────────────────────
	// cancelA stops the keepalive goroutine. etcd expires the lease after the
	// TTL, which releases the lock and unblocks Client B.
	log.Println("Client A: got stuck (keepalives stopped — lease expires in 5s) …")
	cancelA()

	// ── Step 4: Wait for Client B to write ────────────────────────────────────
	<-bWritten

	// ── Step 5: Client A wakes up and tries to write with its stale token ─────
	// The DB rejects it because A's token is lower than the stored high-water
	// mark — no in-memory guard or middleware needed.
	log.Println("Client A: woke up — attempting write with stale token …")
	if err := db.Write(sessionA.FencingToken(), "hello from client A"); err != nil {
		if errors.Is(err, fencing.ErrTokenStale) {
			log.Printf("Client A: write rejected by DB — %v", err)
		} else {
			log.Printf("Client A: write error: %v", err)
		}
	} else {
		log.Println("Client A: write accepted (BUG — should have been rejected!)")
	}
}
