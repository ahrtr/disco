// Command client demonstrates the end-to-end disco flow over gRPC, including
// the core safety guarantee: a zombie client whose lease has expired is
// rejected by the resource server even though it once held the lock.
//
// Scenario:
//  1. Client A acquires the lock with a 5s TTL.
//  2. Client A gets stuck — keepalives stop, etcd expires the lease after 5s.
//  3. Client B (goroutine) acquires the lock with a strictly higher token.
//  4. Client B calls pb.Resource/Write — accepted, high-water mark advances.
//  5. Client A wakes up, calls pb.Resource/Write with its stale token — rejected.
//
// Prerequisites:
//   - A running etcd cluster reachable at localhost:2379.
//   - The gRPC resource server running in a separate terminal:
//     go run ./examples/grpc/resource
//
//	go run ./examples/grpc/client
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/ahrtr/disco/examples/grpc/pb"
	"github.com/ahrtr/disco/lock"
	"github.com/ahrtr/disco/lock/fencing"
	etcdprovider "github.com/ahrtr/disco/provider/etcd"
)

const lockKey = "/locks/my-resource"

func main() {
	// ── gRPC connection to the resource server ────────────────────────────────
	// ForceCodec tells this connection to use the JSON codec registered by the
	// pb package. This does not affect the etcd client's connection, which
	// continues to use real protobuf encoding on its own separate connection.
	conn, err := grpc.NewClient("localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(pb.JSONCodec())),
	)
	if err != nil {
		log.Fatalf("connect to grpc/resource: %v", err)
	}
	defer conn.Close()
	rc := pb.NewResourceClient(conn)

	// ── etcd client + lock providers ──────────────────────────────────────────
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("create etcd client: %v", err)
	}
	defer cli.Close()

	// Provider A uses a short TTL and a cancellable context.
	// Cancelling the context stops keepalives; etcd then expires the lease
	// after the TTL — exactly what happens when a real process freezes.
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
	grantA, err := providerA.Lock(ctx)
	if err != nil {
		log.Fatalf("client A lock: %v", err)
	}
	log.Printf("Client A: lock acquired  fencing_token=%d  TTL=5s", grantA.FencingToken)

	// ── Step 2: Client B waits for the lock in a goroutine ───────────────────
	bWritten := make(chan struct{})
	go func() {
		log.Println("Client B: waiting for the lock …")
		grantB, err := providerB.Lock(ctx)
		if err != nil {
			log.Fatalf("client B lock: %v", err)
		}
		log.Printf("Client B: lock acquired  fencing_token=%d", grantB.FencingToken)

		log.Println("Client B: calling Resource/Write …")
		doWrite(ctx, "Client B", rc, grantB)
		close(bWritten)

		if err := providerB.Unlock(ctx); err != nil {
			log.Printf("client B unlock: %v", err)
		} else {
			log.Println("Client B: lock released")
		}
	}()

	// ── Step 3: Client A gets stuck ───────────────────────────────────────────
	// cancelA stops the keepalive goroutine; etcd expires the lease after 5s,
	// which unblocks Client B.
	log.Println("Client A: got stuck (keepalives stopped — lease expires in 5s) …")
	cancelA()

	// ── Step 4: Wait for Client B to write ────────────────────────────────────
	// Blocks until B has acquired the lock and advanced the resource's
	// high-water mark to its (higher) fencing token.
	<-bWritten

	// ── Step 5: Client A wakes up and tries to write ──────────────────────────
	// A's grant still holds the old token in memory, but the resource server's
	// high-water mark is now higher. The server rejects the RPC.
	log.Println("Client A: woke up — calling Resource/Write with stale token …")
	doWrite(ctx, "Client A", rc, grantA)
}

// doWrite calls pb.Resource/Write, attaching the grant's fencing token as
// gRPC metadata so the server's guard interceptor can validate it.
func doWrite(ctx context.Context, name string, rc pb.ResourceClient, grant *lock.Grant) {
	outCtx := metadata.NewOutgoingContext(ctx, fencing.ToGRPCMetadata(grant.Token()))

	resp, err := rc.Write(outCtx, &pb.WriteRequest{
		Data: fmt.Sprintf("hello from %s", name),
	})
	if err != nil {
		log.Printf("%s: Write RPC error: %v", name, err)
		return
	}
	log.Printf("%s: Write OK — %s", name, resp.Message)
}
