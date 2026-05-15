// Command resource is a gRPC resource server protected by a fencing token
// guard. It is the gRPC counterpart to the http/resource example.
//
// Every Write RPC must carry a valid x-fencing-token metadata entry.
// A token lower than the highest token already accepted is rejected, preventing
// stale lock owners from corrupting the resource.
//
// Run the resource server:
//
//	go run ./examples/grpc/resource
//
// Then run the client in a separate terminal:
//
//	go run ./examples/grpc/client
package main

import (
	"context"
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"

	"github.com/ahrtr/disco/examples/grpc/pb"
	"github.com/ahrtr/disco/lock/guard"
)

type resourceServer struct {
	g *guard.Guard
}

func (s *resourceServer) Write(_ context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	return &pb.WriteResponse{
		Message: fmt.Sprintf("write accepted: %q  high-water=%d", req.Data, s.g.HighWater()),
	}, nil
}

func main() {
	g := guard.New()

	srv := grpc.NewServer(
		grpc.UnaryInterceptor(g.UnaryInterceptor()),
	)
	pb.RegisterResourceServer(srv, &resourceServer{g: g})

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	log.Println("grpc/resource listening on :50051")
	log.Println("  pb.Resource/Write — requires x-fencing-token metadata")
	log.Fatal(srv.Serve(lis))
}
