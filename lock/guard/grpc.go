package guard

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ahrtr/disco/lock/fencing"
)

// UnaryInterceptor returns a gRPC unary server interceptor that validates the
// fencing token carried in incoming metadata before calling the handler.
//
// Requests with a missing or malformed fencing-token metadata key are rejected
// with codes.InvalidArgument. Requests carrying a stale token are rejected
// with codes.Aborted.
//
// Register it when creating the server:
//
//	grpc.NewServer(
//	    grpc.UnaryInterceptor(g.UnaryInterceptor()),
//	)
func (g *Guard) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		_ *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		token, err := fencing.FromGRPCContext(ctx)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if err := g.Check(token); err != nil {
			return nil, status.Error(codes.Aborted, err.Error())
		}
		return handler(ctx, req)
	}
}

// StreamInterceptor returns a gRPC streaming server interceptor that validates
// the fencing token once at stream establishment time.
//
// The same rejection rules as UnaryInterceptor apply. Register both together
// when the server handles both unary and streaming RPCs:
//
//	grpc.NewServer(
//	    grpc.UnaryInterceptor(g.UnaryInterceptor()),
//	    grpc.StreamInterceptor(g.StreamInterceptor()),
//	)
func (g *Guard) StreamInterceptor() grpc.StreamServerInterceptor {
	return func(
		srv any,
		ss grpc.ServerStream,
		_ *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		token, err := fencing.FromGRPCContext(ss.Context())
		if err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
		if err := g.Check(token); err != nil {
			return status.Error(codes.Aborted, err.Error())
		}
		return handler(srv, ss)
	}
}
