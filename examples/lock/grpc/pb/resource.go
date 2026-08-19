// Package pb defines the Resource gRPC service used by the grpc/resource and
// grpc/client examples.
//
// To keep the examples self-contained (no protobuf toolchain required), this
// package registers a "json" gRPC codec and defines the service descriptor
// directly in Go — the same structure that protoc-gen-go-grpc would generate
// from a .proto file.
//
// Do not use this approach in production code; use protoc-generated files there.
package pb

import (
	"context"
	"encoding/json"

	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
)

func init() {
	// Register a JSON codec under the name "json". We deliberately do NOT
	// replace the built-in "proto" codec so that other gRPC connections in the
	// same binary (e.g. the etcd client) continue to use real protobuf encoding.
	encoding.RegisterCodec(jsonCodec{})
}

// jsonCodec serialises gRPC messages as JSON instead of the protobuf wire format.
type jsonCodec struct{}

func (jsonCodec) Name() string                       { return "json" }
func (jsonCodec) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (jsonCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

// JSONCodec returns the JSON codec. Pass it to grpc.ForceCodec when dialling
// the resource server so the connection uses JSON instead of protobuf:
//
//	grpc.WithDefaultCallOptions(grpc.ForceCodec(pb.JSONCodec()))
func JSONCodec() encoding.Codec { return jsonCodec{} }

// ── Messages ──────────────────────────────────────────────────────────────────

// WriteRequest is the request for the Resource.Write RPC.
type WriteRequest struct {
	Data string `json:"data"`
}

// WriteResponse is the response for the Resource.Write RPC.
type WriteResponse struct {
	Message string `json:"message"`
}

// ── Client ────────────────────────────────────────────────────────────────────

// ResourceClient is the client API for the Resource gRPC service.
type ResourceClient interface {
	Write(ctx context.Context, in *WriteRequest, opts ...grpc.CallOption) (*WriteResponse, error)
}

type resourceClient struct{ cc grpc.ClientConnInterface }

// NewResourceClient creates a new ResourceClient backed by cc.
func NewResourceClient(cc grpc.ClientConnInterface) ResourceClient {
	return &resourceClient{cc}
}

func (c *resourceClient) Write(ctx context.Context, in *WriteRequest, opts ...grpc.CallOption) (*WriteResponse, error) {
	out := new(WriteResponse)
	if err := c.cc.Invoke(ctx, "/pb.Resource/Write", in, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}

// ── Server ────────────────────────────────────────────────────────────────────

// ResourceServer is the server API for the Resource gRPC service.
type ResourceServer interface {
	Write(context.Context, *WriteRequest) (*WriteResponse, error)
}

// RegisterResourceServer registers srv with the gRPC server s.
func RegisterResourceServer(s *grpc.Server, srv ResourceServer) {
	s.RegisterService(&resourceServiceDesc, srv)
}

func resourceWriteHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(WriteRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ResourceServer).Write(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/pb.Resource/Write"}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(ResourceServer).Write(ctx, req.(*WriteRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var resourceServiceDesc = grpc.ServiceDesc{
	ServiceName: "pb.Resource",
	HandlerType: (*ResourceServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "Write", Handler: resourceWriteHandler},
	},
	Streams: []grpc.StreamDesc{},
}
