package fencing

import (
	"context"
	"fmt"
	"strconv"

	"google.golang.org/grpc/metadata"
)

// GRPCKey is the gRPC metadata key used to carry fencing tokens.
const GRPCKey = "x-fencing-token"

// ToGRPCMetadata builds a metadata.MD containing the fencing token.
// Merge it into outgoing context with metadata.NewOutgoingContext or
// metadata.AppendToOutgoingContext.
func ToGRPCMetadata(t Token) metadata.MD {
	return metadata.Pairs(GRPCKey, strconv.FormatInt(int64(t), 10))
}

// FromGRPCContext extracts the fencing token from an incoming gRPC context.
// Returns ErrNoToken if the metadata key is absent and a wrapped error if the
// value cannot be parsed as an int64.
func FromGRPCContext(ctx context.Context) (Token, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return Zero, ErrNoToken
	}
	vals := md.Get(GRPCKey)
	if len(vals) == 0 {
		return Zero, ErrNoToken
	}
	n, err := strconv.ParseInt(vals[0], 10, 64)
	if err != nil {
		return Zero, fmt.Errorf("fencing: invalid token %q: %w", vals[0], err)
	}
	return Token(n), nil
}
