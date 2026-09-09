package fencing

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc/metadata"
)

func TestFromGRPCContextNoMetadataIsErrNoToken(t *testing.T) {
	if _, err := FromGRPCContext(context.Background()); !errors.Is(err, ErrNoToken) {
		t.Fatalf("expected ErrNoToken with no incoming metadata, got %v", err)
	}
}

func TestFromGRPCContextMissingKeyIsErrNoToken(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.MD{})
	if _, err := FromGRPCContext(ctx); !errors.Is(err, ErrNoToken) {
		t.Fatalf("expected ErrNoToken for metadata without the fencing key, got %v", err)
	}
}

// TestFromGRPCContextLiteralZeroIsErrNoToken mirrors the HTTP-side fix:
// a literal "0" fencing metadata value must be treated as no token, not a
// valid (but never-actually-issued) zero token.
func TestFromGRPCContextLiteralZeroIsErrNoToken(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(GRPCKey, "0"))
	if _, err := FromGRPCContext(ctx); !errors.Is(err, ErrNoToken) {
		t.Fatalf("expected ErrNoToken for a literal zero token, got %v", err)
	}
}

func TestToGRPCMetadataFromGRPCContextRoundTrip(t *testing.T) {
	md := ToGRPCMetadata(Token(42))
	ctx := metadata.NewIncomingContext(context.Background(), md)
	got, err := FromGRPCContext(ctx)
	if err != nil {
		t.Fatalf("FromGRPCContext: %v", err)
	}
	if got != 42 {
		t.Fatalf("want token 42, got %d", got)
	}
}
