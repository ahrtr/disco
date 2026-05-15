package lock

import (
	"time"

	"github.com/ahrtr/disco/lock/fencing"
)

// Grant represents the metadata for a successfully acquired lock.
//
// The fencing token carried by a Grant must be attached to every request sent
// to a guarded resource (database write, external API call, etc.) so that the
// resource can reject requests from stale (zombie) clients.
// Use grant.Token() to obtain a fencing.Token and pass it to the helpers in
// the fencing package (fencing.InjectHTTP, fencing.ToGRPCMetadata, etc.).
type Grant struct {
	// Key is the distributed lock key that was acquired.
	Key string

	// FencingToken is a monotonically increasing integer assigned at lock
	// acquisition time. It must be attached to every resource request so the
	// resource can reject requests from stale (lower-token) owners.
	FencingToken int64

	// ExpiresAt is the wall-clock time at which the lease will expire if not
	// renewed. This is informational; the authoritative expiry lives in the
	// backend.
	ExpiresAt time.Time
}

// Token returns the fencing token as a fencing.Token, ready to pass to
// fencing.InjectHTTP, fencing.ToGRPCMetadata, or a resource's Check method.
func (g *Grant) Token() fencing.Token {
	return fencing.Token(g.FencingToken)
}
