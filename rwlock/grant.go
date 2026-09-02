package rwlock

import (
	"github.com/ahrtr/disco/fencing"
)

// Grant represents the metadata for a successfully acquired read or write
// lock.
//
// The fencing token carried by a Grant must be attached to every request
// sent to a guarded resource, exactly like lock.Grant's. Concurrent RLock
// (read) grants all wait for the same last writer to release before being
// issued, so they carry identical tokens.
// Use grant.Token() to obtain a fencing.Token and pass it to the helpers
// in the fencing package (fencing.InjectHTTP, fencing.ToGRPCMetadata,
// etc.).
type Grant struct {
	// Key is the distributed rwlock key that was acquired.
	Key string

	// FencingToken is a monotonically increasing integer assigned at
	// acquisition time.
	FencingToken int64
}

// Token returns the fencing token as a fencing.Token, ready to pass to
// fencing.InjectHTTP, fencing.ToGRPCMetadata, or a resource's Check method.
func (g *Grant) Token() fencing.Token {
	return fencing.Token(g.FencingToken)
}
