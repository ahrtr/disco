package lock

import (
	"context"
	"net/http"

	"google.golang.org/grpc/metadata"

	"github.com/ahrtr/disco/lock/fencing"
)

// Session represents an active, leased lock ownership.
//
// The fencing token carried by this session must be attached to every request
// sent to a guarded resource (database write, external API call, etc.) so that
// the resource can reject requests from stale sessions.
//
// A Session is not safe for concurrent calls to Unlock; all other methods
// are safe for concurrent use.
type Session struct {
	grant  *Grant
	done   <-chan struct{}
	unlock func(context.Context) error
}

// NewSession creates a Session for use by Service implementations.
// done is closed when the lease is lost; unlock releases the lock.
// Callers should obtain a Session via Service.Lock or Service.TryLock.
func NewSession(grant *Grant, done <-chan struct{}, unlock func(context.Context) error) *Session {
	return &Session{grant: grant, done: done, unlock: unlock}
}

// FencingToken returns the monotonically increasing token for this lock
// generation. Attach it to every resource request via InjectHTTP or
// GRPCMetadata.
func (s *Session) FencingToken() fencing.Token {
	return fencing.Token(s.grant.FencingToken)
}

// Grant returns the underlying Grant with metadata such as the fencing
// token, lock key, and lease expiry time.
func (s *Session) Grant() *Grant { return s.grant }

// Done returns a channel that is closed when the lease is lost.
// Once closed, callers must immediately stop accessing guarded resources.
func (s *Session) Done() <-chan struct{} { return s.done }

// Err returns ErrLockLost if the lease has been lost, or nil if the
// session is still alive. Safe to call before or after Done() is closed.
func (s *Session) Err() error {
	select {
	case <-s.done:
		return ErrLockLost
	default:
		return nil
	}
}

// Unlock explicitly releases the lock.
func (s *Session) Unlock(ctx context.Context) error {
	return s.unlock(ctx)
}

// InjectHTTP sets the X-Fencing-Token header on req. Call this before
// executing any HTTP request against a guarded resource.
func (s *Session) InjectHTTP(req *http.Request) {
	fencing.InjectHTTP(req, s.FencingToken())
}

// GRPCMetadata returns gRPC metadata containing the fencing token. Attach it
// to the outgoing context:
//
//	ctx = metadata.NewOutgoingContext(ctx, session.GRPCMetadata())
func (s *Session) GRPCMetadata() metadata.MD {
	return fencing.ToGRPCMetadata(s.FencingToken())
}
