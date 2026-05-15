package etcd

import (
	"context"
	"time"

	"go.uber.org/zap"

	v3 "go.etcd.io/etcd/client/v3"
)

// session represents a lease kept alive for the lifetime of a Provider.
type session struct {
	client *v3.Client
	opts   *sessionOptions
	id     v3.LeaseID

	ctx    context.Context
	cancel context.CancelFunc
	donec  <-chan struct{}
}

// newSession creates a leased session for the given client.
// ctx governs the session lifetime; if nil, client.Ctx() is used.
func newSession(ctx context.Context, client *v3.Client, opts ...sessionOption) (*session, error) {
	if ctx == nil {
		ctx = client.Ctx()
	}
	lg := client.GetLogger()
	ops := &sessionOptions{ttl: int(defaultTTL.Seconds()), ctx: ctx}
	for _, opt := range opts {
		opt(ops, lg)
	}

	resp, err := client.Grant(ops.ctx, int64(ops.ttl))
	if err != nil {
		return nil, err
	}
	id := resp.ID

	ctx, cancel := context.WithCancel(ops.ctx)
	keepAlive, err := client.KeepAlive(ctx, id)
	if err != nil || keepAlive == nil {
		cancel()
		return nil, err
	}

	donec := make(chan struct{})
	s := &session{client: client, opts: ops, id: id, ctx: ctx, cancel: cancel, donec: donec}

	// keep the lease alive until client error or cancelled context
	go func() {
		defer func() {
			close(donec)
			cancel()
		}()
		for range keepAlive {
			// eat messages until keep alive channel closes
		}
	}()

	return s, nil
}

// orphan ends the keepalive refresh for the session lease.
func (s *session) orphan() {
	s.cancel()
	<-s.donec
}

// close orphans the session and revokes the session lease.
func (s *session) close() error {
	s.orphan()
	// Use a fresh context for revoke: the session's own context may already be
	// cancelled (e.g. the caller cancelled it to stop keepalives), but we still
	// want to attempt a clean revoke. If revoke takes longer than the TTL the
	// lease is expired anyway.
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(s.opts.ttl)*time.Second)
	_, err := s.client.Revoke(ctx, s.id)
	cancel()
	return err
}

// sessionOptions holds the resolved configuration for a session.
type sessionOptions struct {
	ttl int
	ctx context.Context
}

// sessionOption configures a session.
type sessionOption func(*sessionOptions, *zap.Logger)

// withTTL configures the session's TTL in seconds.
func withTTL(ttl int) sessionOption {
	return func(so *sessionOptions, lg *zap.Logger) {
		if ttl > 0 {
			so.ttl = ttl
		} else {
			lg.Warn("withTTL(): TTL should be > 0, preserving current TTL", zap.Int64("current-session-ttl", int64(so.ttl)))
		}
	}
}
