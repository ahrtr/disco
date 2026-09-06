package etcd

import (
	"context"
	"errors"
	"fmt"

	clientv3 "go.etcd.io/etcd/client/v3"

	semaphoreapi "github.com/ahrtr/disco/semaphore"
)

// Compile-time proof that *SemaphoreProvider satisfies semaphoreapi.Service.
var _ semaphoreapi.Service = (*SemaphoreProvider)(nil)

// SemaphoreProvider implements semaphoreapi.Service using etcd.
//
// A SemaphoreProvider is bound to a single semaphore key and permit limit
// for its lifetime. The session (lease + keepalive goroutine) and the
// semaphore are created once in NewSemaphore and reused across multiple
// Acquire and TryAcquire calls.
type SemaphoreProvider struct {
	key       string
	session   *session
	semaphore *semaphore
}

// NewSemaphore creates a semaphoreapi.Service for the given key backed by
// etcd, allowing up to limit concurrently held permits. limit must be at
// least 1; use NewLock instead of a semaphore with limit 1.
//
// It establishes one lease (with automatic keepalive) and one semaphore for
// key. Both are reused across Acquire and TryAcquire calls for the lifetime
// of the returned service.
//
// The effective TTL is clamped to a minimum of 5 seconds regardless of the
// value passed via WithDefaultTTL.
//
// The caller is responsible for creating, configuring, and eventually closing
// the etcd client. Close revokes the session lease; it never closes the client.
//
//	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"localhost:2379"}})
//	if err != nil { ... }
//	defer cli.Close()
//
//	svc, err := etcd.NewSemaphore(cli, "/semaphores/my-pool", 3)
func NewSemaphore(client *clientv3.Client, key string, limit int, opts ...ProviderOption) (semaphoreapi.Service, error) {
	if limit < 1 {
		return nil, fmt.Errorf("etcd provider: semaphore %q: limit must be >= 1, got %d", key, limit)
	}

	session, err := newProviderSession(client, key, opts...)
	if err != nil {
		return nil, err
	}

	return &SemaphoreProvider{
		key:       key,
		session:   session,
		semaphore: newSemaphore(session, key, limit),
	}, nil
}

// Acquire acquires a permit, blocking until one is available or ctx is
// canceled.
//
// The fencing token is the etcd cluster revision at the moment the permit
// is acquired, a globally monotonically increasing value across the etcd
// cluster.
func (p *SemaphoreProvider) Acquire(ctx context.Context) (*semaphoreapi.Grant, error) {
	if err := p.semaphore.acquire(ctx); err != nil {
		return nil, fmt.Errorf("etcd provider: acquire %q: %w", p.key, err)
	}
	return p.newGrant(), nil
}

// TryAcquire attempts to acquire a permit without blocking.
// Returns semaphoreapi.ErrNoPermitsAvailable immediately if every permit is
// currently held by other owners.
func (p *SemaphoreProvider) TryAcquire(ctx context.Context) (*semaphoreapi.Grant, error) {
	if err := p.semaphore.tryAcquire(ctx); err != nil {
		if errors.Is(err, errSemaphoreFull) {
			return nil, semaphoreapi.ErrNoPermitsAvailable
		}
		return nil, fmt.Errorf("etcd provider: tryacquire %q: %w", p.key, err)
	}
	return p.newGrant(), nil
}

// Release releases the held permit. The session and its lease remain alive
// so Acquire can be called again without creating a new Provider.
func (p *SemaphoreProvider) Release(ctx context.Context) error {
	if err := p.semaphore.release(ctx); err != nil && !errors.Is(err, errLockReleased) {
		return fmt.Errorf("etcd provider: release %q: %w", p.key, err)
	}
	return nil
}

// Stat reports the semaphore's current usage: how many candidates are
// registered (held + waiting) and the total limit.
func (p *SemaphoreProvider) Stat(ctx context.Context) (*semaphoreapi.Stat, error) {
	registered, err := p.semaphore.stat(ctx)
	if err != nil {
		return nil, fmt.Errorf("etcd provider: stat %q: %w", p.key, err)
	}
	return &semaphoreapi.Stat{Registered: int(registered), Limit: p.semaphore.limit}, nil
}

// Done returns a channel that is closed when the session lease is lost.
// The channel is created once at NewSemaphore time and never changes.
func (p *SemaphoreProvider) Done() <-chan struct{} {
	return p.session.donec
}

// Err returns semaphoreapi.ErrSemaphoreLost if the session lease has been
// lost, nil otherwise.
func (p *SemaphoreProvider) Err() error {
	select {
	case <-p.session.donec:
		return semaphoreapi.ErrSemaphoreLost
	default:
		return nil
	}
}

// Close revokes the session lease, releasing any held permit. The
// underlying etcd client is not closed; the caller that created it is
// responsible for that.
func (p *SemaphoreProvider) Close() error {
	return p.session.close()
}

// newGrant builds a semaphoreapi.Grant from the current semaphore state
// after a successful Acquire/TryAcquire.
func (p *SemaphoreProvider) newGrant() *semaphoreapi.Grant {
	return &semaphoreapi.Grant{
		Key:          p.key,
		FencingToken: p.semaphore.header().Revision,
	}
}
