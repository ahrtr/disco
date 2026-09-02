package etcd

import (
	"context"
	"errors"
	"fmt"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/ahrtr/disco/rwlock"
)

// Compile-time proof that *RWProvider satisfies rwlock.Service.
var _ rwlock.Service = (*RWProvider)(nil)

// RWProvider implements rwlock.Service using etcd.
//
// An RWProvider is bound to a single rwlock key for its lifetime. The
// session (lease + keepalive goroutine) and the read/write mutex are
// created once in NewRWLock and reused across multiple RLock/Lock calls.
type RWProvider struct {
	key     string
	session *session
	mutex   *rwmutex
}

// NewRWLock creates an rwlock.Service for the given key backed by etcd.
//
// It establishes one lease (with automatic keepalive) and one read/write
// mutex for key. Both are reused across RLock/Lock calls for the lifetime
// of the returned service.
//
// The effective TTL is clamped to a minimum of 5 seconds regardless of the
// value passed via WithDefaultTTL.
//
// The caller is responsible for creating, configuring, and eventually
// closing the etcd client. Close revokes the session lease; it never
// closes the client.
//
//	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"localhost:2379"}})
//	if err != nil { ... }
//	defer cli.Close()
//
//	svc, err := etcd.NewRWLock(cli, "/rwlocks/my-resource")
func NewRWLock(client *clientv3.Client, key string, opts ...ProviderOption) (rwlock.Service, error) {
	session, err := newProviderSession(client, key, opts...)
	if err != nil {
		return nil, err
	}

	return &RWProvider{
		key:     key,
		session: session,
		mutex:   newRWMutex(session, key),
	}, nil
}

// RLock acquires a shared (read) lock, blocking until it is available or
// ctx is canceled.
//
// The fencing token is the etcd cluster revision at the moment the lock
// is acquired, a globally monotonically increasing value across the etcd
// cluster.
func (p *RWProvider) RLock(ctx context.Context) (*rwlock.Grant, error) {
	if err := p.mutex.rLock(ctx); err != nil {
		if errors.Is(err, errSessionExpired) {
			return nil, rwlock.ErrRWLockLost
		}
		return nil, fmt.Errorf("etcd provider: rlock %q: %w", p.key, err)
	}
	return p.newGrant(), nil
}

// Lock acquires an exclusive (write) lock, blocking until it is available
// or ctx is canceled.
func (p *RWProvider) Lock(ctx context.Context) (*rwlock.Grant, error) {
	if err := p.mutex.lock(ctx); err != nil {
		if errors.Is(err, errSessionExpired) {
			return nil, rwlock.ErrRWLockLost
		}
		return nil, fmt.Errorf("etcd provider: lock %q: %w", p.key, err)
	}
	return p.newGrant(), nil
}

// TryRLock attempts to acquire a shared (read) lock without blocking.
// Returns rwlock.ErrRWLockTaken immediately if a writer registered before
// this attempt is currently in the way.
func (p *RWProvider) TryRLock(ctx context.Context) (*rwlock.Grant, error) {
	if err := p.mutex.tryRLock(ctx); err != nil {
		if errors.Is(err, errLocked) {
			return nil, rwlock.ErrRWLockTaken
		}
		return nil, fmt.Errorf("etcd provider: tryrlock %q: %w", p.key, err)
	}
	return p.newGrant(), nil
}

// TryLock attempts to acquire an exclusive (write) lock without blocking.
// Returns rwlock.ErrRWLockTaken immediately if a reader or writer
// registered before this attempt is currently in the way.
func (p *RWProvider) TryLock(ctx context.Context) (*rwlock.Grant, error) {
	if err := p.mutex.tryLock(ctx); err != nil {
		if errors.Is(err, errLocked) {
			return nil, rwlock.ErrRWLockTaken
		}
		return nil, fmt.Errorf("etcd provider: trylock %q: %w", p.key, err)
	}
	return p.newGrant(), nil
}

// RUnlock releases a lock acquired via RLock. Returns an error without
// releasing anything if the currently held lock is actually a write
// lock (i.e. Unlock, not RUnlock, was the right call).
func (p *RWProvider) RUnlock(ctx context.Context) error {
	if err := p.mutex.runlock(ctx); err != nil && !errors.Is(err, errLockReleased) {
		return fmt.Errorf("etcd provider: runlock %q: %w", p.key, err)
	}
	return nil
}

// Unlock releases a lock acquired via Lock. Returns an error without
// releasing anything if the currently held lock is actually a read
// lock (i.e. RUnlock, not Unlock, was the right call).
func (p *RWProvider) Unlock(ctx context.Context) error {
	if err := p.mutex.wunlock(ctx); err != nil && !errors.Is(err, errLockReleased) {
		return fmt.Errorf("etcd provider: unlock %q: %w", p.key, err)
	}
	return nil
}

// Done returns a channel that is closed when the session lease is lost.
// The channel is created once at NewRWLock time and never changes.
func (p *RWProvider) Done() <-chan struct{} {
	return p.session.donec
}

// Err returns rwlock.ErrRWLockLost if the session lease has been lost,
// nil otherwise.
func (p *RWProvider) Err() error {
	select {
	case <-p.session.donec:
		return rwlock.ErrRWLockLost
	default:
		return nil
	}
}

// Close revokes the session lease, releasing any held lock. The
// underlying etcd client is not closed; the caller that created it is
// responsible for that.
func (p *RWProvider) Close() error {
	return p.session.close()
}

// newGrant builds an rwlock.Grant from the current mutex state after a
// successful RLock/Lock acquisition.
func (p *RWProvider) newGrant() *rwlock.Grant {
	return &rwlock.Grant{
		Key:          p.key,
		FencingToken: p.mutex.header().Revision,
	}
}
