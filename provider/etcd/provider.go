package etcd

import (
	"context"
	"errors"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/ahrtr/disco/lock"
)

// Compile-time proof that *Provider satisfies lock.Service.
var _ lock.Service = (*Provider)(nil)

// Provider implements lock.Service using etcd.
//
// A Provider is bound to a single lock key for its lifetime. The session
// (lease + keepalive goroutine) and the mutex are created once in NewLock and
// reused across multiple Lock and TryLock calls.
type Provider struct {
	key     string
	session *session
	mutex   *mutex
}

// NewLock creates a lock.Service for the given lock key backed by etcd.
//
// It establishes one lease (with automatic keepalive) and one distributed
// mutex for key. Both are reused across Lock and TryLock calls for the
// lifetime of the returned service.
//
// The caller is responsible for creating, configuring, and eventually closing
// the etcd client. Close revokes the session lease; it never closes the client.
//
//	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"localhost:2379"}})
//	if err != nil { ... }
//	defer cli.Close()
//
//	svc, err := etcd.NewLock(cli, "/locks/my-resource")
func NewLock(client *clientv3.Client, key string, opts ...ProviderOption) (lock.Service, error) {
	o := defaultProviderOptions()
	for _, opt := range opts {
		opt(&o)
	}

	ttlSecs := int(o.cfg.defaultTTL().Seconds())
	if ttlSecs < 5 {
		ttlSecs = 5
	}

	session, err := newSession(o.ctx, client, withTTL(ttlSecs))
	if err != nil {
		return nil, fmt.Errorf("etcd provider: create session for %q: %w", key, err)
	}

	return &Provider{
		key:     key,
		session: session,
		mutex:   newMutex(session, key),
	}, nil
}

// Lock acquires the distributed lock, blocking until it is available or ctx
// is canceled.
//
// The fencing token is the etcd cluster revision at the moment the lock is
// acquired, a globally monotonically increasing value across the etcd cluster.
func (p *Provider) Lock(ctx context.Context) (*lock.Session, error) {
	if err := p.mutex.lock(ctx); err != nil {
		return nil, fmt.Errorf("etcd provider: lock %q: %w", p.key, err)
	}
	return p.newSession(), nil
}

// TryLock attempts to acquire the lock without blocking.
// Returns lock.ErrLockTaken immediately if the lock is held by another owner.
func (p *Provider) TryLock(ctx context.Context) (*lock.Session, error) {
	if err := p.mutex.tryLock(ctx); err != nil {
		if errors.Is(err, errLocked) {
			return nil, lock.ErrLockTaken
		}
		return nil, fmt.Errorf("etcd provider: trylock %q: %w", p.key, err)
	}
	return p.newSession(), nil
}

// unlock releases the lock. The session and its lease remain alive so Lock
// can be called again without creating a new Provider.
func (p *Provider) unlock(ctx context.Context) error {
	if err := p.mutex.unlock(ctx); err != nil && !errors.Is(err, errLockReleased) {
		return fmt.Errorf("etcd provider: unlock %q: %w", p.key, err)
	}
	return nil
}

// Close revokes the session lease, releasing any held lock. The underlying
// etcd client is not closed; the caller that created it is responsible for that.
func (p *Provider) Close() error {
	return p.session.close()
}

// newSession builds a lock.Session from the current mutex state after a
// successful lock acquisition.
func (p *Provider) newSession() *lock.Session {
	grant := &lock.Grant{
		Key:          p.key,
		FencingToken: p.mutex.header().Revision,
		ExpiresAt:    time.Now().Add(time.Duration(p.session.opts.ttl) * time.Second),
	}
	return lock.NewSession(grant, p.session.donec, p.unlock)
}
