package etcd

import (
	"context"

	v3 "go.etcd.io/etcd/client/v3"
)

// mutex implements a distributed mutex backed by etcd. It embeds
// candidate for the shared registration/release machinery — see
// candidate's doc comment — and adds the exclusive-locking-specific
// "who's the current owner" decision on top.
type mutex struct {
	candidate
}

// newMutex returns a mutex for pfx backed by session s.
// All lock keys are stored under pfx + "/".
func newMutex(s *session, pfx string) *mutex {
	return &mutex{candidate{s: s, pfx: pfx + "/", myRev: -1}}
}

// tryLock locks the mutex if not already locked by another session.
// If the lock is held by another session, it returns immediately after
// attempting necessary cleanup.
func (m *mutex) tryLock(ctx context.Context) error {
	if ctx == nil {
		ctx = m.s.ctx
	}
	resp, err := m.tryAcquire(ctx)
	if err != nil {
		return err
	}
	// No key exists under the prefix, or our key has the lowest create revision:
	// we are already the lock holder.
	ownerKey := resp.Responses[1].GetResponseRange().Kvs
	if len(ownerKey) == 0 || ownerKey[0].CreateRevision == m.myRev {
		m.hdr = resp.Header
		return nil
	}
	// Another session holds the lock; clean up our candidate key and return.
	if err := m.release(ctx); err != nil {
		return err
	}
	return errLocked
}

// lock locks the mutex with a cancelable context. If the context is canceled
// while trying to acquire the lock, the mutex tries to clean its stale lock entry.
func (m *mutex) lock(ctx context.Context) error {
	if ctx == nil {
		ctx = m.s.ctx
	}
	resp, err := m.tryAcquire(ctx)
	if err != nil {
		return err
	}
	// No key exists under the prefix, or our key has the lowest create revision:
	// we are already the lock holder.
	ownerKey := resp.Responses[1].GetResponseRange().Kvs
	if len(ownerKey) == 0 || ownerKey[0].CreateRevision == m.myRev {
		m.hdr = resp.Header
		return nil
	}
	// wait for deletion revisions prior to myKey
	werr := waitDeletes(ctx, m.s.client, m.pfx, m.myRev-1)
	// release lock key if wait failed
	if werr != nil {
		m.unlock(m.s.client.Ctx())
		return werr
	}

	// make sure the session is not expired, and the owner key still exists.
	gresp, werr := m.s.client.Get(ctx, m.myKey)
	if werr != nil {
		m.unlock(m.s.client.Ctx())
		return werr
	}

	if len(gresp.Kvs) == 0 { // is the session key lost?
		return errSessionExpired
	}
	m.hdr = gresp.Header

	return nil
}

// tryAcquire registers this session as a lock candidate and, in the same
// round trip, fetches the current owner (the earliest-created key under
// the prefix) via candidate.register's extra-ops mechanism — the
// optimization that lets the uncontended path complete in a single RPC.
func (m *mutex) tryAcquire(ctx context.Context) (*v3.TxnResponse, error) {
	getOwner := v3.OpGet(m.pfx, v3.WithFirstCreate()...)
	return m.register(ctx, m.pfx, []v3.Op{getOwner}, []v3.Op{getOwner})
}

// unlock deletes this session's candidate key, releasing the lock.
// Returns errLockReleased if the key has already been deleted.
func (m *mutex) unlock(ctx context.Context) error {
	return m.release(ctx)
}
