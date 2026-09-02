package etcd

import (
	"context"
	"fmt"
	"strings"

	v3 "go.etcd.io/etcd/client/v3"
)

// rwmutex implements a distributed read/write mutex following a
// prefix-key protocol: every acquirer registers a key under the shared
// prefix — readers under readPfx, writers under writePfx — then waits
// for whichever set of predecessors would actually conflict with it to
// disappear: a writer waits for every key under pfx (both readers and
// writers) created before its own, while a reader only waits for writer
// keys created before its own, so concurrent readers never block each
// other.
//
// It embeds candidate for the shared registration/release machinery
// (myKey/myRev/hdr bookkeeping, the CAS-create Txn, waitDelete/
// waitDeletes) — the exact same primitives mutex already needs, reused
// unchanged rather than reimplemented, including mutex's deterministic,
// reusable-on-retry candidate key.
//
// rLock/lock block until acquired; tryRLock/tryLock make a single
// non-blocking attempt, mirroring lock.Service's TryLock and mutex's own
// tryLock/lock split.
type rwmutex struct {
	candidate

	readPfx  string
	writePfx string
}

// newRWMutex returns an rwmutex for pfx backed by session s.
func newRWMutex(s *session, pfx string) *rwmutex {
	pfx += "/"
	return &rwmutex{
		candidate: candidate{s: s, pfx: pfx, myRev: -1},
		readPfx:   pfx + "read/",
		writePfx:  pfx + "write/",
	}
}

// rLock acquires a shared (read) lock: it only waits on writer keys
// registered before its own.
func (m *rwmutex) rLock(ctx context.Context) error {
	return m.acquire(ctx, m.readPfx, m.writePfx)
}

// lock acquires an exclusive (write) lock: it waits on every reader and
// writer key registered before its own.
func (m *rwmutex) lock(ctx context.Context) error {
	return m.acquire(ctx, m.writePfx, m.pfx)
}

// tryRLock acquires a shared (read) lock only if no writer registered
// before it is currently in the way; it does not block.
func (m *rwmutex) tryRLock(ctx context.Context) error {
	return m.tryAcquire(ctx, m.readPfx, m.writePfx)
}

// tryLock acquires an exclusive (write) lock only if no reader or writer
// registered before it is currently in the way; it does not block.
func (m *rwmutex) tryLock(ctx context.Context) error {
	return m.tryAcquire(ctx, m.writePfx, m.pfx)
}

// acquire registers this session under keyPfx (via the shared
// candidate.register, with no extra ops — unlike mutex.tryAcquire,
// there's no separate "am I already the owner" pre-check needed: the
// wait below is simply a no-op when nothing conflicts, so registering
// unconditionally and then waiting is both correct and simpler), then
// waits until every key under waitPfx created before that registration
// is gone.
func (m *rwmutex) acquire(ctx context.Context, keyPfx, waitPfx string) error {
	if ctx == nil {
		ctx = m.s.ctx
	}
	client := m.s.client

	if _, err := m.register(ctx, keyPfx, nil, nil); err != nil {
		return err
	}

	if err := waitDeletes(ctx, client, waitPfx, m.myRev-1); err != nil {
		_ = m.release(client.Ctx())
		return err
	}

	// Make sure the session isn't expired, i.e. our own key still exists.
	gresp, err := client.Get(ctx, m.myKey)
	if err != nil {
		_ = m.release(client.Ctx())
		return err
	}
	if len(gresp.Kvs) == 0 {
		return errSessionExpired
	}
	m.hdr = gresp.Header
	return nil
}

// tryAcquire registers this session under keyPfx and, in the same round
// trip, fetches the earliest-created key under waitPfx via
// candidate.register's extra-ops mechanism — exactly mirroring
// mutex.tryAcquire's single-Txn shape, and critically avoiding a second,
// independent RPC that could fail after the candidate key already exists
// (a prior version issued a separate client.Get here, which — on a
// timeout or transient error — returned early without releasing the
// just-registered key, leaking it until lease expiry).
//
// Unlike mutex, which can compare the fetched owner's CreateRevision to
// its own with strict equality (its own key is always part of the same
// searched prefix, so nothing "in the way" can have a CreateRevision
// higher than its own), a reader's waitPfx (writePfx) doesn't contain its
// own key at all — a writer registered *after* the read attempt is not a
// conflict. So the check here is ">=", not "==": no conflict exists as
// long as nothing under waitPfx has a strictly lower CreateRevision.
func (m *rwmutex) tryAcquire(ctx context.Context, keyPfx, waitPfx string) error {
	if ctx == nil {
		ctx = m.s.ctx
	}

	getOwner := v3.OpGet(waitPfx, v3.WithFirstCreate()...)
	resp, err := m.register(ctx, keyPfx, []v3.Op{getOwner}, []v3.Op{getOwner})
	if err != nil {
		return err
	}

	ownerKey := resp.Responses[1].GetResponseRange().Kvs
	if len(ownerKey) == 0 || ownerKey[0].CreateRevision >= m.myRev {
		m.hdr = resp.Header
		return nil
	}

	// Someone registered before us is still in the way; clean up our
	// candidate key and report it as taken, exactly like mutex.tryLock.
	if err := m.release(ctx); err != nil {
		return err
	}
	return errLocked
}

// runlock releases a lock acquired via rLock. If a lock is currently
// held but it's a write lock, that means the caller mixed up RUnlock and
// Unlock — refuse rather than silently releasing the wrong kind of lock.
func (m *rwmutex) runlock(ctx context.Context) error {
	if m.myKey != "" && !strings.HasPrefix(m.myKey, m.readPfx) {
		return fmt.Errorf("rwmutex: RUnlock called, but the currently held lock is a write lock (key %q)", m.myKey)
	}
	return m.release(ctx)
}

// wunlock releases a lock acquired via lock. If a lock is currently held
// but it's a read lock, that means the caller mixed up Unlock and
// RUnlock — refuse rather than silently releasing the wrong kind of lock.
func (m *rwmutex) wunlock(ctx context.Context) error {
	if m.myKey != "" && !strings.HasPrefix(m.myKey, m.writePfx) {
		return fmt.Errorf("rwmutex: Unlock called, but the currently held lock is a read lock (key %q)", m.myKey)
	}
	return m.release(ctx)
}
