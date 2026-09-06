package etcd

import (
	"context"
	"errors"

	v3 "go.etcd.io/etcd/client/v3"
)

var errSemaphoreFull = errors.New("semaphore: no permits available")

// semaphore implements a distributed counting semaphore backed by etcd. It
// embeds candidate for the shared registration/release machinery — see
// candidate's doc comment — generalizing mutex's exclusive "am I the
// first-created key" decision into "are there at most limit keys
// registered", i.e. mutex is the limit-1 special case of semaphore.
type semaphore struct {
	candidate

	limit int
}

// newSemaphore returns a semaphore for pfx, allowing up to limit
// concurrently held permits, backed by session s.
// All permit keys are stored under pfx + "/".
func newSemaphore(s *session, pfx string, limit int) *semaphore {
	return &semaphore{candidate{s: s, pfx: pfx + "/", myRev: -1}, limit}
}

// tryAcquire acquires a permit if fewer than limit are currently held.
// If every permit is taken, it returns immediately after attempting
// necessary cleanup.
func (m *semaphore) tryAcquire(ctx context.Context) error {
	if ctx == nil {
		ctx = m.s.ctx
	}
	resp, count, err := m.registerAndCount(ctx)
	if err != nil {
		return err
	}
	// count includes the key we just registered: at most limit keys total
	// (including ours) means we are within the first limit acquirers.
	if count <= int64(m.limit) {
		m.hdr = resp.Header
		return nil
	}
	// Every permit is already held; clean up our candidate key and return.
	if err := m.release(ctx); err != nil {
		return err
	}
	return errSemaphoreFull
}

// acquire acquires a permit with a cancelable context, blocking until one
// is available. If the context is canceled while waiting, acquire tries to
// clean up its stale candidate entry.
func (m *semaphore) acquire(ctx context.Context) error {
	if ctx == nil {
		ctx = m.s.ctx
	}
	resp, count, err := m.registerAndCount(ctx)
	if err != nil {
		return err
	}
	if count <= int64(m.limit) {
		m.hdr = resp.Header
		return nil
	}

	// Wait until at most limit-1 predecessors (keys created before ours)
	// remain, so that together with our own key at most limit are held.
	// limit-1 == 0 (i.e. limit == 1, the mutex-equivalent case) uses
	// waitDeletes instead of waitLimit, per waitLimit's own doc comment:
	// it can watch a single key rather than the whole prefix.
	var werr error
	if m.limit == 1 {
		werr = waitDeletes(ctx, m.s.client, m.pfx, m.myRev-1)
	} else {
		werr = waitLimit(ctx, m.s.client, m.pfx, m.myRev-1, m.limit-1)
	}
	if werr != nil {
		_ = m.release(m.s.client.Ctx())
		return werr
	}

	// make sure the session is not expired, and our own key still exists.
	gresp, werr := m.s.client.Get(ctx, m.myKey)
	if werr != nil {
		_ = m.release(m.s.client.Ctx())
		return werr
	}
	if len(gresp.Kvs) == 0 { // is the session key lost?
		return errSessionExpired
	}
	m.hdr = gresp.Header

	return nil
}

// registerAndCount registers this session as a permit candidate and, in the
// same round trip, counts how many keys are now registered under the
// prefix (including the one just registered) via candidate.register's
// extra-ops mechanism — the same single-RPC optimization mutex.tryAcquire
// uses for its owner lookup.
func (m *semaphore) registerAndCount(ctx context.Context) (*v3.TxnResponse, int64, error) {
	getCount := v3.OpGet(m.pfx, v3.WithPrefix(), v3.WithCountOnly())
	resp, err := m.register(ctx, m.pfx, []v3.Op{getCount}, []v3.Op{getCount})
	if err != nil {
		return nil, 0, err
	}
	return resp, resp.Responses[1].GetResponseRange().Count, nil
}

// stat reports how many candidates are currently registered under the
// prefix — both permits actually held and callers still blocked in acquire
// waiting for one (see semaphoreapi.Stat.Registered for why the raw count
// is exposed rather than just the held-only count).
func (m *semaphore) stat(ctx context.Context) (int64, error) {
	if ctx == nil {
		ctx = m.s.ctx
	}
	resp, err := m.s.client.Get(ctx, m.pfx, v3.WithPrefix(), v3.WithCountOnly())
	if err != nil {
		return 0, err
	}
	return resp.Count, nil
}
