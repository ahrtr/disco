package etcd

import (
	"context"
	"errors"
	"fmt"
	"strings"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	v3 "go.etcd.io/etcd/client/v3"
)

var (
	errLocked         = errors.New("mutex: locked by another session")
	errSessionExpired = errors.New("mutex: session is expired")
	errLockReleased   = errors.New("mutex: lock has already been released")
)

// mutex implements a distributed mutex backed by etcd.
type mutex struct {
	s *session

	pfx   string             // key prefix; all candidate keys are put under pfx
	myKey string             // this session's candidate key in etcd
	myRev int64              // create revision of myKey; lowest revision is the lock holder
	hdr   *pb.ResponseHeader // response header from the last successful lock acquisition
}

// newMutex returns a mutex for pfx backed by session s.
// All lock keys are stored under pfx + "/".
func newMutex(s *session, pfx string) *mutex {
	return &mutex{s, pfx + "/", "", -1, nil}
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
	if _, err := m.s.client.Delete(ctx, m.myKey); err != nil {
		return err
	}
	m.myKey = "\x00"
	m.myRev = -1
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

// tryAcquire registers this session as a lock candidate via a single
// compare-and-swap transaction and returns the transaction response.
// The caller determines from the response whether the lock is already held.
func (m *mutex) tryAcquire(ctx context.Context) (*v3.TxnResponse, error) {
	m.myKey = fmt.Sprintf("%s%x", m.pfx, m.s.id)
	cmp := v3.Compare(v3.CreateRevision(m.myKey), "=", 0)
	// put self in lock waiters via myKey; oldest waiter holds lock
	put := v3.OpPut(m.myKey, "", v3.WithLease(m.s.id))
	// reuse key in case this session already holds the lock
	get := v3.OpGet(m.myKey)
	// fetch current holder to complete uncontended path with only one RPC
	getOwner := v3.OpGet(m.pfx, v3.WithFirstCreate()...)
	resp, err := m.s.client.Txn(ctx).If(cmp).Then(put, getOwner).Else(get, getOwner).Commit()
	if err != nil {
		return nil, err
	}
	m.myRev = resp.Header.Revision
	if !resp.Succeeded {
		m.myRev = resp.Responses[0].GetResponseRange().Kvs[0].CreateRevision
	}
	return resp, nil
}

// unlock deletes this session's candidate key, releasing the lock.
// Returns errLockReleased if the key has already been deleted.
func (m *mutex) unlock(ctx context.Context) error {
	if m.myKey == "" || m.myRev <= 0 || m.myKey == "\x00" {
		return errLockReleased
	}

	if !strings.HasPrefix(m.myKey, m.pfx) {
		return fmt.Errorf("invalid key %q, it should have prefix %q", m.myKey, m.pfx)
	}

	if _, err := m.s.client.Delete(ctx, m.myKey); err != nil {
		return err
	}
	m.myKey = "\x00"
	m.myRev = -1
	return nil
}

// header returns the response header received from etcd on acquiring the lock.
func (m *mutex) header() *pb.ResponseHeader { return m.hdr }

// waitDelete blocks until a DELETE event is observed for key at or after rev,
// or until ctx is canceled or the watch channel is closed unexpectedly.
func waitDelete(ctx context.Context, client *v3.Client, key string, rev int64) error {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wr v3.WatchResponse
	wch := client.Watch(cctx, key, v3.WithRev(rev))
	for wr = range wch {
		for _, ev := range wr.Events {
			if ev.Type == mvccpb.DELETE {
				return nil
			}
		}
	}
	if err := wr.Err(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return errors.New("lost watcher waiting for delete")
}

// waitDeletes efficiently waits until all keys matching the prefix and no
// greater than the create revision are deleted.
func waitDeletes(ctx context.Context, client *v3.Client, pfx string, maxCreateRev int64) error {
	getOpts := append(v3.WithLastCreate(), v3.WithMaxCreateRev(maxCreateRev))
	for {
		resp, err := client.Get(ctx, pfx, getOpts...)
		if err != nil {
			return err
		}
		if len(resp.Kvs) == 0 {
			return nil
		}
		lastKey := string(resp.Kvs[0].Key)
		if err = waitDelete(ctx, client, lastKey, resp.Header.Revision); err != nil {
			return err
		}
	}
}
