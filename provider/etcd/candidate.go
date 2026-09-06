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

// candidate is the shared state and registration/release machinery behind
// every prefix-key coordination primitive in this package (mutex,
// rwmutex, semaphore): a session's own key under a shared prefix, the
// revision it was created at, and the response header from the last
// successful acquisition (the fencing-token source). mutex, rwmutex, and
// semaphore all embed a candidate, since all three follow the identical
// "CAS-create a deterministic per-session key, then decide/wait based on
// what else is registered under the prefix" protocol — only that
// deciding/waiting step differs enough between exclusive locking (mutex),
// shared/exclusive locking (rwmutex), and counting permits (semaphore) to
// stay separate.
type candidate struct {
	s   *session
	pfx string // key prefix; every candidate key for this instance is created under some sub-prefix of pfx

	myKey string
	myRev int64
	hdr   *pb.ResponseHeader
}

// register creates this session's candidate key under keyPfx via a
// CAS-create Txn — put if absent, otherwise reuse the caller's own
// pre-existing key, so a retry of a previously interrupted acquisition
// attempt picks up the same deterministic key instead of minting a new
// one each time. extraThen/extraElse let a caller piggyback additional
// read-only ops onto the same Txn — mutex uses this to fetch the current
// owner in the same round trip; pass nil for neither. Sets myKey/myRev
// and returns the full TxnResponse so the caller can inspect any extra
// ops' results, which land at Responses[1], Responses[2], ... right
// after the put/get response at Responses[0].
func (c *candidate) register(ctx context.Context, keyPfx string, extraThen, extraElse []v3.Op) (*v3.TxnResponse, error) {
	c.myKey = fmt.Sprintf("%s%x", keyPfx, c.s.id)
	cmp := v3.Compare(v3.CreateRevision(c.myKey), "=", 0)
	then := append([]v3.Op{v3.OpPut(c.myKey, "", v3.WithLease(c.s.id))}, extraThen...)
	els := append([]v3.Op{v3.OpGet(c.myKey)}, extraElse...)
	resp, err := c.s.client.Txn(ctx).If(cmp).Then(then...).Else(els...).Commit()
	if err != nil {
		return nil, err
	}
	c.myRev = resp.Header.Revision
	if !resp.Succeeded {
		c.myRev = resp.Responses[0].GetResponseRange().Kvs[0].CreateRevision
	}
	return resp, nil
}

// release deletes the candidate key, if any, and clears myKey/myRev.
// Returns errLockReleased if there's nothing to release (already
// released, or never registered). Used both for an explicit
// unlock/RUnlock and for best-effort cleanup after a failed or abandoned
// acquisition attempt.
func (c *candidate) release(ctx context.Context) error {
	if c.myKey == "" || c.myRev <= 0 {
		return errLockReleased
	}
	if !strings.HasPrefix(c.myKey, c.pfx) {
		return fmt.Errorf("invalid key %q, it should have prefix %q", c.myKey, c.pfx)
	}
	if _, err := c.s.client.Delete(ctx, c.myKey); err != nil {
		return err
	}
	c.myKey = ""
	c.myRev = -1
	return nil
}

// header returns the response header from the last successful
// acquisition — the fencing-token source.
func (c *candidate) header() *pb.ResponseHeader { return c.hdr }

// waitDelete blocks until a DELETE event is observed for key at or after
// rev, or until ctx is canceled or the watch channel is closed
// unexpectedly. Pass v3.WithPrefix() as an extra option to watch every key
// under a prefix instead of a single key.
func waitDelete(ctx context.Context, client *v3.Client, key string, rev int64, extraOpts ...v3.OpOption) error {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	opts := append([]v3.OpOption{v3.WithRev(rev)}, extraOpts...)
	var wr v3.WatchResponse
	wch := client.Watch(cctx, key, opts...)
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
// greater than the create revision are deleted, watching only the newest
// such key at a time rather than the whole prefix: since every matching key
// must eventually be deleted for the wait to succeed, it doesn't matter
// which one is watched first — this one will also go eventually, and each
// deletion triggers a recheck for whatever (if anything) remains.
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

// waitLimit blocks until at most limit keys matching the prefix and no
// greater than maxCreateRev remain — the semaphore generalization of
// waitDeletes, for limit > 0. (limit == 0 should use waitDeletes instead,
// which can watch a single key rather than the whole prefix — see below.)
//
// Each iteration fetches every matching key (not just the newest): resp.Count
// does not respect the WithMaxCreateRev filter — it reflects the total number
// of keys under pfx regardless of create revision — so it cannot be used to
// decide whether we're under the limit; only the returned, correctly
// filtered Kvs slice can.
//
// Crossing the limit threshold can be triggered by deleting *any* qualifying
// key, not necessarily the newest one, so this watches every key under the
// prefix for a delete rather than picking one — an unrelated delete under
// the prefix (e.g. of a key with a higher create revision, outside the
// maxCreateRev filter) just causes a harmless extra recheck.
func waitLimit(ctx context.Context, client *v3.Client, pfx string, maxCreateRev int64, limit int) error {
	// Only the count of matches is used below, so no sort order is
	// requested — nothing here cares which matching keys they are.
	getOpts := []v3.OpOption{
		v3.WithPrefix(),
		v3.WithMaxCreateRev(maxCreateRev),
	}
	for {
		resp, err := client.Get(ctx, pfx, getOpts...)
		if err != nil {
			return err
		}
		if len(resp.Kvs) <= limit {
			return nil
		}
		if err = waitDelete(ctx, client, pfx, resp.Header.Revision, v3.WithPrefix()); err != nil {
			return err
		}
	}
}
