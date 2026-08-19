package etcd

import (
	"context"
	"errors"
	"fmt"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	v3 "go.etcd.io/etcd/client/v3"
)

var (
	errElectionNotLeader = errors.New("election: not leader")
	errElectionNoLeader  = errors.New("election: no leader")
)

type election struct {
	session *session

	keyPrefix string

	leaderKey     string
	leaderRev     int64
	leaderSession *session
}

// newElection returns a new election on a given key prefix.
func newElection(s *session, pfx string) *election {
	return &election{session: s, keyPrefix: pfx + "/"}
}

// campaign puts a value as eligible for the election on the prefix
// key.
// Multiple sessions can participate in the election for the
// same prefix, but only one can be the leader at a time.
//
// If the context is 'context.TODO()/context.Background()', the campaign
// will continue to be blocked for other keys to be deleted, unless server
// returns a non-recoverable error (e.g. ErrCompacted).
// Otherwise, until the context is not cancelled or timed-out, campaign will
// continue to be blocked until it becomes the leader.
func (e *election) campaign(ctx context.Context, val string) error {
	s := e.session
	client := e.session.client

	k := fmt.Sprintf("%s%x", e.keyPrefix, s.id)
	resp, err := client.Txn(ctx).
		If(v3.Compare(v3.CreateRevision(k), "=", 0)).
		Then(v3.OpPut(k, val, v3.WithLease(s.id))).
		Else(v3.OpGet(k)).
		Commit()
	if err != nil {
		return err
	}
	e.leaderKey, e.leaderRev, e.leaderSession = k, resp.Header.Revision, s
	if !resp.Succeeded {
		kv := resp.Responses[0].GetResponseRange().Kvs[0]
		e.leaderRev = kv.CreateRevision
		if string(kv.Value) != val {
			if err = e.proclaim(ctx, val); err != nil {
				_ = e.resign(ctx)
				return err
			}
		}
	}

	if err = waitDeletes(ctx, client, e.keyPrefix, e.leaderRev-1); err != nil {
		// Clean up regardless of why waitDeletes failed (context cancellation
		// or a non-recoverable server error, e.g. ErrCompacted), so the
		// candidate key doesn't linger and block other candidates. Use a
		// fresh context since ctx may already be canceled.
		_ = e.resign(client.Ctx())
		return err
	}

	return nil
}

// proclaim lets the leader announce a new value without another election.
func (e *election) proclaim(ctx context.Context, val string) error {
	if e.leaderSession == nil {
		return errElectionNotLeader
	}
	client := e.session.client
	cmp := v3.Compare(v3.CreateRevision(e.leaderKey), "=", e.leaderRev)
	txn := client.Txn(ctx).If(cmp)
	txn = txn.Then(v3.OpPut(e.leaderKey, val, v3.WithLease(e.leaderSession.id)))
	tresp, terr := txn.Commit()
	if terr != nil {
		return terr
	}
	if !tresp.Succeeded {
		e.leaderKey = ""
		return errElectionNotLeader
	}

	return nil
}

// resign lets a leader start a new election.
func (e *election) resign(ctx context.Context) error {
	if e.leaderSession == nil {
		return nil
	}
	client := e.session.client
	cmp := v3.Compare(v3.CreateRevision(e.leaderKey), "=", e.leaderRev)
	_, err := client.Txn(ctx).If(cmp).Then(v3.OpDelete(e.leaderKey)).Commit()
	e.leaderKey = ""
	e.leaderSession = nil
	return err
}

// leader returns the leader value for the current election.
func (e *election) leader(ctx context.Context) (*v3.GetResponse, error) {
	client := e.session.client
	resp, err := client.Get(ctx, e.keyPrefix, v3.WithFirstCreate()...)
	if err != nil {
		return nil, err
	} else if len(resp.Kvs) == 0 {
		// no leader currently elected
		return nil, errElectionNoLeader
	}
	return resp, nil
}

// observe returns a channel that reliably observes ordered leader proposals
// as GetResponse pointers on every current elected leader key. It will not
// necessarily fetch all historical leader updates, but will always post the
// most recent leader value.
//
// The channel closes when the context is canceled or the underlying watcher
// is otherwise disrupted.
func (e *election) observe(ctx context.Context) <-chan *v3.GetResponse {
	retc := make(chan *v3.GetResponse)
	go e.observeLoop(ctx, retc)
	return retc
}

func (e *election) observeLoop(ctx context.Context, ch chan<- *v3.GetResponse) {
	defer close(ch)
	for {
		hdr, kv, err := e.firstLeader(ctx)
		if err != nil {
			return
		}

		select {
		case ch <- &v3.GetResponse{Header: hdr, Kvs: []*mvccpb.KeyValue{kv}}:
		case <-ctx.Done():
			return
		}

		if !e.watchUntilDeleted(ctx, ch, kv.Key, hdr.Revision+1) {
			return
		}
	}
}

// firstLeader returns the current leader on the prefix, waiting for one to
// be put if none currently exists.
func (e *election) firstLeader(ctx context.Context) (*pb.ResponseHeader, *mvccpb.KeyValue, error) {
	client := e.session.client
	resp, err := client.Get(ctx, e.keyPrefix, v3.WithFirstCreate()...)
	if err != nil {
		return nil, nil, err
	}
	if len(resp.Kvs) > 0 {
		return resp.Header, resp.Kvs[0], nil
	}

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// wait for first key put on prefix
	opts := []v3.OpOption{v3.WithRev(resp.Header.Revision), v3.WithPrefix()}
	wch := client.Watch(cctx, e.keyPrefix, opts...)
	for {
		wr, ok := <-wch
		if !ok || wr.Err() != nil {
			return nil, nil, fmt.Errorf("watch on prefix closed unexpectedly")
		}
		// only accept puts; a delete will make observe() spin
		for _, ev := range wr.Events {
			if ev.Type == mvccpb.Event_PUT {
				hdr, kv := wr.Header, ev.Kv
				// may have multiple revs; hdr.rev = the last rev
				// set to kv's rev in case batch has multiple Puts
				hdr.Revision = kv.ModRevision
				return hdr, kv, nil
			}
		}
	}
}

// watchUntilDeleted streams every update to key on ch until it is deleted or
// the watch is otherwise disrupted, returning false in the latter case.
func (e *election) watchUntilDeleted(ctx context.Context, ch chan<- *v3.GetResponse, key []byte, rev int64) bool {
	client := e.session.client
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wch := client.Watch(cctx, string(key), v3.WithRev(rev))
	for {
		wr, ok := <-wch
		if !ok {
			return false
		}
		for _, ev := range wr.Events {
			if ev.Type == mvccpb.Event_DELETE {
				return true
			}
			select {
			case ch <- &v3.GetResponse{Header: wr.Header, Kvs: []*mvccpb.KeyValue{ev.Kv}}:
			case <-cctx.Done():
				return false
			}
		}
	}
}
