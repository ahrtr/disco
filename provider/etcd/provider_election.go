package etcd

import (
	"context"
	"errors"
	"fmt"

	clientv3 "go.etcd.io/etcd/client/v3"

	electionapi "github.com/ahrtr/disco/election"
)

// Compile-time proof that *ElectionProvider satisfies electionapi.Service.
var _ electionapi.Service = (*ElectionProvider)(nil)

// ElectionProvider implements electionapi.Service using etcd.
//
// An ElectionProvider is bound to a single election key prefix for its
// lifetime. The session (lease + keepalive goroutine) and the election are
// created once in NewElection and reused across multiple Campaign calls.
type ElectionProvider struct {
	key      string
	session  *session
	election *election
}

// NewElection creates an electionapi.Service for the given key prefix backed
// by etcd.
//
// It establishes one lease (with automatic keepalive) and one election for
// key. Both are reused across Campaign calls for the lifetime of the
// returned service.
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
//	svc, err := etcd.NewElection(cli, "/elections/my-resource")
func NewElection(client *clientv3.Client, key string, opts ...ProviderOption) (electionapi.Service, error) {
	session, err := newProviderSession(client, key, opts...)
	if err != nil {
		return nil, err
	}

	return &ElectionProvider{
		key:      key,
		session:  session,
		election: newElection(session, key),
	}, nil
}

// Campaign puts val forward as a candidate for leadership on key, blocking
// until it becomes the leader or ctx is canceled.
func (p *ElectionProvider) Campaign(ctx context.Context, val string) error {
	if err := p.election.campaign(ctx, val); err != nil {
		if errors.Is(err, errElectionNotLeader) {
			return electionapi.ErrElectionNotLeader
		}
		return fmt.Errorf("etcd provider: campaign %q: %w", p.key, err)
	}
	return nil
}

// Proclaim lets the current leader announce a new value without resigning
// and re-campaigning.
func (p *ElectionProvider) Proclaim(ctx context.Context, val string) error {
	if err := p.election.proclaim(ctx, val); err != nil {
		if errors.Is(err, errElectionNotLeader) {
			return electionapi.ErrElectionNotLeader
		}
		return fmt.Errorf("etcd provider: proclaim %q: %w", p.key, err)
	}
	return nil
}

// Resign releases leadership, if held, allowing another candidate to be
// elected.
func (p *ElectionProvider) Resign(ctx context.Context) error {
	if err := p.election.resign(ctx); err != nil {
		return fmt.Errorf("etcd provider: resign %q: %w", p.key, err)
	}
	return nil
}

// Leader returns the current leader for this election prefix.
func (p *ElectionProvider) Leader(ctx context.Context) (*electionapi.Leadership, error) {
	resp, err := p.election.leader(ctx)
	if err != nil {
		if errors.Is(err, errElectionNoLeader) {
			return nil, electionapi.ErrElectionNoLeader
		}
		return nil, fmt.Errorf("etcd provider: leader %q: %w", p.key, err)
	}
	kv := resp.Kvs[0]
	return &electionapi.Leadership{
		Key:          string(kv.Key),
		Value:        string(kv.Value),
		FencingToken: kv.CreateRevision,
	}, nil
}

// Observe returns a channel that reliably observes ordered leadership
// changes on this election prefix.
func (p *ElectionProvider) Observe(ctx context.Context) <-chan *electionapi.Leadership {
	src := p.election.observe(ctx)
	dst := make(chan *electionapi.Leadership)
	go func() {
		defer close(dst)
		for resp := range src {
			kv := resp.Kvs[0]
			select {
			case dst <- &electionapi.Leadership{
				Key:          string(kv.Key),
				Value:        string(kv.Value),
				FencingToken: kv.CreateRevision,
			}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return dst
}

// Done returns a channel that is closed when the session lease is lost.
// The channel is created once at NewElection time and never changes.
func (p *ElectionProvider) Done() <-chan struct{} {
	return p.session.donec
}

// Err returns electionapi.ErrElectionLost if the session lease has been lost,
// nil otherwise.
func (p *ElectionProvider) Err() error {
	select {
	case <-p.session.donec:
		return electionapi.ErrElectionLost
	default:
		return nil
	}
}

// Close revokes the session lease, releasing any held leadership. The
// underlying etcd client is not closed; the caller that created it is
// responsible for that.
func (p *ElectionProvider) Close() error {
	return p.session.close()
}
