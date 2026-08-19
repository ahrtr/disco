package election

import "context"

// Service is the single abstraction over all leader-election backends.
//
// A Service instance is bound to a single election prefix, established at
// construction time (e.g. etcd.NewElection). The underlying lease and its
// keepalive are managed internally; callers do not need to renew it.
//
// The Done channel and Err reflect the health of the lease — they are
// properties of the Service lifetime, not of any individual Campaign.
// Monitor Done in a background goroutine to detect involuntary lease loss:
//
//	go func() {
//	    <-svc.Done()
//	    log.Println("lease lost — this instance may no longer be leader")
//	}()
type Service interface {
	// Campaign puts val forward as a candidate for leadership and blocks
	// until it becomes the leader or ctx is canceled.
	Campaign(ctx context.Context, val string) error

	// Proclaim lets the current leader announce a new value without
	// resigning and re-campaigning. Returns ErrElectionNotLeader if the
	// caller does not currently hold leadership.
	Proclaim(ctx context.Context, val string) error

	// Resign releases leadership, if held, allowing another candidate to be
	// elected. It is a no-op if the caller is not the leader.
	Resign(ctx context.Context) error

	// Leader returns the current leader for this election prefix.
	// Returns ErrElectionNoLeader if no leader is currently elected.
	Leader(ctx context.Context) (*Leadership, error)

	// Observe returns a channel that reliably observes ordered leadership
	// changes on this election prefix. It will not necessarily fetch every
	// historical leader update, but will always post the most recent one.
	//
	// The channel closes when ctx is canceled or the underlying watch is
	// otherwise disrupted.
	Observe(ctx context.Context) <-chan *Leadership

	// Done returns a channel that is closed when the underlying lease is
	// lost (expired or revoked). Once closed, any held leadership has been
	// relinquished and callers must not call Campaign again.
	// Call Close to release backend resources.
	Done() <-chan struct{}

	// Err returns ErrElectionLost if the lease has been lost, nil otherwise.
	// Safe to call concurrently at any point in the Service lifetime.
	Err() error

	// Close revokes the lease and releases all backend resources.
	Close() error
}
