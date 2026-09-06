package semaphore

import "context"

// Service is the single abstraction over all distributed counting-semaphore
// backends.
//
// A Service instance is bound to a single semaphore key and permit limit,
// established at construction time (e.g. etcd.NewSemaphore). The underlying
// lease and its keepalive are managed internally; callers do not need to
// renew it.
//
// A Service instance holds at most one permit at a time — calling Acquire
// again while already holding a permit reuses the same permit rather than
// consuming a second one, exactly like lock.Service's Lock. Create one
// Service per concurrent permit a process wants to hold.
//
// The Done channel and Err reflect the health of the lease — they are
// properties of the Service lifetime, not of any individual Acquire call.
// Monitor Done in a background goroutine to detect involuntary lease loss:
//
//	go func() {
//	    <-svc.Done()
//	    log.Println("lease lost — stop accessing guarded resources")
//	}()
type Service interface {
	// Acquire acquires a permit, blocking until one is available or ctx is
	// canceled. Returns a Grant carrying the fencing token and lease
	// metadata for this acquisition.
	Acquire(ctx context.Context) (*Grant, error)

	// TryAcquire attempts to acquire a permit without blocking.
	// Returns ErrNoPermitsAvailable immediately if every permit is currently
	// held by other owners.
	TryAcquire(ctx context.Context) (*Grant, error)

	// Release explicitly releases the held permit. The underlying lease
	// remains alive so Acquire can be called again without creating a new
	// Service.
	Release(ctx context.Context) error

	// Stat reports the semaphore's current usage: how many permits are held
	// and the total limit.
	Stat(ctx context.Context) (*Stat, error)

	// Done returns a channel that is closed when the underlying lease is lost
	// (expired or revoked). Once closed, callers must immediately stop
	// accessing guarded resources and must not call Acquire again.
	// Call Close to release backend resources.
	Done() <-chan struct{}

	// Err returns ErrSemaphoreLost if the lease has been lost, nil otherwise.
	// Safe to call concurrently at any point in the Service lifetime.
	Err() error

	// Close revokes the lease and releases all backend resources.
	Close() error
}
