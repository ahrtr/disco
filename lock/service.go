package lock

import "context"

// Service is the single abstraction over all distributed lock backends.
//
// A Service instance is bound to a single lock key, established at construction
// time (e.g. etcd.NewLock). The underlying lease and its keepalive are managed
// internally; callers do not need to renew it.
//
// The Done channel and Err reflect the health of the lease — they are
// properties of the Service lifetime, not of any individual Lock call.
// Monitor Done in a background goroutine to detect involuntary lease loss:
//
//	go func() {
//	    <-svc.Done()
//	    log.Println("lease lost — stop accessing guarded resources")
//	}()
type Service interface {
	// Lock acquires the distributed lock, blocking until it is available or
	// ctx is canceled. Returns a Grant carrying the fencing token and lease
	// metadata for this acquisition.
	Lock(ctx context.Context) (*Grant, error)

	// TryLock attempts to acquire the lock without blocking.
	// Returns ErrLockTaken immediately if the lock is held by another owner.
	TryLock(ctx context.Context) (*Grant, error)

	// Unlock explicitly releases the lock. The underlying lease remains alive
	// so Lock can be called again without creating a new Service.
	Unlock(ctx context.Context) error

	// Done returns a channel that is closed when the underlying lease is lost
	// (expired or revoked). Once closed, callers must immediately stop
	// accessing guarded resources and must not call Lock again.
	// Call Close to release backend resources.
	Done() <-chan struct{}

	// Err returns ErrLockLost if the lease has been lost, nil otherwise.
	// Safe to call concurrently at any point in the Service lifetime.
	Err() error

	// Close revokes the lease and releases all backend resources.
	Close() error
}
