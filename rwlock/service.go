package rwlock

import "context"

// Service is the single abstraction over all distributed read/write lock
// backends.
//
// A Service instance is bound to a single rwlock key, established at
// construction time (e.g. etcd.NewRWLock). The underlying lease and its
// keepalive are managed internally; callers do not need to renew it.
//
// The Done channel and Err reflect the health of the lease — they are
// properties of the Service lifetime, not of any individual RLock/Lock
// call. Monitor Done in a background goroutine to detect involuntary
// lease loss:
//
//	go func() {
//	    <-svc.Done()
//	    log.Println("lease lost — stop accessing guarded resources")
//	}()
type Service interface {
	// RLock acquires a shared (read) lock, blocking until it is available
	// or ctx is canceled. It only waits on writers registered before it —
	// concurrent readers never block each other. Returns a Grant carrying
	// the fencing token and lease metadata for this acquisition.
	RLock(ctx context.Context) (*Grant, error)

	// Lock acquires an exclusive (write) lock, blocking until it is
	// available or ctx is canceled. It waits on every reader and writer
	// registered before it. Returns a Grant carrying the fencing token
	// and lease metadata for this acquisition.
	Lock(ctx context.Context) (*Grant, error)

	// TryRLock attempts to acquire a shared (read) lock without blocking.
	// Returns ErrRWLockTaken immediately if a writer registered before
	// this attempt is currently in the way.
	TryRLock(ctx context.Context) (*Grant, error)

	// TryLock attempts to acquire an exclusive (write) lock without
	// blocking. Returns ErrRWLockTaken immediately if a reader or writer
	// registered before this attempt is currently in the way.
	TryLock(ctx context.Context) (*Grant, error)

	// RUnlock releases a lock acquired via RLock. The underlying lease
	// remains alive so RLock/Lock can be called again without creating a
	// new Service.
	RUnlock(ctx context.Context) error

	// Unlock releases a lock acquired via Lock. The underlying lease
	// remains alive so RLock/Lock can be called again without creating a
	// new Service.
	Unlock(ctx context.Context) error

	// Done returns a channel that is closed when the underlying lease is
	// lost (expired or revoked). Once closed, callers must immediately
	// stop accessing guarded resources and must not call
	// RLock/Lock/TryRLock/TryLock again. Call Close to release backend
	// resources.
	Done() <-chan struct{}

	// Err returns ErrRWLockLost if the lease has been lost, nil otherwise.
	// Safe to call concurrently at any point in the Service lifetime.
	Err() error

	// Close revokes the lease and releases all backend resources.
	Close() error
}
