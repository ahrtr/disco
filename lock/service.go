package lock

import "context"

// Service is the single abstraction over all distributed lock backends.
//
// Each Service instance is bound to a single lock key, established at
// construction time. The lease and its keepalive are managed internally;
// callers do not need to renew it. Use the Session returned by Lock or
// TryLock to monitor for involuntary lease loss via Session.Done.
type Service interface {
	// Lock acquires the distributed lock, blocking until it is available or
	// ctx is canceled.
	Lock(ctx context.Context) (*Session, error)

	// TryLock attempts to acquire the lock without blocking.
	// Returns ErrLockTaken immediately if the lock is held by another owner.
	TryLock(ctx context.Context) (*Session, error)

	// Close revokes the lease and releases all backend resources.
	Close() error
}
