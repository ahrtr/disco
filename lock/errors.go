package lock

import "errors"

var (
	// ErrLockLost is returned when a held lease expires or is otherwise lost.
	ErrLockLost = errors.New("lock: lease expired or lost")

	// ErrLockTaken is returned by TryLock when the lock is held by another owner.
	ErrLockTaken = errors.New("lock: already held by another owner")
)
