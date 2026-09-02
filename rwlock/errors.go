package rwlock

import "errors"

// ErrRWLockLost is returned when a held lease expires or is otherwise
// lost, for either a read or a write grant.
var ErrRWLockLost = errors.New("rwlock: lease expired or lost")

// ErrRWLockTaken is returned by TryLock/TryRLock when the lock is held by
// another owner in a way that conflicts with the requested role.
var ErrRWLockTaken = errors.New("rwlock: already held by another owner")
