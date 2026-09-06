package semaphore

import "errors"

// ErrSemaphoreLost is returned when a held lease expires or is otherwise lost.
var ErrSemaphoreLost = errors.New("semaphore: lease expired or lost")

// ErrNoPermitsAvailable is returned by TryAcquire when every permit is
// currently held by other owners.
var ErrNoPermitsAvailable = errors.New("semaphore: no permits available")
