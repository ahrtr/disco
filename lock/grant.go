package lock

import "time"

// Grant represents a successfully acquired lock.
type Grant struct {
	// Key is the distributed lock key that was acquired.
	Key string

	// FencingToken is a monotonically increasing integer assigned at lock
	// grant time. It must be attached to every resource request so the
	// resource can reject requests from stale (lower-token) owners.
	FencingToken int64

	// ExpiresAt is the wall-clock time at which the lease will expire if not
	// renewed. This is informational; the authoritative expiry lives in the
	// backend.
	ExpiresAt time.Time
}
