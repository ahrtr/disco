package semaphore

// Stat reports a snapshot of a semaphore's current usage.
type Stat struct {
	// Registered is the number of candidates currently registered against
	// the semaphore: both permits actually held and callers still blocked
	// in Acquire waiting for one. It's exposed instead of just the held
	// count because Held is trivially derived from it (see Stat.Held), but
	// the reverse isn't true — a bare held count can't tell you whether
	// anyone is waiting.
	Registered int

	// Limit is the total number of permits the semaphore was created with.
	Limit int
}

// Held returns the number of permits currently held by any owner. Permits
// are always granted to the oldest Registered candidates first, so this is
// just Registered capped at Limit.
func (s Stat) Held() int {
	if s.Registered > s.Limit {
		return s.Limit
	}
	return s.Registered
}
