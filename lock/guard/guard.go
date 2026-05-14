package guard

import (
	"sync/atomic"

	"github.com/ahrtr/disco/lock/fencing"
)

// Guard is a concurrency-safe high-water mark for fencing tokens.
//
// Resources embed or hold a Guard and call Check on every incoming request.
// The check accepts the request when token >= high-water mark, advancing the
// mark only when token > high-water mark. It rejects the request when token
// is strictly lower than the current high-water mark.
type Guard struct {
	// highWater is the highest token ever accepted. Stored as int64 to allow
	// use of atomic operations without a mutex.
	highWater atomic.Int64
}

// Option configures a Guard at construction time.
type Option func(*Guard)

// WithInitialToken seeds the Guard's high-water mark with token. Use this
// when the last accepted fencing token is known at startup — for example,
// after reading it from a persistent store — so that stale clients cannot
// write through a fresh guard before the first legitimate request arrives.
func WithInitialToken(token fencing.Token) Option {
	return func(g *Guard) {
		g.highWater.Store(int64(token))
	}
}

// New returns a ready-to-use Guard. Without options the high-water mark
// starts at zero, which accepts any valid token on the first request.
func New(opts ...Option) *Guard {
	g := &Guard{}
	for _, opt := range opts {
		opt(g)
	}
	return g
}

// Check validates token against the current high-water mark.
//
// It returns nil when token >= high-water mark, advancing the mark when
// token > high-water mark. It returns fencing.ErrTokenStale when token is
// strictly lower than the current high-water mark.
//
// Check is safe for concurrent use from multiple goroutines.
func (g *Guard) Check(token fencing.Token) error {
	t := int64(token)
	for {
		cur := g.highWater.Load()
		if t < cur {
			return fencing.ErrTokenStale
		}
		// t >= cur: attempt to accept. CompareAndSwap is a no-op when t == cur
		// but correctly fails and retries if another goroutine has advanced
		// highWater above t since the Load, preventing a stale accept.
		if g.highWater.CompareAndSwap(cur, t) {
			return nil
		}
		// Another goroutine changed the mark; re-evaluate.
	}
}

// HighWater returns the current high-water mark. Primarily useful for
// diagnostics and testing.
func (g *Guard) HighWater() fencing.Token {
	return fencing.Token(g.highWater.Load())
}
