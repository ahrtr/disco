package etcd

import (
	"context"
	"time"
)

// ProviderOption configures a Provider.
type ProviderOption func(*providerOptions)

// providerOptions is the resolved configuration built from applied options.
type providerOptions struct {
	cfg Config
	// ctx is the parent context for the session lease keepalive loop.
	// Defaults to the etcd client's context when nil.
	ctx context.Context
}

func defaultProviderOptions() providerOptions {
	return providerOptions{
		cfg: Config{
			DefaultTTL: defaultTTL,
		},
	}
}

// WithContext sets the parent context for the session's lease keepalive loop.
// When the context is cancelled the keepalive stops, the lease expires, and
// the service's Done channel is closed.
// If not set, the etcd client's own context is used.
func WithContext(ctx context.Context) ProviderOption {
	return func(o *providerOptions) {
		o.ctx = ctx
	}
}

// WithDefaultTTL sets the default lease TTL. Defaults to 30 s.
// Values below 5 s are clamped to 5 s by NewLock.
func WithDefaultTTL(d time.Duration) ProviderOption {
	return func(o *providerOptions) {
		o.cfg.DefaultTTL = d
	}
}
