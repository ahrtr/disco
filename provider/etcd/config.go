package etcd

import "time"

// defaultTTL is the default lease TTL used when no TTL is configured.
const defaultTTL = 30 * time.Second

// Config holds provider-level configuration.
// Connection parameters (endpoints, TLS, dial timeout, etc.) are the caller's
// responsibility and belong in the *clientv3.Client passed to NewLock.
type Config struct {
	// DefaultTTL is the lease TTL used when not overridden by WithDefaultTTL.
	// Defaults to 30 s.
	DefaultTTL time.Duration
}

func (c *Config) defaultTTL() time.Duration {
	if c.DefaultTTL > 0 {
		return c.DefaultTTL
	}
	return defaultTTL
}
