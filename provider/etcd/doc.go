// Package etcd implements lock.Service, rwlock.Service, semaphore.Service,
// and election.Service using etcd as the distributed coordination backend.
//
// Locking strategy
//
// NewLock creates one lease (session) and one distributed mutex for the given
// lock key. Both are established once and reused across multiple Lock and
// TryLock calls. The mutex uses the standard etcd prefix-key election
// protocol: clients race to put a key under the lock prefix; the holder with
// the lowest create-revision wins.
//
// NewSemaphore generalizes the same protocol from a single owner to a fixed
// number of permits: an acquirer holds a permit as long as at most limit
// keys are registered under the prefix, and one waiting to acquire blocks
// only until enough predecessors — keys registered before its own — have
// released, rather than waiting for every one of them.
//
// Fencing token
//
// The fencing token is the etcd cluster revision recorded in the response
// header at the moment the lock or permit is acquired. This value is a
// global, monotonically increasing integer that advances on every write to
// the cluster, so every successful acquisition receives a strictly higher
// token than any previous one.
//
// Keepalive
//
// The session manages its own lease keepalive goroutine. Callers do not need
// to renew the lease manually; they should instead monitor the channel returned
// by Service.Done to detect involuntary lease loss.
package etcd
