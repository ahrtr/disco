// Package etcd implements lock.Service using etcd as the distributed lock
// backend.
//
// Locking strategy
//
// NewLock creates one lease (session) and one distributed mutex for the given
// lock key. Both are established once and reused across multiple Lock and
// TryLock calls. The mutex uses the standard etcd prefix-key election
// protocol: clients race to put a key under the lock prefix; the holder with
// the lowest create-revision wins.
//
// Fencing token
//
// The fencing token is the etcd cluster revision recorded in the response
// header at the moment the lock is acquired. This value is a global,
// monotonically increasing integer that advances on every write to the cluster,
// so every successful lock acquisition receives a strictly higher token than
// any previous acquisition.
//
// Keepalive
//
// The session manages its own lease keepalive goroutine. Callers do not need
// to renew the lease manually; they should instead monitor the channel returned
// by Session.Done to detect involuntary lease loss.
package etcd
