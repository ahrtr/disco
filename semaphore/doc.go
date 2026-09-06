// Package semaphore defines the core abstractions for disco's distributed
// counting semaphore: the Service interface, the Grant type, and Stat.
//
// It generalizes lock's prefix-key protocol from a single owner to a fixed
// number of permits: every acquirer registers a key under a shared prefix,
// then either holds a permit immediately (if fewer than limit keys were
// already registered) or waits for enough predecessors — keys registered
// before its own — to release before it does.
//
// Concrete backend implementations live under provider/.
package semaphore
