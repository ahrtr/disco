// Package rwlock defines the core abstractions for disco's distributed
// read/write lock: the Service interface and the Grant type.
//
// It follows a prefix-key protocol: every acquirer registers a key under
// a shared prefix (readers and writers each in their own sub-namespace),
// then waits for whichever set of predecessors would actually conflict
// with it to disappear —
//
//   - a writer waits for every key registered before it, both readers and
//     writers, since a write needs full exclusivity;
//   - a reader only waits for writer keys registered before it, so
//     concurrent readers never block each other.
//
// Concrete backend implementations live under provider/.
package rwlock
