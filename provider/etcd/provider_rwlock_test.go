package etcd

import (
	"context"
	"errors"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/ahrtr/disco/rwlock"
)

// dialTestEtcd connects to a real etcd cluster at localhost:2379 and skips
// the test if one isn't reachable — this package has no mock/in-process
// etcd, and CI (.github/workflows/test.yml) runs plain `go test ./...`
// with no etcd service, so this must degrade to a skip rather than a
// failure when nothing is listening.
func dialTestEtcd(t *testing.T) *clientv3.Client {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Skipf("no etcd reachable at localhost:2379: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := cli.Get(ctx, "disco-rwlock-test-connectivity-check"); err != nil {
		_ = cli.Close()
		t.Skipf("no etcd reachable at localhost:2379: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

func newTestRWLock(t *testing.T, cli *clientv3.Client, key string, opts ...ProviderOption) rwlock.Service {
	t.Helper()
	svc, err := NewRWLock(cli, key, opts...)
	if err != nil {
		t.Fatalf("NewRWLock: %v", err)
	}
	t.Cleanup(func() { _ = svc.Close() })
	return svc
}

func TestRWLockMultipleReadersDoNotBlockEachOther(t *testing.T) {
	cli := dialTestEtcd(t)
	key := uniqueTestKey(t)

	r1 := newTestRWLock(t, cli, key, WithDefaultTTL(10*time.Second))
	r2 := newTestRWLock(t, cli, key, WithDefaultTTL(10*time.Second))

	g1, err := r1.RLock(context.Background())
	if err != nil {
		t.Fatalf("r1.RLock: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := r2.RLock(context.Background())
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("r2.RLock: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("r2.RLock blocked on a concurrent reader, but readers must not exclude each other")
	}

	if g1.FencingToken <= 0 {
		t.Fatalf("expected a positive fencing token, got %d", g1.FencingToken)
	}
	if err := r1.RUnlock(context.Background()); err != nil {
		t.Fatalf("r1.RUnlock: %v", err)
	}
	if err := r2.RUnlock(context.Background()); err != nil {
		t.Fatalf("r2.RUnlock: %v", err)
	}
}

func TestRWLockWriterExcludesReader(t *testing.T) {
	cli := dialTestEtcd(t)
	key := uniqueTestKey(t)

	writer := newTestRWLock(t, cli, key, WithDefaultTTL(10*time.Second))
	reader := newTestRWLock(t, cli, key, WithDefaultTTL(10*time.Second))

	if _, err := writer.Lock(context.Background()); err != nil {
		t.Fatalf("writer.Lock: %v", err)
	}

	rlocked := make(chan struct{})
	go func() {
		if _, err := reader.RLock(context.Background()); err != nil {
			t.Errorf("reader.RLock: %v", err)
		}
		close(rlocked)
	}()

	select {
	case <-rlocked:
		t.Fatalf("reader acquired RLock while a writer held Lock")
	case <-time.After(300 * time.Millisecond):
	}

	if err := writer.Unlock(context.Background()); err != nil {
		t.Fatalf("writer.Unlock: %v", err)
	}

	select {
	case <-rlocked:
	case <-time.After(3 * time.Second):
		t.Fatalf("reader never acquired RLock after the writer released")
	}
	_ = reader.RUnlock(context.Background())
}

func TestRWLockReaderExcludesWriter(t *testing.T) {
	cli := dialTestEtcd(t)
	key := uniqueTestKey(t)

	reader := newTestRWLock(t, cli, key, WithDefaultTTL(10*time.Second))
	writer := newTestRWLock(t, cli, key, WithDefaultTTL(10*time.Second))

	if _, err := reader.RLock(context.Background()); err != nil {
		t.Fatalf("reader.RLock: %v", err)
	}

	locked := make(chan struct{})
	go func() {
		if _, err := writer.Lock(context.Background()); err != nil {
			t.Errorf("writer.Lock: %v", err)
		}
		close(locked)
	}()

	select {
	case <-locked:
		t.Fatalf("writer acquired Lock while a reader held RLock")
	case <-time.After(300 * time.Millisecond):
	}

	if err := reader.RUnlock(context.Background()); err != nil {
		t.Fatalf("reader.RUnlock: %v", err)
	}

	select {
	case <-locked:
	case <-time.After(3 * time.Second):
		t.Fatalf("writer never acquired Lock after the reader released")
	}
	_ = writer.Unlock(context.Background())
}

func TestRWLockWriterWaitsForAllReaders(t *testing.T) {
	cli := dialTestEtcd(t)
	key := uniqueTestKey(t)

	r1 := newTestRWLock(t, cli, key, WithDefaultTTL(10*time.Second))
	r2 := newTestRWLock(t, cli, key, WithDefaultTTL(10*time.Second))
	writer := newTestRWLock(t, cli, key, WithDefaultTTL(10*time.Second))

	if _, err := r1.RLock(context.Background()); err != nil {
		t.Fatalf("r1.RLock: %v", err)
	}
	if _, err := r2.RLock(context.Background()); err != nil {
		t.Fatalf("r2.RLock: %v", err)
	}

	locked := make(chan struct{})
	go func() {
		if _, err := writer.Lock(context.Background()); err != nil {
			t.Errorf("writer.Lock: %v", err)
		}
		close(locked)
	}()

	select {
	case <-locked:
		t.Fatalf("writer acquired Lock while readers were still held")
	case <-time.After(300 * time.Millisecond):
	}

	if err := r1.RUnlock(context.Background()); err != nil {
		t.Fatalf("r1.RUnlock: %v", err)
	}

	select {
	case <-locked:
		t.Fatalf("writer acquired Lock while r2 still held RLock")
	case <-time.After(300 * time.Millisecond):
	}

	if err := r2.RUnlock(context.Background()); err != nil {
		t.Fatalf("r2.RUnlock: %v", err)
	}

	select {
	case <-locked:
	case <-time.After(3 * time.Second):
		t.Fatalf("writer never acquired Lock after every reader released")
	}
	_ = writer.Unlock(context.Background())
}

func TestRWLockDoneFiresOnLeaseLoss(t *testing.T) {
	cli := dialTestEtcd(t)
	key := uniqueTestKey(t)

	svc := newTestRWLock(t, cli, key, WithDefaultTTL(5*time.Second))
	if _, err := svc.Lock(context.Background()); err != nil {
		t.Fatalf("Lock: %v", err)
	}
	if err := svc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case <-svc.Done():
	case <-time.After(time.Second):
		t.Fatalf("expected Done to close after Close")
	}
	if !errors.Is(svc.Err(), rwlock.ErrRWLockLost) {
		t.Fatalf("expected ErrRWLockLost, got %v", svc.Err())
	}
}

func TestRWLockTryLockSucceedsWhenFree(t *testing.T) {
	cli := dialTestEtcd(t)
	key := uniqueTestKey(t)
	svc := newTestRWLock(t, cli, key, WithDefaultTTL(10*time.Second))

	grant, err := svc.TryLock(context.Background())
	if err != nil {
		t.Fatalf("TryLock: %v", err)
	}
	if grant.FencingToken <= 0 {
		t.Fatalf("expected a positive fencing token, got %d", grant.FencingToken)
	}
	_ = svc.Unlock(context.Background())
}

func TestRWLockTryRLockSucceedsWhenFree(t *testing.T) {
	cli := dialTestEtcd(t)
	key := uniqueTestKey(t)
	svc := newTestRWLock(t, cli, key, WithDefaultTTL(10*time.Second))

	if _, err := svc.TryRLock(context.Background()); err != nil {
		t.Fatalf("TryRLock: %v", err)
	}
	_ = svc.RUnlock(context.Background())
}

func TestRWLockTryLockFailsWhenWriterHolds(t *testing.T) {
	cli := dialTestEtcd(t)
	key := uniqueTestKey(t)
	writer := newTestRWLock(t, cli, key, WithDefaultTTL(10*time.Second))
	other := newTestRWLock(t, cli, key, WithDefaultTTL(10*time.Second))

	if _, err := writer.Lock(context.Background()); err != nil {
		t.Fatalf("writer.Lock: %v", err)
	}
	if _, err := other.TryLock(context.Background()); !errors.Is(err, rwlock.ErrRWLockTaken) {
		t.Fatalf("expected ErrRWLockTaken, got %v", err)
	}

	// The failed TryLock must have cleaned up its own candidate key, so a
	// later blocking Lock from the same service isn't left permanently
	// queued behind a phantom entry it already gave up on.
	_ = writer.Unlock(context.Background())
	if _, err := other.TryLock(context.Background()); err != nil {
		t.Fatalf("expected TryLock to succeed once the writer released, got %v", err)
	}
}

func TestRWLockTryLockFailsWhenReaderHolds(t *testing.T) {
	cli := dialTestEtcd(t)
	key := uniqueTestKey(t)
	reader := newTestRWLock(t, cli, key, WithDefaultTTL(10*time.Second))
	writer := newTestRWLock(t, cli, key, WithDefaultTTL(10*time.Second))

	if _, err := reader.RLock(context.Background()); err != nil {
		t.Fatalf("reader.RLock: %v", err)
	}
	if _, err := writer.TryLock(context.Background()); !errors.Is(err, rwlock.ErrRWLockTaken) {
		t.Fatalf("expected ErrRWLockTaken, got %v", err)
	}
}

func TestRWLockTryRLockFailsWhenWriterHolds(t *testing.T) {
	cli := dialTestEtcd(t)
	key := uniqueTestKey(t)
	writer := newTestRWLock(t, cli, key, WithDefaultTTL(10*time.Second))
	reader := newTestRWLock(t, cli, key, WithDefaultTTL(10*time.Second))

	if _, err := writer.Lock(context.Background()); err != nil {
		t.Fatalf("writer.Lock: %v", err)
	}
	if _, err := reader.TryRLock(context.Background()); !errors.Is(err, rwlock.ErrRWLockTaken) {
		t.Fatalf("expected ErrRWLockTaken, got %v", err)
	}
}

func TestRWLockTryRLockSucceedsWhileOtherReadersHold(t *testing.T) {
	cli := dialTestEtcd(t)
	key := uniqueTestKey(t)
	r1 := newTestRWLock(t, cli, key, WithDefaultTTL(10*time.Second))
	r2 := newTestRWLock(t, cli, key, WithDefaultTTL(10*time.Second))

	if _, err := r1.RLock(context.Background()); err != nil {
		t.Fatalf("r1.RLock: %v", err)
	}
	if _, err := r2.TryRLock(context.Background()); err != nil {
		t.Fatalf("expected TryRLock to succeed alongside another reader, got %v", err)
	}
}

// TestRWLockUnlockRejectsReadLock guards against a real bug found in
// review: RUnlock and Unlock used to both delegate to the same
// role-blind release, so calling Unlock after RLock (a caller mistake)
// silently succeeded instead of surfacing it.
func TestRWLockUnlockRejectsReadLock(t *testing.T) {
	cli := dialTestEtcd(t)
	key := uniqueTestKey(t)
	svc := newTestRWLock(t, cli, key, WithDefaultTTL(10*time.Second))

	if _, err := svc.RLock(context.Background()); err != nil {
		t.Fatalf("RLock: %v", err)
	}
	if err := svc.Unlock(context.Background()); err == nil {
		t.Fatalf("expected Unlock to reject a currently held read lock")
	}

	// The read lock must still be held — a second, independent reader is
	// fine, but a writer must still be excluded.
	other := newTestRWLock(t, cli, key, WithDefaultTTL(10*time.Second))
	if _, err := other.TryLock(context.Background()); !errors.Is(err, rwlock.ErrRWLockTaken) {
		t.Fatalf("expected the read lock to still be held after a rejected Unlock, got %v", err)
	}

	if err := svc.RUnlock(context.Background()); err != nil {
		t.Fatalf("RUnlock: %v", err)
	}
}

// TestRWLockRUnlockRejectsWriteLock is the mirror image of
// TestRWLockUnlockRejectsReadLock: RUnlock must reject releasing a
// currently held write lock.
func TestRWLockRUnlockRejectsWriteLock(t *testing.T) {
	cli := dialTestEtcd(t)
	key := uniqueTestKey(t)
	svc := newTestRWLock(t, cli, key, WithDefaultTTL(10*time.Second))

	if _, err := svc.Lock(context.Background()); err != nil {
		t.Fatalf("Lock: %v", err)
	}
	if err := svc.RUnlock(context.Background()); err == nil {
		t.Fatalf("expected RUnlock to reject a currently held write lock")
	}

	other := newTestRWLock(t, cli, key, WithDefaultTTL(10*time.Second))
	if _, err := other.TryRLock(context.Background()); !errors.Is(err, rwlock.ErrRWLockTaken) {
		t.Fatalf("expected the write lock to still be held after a rejected RUnlock, got %v", err)
	}

	if err := svc.Unlock(context.Background()); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
}

func uniqueTestKey(t *testing.T) string {
	t.Helper()
	return "/disco-test/rwlock/" + t.Name() + "/" + time.Now().Format("150405.000000000")
}
