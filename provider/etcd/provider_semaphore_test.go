package etcd

import (
	"context"
	"errors"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	semaphoreapi "github.com/ahrtr/disco/semaphore"
)

func newTestSemaphore(t *testing.T, cli *clientv3.Client, key string, limit int, opts ...ProviderOption) semaphoreapi.Service {
	t.Helper()
	svc, err := NewSemaphore(cli, key, limit, opts...)
	if err != nil {
		t.Fatalf("NewSemaphore: %v", err)
	}
	t.Cleanup(func() { _ = svc.Close() })
	return svc
}

func TestSemaphoreLimitEnforced(t *testing.T) {
	cli := dialTestEtcd(t)
	key := uniqueTestKey(t)

	a := newTestSemaphore(t, cli, key, 2, WithDefaultTTL(10*time.Second))
	b := newTestSemaphore(t, cli, key, 2, WithDefaultTTL(10*time.Second))
	c := newTestSemaphore(t, cli, key, 2, WithDefaultTTL(10*time.Second))

	if _, err := a.Acquire(context.Background()); err != nil {
		t.Fatalf("a.Acquire: %v", err)
	}
	if _, err := b.Acquire(context.Background()); err != nil {
		t.Fatalf("b.Acquire: %v", err)
	}

	cAcquired := make(chan struct{})
	go func() {
		if _, err := c.Acquire(context.Background()); err != nil {
			t.Errorf("c.Acquire: %v", err)
		}
		close(cAcquired)
	}()

	select {
	case <-cAcquired:
		t.Fatalf("c acquired a permit while both existing permits were still held")
	case <-time.After(300 * time.Millisecond):
	}

	if err := a.Release(context.Background()); err != nil {
		t.Fatalf("a.Release: %v", err)
	}

	select {
	case <-cAcquired:
	case <-time.After(3 * time.Second):
		t.Fatalf("c never acquired a permit after a released")
	}
	_ = b.Release(context.Background())
	_ = c.Release(context.Background())
}

func TestSemaphoreTryAcquireFailsWhenFull(t *testing.T) {
	cli := dialTestEtcd(t)
	key := uniqueTestKey(t)

	a := newTestSemaphore(t, cli, key, 1, WithDefaultTTL(10*time.Second))
	b := newTestSemaphore(t, cli, key, 1, WithDefaultTTL(10*time.Second))

	if _, err := a.Acquire(context.Background()); err != nil {
		t.Fatalf("a.Acquire: %v", err)
	}
	if _, err := b.TryAcquire(context.Background()); !errors.Is(err, semaphoreapi.ErrNoPermitsAvailable) {
		t.Fatalf("expected ErrNoPermitsAvailable, got %v", err)
	}

	// The failed TryAcquire must have cleaned up its own candidate key, so a
	// later acquire from the same service isn't left permanently queued
	// behind a phantom entry it already gave up on.
	_ = a.Release(context.Background())
	if _, err := b.TryAcquire(context.Background()); err != nil {
		t.Fatalf("expected TryAcquire to succeed once a released, got %v", err)
	}
}

func TestSemaphoreTryAcquireSucceedsWhenFree(t *testing.T) {
	cli := dialTestEtcd(t)
	key := uniqueTestKey(t)
	svc := newTestSemaphore(t, cli, key, 2, WithDefaultTTL(10*time.Second))

	grant, err := svc.TryAcquire(context.Background())
	if err != nil {
		t.Fatalf("TryAcquire: %v", err)
	}
	if grant.FencingToken <= 0 {
		t.Fatalf("expected a positive fencing token, got %d", grant.FencingToken)
	}
	_ = svc.Release(context.Background())
}

func TestSemaphoreReleaseUnblocksWaiter(t *testing.T) {
	cli := dialTestEtcd(t)
	key := uniqueTestKey(t)

	a := newTestSemaphore(t, cli, key, 1, WithDefaultTTL(10*time.Second))
	b := newTestSemaphore(t, cli, key, 1, WithDefaultTTL(10*time.Second))

	if _, err := a.Acquire(context.Background()); err != nil {
		t.Fatalf("a.Acquire: %v", err)
	}

	bAcquired := make(chan struct{})
	go func() {
		if _, err := b.Acquire(context.Background()); err != nil {
			t.Errorf("b.Acquire: %v", err)
		}
		close(bAcquired)
	}()

	select {
	case <-bAcquired:
		t.Fatalf("b acquired a permit while a still held the only one")
	case <-time.After(300 * time.Millisecond):
	}

	if err := a.Release(context.Background()); err != nil {
		t.Fatalf("a.Release: %v", err)
	}

	select {
	case <-bAcquired:
	case <-time.After(3 * time.Second):
		t.Fatalf("b never acquired a permit after a released")
	}
	_ = b.Release(context.Background())
}

func TestSemaphoreStatReflectsHeldCount(t *testing.T) {
	cli := dialTestEtcd(t)
	key := uniqueTestKey(t)

	a := newTestSemaphore(t, cli, key, 3, WithDefaultTTL(10*time.Second))
	b := newTestSemaphore(t, cli, key, 3, WithDefaultTTL(10*time.Second))

	stat, err := a.Stat(context.Background())
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if stat.Registered != 0 || stat.Held() != 0 || stat.Limit != 3 {
		t.Fatalf("expected {Registered:0 Held:0 Limit:3}, got %+v", stat)
	}

	if _, err := a.Acquire(context.Background()); err != nil {
		t.Fatalf("a.Acquire: %v", err)
	}
	if _, err := b.Acquire(context.Background()); err != nil {
		t.Fatalf("b.Acquire: %v", err)
	}

	stat, err = a.Stat(context.Background())
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if stat.Registered != 2 || stat.Held() != 2 || stat.Limit != 3 {
		t.Fatalf("expected {Registered:2 Held:2 Limit:3}, got %+v", stat)
	}

	_ = a.Release(context.Background())
	_ = b.Release(context.Background())
}

func TestSemaphoreStatCountsWaitersSeparatelyFromHeld(t *testing.T) {
	cli := dialTestEtcd(t)
	key := uniqueTestKey(t)

	a := newTestSemaphore(t, cli, key, 1, WithDefaultTTL(10*time.Second))
	b := newTestSemaphore(t, cli, key, 1, WithDefaultTTL(10*time.Second))

	if _, err := a.Acquire(context.Background()); err != nil {
		t.Fatalf("a.Acquire: %v", err)
	}

	bAcquired := make(chan struct{})
	go func() {
		if _, err := b.Acquire(context.Background()); err != nil {
			t.Errorf("b.Acquire: %v", err)
		}
		close(bAcquired)
	}()

	select {
	case <-bAcquired:
		t.Fatalf("b acquired a permit while a still held the only one")
	case <-time.After(300 * time.Millisecond):
	}

	// b is registered as a waiter but has not been granted the permit, so
	// Registered must count it while Held stays capped at Limit.
	stat, err := a.Stat(context.Background())
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if stat.Registered != 2 || stat.Held() != 1 || stat.Limit != 1 {
		t.Fatalf("expected {Registered:2 Held:1 Limit:1}, got %+v", stat)
	}

	_ = a.Release(context.Background())
	select {
	case <-bAcquired:
	case <-time.After(3 * time.Second):
		t.Fatalf("b never acquired a permit after a released")
	}
	_ = b.Release(context.Background())
}

func TestSemaphoreDoneFiresOnLeaseLoss(t *testing.T) {
	cli := dialTestEtcd(t)
	key := uniqueTestKey(t)

	svc := newTestSemaphore(t, cli, key, 1, WithDefaultTTL(5*time.Second))
	if _, err := svc.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if err := svc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case <-svc.Done():
	case <-time.After(time.Second):
		t.Fatalf("expected Done to close after Close")
	}
	if !errors.Is(svc.Err(), semaphoreapi.ErrSemaphoreLost) {
		t.Fatalf("expected ErrSemaphoreLost, got %v", svc.Err())
	}
}

func TestSemaphoreNewSemaphoreRejectsInvalidLimit(t *testing.T) {
	cli := dialTestEtcd(t)
	key := uniqueTestKey(t)

	if _, err := NewSemaphore(cli, key, 0); err == nil {
		t.Fatalf("expected NewSemaphore to reject limit 0")
	}
}
