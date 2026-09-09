package guard

import (
	"errors"
	"testing"

	"github.com/ahrtr/disco/fencing"
)

func TestCheckRejectsZeroToken(t *testing.T) {
	g := New()
	if err := g.Check(fencing.Zero); !errors.Is(err, fencing.ErrNoToken) {
		t.Fatalf("expected ErrNoToken for a zero token on a fresh Guard, got %v", err)
	}
	if g.HighWater() != 0 {
		t.Fatalf("expected a rejected zero token not to advance the high-water mark, got %d", g.HighWater())
	}
}

func TestCheckRejectsZeroTokenEvenAfterRealAcceptance(t *testing.T) {
	g := New()
	if err := g.Check(fencing.Token(5)); err != nil {
		t.Fatalf("Check(5): %v", err)
	}
	// A zero token must never be treated as valid, regardless of the
	// current high-water mark.
	if err := g.Check(fencing.Zero); !errors.Is(err, fencing.ErrNoToken) {
		t.Fatalf("expected ErrNoToken for a zero token, got %v", err)
	}
}

func TestCheckAcceptsAndAdvancesHighWater(t *testing.T) {
	g := New()
	if err := g.Check(fencing.Token(10)); err != nil {
		t.Fatalf("Check(10): %v", err)
	}
	if g.HighWater() != 10 {
		t.Fatalf("want high-water 10, got %d", g.HighWater())
	}
	// Re-accepting the same token is fine (t == cur).
	if err := g.Check(fencing.Token(10)); err != nil {
		t.Fatalf("Check(10) again: %v", err)
	}
}

func TestCheckRejectsStaleToken(t *testing.T) {
	g := New()
	if err := g.Check(fencing.Token(10)); err != nil {
		t.Fatalf("Check(10): %v", err)
	}
	if err := g.Check(fencing.Token(5)); !errors.Is(err, fencing.ErrTokenStale) {
		t.Fatalf("expected ErrTokenStale for a lower token, got %v", err)
	}
	if g.HighWater() != 10 {
		t.Fatalf("expected a rejected token not to move the high-water mark, got %d", g.HighWater())
	}
}

func TestWithInitialTokenSeedsHighWater(t *testing.T) {
	g := New(WithInitialToken(fencing.Token(42)))
	if g.HighWater() != 42 {
		t.Fatalf("want seeded high-water 42, got %d", g.HighWater())
	}
	if err := g.Check(fencing.Token(41)); !errors.Is(err, fencing.ErrTokenStale) {
		t.Fatalf("expected a token below the seeded mark to be rejected, got %v", err)
	}
}
