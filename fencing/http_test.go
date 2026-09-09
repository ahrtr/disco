package fencing

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestExtractHTTPMissingHeaderIsErrNoToken(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	if _, err := ExtractHTTP(req); !errors.Is(err, ErrNoToken) {
		t.Fatalf("expected ErrNoToken for a missing header, got %v", err)
	}
}

// TestExtractHTTPLiteralZeroIsErrNoToken guards against a real bug found in
// review: a request carrying a literal "0" header parsed successfully as
// Token(0) instead of being rejected, even though Zero's doc explicitly
// says no backend ever issues it and it should be treated as no token —
// letting a client that never actually held a lock pass an unseeded
// Guard.
func TestExtractHTTPLiteralZeroIsErrNoToken(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set(HTTPHeader, "0")
	if _, err := ExtractHTTP(req); !errors.Is(err, ErrNoToken) {
		t.Fatalf("expected ErrNoToken for a literal zero token, got %v", err)
	}
}

func TestExtractHTTPInvalidValue(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set(HTTPHeader, "not-a-number")
	if _, err := ExtractHTTP(req); err == nil || errors.Is(err, ErrNoToken) {
		t.Fatalf("expected a parse error (not ErrNoToken), got %v", err)
	}
}

func TestInjectExtractHTTPRoundTrip(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	InjectHTTP(req, Token(42))
	got, err := ExtractHTTP(req)
	if err != nil {
		t.Fatalf("ExtractHTTP: %v", err)
	}
	if got != 42 {
		t.Fatalf("want token 42, got %d", got)
	}
}
