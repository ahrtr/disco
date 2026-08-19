package fencing

import (
	"fmt"
	"net/http"
	"strconv"
)

// HTTPHeader is the canonical request header used to carry fencing tokens.
const HTTPHeader = "X-Fencing-Token"

// InjectHTTP sets the fencing token header on req.
func InjectHTTP(req *http.Request, t Token) {
	req.Header.Set(HTTPHeader, strconv.FormatInt(int64(t), 10))
}

// ExtractHTTP reads the fencing token from an incoming HTTP request.
// Returns ErrNoToken if the header is absent and a wrapped error if the value
// cannot be parsed as an int64.
func ExtractHTTP(r *http.Request) (Token, error) {
	v := r.Header.Get(HTTPHeader)
	if v == "" {
		return Zero, ErrNoToken
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return Zero, fmt.Errorf("fencing: invalid token %q: %w", v, err)
	}
	return Token(n), nil
}
