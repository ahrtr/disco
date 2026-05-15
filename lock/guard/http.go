package guard

import (
	"net/http"

	"github.com/ahrtr/disco/lock/fencing"
)

// HTTPMiddleware is an HTTP middleware that extracts the fencing token from
// every incoming request and validates it against the Guard before calling next.
//
// Requests with a missing or malformed X-Fencing-Token header are rejected
// with 400 Bad Request. Requests carrying a stale token are rejected with
// 409 Conflict.
//
// Use it directly as a handler wrapper:
//
//	http.Handle("/write", g.HTTPMiddleware(writeHandler))
//
// Or pass the method value to a middleware chain (it already has the right
// signature func(http.Handler) http.Handler):
//
//	chain(g.HTTPMiddleware, otherMiddleware, finalHandler)
func (g *Guard) HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, err := fencing.ExtractHTTP(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := g.Check(token); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		next.ServeHTTP(w, r)
	})
}
