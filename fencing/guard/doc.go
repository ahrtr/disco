// Package guard provides server-side fencing token validation for protected
// resources such as databases and external APIs.
//
// A Guard tracks the highest fencing token it has ever accepted. Any request
// carrying a token lower than that high-water mark is rejected with
// fencing.ErrTokenStale, which prevents stale owners (zombie lock holders or
// zombie leaders) from corrupting shared state even after their lease has
// expired.
//
// Guard is safe for concurrent use and can be shared across goroutines.
package guard
