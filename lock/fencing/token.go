package fencing

import "errors"

// ErrNoToken is returned when a request carries no fencing token.
var ErrNoToken = errors.New("fencing: no token present in request")

// ErrTokenStale is returned by a Guard when the incoming token is lower than
// the highest token the resource has already accepted.
var ErrTokenStale = errors.New("fencing: token is stale")

// Token is a monotonically increasing integer that identifies a lock
// generation. Every new lock owner receives a strictly higher token than any
// previous owner, so resources can safely reject requests from zombie clients
// by comparing tokens.
type Token int64

// Zero is the zero value of Token. A zero token is never issued by a lock
// backend and should be treated as "no token".
const Zero Token = 0
