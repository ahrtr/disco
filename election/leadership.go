package election

import "github.com/ahrtr/disco/fencing"

// Leadership represents the metadata for the current leader of an election.
//
// The fencing token carried by a Leadership must be attached to every request
// sent to a resource the leader guards (database write, external API call,
// etc.) so that the resource can reject requests from a stale (zombie)
// leader whose lease has already expired. Detecting lease loss via Done/Err
// is necessarily best-effort — there is an unavoidable delay between the
// lease actually expiring and the process noticing — so the token, not
// self-detection, is what makes it safe.
// Use leadership.Token() to obtain a fencing.Token and pass it to the
// helpers in the fencing package (fencing.InjectHTTP, fencing.ToGRPCMetadata,
// etc.).
type Leadership struct {
	// Key is the backend key that won the current election term.
	Key string

	// Value is the leader-proclaimed value associated with Key.
	Value string

	// FencingToken is the creation revision of Key, a monotonically increasing
	// value that orders successive election terms on the same prefix and
	// serves as the fencing token for this leadership term. It must be
	// attached to every request sent to a resource this leadership guards.
	FencingToken int64
}

// Token returns FencingToken as a fencing.Token, ready to pass to
// fencing.InjectHTTP, fencing.ToGRPCMetadata, or a resource's Check method.
func (l *Leadership) Token() fencing.Token {
	return fencing.Token(l.FencingToken)
}
