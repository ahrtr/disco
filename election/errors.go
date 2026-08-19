package election

import "errors"

var (
	// ErrElectionNotLeader is returned by Proclaim when the caller does not
	// currently hold leadership.
	ErrElectionNotLeader = errors.New("election: not leader")

	// ErrElectionNoLeader is returned by Leader when no leader is currently elected.
	ErrElectionNoLeader = errors.New("election: no leader")

	// ErrElectionLost is returned when a held lease expires or is otherwise
	// lost, causing any held leadership to be relinquished.
	ErrElectionLost = errors.New("election: lease expired or lost")
)
