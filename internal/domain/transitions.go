package domain

// Transition is one arrow of the state machine in ADR-0004 section 3.
type Transition struct {
	Number   int
	From, To Status
	Actor    string
}

var transitions = []Transition{
	{1, StatusPending, StatusQueued, "relay"},
	{2, StatusRetrying, StatusQueued, "relay"},
	{3, StatusQueued, StatusProcessing, "worker"},
	{3, StatusPending, StatusProcessing, "worker"},
	{3, StatusRetrying, StatusProcessing, "worker"},
	{3, StatusPending, StatusFailed, "worker"}, // expired at claim
	{3, StatusRetrying, StatusFailed, "worker"},
	{3, StatusQueued, StatusFailed, "worker"},
	{4, StatusProcessing, StatusSent, "worker"},
	{5, StatusProcessing, StatusRetrying, "worker"},
	{6, StatusProcessing, StatusFailed, "worker"},
	{7, StatusProcessing, StatusRetrying, "sweeper"},
	{7, StatusProcessing, StatusFailed, "sweeper"},
	{8, StatusQueued, StatusPending, "relay"},
	{9, StatusPending, StatusCancelled, "api"},
	{9, StatusRetrying, StatusCancelled, "api"},
	{9, StatusQueued, StatusCancelled, "api"},
	{10, StatusPending, StatusFailed, "relay"},
	{10, StatusRetrying, StatusFailed, "relay"},
	{10, StatusQueued, StatusFailed, "relay"},
	{11, StatusFailed, StatusPending, "api"},
	{12, StatusPending, StatusSent, "worker"},
	{12, StatusQueued, StatusSent, "worker"},
	{12, StatusRetrying, StatusSent, "worker"},
	{12, StatusFailed, StatusSent, "worker"},
	{12, StatusCancelled, StatusSent, "worker"},
}

// Transitions returns one entry per (number, from, to) arrow of the ADR's table.
func Transitions() []Transition {
	return append([]Transition(nil), transitions...)
}

// CanTransition reports whether an arrow from -> to exists.
func CanTransition(from, to Status) bool {
	for _, t := range transitions {
		if t.From == from && t.To == to {
			return true
		}
	}
	return false
}
