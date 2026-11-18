package domain

import "testing"

func TestCanTransitionAllPairs(t *testing.T) {
	// The arrows of the ADR-0004 section 3 state diagram, written out literally.
	allowed := map[[2]Status]bool{
		{StatusPending, StatusQueued}:      true,
		{StatusRetrying, StatusQueued}:     true,
		{StatusQueued, StatusProcessing}:   true,
		{StatusPending, StatusProcessing}:  true,
		{StatusRetrying, StatusProcessing}: true,
		{StatusProcessing, StatusSent}:     true,
		{StatusProcessing, StatusRetrying}: true,
		{StatusProcessing, StatusFailed}:   true,
		{StatusQueued, StatusPending}:      true,
		{StatusPending, StatusCancelled}:   true,
		{StatusRetrying, StatusCancelled}:  true,
		{StatusQueued, StatusCancelled}:    true,
		{StatusPending, StatusFailed}:      true,
		{StatusRetrying, StatusFailed}:     true,
		{StatusQueued, StatusFailed}:       true,
		{StatusFailed, StatusPending}:      true,
		{StatusPending, StatusSent}:        true,
		{StatusQueued, StatusSent}:         true,
		{StatusRetrying, StatusSent}:       true,
		{StatusFailed, StatusSent}:         true,
		{StatusCancelled, StatusSent}:      true,
	}
	for _, from := range Statuses() {
		for _, to := range Statuses() {
			want := allowed[[2]Status{from, to}]
			if got := CanTransition(from, to); got != want {
				t.Errorf("CanTransition(%s, %s) = %v, want %v", from, to, got, want)
			}
			if from == StatusSent && CanTransition(from, to) {
				t.Errorf("sent must be terminal, but sent -> %s allowed", to)
			}
			if to == StatusProcessing && CanTransition(from, to) {
				if from != StatusPending && from != StatusQueued && from != StatusRetrying {
					t.Errorf("%s -> processing must not be allowed", from)
				}
			}
		}
	}
}

func TestTransitionsData(t *testing.T) {
	for _, tr := range Transitions() {
		if tr.Number < 1 || tr.Number > 12 || tr.Actor == "" || !CanTransition(tr.From, tr.To) {
			t.Errorf("bad transition %+v", tr)
		}
		if tr.To == StatusProcessing && tr.Number != 3 {
			t.Errorf("only the claim (3) may enter processing: %+v", tr)
		}
	}
}
