//go:build integration

package store

import (
	"context"
	"sync"
	"testing"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

// Two api calls on the same notification at the same time (the cell
// api × api of the actor-pair matrix, ADR-0010 §1; TestCancel_Idempotent is
// the same pair one after the other): of several concurrent cancels of one
// pending notification exactly one applies the transition and returns its
// event; every other one is answered as a repeated cancel, without an error
// and without an event. The row's version rises by one, so a subscriber sees
// one cancelled event however many requests arrived.
func TestCancel_ConcurrentSameNotification(t *testing.T) {
	const (
		rounds  = 25
		callers = 8
	)
	s, _ := newTestStore(t, delayed)
	for round := range rounds {
		n := create(t, s)
		start := make(chan struct{})
		results := make([]cancelResult, callers)
		var wg sync.WaitGroup
		for i := range callers {
			wg.Go(func() {
				<-start
				r, ev, err := s.CancelNotification(context.Background(), n.ID)
				results[i] = cancelResult{r, ev, err}
			})
		}
		close(start)
		wg.Wait()

		applied := 0
		for i, c := range results {
			if c.err != nil {
				t.Fatalf("round %d, cancel %d: %v", round, i, c.err)
			}
			if c.res.MayHaveBeenSent {
				t.Errorf("round %d, cancel %d: may_have_been_sent for a notification that was never attempted", round, i)
			}
			if !c.res.Applied {
				if c.event != (domain.StatusChanged{}) {
					t.Errorf("round %d, cancel %d: not applied, but it returned the event %+v", round, i, c.event)
				}
				continue
			}
			applied++
			if c.event.Status != domain.StatusCancelled || c.event.Version != n.Version+1 {
				t.Errorf("round %d, cancel %d: event %+v, want cancelled at version %d", round, i, c.event, n.Version+1)
			}
		}
		if applied != 1 {
			t.Fatalf("round %d: %d of %d concurrent cancels applied the transition, want exactly 1", round, applied, callers)
		}
		if got := get(t, s, n.ID); got.Status != domain.StatusCancelled || got.Version != n.Version+1 {
			t.Fatalf("round %d: row %s at version %d, want cancelled at version %d", round, got.Status, got.Version, n.Version+1)
		}
	}
}
