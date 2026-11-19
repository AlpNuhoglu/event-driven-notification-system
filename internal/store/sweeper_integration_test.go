//go:build integration

package store

import (
	"reflect"
	"testing"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

func zeroJitter() float64 { return 0 }

func TestSweepExpiredLeases_Retry(t *testing.T) {
	s, pool := newTestStore(t, delayed)
	n := create(t, s)
	claim(t, s, n.ID)
	expireLease(t, pool, n.ID)
	before := get(t, s, n.ID)

	events, err := s.SweepExpiredLeases(ctxT(t), 10, zeroJitter)
	if err != nil || len(events) != 1 {
		t.Fatalf("events %+v err %v", events, err)
	}
	after := get(t, s, n.ID)
	checkTransition(t, before, after, events[0], domain.StatusRetrying)
	if after.Failures != before.Failures+1 {
		t.Errorf("failures = %d, want %d", after.Failures, before.Failures+1)
	}
	if d := after.DueAt.Sub(after.UpdatedAt); d != testDelay {
		t.Errorf("due_at = now() + %s, want a future due_at at now() + %s", d, testDelay)
	}
	if after.LeaseExpiresAt != nil || after.AttemptCount != before.AttemptCount {
		t.Errorf("row = %+v", after)
	}
	a := attempts(t, s, n.ID)
	if len(a) != 1 || a[0].Outcome != domain.OutcomeUnknown || a[0].FinishedAt == nil || !a[0].FinishedAt.Equal(after.UpdatedAt) {
		t.Errorf("attempt = %+v", a)
	}
}

func TestSweepExpiredLeases_FailWhenBudgetEnds(t *testing.T) {
	s, pool := newTestStore(t, delayed)
	n := create(t, s, withMaxFailures(1))
	claim(t, s, n.ID)
	expireLease(t, pool, n.ID)
	before := get(t, s, n.ID)

	events, err := s.SweepExpiredLeases(ctxT(t), 10, zeroJitter)
	if err != nil || len(events) != 1 {
		t.Fatalf("events %+v err %v", events, err)
	}
	after := get(t, s, n.ID)
	checkTransition(t, before, after, events[0], domain.StatusFailed)
	if after.FailureReason == nil || *after.FailureReason != domain.ReasonMaxFailuresUnknownOutcome || after.Failures != 1 {
		t.Errorf("reason %v failures %d", after.FailureReason, after.Failures)
	}
	if a := attempts(t, s, n.ID); a[0].Outcome != domain.OutcomeUnknown || a[0].FinishedAt == nil {
		t.Errorf("attempt = %+v", a)
	}
}

func TestSweepExpiredLeases_UnexpiredLeasesUntouched(t *testing.T) {
	s, _ := newTestStore(t, delayed)
	n := create(t, s)
	claim(t, s, n.ID)
	before := get(t, s, n.ID)
	events, err := s.SweepExpiredLeases(ctxT(t), 10, zeroJitter)
	if err != nil || len(events) != 0 {
		t.Fatalf("events %+v err %v", events, err)
	}
	if after := get(t, s, n.ID); !reflect.DeepEqual(before, after) {
		t.Error("row with an unexpired lease changed")
	}
	if a := attempts(t, s, n.ID); a[0].Outcome != domain.OutcomeInFlight || a[0].FinishedAt != nil {
		t.Errorf("attempt = %+v", a)
	}
}

// Two sweeps running at the same time never take the same row: the second
// sweep runs while the first holds its row locks (from inside its jitter
// callback, which is called within the first sweep's transaction), and
// skips them. Every swept attempt is unknown with finished_at set.
func TestLeaseSweeper_TwoSweepersSkipLocked(t *testing.T) {
	s, pool := newTestStore(t, delayed)
	all := map[uuid.UUID]bool{}
	for range 6 {
		n := create(t, s)
		claim(t, s, n.ID)
		expireLease(t, pool, n.ID)
		all[n.ID] = true
	}
	var second []domain.StatusChanged
	calls := 0
	first, err := s.SweepExpiredLeases(ctxT(t), 3, func() float64 {
		calls++
		if calls == 1 {
			var err error
			second, err = s.SweepExpiredLeases(ctxT(t), 10, zeroJitter)
			if err != nil {
				t.Errorf("second sweep: %v", err)
			}
		}
		return 0
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 3 || len(second) != 3 {
		t.Fatalf("first sweep %d rows, second %d rows; want 3 and 3", len(first), len(second))
	}
	seen := map[uuid.UUID]bool{}
	for _, e := range append(first, second...) {
		if seen[e.ID] || !all[e.ID] {
			t.Errorf("row %s swept twice or unknown", e.ID)
		}
		seen[e.ID] = true
		got := get(t, s, e.ID)
		if got.Failures != 1 || got.Status != domain.StatusRetrying {
			t.Errorf("row %s: %s failures %d", e.ID, got.Status, got.Failures)
		}
		a := attempts(t, s, e.ID)
		if len(a) != 1 || a[0].Outcome != domain.OutcomeUnknown || a[0].FinishedAt == nil ||
			!a[0].FinishedAt.Equal(got.UpdatedAt) {
			t.Errorf("row %s: attempt %+v, want unknown with finished_at = the sweep's now()", e.ID, a)
		}
	}
}

func TestSweepExpiredLeases_Limit(t *testing.T) {
	s, pool := newTestStore(t, delayed)
	for range 4 {
		n := create(t, s)
		claim(t, s, n.ID)
		expireLease(t, pool, n.ID)
	}
	events, err := s.SweepExpiredLeases(ctxT(t), 3, zeroJitter)
	if err != nil || len(events) != 3 {
		t.Fatalf("events %d err %v", len(events), err)
	}
	events, err = s.SweepExpiredLeases(ctxT(t), 3, zeroJitter)
	if err != nil || len(events) != 1 {
		t.Fatalf("events %d err %v", len(events), err)
	}
}
