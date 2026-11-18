//go:build integration

package store

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

func TestCompleteSent(t *testing.T) {
	s, _ := newTestStore(t, zeroDelay)
	n := create(t, s)
	claim(t, s, n.ID)
	before := get(t, s, n.ID)
	r := AttemptResult{
		Outcome: domain.OutcomeSuccess, HTTPStatus: ptr(202), Latency: 37 * time.Millisecond,
		ProviderMessageID: ptr("pm-1"),
	}
	ev, err := s.CompleteSent(ctxT(t), n.ID, 1, "webhook", r)
	if err != nil {
		t.Fatal(err)
	}
	after := get(t, s, n.ID)
	checkTransition(t, before, after, ev, domain.StatusSent)
	if after.SentAt == nil || !after.SentAt.Equal(after.UpdatedAt) || after.LeaseExpiresAt != nil ||
		after.Provider == nil || *after.Provider != "webhook" ||
		after.ProviderMessageID == nil || *after.ProviderMessageID != "pm-1" {
		t.Errorf("row = %+v", after)
	}
	a := attempts(t, s, n.ID)
	if len(a) != 1 || a[0].Outcome != domain.OutcomeSuccess || a[0].FinishedAt == nil ||
		*a[0].HTTPStatus != 202 || *a[0].LatencyMS != 37 || *a[0].ProviderMessageID != "pm-1" {
		t.Errorf("attempt = %+v", a)
	}
}

func TestCompleteSent_RejectsNonSuccess(t *testing.T) {
	s, _ := newTestStore(t, zeroDelay)
	if _, err := s.CompleteSent(ctxT(t), uuid.New(), 1, "p", AttemptResult{Outcome: domain.OutcomeRetryable}); err == nil {
		t.Error("expected an error")
	}
	if _, err := s.CompleteFailure(ctxT(t), uuid.New(), 1, AttemptResult{Outcome: domain.OutcomeSuccess}, 0); err == nil {
		t.Error("expected an error")
	}
}

func TestCompleteFailure_Retry(t *testing.T) {
	s, _ := newTestStore(t, delayed)
	n := create(t, s)
	claim(t, s, n.ID)
	before := get(t, s, n.ID)
	r := AttemptResult{
		Outcome: domain.OutcomeRetryable, HTTPStatus: ptr(503), Latency: 12 * time.Millisecond,
		Error: ptr("service unavailable"),
	}
	ev, err := s.CompleteFailure(ctxT(t), n.ID, 1, r, 0.5)
	if err != nil {
		t.Fatal(err)
	}
	after := get(t, s, n.ID)
	checkTransition(t, before, after, ev, domain.StatusRetrying)
	if after.Failures != before.Failures+1 {
		t.Errorf("failures = %d, want %d", after.Failures, before.Failures+1)
	}
	if d := after.DueAt.Sub(after.UpdatedAt); d != testDelay {
		t.Errorf("due_at = now() + %s, want + %s", d, testDelay)
	}
	if after.LeaseExpiresAt != nil || after.QueuedAt != nil || after.FailureReason != nil {
		t.Errorf("row = %+v", after)
	}
	a := attempts(t, s, n.ID)
	if len(a) != 1 || a[0].Outcome != domain.OutcomeRetryable || a[0].FinishedAt == nil ||
		*a[0].HTTPStatus != 503 || *a[0].Error != "service unavailable" || *a[0].LatencyMS != 12 {
		t.Errorf("attempt = %+v", a)
	}
}

func TestCompleteFailure_Fail(t *testing.T) {
	t.Run("permanent", func(t *testing.T) {
		s, _ := newTestStore(t, delayed)
		n := create(t, s, withMaxFailures(1))
		claim(t, s, n.ID)
		before := get(t, s, n.ID)
		ev, err := s.CompleteFailure(ctxT(t), n.ID, 1, AttemptResult{Outcome: domain.OutcomePermanent, HTTPStatus: ptr(400)}, 0)
		if err != nil {
			t.Fatal(err)
		}
		after := get(t, s, n.ID)
		checkTransition(t, before, after, ev, domain.StatusFailed)
		if after.FailureReason == nil || *after.FailureReason != domain.ReasonMaxFailures ||
			after.Failures != 1 || after.LeaseExpiresAt != nil {
			t.Errorf("row = %+v", after)
		}
		if a := attempts(t, s, n.ID); a[0].Outcome != domain.OutcomePermanent || a[0].FinishedAt == nil {
			t.Errorf("attempt = %+v", a)
		}
	})

	t.Run("budget exhausted on the second attempt", func(t *testing.T) {
		s, _ := newTestStore(t, zeroDelay) // retried rows are due at once
		n := create(t, s, withMaxFailures(2))
		claim(t, s, n.ID)
		if ev, err := s.CompleteFailure(ctxT(t), n.ID, 1, AttemptResult{Outcome: domain.OutcomeRetryable}, 0); err != nil ||
			ev.Status != domain.StatusRetrying {
			t.Fatalf("first failure: %+v %v", ev, err)
		}
		if c := claim(t, s, n.ID); c.Notification.AttemptCount != 2 {
			t.Fatalf("attempt_count = %d", c.Notification.AttemptCount)
		}
		before := get(t, s, n.ID)
		ev, err := s.CompleteFailure(ctxT(t), n.ID, 2, AttemptResult{Outcome: domain.OutcomeRetryable}, 0)
		if err != nil {
			t.Fatal(err)
		}
		after := get(t, s, n.ID)
		checkTransition(t, before, after, ev, domain.StatusFailed)
		if *after.FailureReason != domain.ReasonMaxFailures || after.Failures != 2 {
			t.Errorf("reason %s failures %d", *after.FailureReason, after.Failures)
		}
		a := attempts(t, s, n.ID)
		if len(a) != 2 || a[0].Outcome != domain.OutcomeRetryable || a[1].Outcome != domain.OutcomeRetryable {
			t.Errorf("attempts = %+v", a)
		}
	})
}

// failures is incremented only when the decision consumes budget, and the
// decision receives the database now() of the transaction.
func TestCompleteFailure_BudgetFromDecision(t *testing.T) {
	cases := []struct {
		name     string
		decision domain.Decision
		want     domain.Status
		delta    int
	}{
		{"retry consuming", domain.Decision{Action: domain.ActionRetry, Delay: 250 * time.Millisecond, ConsumesBudget: true}, domain.StatusRetrying, 1},
		{"retry not consuming", domain.Decision{Action: domain.ActionRetry, Delay: 250 * time.Millisecond}, domain.StatusRetrying, 0},
		{"fail consuming", domain.Decision{Action: domain.ActionFail, Reason: domain.ReasonMaxFailures, ConsumesBudget: true}, domain.StatusFailed, 1},
		{"fail not consuming", domain.Decision{Action: domain.ActionFail, Reason: domain.ReasonProviderRejected}, domain.StatusFailed, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := newTestStore(t, delayed)
			var gotNow time.Time
			var gotArgs []any
			s.decide = func(outcome domain.Outcome, failures, maxFailures int, expiresAt time.Time,
				retryAfter time.Duration, profile domain.Profile, now time.Time, jitter float64,
			) domain.Decision {
				gotNow = now
				gotArgs = []any{outcome, failures, maxFailures, expiresAt, retryAfter, profile, jitter}
				return tc.decision
			}
			n := create(t, s)
			claim(t, s, n.ID)
			before := get(t, s, n.ID)
			r := AttemptResult{Outcome: domain.OutcomeThrottled, RetryAfter: 1500 * time.Millisecond}
			ev, err := s.CompleteFailure(ctxT(t), n.ID, 1, r, 0.25)
			if err != nil {
				t.Fatal(err)
			}
			after := get(t, s, n.ID)
			checkTransition(t, before, after, ev, tc.want)
			if after.Failures != before.Failures+tc.delta {
				t.Errorf("failures %d -> %d, want delta %d", before.Failures, after.Failures, tc.delta)
			}
			if !gotNow.Equal(after.UpdatedAt) {
				t.Errorf("decision now %v, transaction now() %v", gotNow, after.UpdatedAt)
			}
			wantArgs := []any{domain.OutcomeThrottled, 0, 5, before.ExpiresAt, 1500 * time.Millisecond, delayed[domain.PriorityNormal], 0.25}
			if !reflect.DeepEqual(gotArgs, wantArgs) {
				t.Errorf("decision args %v, want %v", gotArgs, wantArgs)
			}
			if tc.want == domain.StatusRetrying && after.DueAt.Sub(after.UpdatedAt) != tc.decision.Delay {
				t.Errorf("due_at - now() = %s", after.DueAt.Sub(after.UpdatedAt))
			}
			if tc.want == domain.StatusFailed && *after.FailureReason != tc.decision.Reason {
				t.Errorf("reason = %s", *after.FailureReason)
			}
		})
	}
}

// After the lease sweeper took the row and a second claim started attempt 2,
// the late worker of attempt 1 writes its result: zero rows on notifications
// (the row is unchanged), while attempt 1's row goes from unknown to the true
// outcome with its provider_message_id.
func TestFencing_LateWriteAffectsZeroRows(t *testing.T) {
	cases := []struct {
		name    string
		outcome domain.Outcome
		write   func(ctx context.Context, s *Store, id uuid.UUID, r AttemptResult) error
	}{
		{"success", domain.OutcomeSuccess, func(ctx context.Context, s *Store, id uuid.UUID, r AttemptResult) error {
			_, err := s.CompleteSent(ctx, id, 1, testProvider, r)
			return err
		}},
		{"failure", domain.OutcomeRetryable, func(ctx context.Context, s *Store, id uuid.UUID, r AttemptResult) error {
			_, err := s.CompleteFailure(ctx, id, 1, r, 0)
			return err
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, pool := newTestStore(t, zeroDelay)
			n := create(t, s)
			claim(t, s, n.ID) // attempt 1: the worker stalls
			expireLease(t, pool, n.ID)
			swept, err := s.SweepExpiredLeases(ctxT(t), 10, func() float64 { return 0 })
			if err != nil || len(swept) != 1 || swept[0].Status != domain.StatusRetrying {
				t.Fatalf("sweep: %+v %v", swept, err)
			}
			if c := claim(t, s, n.ID); c.Notification.AttemptCount != 2 { // attempt 2
				t.Fatalf("second claim: attempt_count %d", c.Notification.AttemptCount)
			}
			before := get(t, s, n.ID)
			if a := attempts(t, s, n.ID); a[0].Outcome != domain.OutcomeUnknown || a[1].Outcome != domain.OutcomeInFlight {
				t.Fatalf("setup attempts %+v", a)
			}

			r := AttemptResult{
				Outcome: tc.outcome, HTTPStatus: ptr(200), Latency: 25_000 * time.Millisecond,
				ProviderMessageID: ptr("late-pm-1"),
			}
			if err := tc.write(ctxT(t), s, n.ID, r); !errors.Is(err, ErrNotApplied) {
				t.Fatalf("late write: err = %v, want ErrNotApplied", err)
			}
			if after := get(t, s, n.ID); !reflect.DeepEqual(before, after) {
				t.Errorf("late write changed the row:\nbefore %+v\nafter  %+v", before, after)
			}
			a := attempts(t, s, n.ID)
			if len(a) != 2 {
				t.Fatalf("attempts %+v", a)
			}
			if a[0].Outcome != tc.outcome || a[0].ProviderMessageID == nil || *a[0].ProviderMessageID != "late-pm-1" ||
				a[0].FinishedAt == nil || *a[0].HTTPStatus != 200 {
				t.Errorf("attempt 1 = %+v, want the true outcome %s", a[0], tc.outcome)
			}
			if a[1].Outcome != domain.OutcomeInFlight || a[1].FinishedAt != nil {
				t.Errorf("attempt 2 touched: %+v", a[1])
			}
		})
	}
}
