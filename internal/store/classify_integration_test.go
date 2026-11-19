//go:build integration

package store

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

// The classification of ADR-0007 section 2 through the store's transaction:
// CompleteFailure with every failure outcome against real Postgres.

// Durations of the profiles below, in milliseconds. They are chosen so that
// the back-off, the hint, the throttle default and the cap all differ.
const (
	classBackoff  = 40_000 * time.Millisecond // Base = Cap: the back-off is jitter x this
	classThrottle = 90_000 * time.Millisecond // ThrottleDefault
	classHintCap  = 120_000 * time.Millisecond
	classJitter   = 0.5
	classDelay    = 20_000 * time.Millisecond // classJitter x classBackoff
)

// classProfiles has a back-off of jitter x 40 s, a floor of 1 ms, a throttle
// default of 90 s and a Retry-After cap of 120 s, for every priority.
func classProfiles() Profiles {
	p := domain.Profile{
		Base: classBackoff, Cap: classBackoff, MinDelay: time.Millisecond,
		RetryAfterCap: classHintCap, ThrottleDefault: classThrottle,
	}
	return Profiles{domain.PriorityHigh: p, domain.PriorityNormal: p, domain.PriorityLow: p}
}

// failureResult is the attempt result of a failure with outcome o, carrying
// an HTTP status and an error text where the outcome has them.
func failureResult(o domain.Outcome) AttemptResult {
	r := AttemptResult{Outcome: o, Latency: 21 * time.Millisecond, Error: ptr("provider said: " + string(o))}
	status := map[domain.Outcome]int{
		domain.OutcomeRetryable:     503,
		domain.OutcomePermanent:     400,
		domain.OutcomeProviderFatal: 404,
		domain.OutcomeThrottled:     429,
	}
	if s, ok := status[o]; ok {
		r.HTTPStatus = ptr(s)
	}
	return r
}

// checkAttempt asserts that the attempt row holds the true outcome, status
// and error of r, and is finished.
func checkAttempt(t *testing.T, a Attempt, r AttemptResult) {
	t.Helper()
	if a.Outcome != r.Outcome || a.FinishedAt == nil {
		t.Errorf("attempt %d: outcome %s finished %v, want %s and finished", a.AttemptNo, a.Outcome, a.FinishedAt != nil, r.Outcome)
	}
	if (a.HTTPStatus == nil) != (r.HTTPStatus == nil) || (a.HTTPStatus != nil && *a.HTTPStatus != *r.HTTPStatus) {
		t.Errorf("attempt %d: http_status %v, want %v", a.AttemptNo, deref(a.HTTPStatus), deref(r.HTTPStatus))
	}
	if (a.Error == nil) != (r.Error == nil) || (a.Error != nil && *a.Error != *r.Error) {
		t.Errorf("attempt %d: error %v, want %v", a.AttemptNo, deref(a.Error), deref(r.Error))
	}
	if a.LatencyMS == nil || *a.LatencyMS != int(r.Latency.Milliseconds()) {
		t.Errorf("attempt %d: latency_ms %v, want %d", a.AttemptNo, deref(a.LatencyMS), r.Latency.Milliseconds())
	}
}

func deref[T any](p *T) any {
	if p == nil {
		return nil
	}
	return *p
}

// One failure of each outcome on a fresh row (failures 0, max_failures 5):
// the provider-scoped outcomes leave failures unchanged, the message-scoped
// ones that consume budget increment it, and permanent fails at once without
// consuming it. The retry delay is the back-off, except for throttled, which
// gets the throttle default.
func TestCompleteFailure_EveryOutcome(t *testing.T) {
	cases := []struct {
		outcome  domain.Outcome
		status   domain.Status
		failures int
		reason   domain.FailureReason // when failed
		delay    time.Duration        // when retrying
	}{
		{domain.OutcomeProviderFatal, domain.StatusRetrying, 0, "", classDelay},
		{domain.OutcomeThrottled, domain.StatusRetrying, 0, "", classThrottle},
		{domain.OutcomeUnreachable, domain.StatusRetrying, 0, "", classDelay},
		{domain.OutcomeRetryable, domain.StatusRetrying, 1, "", classDelay},
		{domain.OutcomeUnknown, domain.StatusRetrying, 1, "", classDelay},
		{domain.OutcomeInternalError, domain.StatusRetrying, 1, "", classDelay},
		{domain.OutcomePermanent, domain.StatusFailed, 0, domain.ReasonProviderRejected, 0},
	}
	s, _ := newTestStore(t, classProfiles())
	for _, tc := range cases {
		t.Run(string(tc.outcome), func(t *testing.T) {
			n := create(t, s)
			claim(t, s, n.ID)
			before := get(t, s, n.ID)
			r := failureResult(tc.outcome)
			ev, err := s.CompleteFailure(ctxT(t), n.ID, 1, r, classJitter)
			if err != nil {
				t.Fatal(err)
			}
			after := get(t, s, n.ID)
			checkTransition(t, before, after, ev, tc.status)
			if after.Failures != tc.failures {
				t.Errorf("failures = %d, want %d", after.Failures, tc.failures)
			}
			switch tc.status {
			case domain.StatusRetrying:
				if after.FailureReason != nil {
					t.Errorf("failure_reason = %s on a retrying row", *after.FailureReason)
				}
				if d := after.DueAt.Sub(after.UpdatedAt); d != tc.delay {
					t.Errorf("due_at = now() + %s, want + %s", d, tc.delay)
				}
			case domain.StatusFailed:
				if after.FailureReason == nil || *after.FailureReason != tc.reason {
					t.Errorf("failure_reason = %v, want %s", deref(after.FailureReason), tc.reason)
				}
			}
			a := attempts(t, s, n.ID)
			if len(a) != 1 {
				t.Fatalf("%d attempts, want 1", len(a))
			}
			checkAttempt(t, a[0], r)
		})
	}
}

// The failure that exhausts the budget, for each consuming outcome: with
// max_failures 2, the first failure retries and the second fails with the
// outcome's reason; the row shows failures = max_failures. Every attempt row
// holds its true outcome.
func TestCompleteFailure_BudgetExhaustedPerOutcome(t *testing.T) {
	cases := []struct {
		outcome domain.Outcome
		reason  domain.FailureReason
	}{
		{domain.OutcomeRetryable, domain.ReasonMaxFailures},
		{domain.OutcomeUnknown, domain.ReasonMaxFailuresUnknownOutcome},
		{domain.OutcomeInternalError, domain.ReasonInternalError},
	}
	s, _ := newTestStore(t, zeroDelay) // retried rows are due at once
	for _, tc := range cases {
		t.Run(string(tc.outcome), func(t *testing.T) {
			n := create(t, s, withMaxFailures(2))
			r := failureResult(tc.outcome)

			claim(t, s, n.ID)
			if ev, err := s.CompleteFailure(ctxT(t), n.ID, 1, r, classJitter); err != nil || ev.Status != domain.StatusRetrying {
				t.Fatalf("first failure: %+v %v", ev, err)
			}
			if got := get(t, s, n.ID).Failures; got != 1 {
				t.Fatalf("failures after the first failure = %d, want 1", got)
			}

			claim(t, s, n.ID)
			before := get(t, s, n.ID)
			ev, err := s.CompleteFailure(ctxT(t), n.ID, 2, r, classJitter)
			if err != nil {
				t.Fatal(err)
			}
			after := get(t, s, n.ID)
			checkTransition(t, before, after, ev, domain.StatusFailed)
			if after.FailureReason == nil || *after.FailureReason != tc.reason {
				t.Errorf("failure_reason = %v, want %s", deref(after.FailureReason), tc.reason)
			}
			if after.Failures != after.MaxFailures || after.Failures != 2 {
				t.Errorf("failures = %d, max_failures = %d; want both 2", after.Failures, after.MaxFailures)
			}
			a := attempts(t, s, n.ID)
			if len(a) != 2 {
				t.Fatalf("%d attempts, want 2", len(a))
			}
			for _, x := range a {
				checkAttempt(t, x, r)
			}
		})
	}
}

// A non-consuming outcome repeated more often than max_failures never fails
// the row on budget: failures stays 0 and the row keeps returning to
// retrying. A consuming failure afterwards still finds the whole budget.
func TestCompleteFailure_NonConsumingNeverExhausts(t *testing.T) {
	const maxFailures = 2
	for _, outcome := range []domain.Outcome{domain.OutcomeProviderFatal, domain.OutcomeThrottled, domain.OutcomeUnreachable} {
		t.Run(string(outcome), func(t *testing.T) {
			s, _ := newTestStore(t, zeroDelay) // throttle default 0 too: due at once
			n := create(t, s, withMaxFailures(maxFailures))
			r := failureResult(outcome)
			const repeats = 3 * maxFailures
			for i := 1; i <= repeats; i++ {
				claim(t, s, n.ID)
				ev, err := s.CompleteFailure(ctxT(t), n.ID, i, r, classJitter)
				if err != nil {
					t.Fatalf("failure %d: %v", i, err)
				}
				if ev.Status != domain.StatusRetrying {
					t.Fatalf("failure %d: status %s, want retrying", i, ev.Status)
				}
				if got := get(t, s, n.ID).Failures; got != 0 {
					t.Fatalf("failure %d: failures = %d, want 0", i, got)
				}
			}
			a := attempts(t, s, n.ID)
			if len(a) != repeats {
				t.Fatalf("%d attempts, want %d", len(a), repeats)
			}
			for _, x := range a {
				checkAttempt(t, x, r)
			}

			// The budget is intact: one consuming failure leaves it at 1 of 2.
			claim(t, s, n.ID)
			ev, err := s.CompleteFailure(ctxT(t), n.ID, repeats+1, failureResult(domain.OutcomeRetryable), classJitter)
			if err != nil || ev.Status != domain.StatusRetrying {
				t.Fatalf("consuming failure: %+v %v", ev, err)
			}
			if got := get(t, s, n.ID); got.Failures != 1 || got.AttemptCount != repeats+1 {
				t.Errorf("failures %d, attempt_count %d; want 1 and %d", got.Failures, got.AttemptCount, repeats+1)
			}
		})
	}
}

// The Retry-After hint through the store: the delay is
// max(back-off, hint, floor), the hint is cut to the cap, and a throttled
// outcome without a hint gets the throttle default. due_at is compared with
// updated_at, both the database now() of the transaction.
func TestCompleteFailure_RetryAfterHint(t *testing.T) {
	cases := []struct {
		name     string
		outcome  domain.Outcome
		hint     time.Duration
		want     time.Duration
		failures int
	}{
		{"throttled, hint above the back-off", domain.OutcomeThrottled, 65_000 * time.Millisecond, 65_000 * time.Millisecond, 0},
		{"retryable 503, hint above the back-off", domain.OutcomeRetryable, 65_000 * time.Millisecond, 65_000 * time.Millisecond, 1},
		{"retryable 503, hint below the back-off", domain.OutcomeRetryable, 7_000 * time.Millisecond, classDelay, 1},
		{"throttled, hint below the back-off", domain.OutcomeThrottled, 7_000 * time.Millisecond, classDelay, 0},
		{"throttled, hint above the cap", domain.OutcomeThrottled, 3_600_000 * time.Millisecond, classHintCap, 0},
		{"retryable 503, hint above the cap", domain.OutcomeRetryable, 3_600_000 * time.Millisecond, classHintCap, 1},
		{"throttled without a hint", domain.OutcomeThrottled, 0, classThrottle, 0},
		{"retryable without a hint", domain.OutcomeRetryable, 0, classDelay, 1},
	}
	s, _ := newTestStore(t, classProfiles())
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := create(t, s)
			claim(t, s, n.ID)
			r := failureResult(tc.outcome)
			r.RetryAfter = tc.hint
			ev, err := s.CompleteFailure(ctxT(t), n.ID, 1, r, classJitter)
			if err != nil || ev.Status != domain.StatusRetrying {
				t.Fatalf("complete failure: %+v %v", ev, err)
			}
			after := get(t, s, n.ID)
			if d := after.DueAt.Sub(after.UpdatedAt); d != tc.want {
				t.Errorf("due_at = now() + %s, want + %s", d, tc.want)
			}
			if after.Failures != tc.failures {
				t.Errorf("failures = %d, want %d", after.Failures, tc.failures)
			}
			checkAttempt(t, attempts(t, s, n.ID)[0], r)
		})
	}
}

// The expiry shortcut: a retry that would fall at or after expires_at fails
// the row with expired at once. The budget accounting of the outcome is kept
// (a consuming outcome still increments failures), budget exhaustion is
// reported in preference to expiry, and the attempt row holds the true
// outcome.
func TestCompleteFailure_ExpiryShortcut(t *testing.T) {
	cases := []struct {
		outcome     domain.Outcome
		maxFailures int
		reason      domain.FailureReason
		failures    int
	}{
		{domain.OutcomeRetryable, 5, domain.ReasonExpired, 1},
		{domain.OutcomeUnknown, 5, domain.ReasonExpired, 1},
		{domain.OutcomeInternalError, 5, domain.ReasonExpired, 1},
		{domain.OutcomeUnreachable, 5, domain.ReasonExpired, 0},
		{domain.OutcomeProviderFatal, 5, domain.ReasonExpired, 0},
		{domain.OutcomeThrottled, 5, domain.ReasonExpired, 0},
		{domain.OutcomeRetryable, 1, domain.ReasonMaxFailures, 1},
		{domain.OutcomeUnknown, 1, domain.ReasonMaxFailuresUnknownOutcome, 1},
	}
	s, pool := newTestStore(t, classProfiles())
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%s max_failures %d", tc.outcome, tc.maxFailures), func(t *testing.T) {
			n := create(t, s, withMaxFailures(tc.maxFailures))
			claim(t, s, n.ID)
			// The retry delay (20 s, 90 s throttled) would fall after expires_at.
			setExpiresIn(t, pool, n.ID, 10_000*time.Millisecond)
			before := get(t, s, n.ID)
			r := failureResult(tc.outcome)
			ev, err := s.CompleteFailure(ctxT(t), n.ID, 1, r, classJitter)
			if err != nil {
				t.Fatal(err)
			}
			after := get(t, s, n.ID)
			checkTransition(t, before, after, ev, domain.StatusFailed)
			if after.FailureReason == nil || *after.FailureReason != tc.reason {
				t.Errorf("failure_reason = %v, want %s", deref(after.FailureReason), tc.reason)
			}
			if after.Failures != tc.failures {
				t.Errorf("failures = %d, want %d", after.Failures, tc.failures)
			}
			if after.LeaseExpiresAt != nil {
				t.Errorf("lease_expires_at still set on a failed row")
			}
			checkAttempt(t, attempts(t, s, n.ID)[0], r)
		})
	}

	t.Run("a retry before expires_at is scheduled", func(t *testing.T) {
		n := create(t, s)
		claim(t, s, n.ID)
		setExpiresIn(t, pool, n.ID, 30_000*time.Millisecond) // after the 20 s back-off
		ev, err := s.CompleteFailure(ctxT(t), n.ID, 1, failureResult(domain.OutcomeUnreachable), classJitter)
		if err != nil || ev.Status != domain.StatusRetrying {
			t.Fatalf("complete failure: %+v %v", ev, err)
		}
	})
}

// setExpiresIn sets expires_at to the database now() plus d.
func setExpiresIn(t *testing.T, pool *pgxpool.Pool, id uuid.UUID, d time.Duration) {
	t.Helper()
	exec(t, pool, `UPDATE notifications SET expires_at = now() + make_interval(secs => $2::float8 / 1000) WHERE id = $1`,
		id, d.Milliseconds())
}
