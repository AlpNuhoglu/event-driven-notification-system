//go:build integration

package store

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

// Every transition 1-8, 10 and 12 increments version, sets updated_at,
// returns an event that matches the stored row and leaves every immutable
// column (ADR-0004 section 6) untouched. The rows carry a value in every
// immutable column. Transition 10 (the relay's expiry pass) changes nothing
// else than status, failure_reason, version and updated_at, and no attempt
// row; transition 8 nothing else than status, queued_at, version and
// updated_at, and no attempt row. Transition 12 is checked from each of its
// five source states by checkLateSuccess.
func TestTransitions_Invariants(t *testing.T) {
	type env struct {
		s    *Store
		pool *pgxpool.Pool
		id   uuid.UUID
	}
	single := func(t *testing.T, evs []domain.StatusChanged, err error) domain.StatusChanged {
		t.Helper()
		if err != nil || len(evs) != 1 {
			t.Fatalf("events %+v err %v", evs, err)
		}
		return evs[0]
	}
	toRetrying := func(t *testing.T, e env) {
		claim(t, e.s, e.id)
		if _, err := e.s.CompleteFailure(ctxT(t), e.id, 1, AttemptResult{Outcome: domain.OutcomeRetryable}, 0); err != nil {
			t.Fatal(err)
		}
	}
	expirePass := func(t *testing.T, e env) domain.StatusChanged {
		evs, err := e.s.ExpireDue(ctxT(t), 10)
		return single(t, evs, err)
	}
	publish := func(t *testing.T, e env) {
		if _, err := e.s.PublishLane(ctxT(t), smsNormal, 10, publishAll); err != nil {
			t.Fatal(err)
		}
	}
	// swept: attempt 1's worker stalls and the lease sweeper takes the row
	// back (retrying with zeroDelay, failed with a budget of 1); the attempt
	// row is unknown. Then the late success of attempt 1 arrives.
	swept := func(t *testing.T, e env) {
		claim(t, e.s, e.id)
		expireLease(t, e.pool, e.id)
		if _, err := e.s.SweepExpiredLeases(ctxT(t), 10, zeroJitter); err != nil {
			t.Fatal(err)
		}
	}
	lateFrom := func(from domain.Status) func(t *testing.T, e env) domain.StatusChanged {
		return func(t *testing.T, e env) domain.StatusChanged {
			sent, ev, err := e.s.CompleteSent(ctxT(t), e.id, 1, testProvider, lateResult())
			if err != nil {
				t.Fatal(err)
			}
			if sent != (Sent{Late: true, From: from}) {
				t.Errorf("result %+v, want transition 12 from %s", sent, from)
			}
			return ev
		}
	}
	cases := []struct {
		name        string
		maxFailures int
		setup       func(t *testing.T, e env)
		act         func(t *testing.T, e env) domain.StatusChanged
		want        domain.Status
	}{
		{"1 pending to queued", 5, nil, func(t *testing.T, e env) domain.StatusChanged {
			evs, err := e.s.PublishLane(ctxT(t), smsNormal, 10, publishAll)
			return single(t, evs, err)
		}, domain.StatusQueued},
		{"2 retrying to queued", 5, toRetrying, func(t *testing.T, e env) domain.StatusChanged {
			evs, err := e.s.PublishLane(ctxT(t), smsNormal, 10, publishAll)
			return single(t, evs, err)
		}, domain.StatusQueued},
		{"3 queued to processing", 5, func(t *testing.T, e env) {
			if _, err := e.s.PublishLane(ctxT(t), smsNormal, 10, publishAll); err != nil {
				t.Fatal(err)
			}
		}, func(t *testing.T, e env) domain.StatusChanged {
			_, ev, err := e.s.Claim(ctxT(t), e.id, testLease, testProvider)
			if err != nil {
				t.Fatal(err)
			}
			return ev
		}, domain.StatusProcessing},
		{"3 pending to processing", 5, nil, func(t *testing.T, e env) domain.StatusChanged {
			_, ev, err := e.s.Claim(ctxT(t), e.id, testLease, testProvider)
			if err != nil {
				t.Fatal(err)
			}
			return ev
		}, domain.StatusProcessing},
		{"3 retrying to processing", 5, toRetrying, func(t *testing.T, e env) domain.StatusChanged {
			_, ev, err := e.s.Claim(ctxT(t), e.id, testLease, testProvider)
			if err != nil {
				t.Fatal(err)
			}
			return ev
		}, domain.StatusProcessing},
		{"3 expired at claim", 5, func(t *testing.T, e env) { expire(t, e.pool, e.id) }, func(t *testing.T, e env) domain.StatusChanged {
			_, ev, err := e.s.Claim(ctxT(t), e.id, testLease, testProvider)
			if err != nil {
				t.Fatal(err)
			}
			return ev
		}, domain.StatusFailed},
		{"4 processing to sent", 5, func(t *testing.T, e env) { claim(t, e.s, e.id) }, func(t *testing.T, e env) domain.StatusChanged {
			_, ev, err := e.s.CompleteSent(ctxT(t), e.id, 1, testProvider, AttemptResult{Outcome: domain.OutcomeSuccess, ProviderMessageID: ptr("pm")})
			if err != nil {
				t.Fatal(err)
			}
			return ev
		}, domain.StatusSent},
		{"5 processing to retrying", 5, func(t *testing.T, e env) { claim(t, e.s, e.id) }, func(t *testing.T, e env) domain.StatusChanged {
			ev, err := e.s.CompleteFailure(ctxT(t), e.id, 1, AttemptResult{Outcome: domain.OutcomeRetryable}, 0)
			if err != nil {
				t.Fatal(err)
			}
			return ev
		}, domain.StatusRetrying},
		{"6 processing to failed", 1, func(t *testing.T, e env) { claim(t, e.s, e.id) }, func(t *testing.T, e env) domain.StatusChanged {
			ev, err := e.s.CompleteFailure(ctxT(t), e.id, 1, AttemptResult{Outcome: domain.OutcomePermanent}, 0)
			if err != nil {
				t.Fatal(err)
			}
			return ev
		}, domain.StatusFailed},
		{"7 lease expired to retrying", 5, func(t *testing.T, e env) {
			claim(t, e.s, e.id)
			expireLease(t, e.pool, e.id)
		}, func(t *testing.T, e env) domain.StatusChanged {
			evs, err := e.s.SweepExpiredLeases(ctxT(t), 10, zeroJitter)
			return single(t, evs, err)
		}, domain.StatusRetrying},
		{"7 lease expired to failed", 1, func(t *testing.T, e env) {
			claim(t, e.s, e.id)
			expireLease(t, e.pool, e.id)
		}, func(t *testing.T, e env) domain.StatusChanged {
			evs, err := e.s.SweepExpiredLeases(ctxT(t), 10, zeroJitter)
			return single(t, evs, err)
		}, domain.StatusFailed},
		{"8 queued to pending (reconcile)", 5, publish, func(t *testing.T, e env) domain.StatusChanged {
			evs, err := e.s.ReconcileQueued(ctxT(t))
			return single(t, evs, err)
		}, domain.StatusPending},
		{"8 queued to pending (requeue-stale)", 5, publish, func(t *testing.T, e env) domain.StatusChanged {
			evs, err := e.s.RequeueStale(ctxT(t), 0)
			return single(t, evs, err)
		}, domain.StatusPending},
		{"10 pending expired", 5, func(t *testing.T, e env) { expire(t, e.pool, e.id) }, expirePass, domain.StatusFailed},
		{"10 retrying expired", 5, func(t *testing.T, e env) {
			toRetrying(t, e)
			expire(t, e.pool, e.id)
		}, expirePass, domain.StatusFailed},
		{"10 queued expired", 5, func(t *testing.T, e env) {
			if _, err := e.s.PublishLane(ctxT(t), smsNormal, 10, publishAll); err != nil {
				t.Fatal(err)
			}
			expire(t, e.pool, e.id)
		}, expirePass, domain.StatusFailed},
		{"12 pending to sent", 5, func(t *testing.T, e env) {
			swept(t, e)
			publish(t, e)
			if _, err := e.s.ReconcileQueued(ctxT(t)); err != nil {
				t.Fatal(err)
			}
		}, lateFrom(domain.StatusPending), domain.StatusSent},
		{"12 queued to sent", 5, func(t *testing.T, e env) {
			swept(t, e)
			publish(t, e)
		}, lateFrom(domain.StatusQueued), domain.StatusSent},
		{"12 retrying to sent", 5, swept, lateFrom(domain.StatusRetrying), domain.StatusSent},
		{"12 failed to sent", 1, swept, lateFrom(domain.StatusFailed), domain.StatusSent},
		{"12 cancelled to sent", 5, func(t *testing.T, e env) {
			swept(t, e)
			if _, _, err := e.s.CancelNotification(ctxT(t), e.id); err != nil {
				t.Fatal(err)
			}
		}, lateFrom(domain.StatusCancelled), domain.StatusSent},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, pool := newTestStore(t, zeroDelay)
			batchID := uuid.Must(uuid.NewV7())
			exec(t, pool, `INSERT INTO batches (id, total, accepted, rejected, correlation_id) VALUES ($1, 1, 1, 0, 'c')`, batchID)
			n := create(t, s, withMaxFailures(tc.maxFailures), func(n *NewNotification) {
				n.BatchID = &batchID
				n.DedupeKey = ptr("dk-" + tc.name)
				n.TemplateName = ptr("otp")
				n.TemplateVersion = ptr(int32(2))
				n.TemplateVars = []byte(`{"code": "9"}`)
				n.ScheduledAt = ptr(time.Now().Add(-1000 * time.Millisecond)) // already due
			})
			e := env{s: s, pool: pool, id: n.ID}
			if tc.setup != nil {
				tc.setup(t, e)
			}
			before := get(t, s, n.ID)
			beforeAttempts := attempts(t, s, n.ID)
			ev := tc.act(t, e)
			after := get(t, s, n.ID)
			checkTransition(t, before, after, ev, tc.want)
			switch {
			case strings.HasPrefix(tc.name, "8 "):
				if before.Status != domain.StatusQueued || after.QueuedAt != nil {
					t.Errorf("status %s -> %s, queued_at %v", before.Status, after.Status, after.QueuedAt)
				}
				b, a := before, after
				b.Status, b.QueuedAt, b.Version, b.UpdatedAt = "", nil, 0, time.Time{}
				a.Status, a.QueuedAt, a.Version, a.UpdatedAt = "", nil, 0, time.Time{}
				if !reflect.DeepEqual(b, a) {
					t.Errorf("reconciliation changed more than status, queued_at, version, updated_at:\n%+v\n%+v", before, after)
				}
				if got := attempts(t, s, n.ID); !reflect.DeepEqual(got, beforeAttempts) {
					t.Errorf("attempts changed: %+v -> %+v", beforeAttempts, got)
				}
			case strings.HasPrefix(tc.name, "12 "):
				if a := beforeAttempts; len(a) != 1 || a[0].Outcome != domain.OutcomeUnknown {
					t.Fatalf("setup: attempts %+v, want attempt 1 unknown", a)
				}
				checkLateSuccess(t, s, before, Sent{Late: true, From: before.Status}, ev)
			}
			if strings.HasPrefix(tc.name, "10 ") {
				if after.FailureReason == nil || *after.FailureReason != domain.ReasonExpired {
					t.Errorf("failure_reason = %v, want expired", after.FailureReason)
				}
				b, a := before, after
				b.Status, b.FailureReason, b.Version, b.UpdatedAt = "", nil, 0, time.Time{}
				a.Status, a.FailureReason, a.Version, a.UpdatedAt = "", nil, 0, time.Time{}
				if !reflect.DeepEqual(b, a) {
					t.Errorf("expiry changed more than status, failure_reason, version, updated_at:\n%+v\n%+v", before, after)
				}
				if got := attempts(t, s, n.ID); !reflect.DeepEqual(got, beforeAttempts) {
					t.Errorf("attempts changed: %+v -> %+v", beforeAttempts, got)
				}
			}
		})
	}
}
