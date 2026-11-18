//go:build integration

package store

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

// Every transition 1-7 increments version, sets updated_at, returns an event
// that matches the stored row and leaves every immutable column (ADR-0004
// section 6) untouched. The rows carry a value in every immutable column.
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
			ev, err := e.s.CompleteSent(ctxT(t), e.id, 1, testProvider, AttemptResult{Outcome: domain.OutcomeSuccess, ProviderMessageID: ptr("pm")})
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
			ev := tc.act(t, e)
			checkTransition(t, before, get(t, s, n.ID), ev, tc.want)
		})
	}
}
