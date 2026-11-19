//go:build integration

package relay

import (
	"context"
	"log/slog"
	"testing"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

// newBrokerlessRelay returns a relay whose broker connection is closed and
// that has no publisher: the expiry pass and the cleanup need neither.
func newBrokerlessRelay(st *store.Store, cfg Config) (*Relay, *recorder) {
	rec := &recorder{}
	return New(st, nil, &fakeConn{}, rec, slog.New(slog.DiscardHandler), cfg, func() float64 { return 0.5 }), rec
}

// TestRelay_ExpiryPassFailsBacklogInOneTick: with more expired rows than one
// batch, one tick of the expiry pass fails all of them, from pending and
// queued, with one event each, while the broker connection is down. A row
// that has not expired is left alone.
func TestRelay_ExpiryPassFailsBacklogInOneTick(t *testing.T) {
	st, pool := newStore(t)
	cfg := integrationConfig
	cfg.ExpiryBatch = 3
	r, rec := newBrokerlessRelay(st, cfg)

	expired := create(t, st, domain.ChannelSMS, domain.PriorityHigh, 8)
	queuedIDs := create(t, st, domain.ChannelEmail, domain.PriorityLow, 2)
	if _, err := st.PublishLane(ctxT(t), store.Lane{Channel: domain.ChannelEmail, Priority: domain.PriorityLow}, 10,
		func(_ context.Context, rows []store.DueRow) ([]uuid.UUID, error) {
			out := make([]uuid.UUID, len(rows))
			for i, row := range rows {
				out[i] = row.ID
			}
			return out, nil
		}); err != nil {
		t.Fatal(err)
	}
	requireStatus(t, st, queuedIDs, domain.StatusQueued)
	expired = append(expired, queuedIDs...)
	live := create(t, st, domain.ChannelPush, domain.PriorityNormal, 1)[0]
	if _, err := pool.Exec(ctxT(t),
		`UPDATE notifications SET expires_at = now() - interval '1 millisecond' WHERE id = ANY($1)`, expired); err != nil {
		t.Fatal(err)
	}

	r.expire(ctxT(t))

	requireStatus(t, st, expired, domain.StatusFailed)
	requireStatus(t, st, []uuid.UUID{live}, domain.StatusPending)
	evs := eventIDs(rec.events())
	if len(rec.events()) != len(expired) || len(evs) != len(expired) {
		t.Errorf("%d events for %d distinct rows, want %d", len(rec.events()), len(evs), len(expired))
	}
	for _, id := range expired {
		if !evs[id] {
			t.Errorf("no event for %s", id)
		}
	}
}

// TestRelay_ExpiryLoopFailsExpiredRow: the loop fails an expired row and
// stops on shutdown.
func TestRelay_ExpiryLoopFailsExpiredRow(t *testing.T) {
	st, pool := newStore(t)
	r, rec := newBrokerlessRelay(st, integrationConfig)
	id := create(t, st, domain.ChannelSMS, domain.PriorityNormal, 1)[0]
	if _, err := pool.Exec(ctxT(t),
		`UPDATE notifications SET expires_at = now() - interval '1 millisecond' WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.RunExpiry(ctx) }()
	testutil.Eventually(t, func() bool { return eventIDs(rec.events())[id] }, waitTimeout)
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	n, err := st.GetNotification(ctxT(t), id)
	if err != nil {
		t.Fatal(err)
	}
	if n.Status != domain.StatusFailed || n.FailureReason == nil || *n.FailureReason != domain.ReasonExpired {
		t.Errorf("row is %s/%v, want failed/expired", n.Status, n.FailureReason)
	}
}

// TestRelay_CleanupDeletesBacklogInOneTick: more than one batch of expired
// idempotency keys is deleted in one tick of the cleanup; live keys stay.
func TestRelay_CleanupDeletesBacklogInOneTick(t *testing.T) {
	st, pool := newStore(t)
	r, _ := newBrokerlessRelay(st, integrationConfig)
	const expiredKeys, liveKeys = 2*store.IdempotencyCleanupBatch + 500, 5
	if _, err := pool.Exec(ctxT(t), `
		INSERT INTO idempotency_keys (key, request_hash, created_at, expires_at)
		SELECT 'expired-' || g, 'h', now() - interval '2 days', now() - g * interval '1 millisecond'
		FROM generate_series(1, $1::int) AS g`, expiredKeys); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctxT(t), `
		INSERT INTO idempotency_keys (key, request_hash, created_at, expires_at)
		SELECT 'live-' || g, 'h', now(), now() + interval '1 day'
		FROM generate_series(1, $1::int) AS g`, liveKeys); err != nil {
		t.Fatal(err)
	}

	r.cleanup(ctxT(t))

	var expiredLeft, liveLeft int
	if err := pool.QueryRow(ctxT(t), `
		SELECT count(*) FILTER (WHERE expires_at <= now()), count(*) FILTER (WHERE expires_at > now())
		FROM idempotency_keys`).Scan(&expiredLeft, &liveLeft); err != nil {
		t.Fatal(err)
	}
	if expiredLeft != 0 || liveLeft != liveKeys {
		t.Errorf("%d expired and %d live keys left, want 0 and %d", expiredLeft, liveLeft, liveKeys)
	}
}
