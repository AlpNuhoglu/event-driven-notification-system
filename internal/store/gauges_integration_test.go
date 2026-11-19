//go:build integration

package store

import (
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store/db"
)

// insertRows inserts n rows of one lane in one status, due at now() + due
// and, when queued is not empty, queued at now() - queued (intervals).
func insertRows(t *testing.T, pool *pgxpool.Pool, n int, channel string, priority int, status, due, queued string) {
	t.Helper()
	exec(t, pool, `
		INSERT INTO notifications (id, channel, priority, recipient, content, status, failure_reason, sent_at,
		                           due_at, expires_at, max_failures, correlation_id, queued_at)
		SELECT uuidv7(), $2, $3, '+905551234567', 'x', $4,
		       CASE WHEN $4 = 'failed' THEN 'provider_rejected' END, CASE WHEN $4 = 'sent' THEN now() END,
		       now() + $5::interval, now() + interval '1 day', 5, 'corr',
		       CASE WHEN $6 = '' THEN NULL ELSE now() - $6::interval END
		FROM generate_series(1, $1)`, n, channel, priority, status, due, queued)
}

// Gauges counts the queued rows of each lane with the age of the oldest on
// the database clock, the pending and retrying rows that are due, and those
// due later; the other statuses are not counted.
func TestGauges_MeasuresLanesAndBacklog(t *testing.T) {
	s, pool := newTestStore(t, zeroDelay)
	insertRows(t, pool, 1, "sms", 0, "queued", "-1 minute", "10 seconds")
	insertRows(t, pool, 1, "sms", 0, "queued", "-1 minute", "3 seconds")
	insertRows(t, pool, 1, "sms", 0, "pending", "-1 second", "")
	insertRows(t, pool, 3, "email", 2, "pending", "-1 second", "")
	insertRows(t, pool, 1, "email", 2, "retrying", "-1 second", "")
	insertRows(t, pool, 2, "push", 1, "pending", "1 hour", "")
	insertRows(t, pool, 1, "push", 1, "retrying", "30 seconds", "")
	for _, st := range []string{"sent", "failed", "processing", "cancelled"} {
		insertRows(t, pool, 2, "push", 1, st, "-1 second", "")
	}

	g, err := s.Gauges(ctxT(t))
	if err != nil {
		t.Fatal(err)
	}
	smsHigh := g.Lanes[Lane{Channel: domain.ChannelSMS, Priority: domain.PriorityHigh}]
	if smsHigh.Queued != 2 || smsHigh.BacklogDue != 1 {
		t.Errorf("sms/high %+v, want 2 queued and 1 due", smsHigh)
	}
	if smsHigh.OldestQueuedAge < 10 || smsHigh.OldestQueuedAge > 15 {
		t.Errorf("sms/high oldest queued age %v s, want the 10 s of the oldest", smsHigh.OldestQueuedAge)
	}
	if got := g.Lanes[Lane{Channel: domain.ChannelEmail, Priority: domain.PriorityLow}]; got != (LaneGauge{BacklogDue: 4}) {
		t.Errorf("email/low %+v, want 4 due", got)
	}
	if got := g.Lanes[Lane{Channel: domain.ChannelPush, Priority: domain.PriorityNormal}]; got != (LaneGauge{}) {
		t.Errorf("push/normal %+v, want nothing queued or due", got)
	}
	if g.ScheduledFuture != 3 {
		t.Errorf("scheduled_future %d, want 3", g.ScheduledFuture)
	}

	empty, _ := newTestStore(t, zeroDelay)
	g, err = empty.Gauges(ctxT(t))
	if err != nil || g.ScheduledFuture != 0 {
		t.Fatalf("empty table: %+v, %v", g, err)
	}
	for lane, lg := range g.Lanes {
		if lg != (LaneGauge{}) {
			t.Errorf("empty table: %v %+v, want zero", lane, lg)
		}
	}
}

// On a table that holds mostly finished rows, as in production (the
// unfinished ones the newest), the two gauge statements read only
// unfinished rows: through the lane's partial index or through the expiry
// index, which holds the same rows and the queued ones; never the table by
// a sequential scan. Every row has its own times, as rows created one after
// the other have (equal values would let B-tree deduplication shrink an
// index and mislead the comparison). The plans are logged: Postgres 18
// chooses the expiry index here, whose order follows the table's.
func TestGauges_PlansReadOnlyUnfinishedRows(t *testing.T) {
	_, pool := newTestStore(t, zeroDelay)
	exec(t, pool, `
		INSERT INTO notifications (id, channel, priority, recipient, content, status, failure_reason, sent_at,
		                           due_at, expires_at, max_failures, correlation_id, queued_at)
		SELECT uuidv7(), (ARRAY['sms','email','push'])[1 + i % 3], i % 3, '+905551234567', 'x', st,
		       CASE WHEN st = 'failed' THEN 'provider_rejected' END, CASE WHEN st = 'sent' THEN now() END,
		       now() + CASE WHEN i % 7 = 0 THEN interval '1 hour' ELSE interval '-1 hour' END + i * interval '1 millisecond',
		       now() + interval '1 day' + i * interval '1 millisecond', 5, 'corr',
		       CASE WHEN st = 'queued' THEN now() - interval '1 minute' + i * interval '1 millisecond' END
		FROM (SELECT i, CASE WHEN i <= 90000 THEN (CASE WHEN i % 20 = 0 THEN 'failed' ELSE 'sent' END)
		                     WHEN i % 5 = 0 THEN 'queued'
		                     WHEN i % 5 = 1 THEN 'retrying'
		                     ELSE 'pending' END AS st
		      FROM generate_series(1, 100000) AS i) g`)
	exec(t, pool, `VACUUM ANALYZE notifications`)

	conn, err := pool.Acquire(ctxT(t))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	for _, c := range []struct {
		name, sql string
		indexes   []string
	}{
		{"QueuedLaneGauges", db.QueuedLaneGaugesSQL, []string{"notifications_queued_lane_idx", "notifications_expiry_idx"}},
		{"BacklogLaneGauges", db.BacklogLaneGaugesSQL, []string{"notifications_relay_lane_idx", "notifications_expiry_idx"}},
	} {
		// The statements have no parameters: the plain plan is the one the
		// unnamed statement gets (customPlanDB).
		plan, text := explain(t, conn, `EXPLAIN (FORMAT JSON) `+c.sql)
		var used []string
		plan.walk(func(n planNode) {
			if n.NodeType == "Seq Scan" {
				t.Errorf("%s: the plan has a Seq Scan:\n%s", c.name, text)
			}
			if n.IndexName != "" {
				used = append(used, n.IndexName)
			}
		})
		if len(used) == 0 || slices.ContainsFunc(used, func(i string) bool { return !slices.Contains(c.indexes, i) }) {
			t.Errorf("%s: indexes %v, want only those of %v:\n%s", c.name, used, c.indexes, text)
		}
		t.Logf("%s reads %v:\n%s", c.name, used, text)
	}
}

// The end-to-end latency of a sent notification runs from scheduled_at when
// it has one and from created_at otherwise, to the sent_at the transition
// wrote; a late success (transition 12) reports it too.
func TestCompleteSent_EndToEndFromScheduledAtOrCreatedAt(t *testing.T) {
	s, pool := newTestStore(t, zeroDelay)
	success := AttemptResult{Outcome: domain.OutcomeSuccess, Latency: 5 * time.Millisecond}

	plain := create(t, s)
	claim(t, s, plain.ID)
	sent, _, err := s.CompleteSent(ctxT(t), plain.ID, 1, testProvider, success)
	if err != nil {
		t.Fatal(err)
	}
	row := get(t, s, plain.ID)
	if want := row.SentAt.Sub(row.CreatedAt); sent.EndToEnd != want || want <= 0 {
		t.Errorf("not scheduled: end-to-end %v, want sent_at - created_at = %v", sent.EndToEnd, want)
	}

	// Scheduled ten minutes before it was created, so that it is due at once:
	// the latency includes those ten minutes.
	scheduled := create(t, s, func(n *NewNotification) { n.ScheduledAt = ptr(time.Now().Add(-10 * time.Minute)) })
	claim(t, s, scheduled.ID)
	sent, _, err = s.CompleteSent(ctxT(t), scheduled.ID, 1, testProvider, success)
	if err != nil {
		t.Fatal(err)
	}
	row = get(t, s, scheduled.ID)
	if want := row.SentAt.Sub(*row.ScheduledAt); sent.EndToEnd != want || want < 9*time.Minute {
		t.Errorf("scheduled: end-to-end %v, want sent_at - scheduled_at = %v", sent.EndToEnd, want)
	}

	// A late success after the lease sweeper moved the row on.
	late := create(t, s)
	claim(t, s, late.ID)
	expireLease(t, pool, late.ID)
	if _, err := s.SweepExpiredLeases(ctxT(t), 10, func() float64 { return 0 }); err != nil {
		t.Fatal(err)
	}
	sent, _, err = s.CompleteSent(ctxT(t), late.ID, 1, testProvider, success)
	if err != nil || !sent.Late {
		t.Fatalf("late success: %+v, %v", sent, err)
	}
	row = get(t, s, late.ID)
	if want := row.SentAt.Sub(row.CreatedAt); sent.EndToEnd != want {
		t.Errorf("late success: end-to-end %v, want %v", sent.EndToEnd, want)
	}
}
