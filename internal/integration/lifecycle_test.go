//go:build integration

package integration

import (
	"testing"
	"time"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/queue"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

// The send deadline, end to end (D77). Lease 400 ms, HTTP timeout 100 ms and
// write margin 100 ms give a send deadline of 200 ms after the claim call.
// The test holds the row lock, as the relay's lane transaction would, for
// longer than that while the worker's claim waits on it. The attempt is then
// aborted without a send and without consuming budget; the relay publishes
// the row again and attempt 2 sends it.
//
// The lease sweeper does not interfere: the relay is started only after the
// abort, with a sweep interval of one hour, so the sweeper never runs.
func TestE2E_SendDeadlineAbortsLateClaim(t *testing.T) {
	const (
		writeMargin  = 100 * time.Millisecond
		sendDeadline = 200 * time.Millisecond // 400 - 100 - 100
	)
	e := newEnv(t)
	cfg := e.config(map[string]string{
		"LEASE_DURATION":       "400ms",
		"PROVIDER_TIMEOUT":     "100ms",
		"LEASE_SWEEP_INTERVAL": "3600000ms",
	})
	e.startWorker(cfg, writeMargin)
	pub := e.publisher()
	id := e.create(1)[0]

	// The test's transaction plays the relay's lane transaction: it locks
	// the row and publishes while it holds the lock.
	ctx, cancel := e.ctx()
	defer cancel()
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT 1 FROM notifications WHERE id = $1 FOR UPDATE`, id); err != nil {
		t.Fatal(err)
	}
	msg := queue.Message{ID: id, Channel: domain.ChannelSMS, Priority: domain.PriorityNormal, TraceParent: testTraceParent}
	if published, err := pub.PublishBatch(ctx, []queue.Message{msg}, confirmTimeout); err != nil || len(published) != 1 {
		t.Fatalf("publish: %v, %d confirmed", err, len(published))
	}
	testutil.Eventually(t, func() bool { return e.claimLockWaiters() == 1 }, waitTimeout)
	waiting := time.Now() // the claim call was issued before this
	testutil.Eventually(t, func() bool { return time.Since(waiting) >= sendDeadline+100*time.Millisecond }, waitTimeout)
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	testutil.Eventually(t, func() bool {
		a := e.attempts(id)
		return len(a) == 1 && a[0].Outcome == domain.OutcomeAborted
	}, waitTimeout)
	n := e.get(id)
	if n.Status != domain.StatusRetrying || n.AttemptCount != 1 || n.Failures != 0 {
		t.Errorf("after the abort: status %s, attempt_count %d, failures %d", n.Status, n.AttemptCount, n.Failures)
	}
	var due bool
	if err := e.pool.QueryRow(ctx, `SELECT due_at <= now() FROM notifications WHERE id = $1`, id).Scan(&due); err != nil || !due {
		t.Errorf("the aborted row is not due at once (due %v, err %v)", due, err)
	}
	if got := e.prov.count(); got != 0 {
		t.Fatalf("the provider received %d requests for the aborted attempt", got)
	}

	e.startRelay(cfg)
	e.waitStatus(id, domain.StatusSent)
	n = e.get(id)
	if n.AttemptCount != 2 || n.Failures != 0 {
		t.Errorf("sent with attempt_count %d, failures %d; want 2 and 0", n.AttemptCount, n.Failures)
	}
	a := e.attempts(id)
	if len(a) != 2 || a[0].Outcome != domain.OutcomeAborted || a[1].AttemptNo != 2 || a[1].Outcome != domain.OutcomeSuccess {
		t.Errorf("attempts = %+v, want aborted then success", a)
	}
	if got := keys(e.prov.requests()); len(got) != 1 || got[id.String()] != 1 {
		t.Errorf("provider requests by key = %v, want one, for attempt 2", got)
	}
}

// Graceful shutdown (ADR-0005): with two sends in flight on a blocked
// provider and two messages buffered behind them, cancelling the role
// requeues the buffered messages at once, lets the in-flight sends complete
// and returns nil within the drain timeout.
func TestE2E_ShutdownRequeuesBufferedAndDrains(t *testing.T) {
	const drain = 5_000 * time.Millisecond
	e := newEnv(t)
	cfg := e.config(map[string]string{
		"WORKER_CONCURRENCY":     "2",
		"SHUTDOWN_DRAIN_TIMEOUT": "5000ms",
	})
	release := e.prov.block(t)
	_, w := e.startWorker(cfg, 0)
	e.startRelay(cfg)
	e.create(5)

	// Two sends are held by the provider. Prefetch equals concurrency, so
	// the broker then delivers two more, which wait in the buffer, and one
	// stays ready.
	testutil.Eventually(t, func() bool { return e.prov.count() == 2 }, waitTimeout)
	testutil.Eventually(t, func() bool {
		return e.countStatus(domain.StatusProcessing) == 2 && e.countStatus(domain.StatusQueued) == 3 && e.ready() == 1
	}, waitTimeout)

	stopped := time.Now()
	w.cancel()
	// The buffered messages are requeued while the in-flight sends still run.
	testutil.Eventually(t, func() bool { return e.ready() == 3 }, waitTimeout)
	if w.returned() {
		t.Fatal("the role returned before its in-flight sends completed")
	}
	release()
	if err := w.wait(t); err != nil {
		t.Errorf("role returned %v, want nil", err)
	}
	if took := time.Since(stopped); took >= drain {
		t.Errorf("shutdown took %s, not within the drain timeout %s", took, drain)
	}

	if got := e.countStatus(domain.StatusSent); got != 2 {
		t.Errorf("%d sent, want the 2 in-flight", got)
	}
	if got := e.countStatus(domain.StatusQueued); got != 3 {
		t.Errorf("%d queued, want 3", got)
	}
	if got := e.prov.count(); got != 2 {
		t.Errorf("provider received %d requests, want 2", got)
	}
	if got := e.ready(); got != 3 {
		t.Errorf("%d ready after the role stopped, want 3", got)
	}
}
