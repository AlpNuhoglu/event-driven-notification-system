//go:build integration

package integration

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/config"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/relay"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/worker"
)

// claimTracer is a pgx tracer on the worker's pool: it records the end of
// every claim statement of one notification, with its row count.
type claimTracer struct {
	id uuid.UUID

	mu    sync.Mutex
	ended []claimEnd
}

type claimEnd struct {
	rows int64
	err  error
}

type tracedClaimKey struct{}

func (c *claimTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	if !strings.Contains(d.SQL, "WITH claimed AS") {
		return ctx
	}
	for _, a := range d.Args {
		if id, ok := a.(uuid.UUID); ok && id == c.id {
			return context.WithValue(ctx, tracedClaimKey{}, true)
		}
	}
	return ctx
}

func (c *claimTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryEndData) {
	if ctx.Value(tracedClaimKey{}) != true {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ended = append(c.ended, claimEnd{rows: d.CommandTag.RowsAffected(), err: d.Err})
}

func (c *claimTracer) claims() []claimEnd {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]claimEnd(nil), c.ended...)
}

// startTracedWorker is startWorker with the tracer on the worker's pool.
func (e *env) startTracedWorker(cfg config.Config, tracer pgx.QueryTracer) *running {
	e.t.Helper()
	pcfg, err := pgxpool.ParseConfig(cfg.Connections.DatabaseURL)
	if err != nil {
		e.t.Fatalf("pool config: %v", err)
	}
	pcfg.ConnConfig.Tracer = tracer
	pool, err := pgxpool.NewWithConfig(context.Background(), pcfg)
	if err != nil {
		e.t.Fatalf("pool: %v", err)
	}
	e.t.Cleanup(pool.Close)
	role, err := worker.NewRole(cfg, worker.Resources{
		Pool:     pool,
		Conn:     e.dial(),
		Redis:    e.rdb,
		Registry: prometheus.NewRegistry(),
	}, e.log)
	if err != nil {
		e.t.Fatalf("worker role: %v", err)
	}
	return start(e.t, role.Run)
}

// cancelLockWaiters counts backends of this test's database that are blocked
// on a lock while running the cancel statement.
func (e *env) cancelLockWaiters() int {
	e.t.Helper()
	ctx, cancel := e.ctx()
	defer cancel()
	var n int
	err := e.pool.QueryRow(ctx, `
		SELECT count(*) FROM pg_stat_activity
		WHERE datname = current_database()
		  AND pid <> pg_backend_pid()
		  AND wait_event_type = 'Lock'
		  AND query LIKE '%SET status = ''cancelled''%'`).Scan(&n)
	if err != nil {
		e.t.Errorf("pg_stat_activity: %v", err)
		return -1
	}
	return n
}

type cancelOutcome struct {
	res store.Cancelled
	err error
}

// A cancel issued while the relay's lane transaction is open (the message is
// already in the broker) waits on the row lock, then applies to the queued
// row after the relay's commit. The worker then receives the message, its
// claim matches zero rows and the delivery is acked: the provider receives
// nothing, and the notification stays cancelled without an attempt
// (ADR-0004 transition 9). If the claim could take a cancelled row, the
// provider would receive the message and this test fails.
func TestCancel_DuringRelayPublish(t *testing.T) {
	e := newEnv(t)
	cfg := e.config(nil)

	hp := &holdingPublisher{published: make(chan struct{}), release: make(chan struct{})}
	e.startRelayWith(cfg, func(p relay.Publisher) relay.Publisher { hp.Publisher = p; return hp })
	t.Cleanup(hp.Release) // before the relay stops, if the test ends early

	id := e.create(1)[0]
	testutil.Eventually(t, func() bool { return closedChan(hp.published) }, waitTimeout)
	// The publish is confirmed: the message is in the broker, the row is
	// still pending and locked by the relay's transaction.
	if got := e.ready(); got != 1 {
		t.Fatalf("%d messages ready, want 1", got)
	}

	done := make(chan cancelOutcome, 1)
	go func() {
		ctx, cancel := e.ctx()
		defer cancel()
		res, _, err := e.store.CancelNotification(ctx, id)
		done <- cancelOutcome{res, err}
	}()
	testutil.Eventually(t, func() bool { return e.cancelLockWaiters() == 1 }, waitTimeout)
	select {
	case out := <-done:
		t.Fatalf("the cancel returned before the relay committed: %+v", out)
	default:
	}
	if got := e.get(id).Status; got != domain.StatusPending {
		t.Fatalf("status %s while the relay's transaction is open, want pending", got)
	}
	hp.Release()

	var out cancelOutcome
	testutil.Eventually(t, func() bool {
		select {
		case out = <-done:
			return true
		default:
			return false
		}
	}, waitTimeout)
	if out.err != nil || !out.res.Applied {
		t.Fatalf("cancel: %+v", out)
	}
	// Applied to the committed queued row: version 1 (create), 2 (queued),
	// 3 (cancelled).
	if n := out.res.Notification; n.Status != domain.StatusCancelled || n.QueuedAt == nil || n.Version != 3 {
		t.Fatalf("cancelled row: status %s queued_at set %v version %d", n.Status, n.QueuedAt != nil, n.Version)
	}

	tracer := &claimTracer{id: id}
	w := e.startTracedWorker(cfg, tracer)
	testutil.Eventually(t, func() bool { return len(tracer.claims()) > 0 }, waitTimeout)
	if c := tracer.claims(); len(c) != 1 || c[0].err != nil || c[0].rows != 0 {
		t.Errorf("claims %+v, want one that matched zero rows", c)
	}
	if !e.queueEmpty(w) {
		t.Error("the dispatch queue is not empty: the delivery was not acked")
	}
	if got := e.prov.count(); got != 0 {
		t.Errorf("the provider received %d requests, want none", got)
	}
	n := e.get(id)
	if n.Status != domain.StatusCancelled || n.AttemptCount != 0 || n.Version != 3 {
		t.Errorf("row: status %s attempt_count %d version %d", n.Status, n.AttemptCount, n.Version)
	}
	if a := e.attempts(id); len(a) != 0 {
		t.Errorf("attempts %+v, want none", a)
	}
}

var _ pgx.QueryTracer = (*claimTracer)(nil)
