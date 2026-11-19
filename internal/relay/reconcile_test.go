package relay

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

// Reconciliation in the publishing loop (transition 8, ADR-0003 §3, D5).
// The loop runs on real tickers of a few milliseconds; the window of
// RECONCILE_MIN_INTERVAL is measured on an injected clock that only the
// test moves.

const reconcileWindow = 60_000 * time.Millisecond

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// syncBuffer is a bytes.Buffer safe for concurrent writers.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// lines returns the JSON log lines with message msg.
func (b *syncBuffer) lines(msg string) []map[string]any {
	b.mu.Lock()
	text := b.buf.String()
	b.mu.Unlock()
	var out []map[string]any
	for line := range strings.SplitSeq(text, "\n") {
		var l map[string]any
		if json.Unmarshal([]byte(line), &l) == nil && l["msg"] == msg {
			out = append(out, l)
		}
	}
	return out
}

const (
	logReconciled      = "relay: reconciliation ran"
	logReconcileFailed = "relay: reconciliation failed; retrying at the next tick"
)

// reconcileUnit is a relay with the fakes, a clock, a log and a registry of
// its own, whose publishing loop the test starts and stops.
type reconcileUnit struct {
	r     *Relay
	st    *fakeStore
	conn  *fakeConn
	rec   *recorder
	clock *fakeClock
	logs  *syncBuffer
	reg   *prometheus.Registry

	cancel context.CancelFunc
	done   chan error
}

func newReconcileUnit(t *testing.T, st *fakeStore) *reconcileUnit {
	t.Helper()
	cfg := unitConfig
	cfg.ReconcileMinInterval = reconcileWindow
	reg := prometheus.NewRegistry()
	m, err := NewMetrics(reg)
	if err != nil {
		t.Fatalf("metrics: %v", err)
	}
	u := &reconcileUnit{
		st: st, conn: &fakeConn{}, rec: &recorder{}, reg: reg,
		clock: &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
		logs:  &syncBuffer{},
	}
	u.conn.open.Store(true)
	u.conn.opens.Store(1) // the first connection: the role declares the topology before the loops start
	u.r = New(st, &fakePublisher{}, u.conn, u.rec, slog.New(slog.NewJSONHandler(u.logs, nil)), cfg,
		func() float64 { return 0.5 }, m)
	u.r.now = u.clock.now
	return u
}

func (u *reconcileUnit) start(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	u.cancel, u.done = cancel, make(chan error, 1)
	go func() { u.done <- u.r.RunPublishing(ctx) }()
	t.Cleanup(func() { u.stop(t) })
}

// stop cancels the loop and waits until it has returned.
func (u *reconcileUnit) stop(t *testing.T) {
	t.Helper()
	if u.done == nil {
		return
	}
	u.cancel()
	if err := <-u.done; err != nil {
		t.Errorf("RunPublishing: %v", err)
	}
	u.done = nil
}

// waitTicks waits until the loop has completed n more ticks. Every tick
// checks reconciliation and then counts the queued rows once, so after two
// more counts at least one whole tick began after the call.
func (u *reconcileUnit) waitTicks(t *testing.T, n int) {
	t.Helper()
	u.st.mu.Lock()
	from := u.st.counts
	u.st.mu.Unlock()
	testutil.Eventually(t, func() bool {
		u.st.mu.Lock()
		defer u.st.mu.Unlock()
		return u.st.counts >= from+n+1
	}, 5_000*time.Millisecond)
}

func (u *reconcileUnit) waitRuns(t *testing.T, n int) {
	t.Helper()
	testutil.Eventually(t, func() bool { return len(u.logs.lines(logReconciled)) >= n }, 5_000*time.Millisecond)
}

// counter returns the value of the counter name on the unit's registry.
func counter(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() == name {
			if len(mf.GetMetric()) != 1 || len(mf.GetMetric()[0].GetLabel()) != 0 {
				t.Fatalf("%s: %d series, want one without labels", name, len(mf.GetMetric()))
			}
			return mf.GetMetric()[0].GetCounter().GetValue()
		}
	}
	t.Fatalf("%s is not registered", name)
	return 0
}

// requireRuns checks the number of completed runs: the store calls, the log
// lines and the counter agree.
func (u *reconcileUnit) requireRuns(t *testing.T, want int) {
	t.Helper()
	if got := u.st.reconcileCalls(); got != want {
		t.Errorf("%d reconciliation calls, want %d", got, want)
	}
	if got := len(u.logs.lines(logReconciled)); got != want {
		t.Errorf("%d reconciliation log lines, want %d", got, want)
	}
	if got := counter(t, u.reg, ReconciliationRunsMetric); got != float64(want) {
		t.Errorf("%s = %v, want %d", ReconciliationRunsMetric, got, want)
	}
}

func queuedEvents(n int) []domain.StatusChanged {
	evs := make([]domain.StatusChanged, n)
	for i := range evs {
		evs[i] = domain.StatusChanged{ID: uuid.New(), Channel: domain.ChannelSMS, Status: domain.StatusPending}
	}
	return evs
}

// The first connection after start-up triggers one run before the first
// cycle publishes; its events are published, its rows counted and one info
// line names the number of rows and the reason.
func TestReconcile_FirstConnectionTriggersOneRun(t *testing.T) {
	evs := queuedEvents(2)
	st := &fakeStore{reconcileEvents: evs}
	u := newReconcileUnit(t, st)
	var lanesBeforeRun atomic.Int64
	lanesBeforeRun.Store(-1)
	st.onReconcile = func(context.Context) { lanesBeforeRun.Store(int64(len(st.laneCalls()))) }
	u.start(t)
	u.waitRuns(t, 1)
	u.waitTicks(t, 3)

	u.requireRuns(t, 1)
	if n := lanesBeforeRun.Load(); n != 0 {
		t.Errorf("%d lanes ran before the first reconciliation, want 0", n)
	}
	if got := counter(t, u.reg, ReconciledRowsMetric); got != 2 {
		t.Errorf("%s = %v, want 2", ReconciledRowsMetric, got)
	}
	if got := u.rec.events(); len(got) < 2 || got[0] != evs[0] || got[1] != evs[1] {
		t.Errorf("events %v, want the reconciled rows' events first", got)
	}
	l := u.logs.lines(logReconciled)[0]
	if l["level"] != "INFO" || l["rows"] != float64(2) || l["reason"] != "first connection" {
		t.Errorf("log line %v, want INFO with rows 2 and reason first connection", l)
	}
}

// A reconnection after the window triggers one more run.
func TestReconcile_ReconnectionTriggersOneRun(t *testing.T) {
	st := &fakeStore{}
	u := newReconcileUnit(t, st)
	u.start(t)
	u.waitRuns(t, 1)

	u.clock.advance(reconcileWindow)
	u.conn.opens.Add(1)
	u.waitRuns(t, 2)
	u.waitTicks(t, 3)
	u.requireRuns(t, 2)
	if l := u.logs.lines(logReconciled)[1]; l["reason"] != "reconnection" || l["rows"] != float64(0) {
		t.Errorf("log line %v, want reason reconnection and rows 0", l)
	}
}

// A trigger inside the window is deferred: the cycles go on meanwhile, and
// it runs exactly once when the window ends, measured from the start of the
// previous run.
func TestReconcile_TriggerInsideWindowIsDeferred(t *testing.T) {
	st := &fakeStore{}
	u := newReconcileUnit(t, st)
	u.start(t)
	u.waitRuns(t, 1)

	u.clock.advance(10_000 * time.Millisecond)
	u.conn.opens.Add(1)
	u.waitTicks(t, 3)
	u.requireRuns(t, 1)

	u.clock.advance(reconcileWindow - 10_000*time.Millisecond - time.Millisecond)
	u.waitTicks(t, 3)
	u.requireRuns(t, 1)

	u.clock.advance(time.Millisecond) // the window ends
	u.waitRuns(t, 2)
	u.waitTicks(t, 3)
	u.requireRuns(t, 2)
	if l := u.logs.lines(logReconciled)[1]; l["reason"] != "reconnection" {
		t.Errorf("deferred run's reason %v, want reconnection", l["reason"])
	}
}

// Three triggers inside one window are one run at its end.
func TestReconcile_ThreeTriggersInOneWindowAreOneRun(t *testing.T) {
	st := &fakeStore{}
	u := newReconcileUnit(t, st)
	u.start(t)
	u.waitRuns(t, 1)

	for range 3 {
		u.clock.advance(5_000 * time.Millisecond)
		u.conn.opens.Add(1)
		u.waitTicks(t, 1)
	}
	u.requireRuns(t, 1)

	u.clock.advance(reconcileWindow)
	u.waitRuns(t, 2)
	u.waitTicks(t, 3)
	u.requireRuns(t, 2)
}

// laneTracker wraps the fake store: it records whether a lane transaction
// (PublishLane) or a reconciliation is in progress, and reports an overlap.
type laneTracker struct {
	*fakeStore
	inLane, inRun atomic.Bool
	overlaps      atomic.Int32
	// triggers raises a (re)connection inside each of the first lanes.
	triggers atomic.Int32
	conn     *fakeConn
}

func (s *laneTracker) PublishLane(ctx context.Context, lane store.Lane, limit int, publish store.PublishFunc) ([]domain.StatusChanged, error) {
	s.inLane.Store(true)
	defer s.inLane.Store(false)
	if s.inRun.Load() {
		s.overlaps.Add(1)
	}
	if s.triggers.Add(-1) >= 0 {
		s.conn.opens.Add(1) // a reconnection while the lane transaction is open
	}
	return s.fakeStore.PublishLane(ctx, lane, limit, publish)
}

func (s *laneTracker) ReconcileQueued(ctx context.Context) ([]domain.StatusChanged, error) {
	s.inRun.Store(true)
	defer s.inRun.Store(false)
	if s.inLane.Load() {
		s.overlaps.Add(1)
	}
	return s.fakeStore.ReconcileQueued(ctx)
}

// No run starts while a lane transaction is open and no lane starts during
// a run, also when the (re)connection happens inside a lane: the run waits
// for the lane to end. (Without a window here, every trigger runs at once.)
func TestReconcile_NeverOverlapsALaneTransaction(t *testing.T) {
	st := &fakeStore{}
	u := newReconcileUnit(t, st)
	tracker := &laneTracker{fakeStore: st, conn: u.conn}
	tracker.triggers.Store(20)
	u.r.store = tracker
	u.r.cfg.ReconcileMinInterval = 0
	st.onReconcile = func(context.Context) {
		// The run is in progress; a lane starting now would overlap.
		testutil.Within(func() bool { return tracker.inLane.Load() }, 20*time.Millisecond)
	}
	u.start(t)
	testutil.Eventually(t, func() bool { return tracker.triggers.Load() < 0 }, 5_000*time.Millisecond)
	u.waitTicks(t, 3)
	u.stop(t)

	if n := tracker.overlaps.Load(); n != 0 {
		t.Errorf("%d overlaps of a reconciliation and a lane transaction", n)
	}
	// The first connection, plus at least one run for the triggers raised
	// inside lanes (several in one cycle are served by one run).
	if n := st.reconcileCalls(); n < 2 {
		t.Errorf("%d runs, want the first one and at least one for the triggers inside lanes", n)
	}
}

// A failed run is logged and retried at the next tick; it is not counted
// and does not open a window: the run that succeeds afterwards is not
// deferred, and the window starts with it.
func TestReconcile_FailedRunIsRetriedAndNotCounted(t *testing.T) {
	st := &fakeStore{reconcileErr: errors.New("database unavailable"), reconcileEvents: queuedEvents(3)}
	u := newReconcileUnit(t, st)
	u.start(t)
	testutil.Eventually(t, func() bool { return st.reconcileCalls() >= 3 }, 5_000*time.Millisecond)

	if got := counter(t, u.reg, ReconciliationRunsMetric); got != 0 {
		t.Errorf("%s = %v after failed runs, want 0", ReconciliationRunsMetric, got)
	}
	if got := counter(t, u.reg, ReconciledRowsMetric); got != 0 {
		t.Errorf("%s = %v after failed runs, want 0", ReconciledRowsMetric, got)
	}
	if len(u.logs.lines(logReconciled)) != 0 {
		t.Error("a failed run was logged as a run")
	}
	for _, e := range u.rec.events() {
		if e.Status == domain.StatusPending {
			t.Fatalf("a failed run published an event: %+v", e)
		}
	}
	failed := u.logs.lines(logReconcileFailed)
	if len(failed) < 3 || failed[0]["level"] != "WARN" {
		t.Errorf("%d failure lines (first %v), want one WARN line per failed try", len(failed), failed)
	}
	if len(st.laneCalls()) == 0 {
		t.Error("the cycles stopped while reconciliation was failing")
	}

	st.mu.Lock()
	st.reconcileErr = nil
	st.mu.Unlock()
	u.waitRuns(t, 1) // same clock reading: no window was opened by the failures
	calls := st.reconcileCalls()
	if got := counter(t, u.reg, ReconciledRowsMetric); got != 3 {
		t.Errorf("%s = %v, want 3", ReconciledRowsMetric, got)
	}

	// The window runs from the successful run.
	u.clock.advance(reconcileWindow - time.Millisecond)
	u.conn.opens.Add(1)
	u.waitTicks(t, 3)
	if got := st.reconcileCalls(); got != calls {
		t.Errorf("%d calls, want %d: a trigger inside the window of the successful run ran", got, calls)
	}
	u.clock.advance(time.Millisecond)
	u.waitRuns(t, 2)
}

// The counters are registered on the given registry, without labels, and a
// second relay in the same process reuses them.
func TestMetrics_RegisteredAndReused(t *testing.T) {
	reg := prometheus.NewRegistry()
	first, err := NewMetrics(reg)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewMetrics(reg)
	if err != nil {
		t.Fatalf("second registration: %v", err)
	}
	first.reconciliationRuns.Inc()
	second.reconciliationRuns.Inc()
	second.reconciledRows.Add(5)
	if got := counter(t, reg, ReconciliationRunsMetric); got != 2 {
		t.Errorf("%s = %v, want 2 (one shared series)", ReconciliationRunsMetric, got)
	}
	if got := counter(t, reg, ReconciledRowsMetric); got != 5 {
		t.Errorf("%s = %v, want 5", ReconciledRowsMetric, got)
	}
}

// Shutdown while a trigger is deferred: the loop returns at once and the
// deferred run never starts. A run that has started when shutdown arrives is
// finished on a context that shutdown does not cancel.
func TestReconcile_ShutdownWhileDeferred(t *testing.T) {
	st := &fakeStore{}
	u := newReconcileUnit(t, st)
	u.start(t)
	u.waitRuns(t, 1)
	u.clock.advance(10_000 * time.Millisecond)
	u.conn.opens.Add(1)
	u.waitTicks(t, 2)
	u.stop(t)
	u.clock.advance(reconcileWindow)
	u.requireRuns(t, 1)

	// Shutdown during a run.
	st2 := &fakeStore{reconcileEvents: queuedEvents(1)}
	u2 := newReconcileUnit(t, st2)
	var runCtxCancelled atomic.Bool
	st2.onReconcile = func(ctx context.Context) {
		u2.cancel() // SIGTERM arrives during the run
		runCtxCancelled.Store(ctx.Err() != nil)
	}
	u2.start(t)
	u2.waitRuns(t, 1)
	u2.stop(t)
	if runCtxCancelled.Load() {
		t.Error("the run's context was cancelled by shutdown")
	}
	u2.requireRuns(t, 1)
	if len(st2.laneCalls()) != 0 {
		t.Error("a lane started after shutdown")
	}
}
