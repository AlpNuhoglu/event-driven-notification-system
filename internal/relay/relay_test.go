package relay

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/queue"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

// laneCall records one PublishLane call of the fake store.
type laneCall struct {
	lane  store.Lane
	limit int
}

// fakeStore returns one due row per lane and records the calls.
type fakeStore struct {
	mu          sync.Mutex
	queued      map[store.Lane]int
	countErr    error
	laneErr     map[store.Lane]error
	counts      int
	calls       []laneCall
	onLane      func(ctx context.Context, lane store.Lane) // runs before the publish
	sweepLimit  int
	sweepEvents []domain.StatusChanged
	sweeps      atomic.Int32
}

func (f *fakeStore) QueuedCounts(context.Context) (map[store.Lane]int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.counts++
	return f.queued, f.countErr
}

func (f *fakeStore) PublishLane(ctx context.Context, lane store.Lane, limit int, publish store.PublishFunc) ([]domain.StatusChanged, error) {
	f.mu.Lock()
	f.calls = append(f.calls, laneCall{lane, limit})
	err := f.laneErr[lane]
	onLane := f.onLane
	f.mu.Unlock()
	if onLane != nil {
		onLane(ctx, lane)
	}
	if err != nil {
		return nil, err
	}
	row := store.DueRow{ID: uuid.New(), TraceParent: "tp-" + string(lane.Channel)}
	published, err := publish(ctx, []store.DueRow{row})
	if err != nil {
		return nil, err
	}
	evs := make([]domain.StatusChanged, len(published))
	for i, id := range published {
		evs[i] = domain.StatusChanged{ID: id, Channel: lane.Channel, Priority: lane.Priority, Status: domain.StatusQueued}
	}
	return evs, nil
}

func (f *fakeStore) SweepExpiredLeases(_ context.Context, limit int, jitter func() float64) ([]domain.StatusChanged, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sweepLimit = limit
	_ = jitter()
	f.sweeps.Add(1)
	return f.sweepEvents, nil
}

func (f *fakeStore) laneCalls() []laneCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.calls)
}

// fakePublisher confirms every message and records them.
type fakePublisher struct {
	mu      sync.Mutex
	msgs    []queue.Message
	timeout time.Duration
}

func (p *fakePublisher) PublishBatch(_ context.Context, msgs []queue.Message, confirmTimeout time.Duration) ([]uuid.UUID, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.msgs = append(p.msgs, msgs...)
	p.timeout = confirmTimeout
	ids := make([]uuid.UUID, len(msgs))
	for i, m := range msgs {
		ids[i] = m.ID
	}
	return ids, nil
}

type fakeConn struct{ open atomic.Bool }

func (c *fakeConn) IsOpen() bool { return c.open.Load() }

// recorder is an events.Publisher that keeps the events.
type recorder struct {
	mu  sync.Mutex
	evs []domain.StatusChanged
}

func (r *recorder) Publish(e domain.StatusChanged) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.evs = append(r.evs, e)
}

func (r *recorder) events() []domain.StatusChanged {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.evs)
}

var unitConfig = Config{
	TickInterval:       5 * time.Millisecond,
	BatchSize:          200,
	ConfirmTimeout:     1234 * time.Millisecond,
	HWMHigh:            500,
	HWMNormal:          1000,
	HWMLow:             500,
	LeaseSweepInterval: 5 * time.Millisecond,
	LeaseSweepBatch:    77,
}

func newUnit(st *fakeStore, cfg Config) (*Relay, *fakePublisher, *fakeConn, *recorder) {
	pub := &fakePublisher{}
	conn := &fakeConn{}
	conn.open.Store(true)
	rec := &recorder{}
	r := New(st, pub, conn, rec, slog.New(slog.DiscardHandler), cfg, func() float64 { return 0.5 })
	return r, pub, conn, rec
}

func lanesOf(calls []laneCall) []store.Lane {
	out := make([]store.Lane, len(calls))
	for i, c := range calls {
		out[i] = c.lane
	}
	return out
}

func TestCycle_LaneOrderHighFirstWithinEachChannel(t *testing.T) {
	st := &fakeStore{}
	r, _, _, _ := newUnit(st, unitConfig)
	r.cycle(context.Background())

	want := []store.Lane{
		{Channel: domain.ChannelSMS, Priority: domain.PriorityHigh},
		{Channel: domain.ChannelSMS, Priority: domain.PriorityNormal},
		{Channel: domain.ChannelSMS, Priority: domain.PriorityLow},
		{Channel: domain.ChannelEmail, Priority: domain.PriorityHigh},
		{Channel: domain.ChannelEmail, Priority: domain.PriorityNormal},
		{Channel: domain.ChannelEmail, Priority: domain.PriorityLow},
		{Channel: domain.ChannelPush, Priority: domain.PriorityHigh},
		{Channel: domain.ChannelPush, Priority: domain.PriorityNormal},
		{Channel: domain.ChannelPush, Priority: domain.PriorityLow},
	}
	if got := lanesOf(st.laneCalls()); !slices.Equal(got, want) {
		t.Errorf("lane order:\n got %v\nwant %v", got, want)
	}
	if st.counts != 1 {
		t.Errorf("%d count queries in one cycle, want 1", st.counts)
	}
}

func TestCycle_HeadroomArithmetic(t *testing.T) {
	sms := func(p domain.Priority) store.Lane { return store.Lane{Channel: domain.ChannelSMS, Priority: p} }
	email := func(p domain.Priority) store.Lane { return store.Lane{Channel: domain.ChannelEmail, Priority: p} }
	st := &fakeStore{queued: map[store.Lane]int{
		sms(domain.PriorityHigh):     499,  // headroom 1
		sms(domain.PriorityNormal):   1000, // full: skipped
		sms(domain.PriorityLow):      600,  // above the mark: skipped
		email(domain.PriorityHigh):   200,  // headroom 300: limited by the batch size
		email(domain.PriorityNormal): 850,  // headroom 150
		// email low, all of push: nothing queued, so the batch size
	}}
	cfg := unitConfig
	r, _, _, _ := newUnit(st, cfg)
	r.cycle(context.Background())

	want := []laneCall{
		{sms(domain.PriorityHigh), 1},
		{email(domain.PriorityHigh), 200},
		{email(domain.PriorityNormal), 150},
		{email(domain.PriorityLow), 200},
		{store.Lane{Channel: domain.ChannelPush, Priority: domain.PriorityHigh}, 200},
		{store.Lane{Channel: domain.ChannelPush, Priority: domain.PriorityNormal}, 200},
		{store.Lane{Channel: domain.ChannelPush, Priority: domain.PriorityLow}, 200},
	}
	if got := st.laneCalls(); !slices.Equal(got, want) {
		t.Errorf("lane calls:\n got %v\nwant %v", got, want)
	}

	// The marks are configured per priority.
	st2 := &fakeStore{}
	cfg.BatchSize = 1000
	cfg.HWMHigh, cfg.HWMNormal, cfg.HWMLow = 3, 7, 11
	r2, _, _, _ := newUnit(st2, cfg)
	r2.cycle(context.Background())
	for _, c := range st2.laneCalls() {
		want := map[domain.Priority]int{domain.PriorityHigh: 3, domain.PriorityNormal: 7, domain.PriorityLow: 11}[c.lane.Priority]
		if c.limit != want {
			t.Errorf("%v: limit %d, want %d", c.lane, c.limit, want)
		}
	}
}

func TestCycle_NoTransactionWhileConnectionDown(t *testing.T) {
	st := &fakeStore{}
	r, pub, conn, _ := newUnit(st, unitConfig)
	conn.open.Store(false)
	r.cycle(context.Background())
	if st.counts != 0 || len(st.laneCalls()) != 0 || len(pub.msgs) != 0 {
		t.Fatalf("connection down: %d count queries, %d lane transactions, %d messages; want none",
			st.counts, len(st.laneCalls()), len(pub.msgs))
	}

	// The connection drops in the middle of a cycle: no further lane starts.
	conn.open.Store(true)
	st.onLane = func(_ context.Context, lane store.Lane) {
		if lane.Channel == domain.ChannelSMS && lane.Priority == domain.PriorityNormal {
			conn.open.Store(false)
		}
	}
	r.cycle(context.Background())
	if got := len(st.laneCalls()); got != 2 {
		t.Errorf("%d lane transactions after the connection dropped in the second, want 2", got)
	}
}

func TestCycle_LaneErrorIsIsolated(t *testing.T) {
	failing := store.Lane{Channel: domain.ChannelEmail, Priority: domain.PriorityNormal}
	st := &fakeStore{laneErr: map[store.Lane]error{failing: errors.New("lane failed")}}
	r, pub, _, rec := newUnit(st, unitConfig)
	r.cycle(context.Background())

	if got := len(st.laneCalls()); got != 9 {
		t.Fatalf("%d lanes visited, want all 9", got)
	}
	evs := rec.events()
	if len(evs) != 8 {
		t.Fatalf("%d events, want 8 (one per successful lane)", len(evs))
	}
	for _, e := range evs {
		if e.Channel == failing.Channel && e.Priority == failing.Priority {
			t.Errorf("event from the failing lane: %+v", e)
		}
	}
	// Each message carries its lane's channel, priority and the row's trace
	// parent, and the configured confirm timeout is passed on.
	for _, m := range pub.msgs {
		if m.TraceParent != "tp-"+string(m.Channel) {
			t.Errorf("message %s: trace parent %q", m.ID, m.TraceParent)
		}
	}
	if pub.timeout != unitConfig.ConfirmTimeout {
		t.Errorf("confirm timeout %s, want %s", pub.timeout, unitConfig.ConfirmTimeout)
	}
	for i, e := range evs {
		if e.ID != pub.msgs[i].ID || e.Priority != pub.msgs[i].Priority || e.Channel != pub.msgs[i].Channel {
			t.Errorf("event %d does not match message %d", i, i)
		}
	}
}

func TestCycle_CountErrorStartsNoLane(t *testing.T) {
	st := &fakeStore{countErr: errors.New("count failed")}
	r, _, _, _ := newUnit(st, unitConfig)
	r.cycle(context.Background())
	if len(st.laneCalls()) != 0 {
		t.Error("a lane started after the count query failed")
	}
}

// On shutdown the current lane transaction is finished on a context that is
// not cancelled, and no further lane starts.
func TestCycle_ShutdownFinishesCurrentLane(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var laneCtxErr error
	st := &fakeStore{onLane: func(laneCtx context.Context, _ store.Lane) {
		cancel() // SIGTERM arrives during the first lane
		laneCtxErr = laneCtx.Err()
	}}
	r, pub, _, rec := newUnit(st, unitConfig)
	r.cycle(ctx)

	if got := len(st.laneCalls()); got != 1 {
		t.Fatalf("%d lanes started, want 1", got)
	}
	if laneCtxErr != nil {
		t.Errorf("the lane's context was cancelled by shutdown: %v", laneCtxErr)
	}
	if len(pub.msgs) != 1 || len(rec.events()) != 1 {
		t.Errorf("the current lane did not finish: %d messages, %d events", len(pub.msgs), len(rec.events()))
	}

	st2 := &fakeStore{}
	r2, _, _, _ := newUnit(st2, unitConfig)
	r2.cycle(ctx) // already cancelled
	if st2.counts != 0 {
		t.Error("a cycle started after shutdown")
	}
}

func TestRunPublishing_TicksUntilCancelled(t *testing.T) {
	st := &fakeStore{}
	r, _, _, _ := newUnit(st, unitConfig)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.RunPublishing(ctx) }()
	testutil.Eventually(t, func() bool { return len(st.laneCalls()) >= 18 }, 5*time.Second) // two cycles
	cancel()
	if err := <-done; err != nil {
		t.Errorf("RunPublishing: %v", err)
	}
}

func TestRunLeaseSweeper_ForwardsEvents(t *testing.T) {
	ev := domain.StatusChanged{ID: uuid.New(), Status: domain.StatusRetrying}
	st := &fakeStore{sweepEvents: []domain.StatusChanged{ev}}
	r, _, _, rec := newUnit(st, unitConfig)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.RunLeaseSweeper(ctx) }()
	testutil.Eventually(t, func() bool { return st.sweeps.Load() >= 2 }, 5*time.Second)
	cancel()
	if err := <-done; err != nil {
		t.Errorf("RunLeaseSweeper: %v", err)
	}
	if st.sweepLimit != unitConfig.LeaseSweepBatch {
		t.Errorf("sweep limit %d, want %d", st.sweepLimit, unitConfig.LeaseSweepBatch)
	}
	evs := rec.events()
	if len(evs) < 2 || evs[0] != ev {
		t.Errorf("events %v, want the sweeper's event once per pass", evs)
	}
}

func TestRun_GiveUpEndsWithError(t *testing.T) {
	r, _, _, _ := newUnit(&fakeStore{}, unitConfig)
	gaveUp := make(chan struct{})
	close(gaveUp)
	cause := errors.New("connection refused")
	err := run(context.Background(), r, gaveUp, func() error { return cause })
	if err == nil || !errors.Is(err, cause) {
		t.Errorf("run after give-up: %v, want an error wrapping the cause", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := run(ctx, r, make(chan struct{}), func() error { return nil }); err != nil {
		t.Errorf("run after shutdown: %v, want nil", err)
	}
}
