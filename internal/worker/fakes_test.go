package worker

import (
	"bytes"
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
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

const (
	ms          = time.Millisecond
	waitTimeout = 5 * time.Second

	prioHigh   uint8 = 6
	prioNormal uint8 = 5
	prioLow    uint8 = 4

	testRecipient = "+905551234567"
	testContent   = "Your code is 4821"
)

var t0 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

// fakeClock only moves when a test advances it. Waiters counts the
// goroutines blocked in WaitUntil, so a test can wait until the code under
// test is blocked on the clock before advancing it.
type fakeClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []*clockWaiter
	// runCtx is the context given to Run. The pacer loop waits on it
	// directly; dispatches and the drain timer wait on derived contexts.
	runCtx context.Context
}

type clockWaiter struct {
	at    time.Time
	pacer bool
	fired chan struct{}
}

func newFakeClock() *fakeClock { return &fakeClock{now: t0} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) WaitUntil(ctx context.Context, t time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	if !t.After(c.now) {
		c.mu.Unlock()
		return nil
	}
	wt := &clockWaiter{at: t, pacer: ctx == c.runCtx, fired: make(chan struct{})}
	c.waiters = append(c.waiters, wt)
	c.mu.Unlock()
	select {
	case <-wt.fired:
		return nil
	case <-ctx.Done():
		c.mu.Lock()
		c.waiters = slices.DeleteFunc(c.waiters, func(x *clockWaiter) bool { return x == wt })
		c.mu.Unlock()
		return ctx.Err()
	}
}

// AdvanceTo sets the time to t and wakes every waiter due by then. A woken
// waiter leaves the count before AdvanceTo returns.
func (c *fakeClock) AdvanceTo(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
	c.waiters = slices.DeleteFunc(c.waiters, func(w *clockWaiter) bool {
		if w.at.After(t) {
			return false
		}
		close(w.fired)
		return true
	})
}

func (c *fakeClock) Advance(d time.Duration) { c.AdvanceTo(c.Now().Add(d)) }

func (c *fakeClock) Waiters() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.waiters)
}

// state returns the number of waiters and whether the pacer loop is one.
func (c *fakeClock) state() (waiters int, pacer bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, w := range c.waiters {
		pacer = pacer || w.pacer
	}
	return len(c.waiters), pacer
}

// Next returns the earliest time a waiter waits for.
func (c *fakeClock) Next() (time.Time, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.waiters) == 0 {
		return time.Time{}, false
	}
	next := c.waiters[0].at
	for _, w := range c.waiters[1:] {
		if w.at.Before(next) {
			next = w.at
		}
	}
	return next, true
}

// trace is the ordered record of calls across the fakes.
type trace struct {
	mu    sync.Mutex
	calls []string
}

func (t *trace) add(s string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.calls = append(t.calls, s)
}

func (t *trace) get() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return slices.Clone(t.calls)
}

type fakeMessage struct {
	id    uuid.UUID
	prio  uint8
	acks  atomic.Int32
	nacks atomic.Int32
}

func (m *fakeMessage) NotificationID() uuid.UUID { return m.id }
func (m *fakeMessage) Priority() uint8           { return m.prio }
func (m *fakeMessage) Epoch() uint64             { return 1 }
func (m *fakeMessage) Ack() error                { m.acks.Add(1); return nil }
func (m *fakeMessage) Nack() error               { m.nacks.Add(1); return nil }

type reserveAnswer struct {
	wait time.Duration
	ok   bool
	err  error
}

// fakeLimiter answers from its script first. After the script it behaves as
// GCRA with zero burst (when interval is set: slots interval apart, nothing
// reserved beyond horizon) or reserves at once (interval zero).
type fakeLimiter struct {
	clk *fakeClock
	tr  *trace

	mu       sync.Mutex
	script   []reserveAnswer
	interval time.Duration
	horizon  time.Duration
	tat      time.Time
	calls    []time.Time
	slots    []time.Time
}

func (l *fakeLimiter) Reserve(context.Context, domain.Channel) (time.Duration, bool, error) {
	l.tr.add("reserve")
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.clk.Now()
	l.calls = append(l.calls, now)
	if len(l.script) > 0 {
		a := l.script[0]
		l.script = l.script[1:]
		if a.ok && a.err == nil {
			l.slots = append(l.slots, now.Add(a.wait))
		}
		return a.wait, a.ok, a.err
	}
	if l.interval == 0 {
		l.slots = append(l.slots, now)
		return 0, true, nil
	}
	start := l.tat
	if start.Before(now) {
		start = now
	}
	wait := start.Sub(now)
	if wait > l.horizon {
		return 0, false, nil
	}
	l.tat = start.Add(l.interval)
	l.slots = append(l.slots, start)
	return wait, true, nil
}

func (l *fakeLimiter) setScript(a ...reserveAnswer) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.script = a
}

func (l *fakeLimiter) callTimes() []time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.calls)
}

func (l *fakeLimiter) granted() []time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.slots)
}

type fakeBreaker struct {
	tr *trace
	mu sync.Mutex
	// answers are returned in order, to Ready and Admit alike; after them,
	// both allow.
	answers []time.Time // a non-zero time means "not allowed until"
	// openUntil is what every permit's Record returns.
	openUntil time.Time
	records   []permitRecord
}

type permitRecord struct {
	outcome    domain.Outcome
	retryAfter time.Duration
}

func (b *fakeBreaker) allow() (bool, time.Time) {
	b.tr.add("allow")
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.answers) == 0 {
		return true, time.Time{}
	}
	until := b.answers[0]
	b.answers = b.answers[1:]
	return until.IsZero(), until
}

func (b *fakeBreaker) Ready() (bool, time.Time) { return b.allow() }

func (b *fakeBreaker) Admit() (Permit, bool, time.Time) {
	ok, until := b.allow()
	return fakePermit{b}, ok, until
}

func (b *fakeBreaker) recorded() []permitRecord {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Clone(b.records)
}

type fakePermit struct{ b *fakeBreaker }

func (p fakePermit) Record(outcome domain.Outcome, retryAfter time.Duration) time.Time {
	p.b.mu.Lock()
	defer p.b.mu.Unlock()
	p.b.records = append(p.b.records, permitRecord{outcome, retryAfter})
	return p.b.openUntil
}

type claimRecord struct {
	id uuid.UUID
	at time.Time
}

type finalRecord struct {
	id        uuid.UUID
	attemptNo int
	provider  string
	result    store.AttemptResult
	jitter    float64
	delay     time.Duration // AbortAttempt only
	at        time.Time
}

// fakeStore claims every id unless claimFn says otherwise. claimDelay makes
// a claim take time on the fake clock.
type fakeStore struct {
	clk *fakeClock
	tr  *trace

	claimDelay func(id uuid.UUID) time.Duration
	claimErr   func(id uuid.UUID) error
	expired    func(id uuid.UUID) bool
	finalErr   error

	mu        sync.Mutex
	claims    []claimRecord
	sent      []finalRecord
	failures  []finalRecord
	aborts    []finalRecord
	active    int
	maxActive int
}

func (s *fakeStore) Claim(ctx context.Context, id uuid.UUID, lease time.Duration, provider string) (store.Claim, domain.StatusChanged, error) {
	_, _ = lease, provider
	s.tr.add("claim " + id.String())
	s.mu.Lock()
	s.claims = append(s.claims, claimRecord{id: id, at: s.clk.Now()})
	s.mu.Unlock()
	if s.claimDelay != nil {
		if err := s.clk.WaitUntil(ctx, s.clk.Now().Add(s.claimDelay(id))); err != nil {
			return store.Claim{}, domain.StatusChanged{}, err
		}
	}
	if s.claimErr != nil {
		if err := s.claimErr(id); err != nil {
			return store.Claim{}, domain.StatusChanged{}, err
		}
	}
	n := store.Notification{
		ID:           id,
		Channel:      domain.ChannelSMS,
		Recipient:    domain.Recipient(testRecipient),
		Content:      testContent,
		Status:       domain.StatusProcessing,
		AttemptCount: 3,
		TraceParent:  "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
	}
	if s.expired != nil && s.expired(id) {
		n.Status = domain.StatusFailed
		return store.Claim{Notification: n, Expired: true}, n.Event(), nil
	}
	s.mu.Lock()
	s.active++
	s.maxActive = max(s.maxActive, s.active)
	s.mu.Unlock()
	return store.Claim{Notification: n}, n.Event(), nil
}

func (s *fakeStore) record(list *[]finalRecord, r finalRecord) (domain.StatusChanged, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r.at = s.clk.Now()
	*list = append(*list, r)
	s.active--
	if s.finalErr != nil {
		return domain.StatusChanged{}, s.finalErr
	}
	return domain.StatusChanged{ID: r.id, Channel: domain.ChannelSMS, Status: domain.StatusRetrying}, nil
}

func (s *fakeStore) CompleteSent(_ context.Context, id uuid.UUID, attemptNo int, provider string, r store.AttemptResult) (store.Sent, domain.StatusChanged, error) {
	ev, err := s.record(&s.sent, finalRecord{id: id, attemptNo: attemptNo, provider: provider, result: r})
	if err != nil {
		return store.Sent{}, ev, err
	}
	return store.Sent{From: domain.StatusProcessing}, ev, nil
}

func (s *fakeStore) CompleteFailure(_ context.Context, id uuid.UUID, attemptNo int, r store.AttemptResult, jitter float64) (domain.StatusChanged, error) {
	return s.record(&s.failures, finalRecord{id: id, attemptNo: attemptNo, result: r, jitter: jitter})
}

func (s *fakeStore) AbortAttempt(_ context.Context, id uuid.UUID, attemptNo int, delay time.Duration) (domain.StatusChanged, error) {
	return s.record(&s.aborts, finalRecord{id: id, attemptNo: attemptNo, delay: delay})
}

func (s *fakeStore) snapshot() (claims []claimRecord, sent, failures, aborts []finalRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.claims), slices.Clone(s.sent), slices.Clone(s.failures), slices.Clone(s.aborts)
}

func (s *fakeStore) finals() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sent) + len(s.failures) + len(s.aborts)
}

type sendRecord struct {
	req Request
	at  time.Time
}

// fakeProvider answers 202 unless respond says otherwise. latency makes a
// send take time on the fake clock.
type fakeProvider struct {
	clk *fakeClock
	tr  *trace

	latency time.Duration
	respond func(req Request) Response

	mu        sync.Mutex
	sends     []sendRecord
	active    int
	maxActive int
}

func (p *fakeProvider) Name() string { return "fake" }

func (p *fakeProvider) Send(ctx context.Context, req Request) Response {
	p.tr.add("send " + req.NotificationID.String())
	p.mu.Lock()
	p.sends = append(p.sends, sendRecord{req: req, at: p.clk.Now()})
	p.active++
	p.maxActive = max(p.maxActive, p.active)
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		p.active--
		p.mu.Unlock()
	}()
	if p.latency > 0 {
		if err := p.clk.WaitUntil(ctx, p.clk.Now().Add(p.latency)); err != nil {
			return Response{Err: err}
		}
	}
	if p.respond != nil {
		return p.respond(req)
	}
	return Response{StatusCode: 202, MessageID: "pm-1", Latency: p.latency}
}

func (p *fakeProvider) sent() []sendRecord {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.sends)
}

type fakePublisher struct {
	mu     sync.Mutex
	events []domain.StatusChanged
}

func (p *fakePublisher) Publish(e domain.StatusChanged) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, e)
}

func (p *fakePublisher) published() []domain.StatusChanged {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.events)
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type harness struct {
	t    *testing.T
	clk  *fakeClock
	tr   *trace
	lim  *fakeLimiter
	br   *fakeBreaker
	st   *fakeStore
	prov *fakeProvider
	pub  *fakePublisher
	logs *syncBuffer
	cfg  Config
	w    *Worker

	cancel context.CancelFunc
	done   chan struct{}
}

// newHarness builds a worker on fakes with the production defaults on the
// fake clock (send deadline 15 s) and concurrency 1. Tests change h.cfg and
// the fakes before start.
func newHarness(t *testing.T) *harness {
	t.Helper()
	clk := newFakeClock()
	tr := &trace{}
	h := &harness{
		t:    t,
		clk:  clk,
		tr:   tr,
		lim:  &fakeLimiter{clk: clk, tr: tr},
		br:   &fakeBreaker{tr: tr},
		st:   &fakeStore{clk: clk, tr: tr},
		prov: &fakeProvider{clk: clk, tr: tr},
		pub:  &fakePublisher{},
		logs: &syncBuffer{},
		cfg: Config{
			Channel:       domain.ChannelSMS,
			Concurrency:   1,
			Lease:         30 * time.Second,
			HTTPTimeout:   10 * time.Second,
			SlotTolerance: 5 * ms,
			RetryInterval: 50 * ms,
			Drain:         15 * time.Second,
			WriteMargin:   5 * time.Second,

			ClaimRetryBase: 100 * ms,
			ClaimRetryMax:  5 * time.Second,
		},
	}
	h.build()
	return h
}

// build (re)creates the worker from h.cfg; call it after changing h.cfg.
func (h *harness) build() {
	h.t.Helper()
	w, err := New(h.cfg, Deps{
		Store:    h.st,
		Limiter:  h.lim,
		Breaker:  h.br,
		Provider: h.prov,
		Events:   h.pub,
		Clock:    h.clk,
		Jitter:   func() float64 { return 0.25 },
		Logger:   slog.New(slog.NewJSONHandler(h.logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})
	if err != nil {
		h.t.Fatal(err)
	}
	h.w = w
}

func (h *harness) push(prio uint8) *fakeMessage {
	m := &fakeMessage{id: uuid.New(), prio: prio}
	h.w.Push(m)
	return m
}

func (h *harness) start() {
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	h.clk.mu.Lock()
	h.clk.runCtx = ctx
	h.clk.mu.Unlock()
	h.done = make(chan struct{})
	go func() {
		defer close(h.done)
		h.w.Run(ctx)
	}()
	h.t.Cleanup(h.stop)
}

func (h *harness) returned() bool {
	select {
	case <-h.done:
		return true
	default:
		return false
	}
}

// stop cancels Run and moves the clock on until it returns.
func (h *harness) stop() {
	h.cancel()
	testutil.Eventually(h.t, func() bool {
		if h.returned() {
			return true
		}
		h.clk.Advance(time.Second)
		return false
	}, waitTimeout)
}

// eventually waits for cond with the package's timeout.
func (h *harness) eventually(cond func() bool) {
	h.t.Helper()
	testutil.Eventually(h.t, cond, waitTimeout)
}

// waitBlocked waits until n goroutines are blocked on the clock.
func (h *harness) waitBlocked(n int) {
	h.t.Helper()
	h.eventually(func() bool { return h.clk.Waiters() == n })
}

// settle waits until the code under test can only move on when the clock
// does: every goroutine holding a concurrency slot is blocked on the clock,
// and the pacer loop is either one of them or about to block on a full
// semaphore. It holds because every clock waiter holds a slot (the pacer in
// steps 2 to 5, a dispatch until it ends) and waiters only leave when the
// test advances the clock: once the waiters counted first equal the slots
// held a moment later, all holders wait. Valid while the buffer is not empty
// (a pacer blocked on an empty buffer holds a slot without waiting on the
// clock) and before shutdown (the drain timer waits without a slot).
func (h *harness) settle() {
	h.t.Helper()
	h.eventually(func() bool {
		waiting, pacer := h.clk.state()
		held := len(h.w.sem)
		return waiting == held && (pacer || held == h.cfg.Concurrency)
	})
}

// runUntil advances the clock waiter by waiter, settling before each step,
// until the next waiter lies after end or there is none.
func (h *harness) runUntil(end time.Time) {
	h.t.Helper()
	for {
		h.settle()
		next, ok := h.clk.Next()
		if !ok || next.After(end) {
			return
		}
		h.clk.AdvanceTo(next)
	}
}

var errDatabase = errors.New("database unavailable")

// perCall returns a function for fakeStore.claimDelay or claimErr that
// answers the calls for an id in order from script[id], and after them (or
// for an id without a script) with after.
func perCall[T any](after T, script map[uuid.UUID][]T) func(uuid.UUID) T {
	var (
		mu    sync.Mutex
		calls = map[uuid.UUID]int{}
	)
	return func(id uuid.UUID) T {
		mu.Lock()
		defer mu.Unlock()
		n := calls[id]
		calls[id]++
		if n < len(script[id]) {
			return script[id][n]
		}
		return after
	}
}
