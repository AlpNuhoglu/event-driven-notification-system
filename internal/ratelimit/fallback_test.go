package ratelimit

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

const (
	testTimeout = 40 * time.Millisecond
	testProbe   = 1000 * time.Millisecond
	testWait    = 5000 * time.Millisecond
	redisWait   = 7 * time.Millisecond // the wait the fake Redis grants
)

var errRedisDown = errors.New("dial tcp: connection refused")

// fakePrimary stands in for the Redis limiter. While down it fails every
// call; while blocked a call waits for the release or for its context.
type fakePrimary struct {
	calls    atomic.Int64
	inflight atomic.Int64
	maxIn    atomic.Int64
	down     atomic.Bool

	mu        sync.Mutex
	block     chan struct{} // non-nil: calls wait until it is closed
	deadlines []time.Duration
}

func (p *fakePrimary) Reserve(ctx context.Context, _ domain.Channel) (time.Duration, bool, error) {
	p.calls.Add(1)
	n := p.inflight.Add(1)
	defer p.inflight.Add(-1)
	for {
		m := p.maxIn.Load()
		if n <= m || p.maxIn.CompareAndSwap(m, n) {
			break
		}
	}
	p.mu.Lock()
	if d, ok := ctx.Deadline(); ok {
		p.deadlines = append(p.deadlines, time.Until(d))
	}
	block := p.block
	p.mu.Unlock()
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return 0, false, ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	if p.down.Load() {
		return 0, false, errRedisDown
	}
	return redisWait, true, nil
}

func (p *fakePrimary) setBlock(b chan struct{}) {
	p.mu.Lock()
	p.block = b
	p.mu.Unlock()
}

type fallbackEnv struct {
	primary *fakePrimary
	clock   *fakeClock
	reg     *prometheus.Registry
	logs    *syncBuffer
	f       *Fallback
}

func newFallbackEnv(t *testing.T) *fallbackEnv {
	t.Helper()
	e := &fallbackEnv{primary: &fakePrimary{}, clock: newFakeClock(), reg: prometheus.NewRegistry(), logs: &syncBuffer{}}
	f, err := NewFallback(e.primary, FallbackConfig{
		PerSecond: 98, Replicas: 2, Horizon: time.Hour,
		Timeout: testTimeout, ProbeInterval: testProbe,
		Registry: e.reg, Logger: slog.New(slog.NewJSONHandler(e.logs, nil)), Now: e.clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	e.f = f
	return e
}

func (e *fallbackEnv) reserve(t *testing.T) time.Duration {
	t.Helper()
	wait, ok, err := e.f.Reserve(context.Background(), domain.ChannelSMS)
	if err != nil || !ok {
		t.Fatalf("reserve: ok %v, error %v", ok, err)
	}
	return wait
}

// gauge reads limiter_fallback_active from the registry.
func (e *fallbackEnv) gauge(t *testing.T) float64 {
	t.Helper()
	mfs, err := e.reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() == FallbackMetric {
			return mf.GetMetric()[0].GetGauge().GetValue()
		}
	}
	t.Fatalf("%s is not registered", FallbackMetric)
	return -1
}

// logLines counts the log lines at level that contain msg.
func (e *fallbackEnv) logLines(level, msg string) int {
	n := 0
	for _, line := range strings.Split(e.logs.String(), "\n") {
		if strings.Contains(line, `"level":"`+level+`"`) && strings.Contains(line, msg) {
			n++
		}
	}
	return n
}

func (e *fallbackEnv) state(t *testing.T, active bool) {
	t.Helper()
	want := 0.0
	if active {
		want = 1
	}
	if e.f.Active() != active || e.gauge(t) != want {
		t.Fatalf("active %v, gauge %v; want %v, %v", e.f.Active(), e.gauge(t), active, want)
	}
}

func TestFallback_Validation(t *testing.T) {
	ok := FallbackConfig{
		PerSecond: 98, Replicas: 1, Horizon: time.Second, Timeout: time.Second, ProbeInterval: time.Second,
		Registry: prometheus.NewRegistry(), Logger: slog.New(slog.DiscardHandler),
	}
	if _, err := NewFallback(&fakePrimary{}, ok); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	for name, mutate := range map[string]func(*FallbackConfig){
		"zero timeout":  func(c *FallbackConfig) { c.Timeout = 0 },
		"zero probe":    func(c *FallbackConfig) { c.ProbeInterval = 0 },
		"zero replicas": func(c *FallbackConfig) { c.Replicas = 0 },
		"no registry":   func(c *FallbackConfig) { c.Registry = nil },
		"no logger":     func(c *FallbackConfig) { c.Logger = nil },
	} {
		c := ok
		c.Registry = prometheus.NewRegistry()
		mutate(&c)
		if _, err := NewFallback(&fakePrimary{}, c); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	if _, err := NewFallback(nil, ok); err == nil {
		t.Error("nil primary accepted")
	}
}

// While Redis answers, its slots are used and the gauge is 0.
func TestFallback_UsesPrimaryWhileItAnswers(t *testing.T) {
	e := newFallbackEnv(t)
	for range 3 {
		if got := e.reserve(t); got != redisWait {
			t.Fatalf("wait %s, want the primary's %s", got, redisWait)
		}
	}
	if e.primary.calls.Load() != 3 {
		t.Errorf("primary called %d times, want 3", e.primary.calls.Load())
	}
	e.state(t, false)
	for _, d := range e.primary.deadlines {
		if d > testTimeout {
			t.Errorf("primary call bounded by %s, more than the timeout %s", d, testTimeout)
		}
	}
}

// An error switches to the fallback, and the same call is answered locally:
// the caller sees no error.
func TestFallback_ErrorSwitchesAndAnswersLocally(t *testing.T) {
	e := newFallbackEnv(t)
	e.primary.down.Store(true)
	if got := e.reserve(t); got != 0 {
		t.Errorf("first local wait %s, want 0", got)
	}
	e.state(t, true)
	// The local schedule continues at 98/2 per second.
	if got := e.reserve(t); got != 20409*time.Microsecond {
		t.Errorf("second local wait %s, want 20.409ms", got)
	}
	if n := e.logLines("WARN", "in-process fallback"); n != 1 {
		t.Errorf("%d warn lines, want 1:\n%s", n, e.logs.String())
	}
}

// A primary that hangs is abandoned at the timeout and the call is answered
// locally.
func TestFallback_HangingPrimaryCostsTheTimeout(t *testing.T) {
	e := newFallbackEnv(t)
	release := make(chan struct{})
	defer close(release)
	e.primary.setBlock(release)
	start := time.Now()
	e.reserve(t)
	if took := time.Since(start); took < testTimeout {
		t.Errorf("returned after %s, before the timeout %s", took, testTimeout)
	}
	e.state(t, true)
}

// The caller's own cancelled context does not switch.
func TestFallback_CancelledCallerDoesNotSwitch(t *testing.T) {
	e := newFallbackEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, ok, err := e.f.Reserve(ctx, domain.ChannelSMS); ok || err == nil {
		t.Fatalf("cancelled caller: ok %v, error %v", ok, err)
	}
	e.state(t, false)

	// Cancelled while the call waits on the primary.
	release := make(chan struct{})
	e.primary.setBlock(release)
	ctx, cancel = context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, _, err := e.f.Reserve(ctx, domain.ChannelSMS); done <- err }()
	testutil.Eventually(t, func() bool { return e.primary.inflight.Load() == 1 }, testWait)
	cancel()
	if err := <-done; err == nil {
		t.Fatal("cancelled during the call: no error")
	}
	close(release)
	e.state(t, false)
	if n := e.logLines("WARN", "in-process fallback"); n != 0 {
		t.Errorf("%d warn lines, want 0", n)
	}
}

// While active, the primary is asked at most once per probe interval; the
// other calls are answered locally.
func TestFallback_ProbesOncePerInterval(t *testing.T) {
	e := newFallbackEnv(t)
	e.primary.down.Store(true)
	e.reserve(t) // switches; the next probe is due one interval later
	for range 20 {
		e.clock.Advance(testProbe / 20)
		e.reserve(t)
	}
	// 20 x 50 ms = one interval: the last call was the first probe.
	if got := e.primary.calls.Load(); got != 2 {
		t.Fatalf("primary called %d times in one probe interval, want 2 (the switch and one probe)", got)
	}
	for range 19 {
		e.clock.Advance(testProbe / 20)
		e.reserve(t)
	}
	if got := e.primary.calls.Load(); got != 2 {
		t.Fatalf("primary called %d times before the next probe is due, want 2", got)
	}
	e.clock.Advance(testProbe / 20)
	e.reserve(t)
	if got := e.primary.calls.Load(); got != 3 {
		t.Fatalf("primary called %d times when the second probe was due, want 3", got)
	}
	e.state(t, true)
	if n := e.logLines("WARN", "in-process fallback"); n != 1 {
		t.Errorf("%d warn lines after failed probes, want 1", n)
	}
}

// Many concurrent callers when a probe is due: exactly one asks the primary;
// the others are answered locally without waiting for it.
func TestFallback_OneProbeAtATime(t *testing.T) {
	const callers = 50
	e := newFallbackEnv(t)
	e.primary.down.Store(true)
	e.reserve(t)
	e.clock.Advance(testProbe)

	release := make(chan struct{})
	e.primary.setBlock(release)
	e.primary.down.Store(false)
	var answered atomic.Int64
	var wg sync.WaitGroup
	for range callers {
		wg.Go(func() {
			if _, ok, err := e.f.Reserve(context.Background(), domain.ChannelSMS); ok && err == nil {
				answered.Add(1)
			}
		})
	}
	// Every caller but the probe returns while the probe is still held.
	testutil.Eventually(t, func() bool { return answered.Load() == callers-1 }, testWait)
	if got := e.primary.calls.Load(); got != 2 {
		t.Errorf("primary called %d times, want 2 (the switch and one probe)", got)
	}
	if e.primary.maxIn.Load() != 1 {
		t.Errorf("%d calls on the primary at once, want 1", e.primary.maxIn.Load())
	}
	e.state(t, true)
	close(release)
	wg.Wait()
	if answered.Load() != callers {
		t.Errorf("%d of %d callers got a slot", answered.Load(), callers)
	}
	e.state(t, false) // the probe succeeded
}

// The first successful call of the primary ends the fallback; its slot is
// used, and the next calls go to the primary again.
func TestFallback_SwitchesBackOnFirstSuccess(t *testing.T) {
	e := newFallbackEnv(t)
	e.primary.down.Store(true)
	e.reserve(t)
	e.clock.Advance(testProbe)
	e.reserve(t) // a failed probe
	e.state(t, true)

	e.primary.down.Store(false)
	e.clock.Advance(testProbe)
	if got := e.reserve(t); got != redisWait {
		t.Fatalf("probe that succeeds: wait %s, want the primary's %s", got, redisWait)
	}
	e.state(t, false)
	calls := e.primary.calls.Load()
	for range 3 {
		if got := e.reserve(t); got != redisWait {
			t.Fatalf("after the switch back: wait %s, want the primary's %s", got, redisWait)
		}
	}
	if got := e.primary.calls.Load(); got != calls+3 {
		t.Errorf("primary called %d times after the switch back, want %d", got-calls, 3)
	}
	if n := e.logLines("WARN", "in-process fallback"); n != 1 {
		t.Errorf("%d warn lines, want 1", n)
	}
	if n := e.logLines("INFO", "fallback has ended"); n != 1 {
		t.Errorf("%d info lines, want 1:\n%s", n, e.logs.String())
	}

	// A second outage logs again and sets the gauge again.
	e.primary.down.Store(true)
	e.reserve(t)
	e.state(t, true)
	if n := e.logLines("WARN", "in-process fallback"); n != 2 {
		t.Errorf("%d warn lines after the second outage, want 2", n)
	}
}

// A refusal of the primary (no slot within the horizon) is an answer: it
// ends the fallback and is passed on.
func TestFallback_RefusalIsAnAnswer(t *testing.T) {
	e := newFallbackEnv(t)
	e.primary.down.Store(true)
	e.reserve(t)
	e.f.primary = refusingPrimary{}
	e.clock.Advance(testProbe)
	if _, ok, err := e.f.Reserve(context.Background(), domain.ChannelSMS); ok || err != nil {
		t.Fatalf("refusal: ok %v, error %v", ok, err)
	}
	e.state(t, false)
}

type refusingPrimary struct{}

func (refusingPrimary) Reserve(context.Context, domain.Channel) (time.Duration, bool, error) {
	return 0, false, nil
}

// The gauge is registered at construction (0), and a second fallback on the
// same registry reuses it.
func TestFallback_GaugeRegistration(t *testing.T) {
	e := newFallbackEnv(t)
	e.state(t, false)
	e.primary.down.Store(true)
	e.reserve(t)
	cfg := FallbackConfig{
		PerSecond: 98, Replicas: 1, Horizon: time.Second, Timeout: time.Second, ProbeInterval: time.Second,
		Registry: e.reg, Logger: slog.New(slog.DiscardHandler),
	}
	if _, err := NewFallback(&fakePrimary{}, cfg); err != nil {
		t.Fatalf("second fallback on the same registry: %v", err)
	}
	if e.gauge(t) != 0 {
		t.Errorf("gauge %v after a new fallback was built, want 0", e.gauge(t))
	}
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

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
