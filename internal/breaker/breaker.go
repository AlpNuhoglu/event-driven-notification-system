// Package breaker is the circuit breaker of one (channel, provider) pair
// (ADR-0007 sections 7 and 8, D26). It is a pure state machine with an
// injected clock: it starts no goroutines and no timers and performs no I/O.
// Every time-dependent decision, including the change from open to half-open,
// is taken lazily when a method is called. A Breaker is safe for concurrent
// use.
package breaker

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

// Config holds the breaker's thresholds and durations. Every field is
// required.
type Config struct {
	Window              time.Duration // BREAKER_WINDOW, 30 s
	MinRequests         int           // BREAKER_MIN_REQUESTS, 20
	FailureRatio        float64       // BREAKER_FAILURE_RATIO, 0.5
	ConsecutiveFailures int           // BREAKER_CONSECUTIVE_FAILURES, 5
	OpenBase            time.Duration // BREAKER_OPEN_BASE, 5 s
	OpenMax             time.Duration // BREAKER_OPEN_MAX, 2 min
	FatalOpen           time.Duration // BREAKER_FATAL_OPEN, 1 min
	ThrottleDefault     time.Duration // THROTTLE_DEFAULT_DELAY, 5 s
	ThrottleMax         time.Duration // THROTTLE_MAX_DELAY, 2 min
	RetryAfterCap       time.Duration // RETRY_AFTER_CAP, 10 min
	ProbeWait           time.Duration // how long a caller is told to wait while a probe is in flight
}

func (c Config) validate() error {
	var errs []error
	positive := func(name string, d time.Duration) {
		if d <= 0 {
			errs = append(errs, fmt.Errorf("%s must be positive, got %s", name, d))
		}
	}
	if c.Window < windowBuckets*time.Nanosecond {
		errs = append(errs, fmt.Errorf("window must be at least %dns, got %s", windowBuckets, c.Window))
	}
	if c.MinRequests < 1 {
		errs = append(errs, fmt.Errorf("min requests must be at least 1, got %d", c.MinRequests))
	}
	if !(c.FailureRatio > 0 && c.FailureRatio <= 1) {
		errs = append(errs, fmt.Errorf("failure ratio must be in (0, 1], got %v", c.FailureRatio))
	}
	if c.ConsecutiveFailures < 1 {
		errs = append(errs, fmt.Errorf("consecutive failures must be at least 1, got %d", c.ConsecutiveFailures))
	}
	positive("open base", c.OpenBase)
	positive("fatal open", c.FatalOpen)
	positive("throttle default", c.ThrottleDefault)
	positive("retry-after cap", c.RetryAfterCap)
	positive("probe wait", c.ProbeWait)
	if c.OpenMax < c.OpenBase {
		errs = append(errs, fmt.Errorf("open max (%s) must not be below open base (%s)", c.OpenMax, c.OpenBase))
	}
	if c.ThrottleMax < c.ThrottleDefault {
		errs = append(errs, fmt.Errorf("throttle max (%s) must not be below throttle default (%s)",
			c.ThrottleMax, c.ThrottleDefault))
	}
	return errors.Join(errs...)
}

// State is the state of a circuit. Its numeric value is suitable for the
// circuit_state gauge: 0 closed, 1 open, 2 half-open.
type State int

// The states of a circuit.
const (
	// Closed lets every send through and counts the outcomes.
	Closed State = iota
	// Open lets nothing through until the open period ends.
	Open
	// HalfOpen lets exactly one send through, the probe.
	HalfOpen
)

// String returns the state's name.
func (s State) String() string {
	switch s {
	case Closed:
		return "closed"
	case Open:
		return "open"
	case HalfOpen:
		return "half_open"
	default:
		return fmt.Sprintf("State(%d)", int(s))
	}
}

// Change describes one state change, for the caller's log and metrics.
type Change struct {
	From, To State
	Outcome  domain.Outcome // the outcome that caused it; empty for Open -> HalfOpen
	Until    time.Time      // end of the open period when To == Open
}

// Breaker guards one (channel, provider) pair.
type Breaker struct {
	cfg      Config
	now      func() time.Time
	onChange func(Change)

	mu        sync.Mutex
	state     State
	gen       uint64    // incremented on every state change; a permit belongs to one generation
	openUntil time.Time // end of the open period while state == Open
	probing   bool      // half-open: the probe is in flight
	win       window    // closed: counted outcomes of the last Window
	streak    int       // closed: counted outcomes in a row that were failures (rule 2)
	probeFail int       // failed probes in a row since the circuit last closed (n)
	throttleK int       // throttled openings without a usable hint since the last success (k)

	pending  []Change // changes not yet delivered to onChange, in order
	draining bool     // a caller is delivering pending changes
}

// New returns a closed breaker. now is the clock; onChange, which may be nil,
// receives every state change after the breaker's lock is released, in the
// order the changes happened.
func New(cfg Config, now func() time.Time, onChange func(Change)) (*Breaker, error) {
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("breaker: invalid config: %w", err)
	}
	if now == nil {
		return nil, errors.New("breaker: clock is nil")
	}
	b := &Breaker{cfg: cfg, now: now, onChange: onChange}
	b.win.init(cfg.Window, now())
	return b, nil
}

// Ready reports whether a message may be prepared for sending now (the pacer
// asks before it reserves a rate-limit slot). It consumes nothing. When
// false, the caller waits until `until`.
func (b *Breaker) Ready() (ok bool, until time.Time) {
	b.mu.Lock()
	defer b.unlock()
	now := b.now()
	b.advance(now)
	switch b.state {
	case Open:
		return false, b.openUntil
	case HalfOpen:
		if b.probing {
			return false, now.Add(b.cfg.ProbeWait)
		}
	}
	return true, time.Time{}
}

// Admit is asked at the slot time, immediately before the send. When ok, the
// caller sends and must call Record on the permit exactly once. When not ok,
// nothing is sent; `until` is when to ask again.
func (b *Breaker) Admit() (p Permit, ok bool, until time.Time) {
	b.mu.Lock()
	defer b.unlock()
	now := b.now()
	b.advance(now)
	switch b.state {
	case Open:
		return Permit{}, false, b.openUntil
	case HalfOpen:
		if b.probing {
			return Permit{}, false, now.Add(b.cfg.ProbeWait)
		}
		b.probing = true
		return Permit{b: b, t: &ticket{gen: b.gen, probe: true}}, true, time.Time{}
	}
	return Permit{b: b, t: &ticket{gen: b.gen}}, true, time.Time{}
}

// State returns the current state.
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.unlock()
	b.advance(b.now())
	return b.state
}

// Permit is the right to send one message. The zero Permit records nothing.
type Permit struct {
	b *Breaker
	t *ticket
}

// ticket is shared by the copies of one Permit; its fields are guarded by
// the breaker's lock.
type ticket struct {
	gen      uint64 // the generation in which the permit was admitted
	probe    bool   // the permit is the half-open probe
	recorded bool
}

// Record reports the outcome of the admitted send. retryAfter is the usable
// Retry-After of the response, zero when there is none. It returns the end of
// the open period if the circuit is open after this record, and the zero time
// otherwise.
//
// A record of a permit admitted in an earlier state (the state has changed
// since the admission) and a second record of the same permit change
// nothing, but still return the end of the current open period.
func (p Permit) Record(outcome domain.Outcome, retryAfter time.Duration) (openUntil time.Time) {
	b := p.b
	if b == nil || p.t == nil {
		return time.Time{}
	}
	b.mu.Lock()
	defer b.unlock()
	now := b.now()
	b.advance(now)
	if !p.t.recorded && p.t.gen == b.gen {
		if p.t.probe {
			b.recordProbe(now, outcome, retryAfter)
		} else {
			b.recordClosed(now, outcome, retryAfter)
		}
	}
	p.t.recorded = true
	if b.state == Open {
		return b.openUntil
	}
	return time.Time{}
}

// recordClosed applies the outcome of a send admitted while closed.
func (b *Breaker) recordClosed(now time.Time, outcome domain.Outcome, retryAfter time.Duration) {
	switch outcome.Breaker() {
	case domain.BreakerSuccess:
		// k needs no reset here: the circuit closes only through a successful
		// probe, which resets it.
		b.win.add(now, false)
		b.streak = 0
	case domain.BreakerFailure:
		b.win.add(now, true)
		b.streak++
		if b.ratioExceeded(now) || b.streakExceeded() {
			b.open(now, outcome, b.cfg.OpenBase)
		}
	case domain.BreakerOpen:
		b.openDirect(now, outcome, retryAfter)
	case domain.BreakerIgnore:
	}
}

// ratioExceeded is rule 1: the window holds at least MinRequests counted
// outcomes and the share of failures among them reaches FailureRatio.
func (b *Breaker) ratioExceeded(now time.Time) bool {
	total, failures := b.win.counts(now)
	return total >= b.cfg.MinRequests && float64(failures)/float64(total) >= b.cfg.FailureRatio
}

// streakExceeded is rule 2: the last ConsecutiveFailures counted outcomes
// were all failures. It protects low traffic, where rule 1's MinRequests is
// never reached.
func (b *Breaker) streakExceeded() bool {
	return b.streak >= b.cfg.ConsecutiveFailures
}

// recordProbe applies the outcome of the half-open probe.
func (b *Breaker) recordProbe(now time.Time, outcome domain.Outcome, retryAfter time.Duration) {
	switch outcome.Breaker() {
	case domain.BreakerSuccess:
		b.win.reset()
		b.streak = 0
		b.probeFail = 0
		b.throttleK = 0
		b.transition(Change{From: b.state, To: Closed, Outcome: outcome})
	case domain.BreakerFailure:
		b.probeFail++
		b.open(now, outcome, doubled(b.cfg.OpenBase, b.cfg.OpenMax, b.probeFail))
	case domain.BreakerOpen:
		b.openDirect(now, outcome, retryAfter)
	case domain.BreakerIgnore:
		// The probe is undecided: the next admitted send is the probe.
		b.probing = false
	}
}

// openDirect opens the circuit for an outcome that opens it by itself:
// throttled for the throttle duration, provider_fatal for FatalOpen.
func (b *Breaker) openDirect(now time.Time, outcome domain.Outcome, retryAfter time.Duration) {
	if outcome != domain.OutcomeThrottled {
		b.open(now, outcome, b.cfg.FatalOpen)
		return
	}
	var d time.Duration
	if retryAfter > 0 {
		d = retryAfter
	} else {
		d = doubled(b.cfg.ThrottleDefault, b.cfg.ThrottleMax, b.throttleK)
		b.throttleK++
	}
	b.open(now, outcome, min(d, b.cfg.RetryAfterCap))
}

func (b *Breaker) open(now time.Time, outcome domain.Outcome, d time.Duration) {
	b.openUntil = now.Add(d)
	b.transition(Change{From: b.state, To: Open, Outcome: outcome, Until: b.openUntil})
}

// advance turns an open circuit half-open once its open period has ended.
func (b *Breaker) advance(now time.Time) {
	if b.state == Open && !now.Before(b.openUntil) {
		b.openUntil = time.Time{}
		b.transition(Change{From: Open, To: HalfOpen})
	}
}

// transition enters c.To and queues c for onChange. Every transition starts
// a new generation, so the permits admitted before it become stale.
func (b *Breaker) transition(c Change) {
	b.state = c.To
	b.gen++
	b.probing = false
	if b.onChange != nil {
		b.pending = append(b.pending, c)
	}
}

// unlock releases the lock and then delivers the pending changes to
// onChange. One caller at a time delivers; a change queued meanwhile, also
// by a call made from inside onChange, is delivered by that same caller, so
// the changes reach onChange in the order they happened.
func (b *Breaker) unlock() {
	if b.draining || len(b.pending) == 0 {
		b.mu.Unlock()
		return
	}
	b.draining = true
	b.mu.Unlock()

	done := false
	defer func() {
		if !done { // onChange panicked: let the next caller deliver the rest.
			b.mu.Lock()
			b.draining = false
			b.mu.Unlock()
		}
	}()
	for {
		b.mu.Lock()
		batch := b.pending
		b.pending = nil
		if len(batch) == 0 {
			b.draining = false
			b.mu.Unlock()
			done = true
			return
		}
		b.mu.Unlock()
		for _, c := range batch {
			b.onChange(c)
		}
	}
}

// doubled returns base × 2^n, cut to limit.
func doubled(base, limit time.Duration, n int) time.Duration {
	d := base
	for range n {
		if d >= limit/2 {
			return limit
		}
		d *= 2
	}
	return min(d, limit)
}

// windowBuckets is the number of buckets of the sliding window. The window's
// memory is fixed; its resolution is Window / windowBuckets (1 s with the
// default 30 s window).
const windowBuckets = 30

// window counts outcomes over the last Window in fixed time buckets. An
// outcome is counted until its bucket leaves the window, so it is never
// counted once it is older than Window, and it may stop counting up to one
// bucket width earlier.
type window struct {
	width   time.Duration // Window / windowBuckets, at least 1ns
	epoch   time.Time     // bucket i covers [epoch + i·width, epoch + (i+1)·width)
	last    int64         // the highest bucket index seen, so a clock that steps back cannot corrupt the ring
	buckets [windowBuckets]bucket
}

type bucket struct {
	index           int64
	total, failures int
}

func (w *window) init(size time.Duration, epoch time.Time) {
	w.width = size / windowBuckets
	w.epoch = epoch
}

func (w *window) reset() {
	w.buckets = [windowBuckets]bucket{}
}

func (w *window) index(now time.Time) int64 {
	if i := int64(now.Sub(w.epoch) / w.width); i > w.last {
		w.last = i
	}
	return w.last
}

func (w *window) add(now time.Time, failure bool) {
	i := w.index(now)
	bk := &w.buckets[i%windowBuckets]
	if bk.index != i {
		*bk = bucket{index: i}
	}
	bk.total++
	if failure {
		bk.failures++
	}
}

func (w *window) counts(now time.Time) (total, failures int) {
	i := w.index(now)
	for _, bk := range w.buckets {
		if bk.index > i-windowBuckets && bk.index <= i {
			total += bk.total
			failures += bk.failures
		}
	}
	return total, failures
}
