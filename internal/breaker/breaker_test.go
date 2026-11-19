package breaker

import (
	"math"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

// fakeClock is the injected clock of the tests. It is safe for concurrent
// use.
type fakeClock struct{ ns atomic.Int64 }

func newClock() *fakeClock {
	c := &fakeClock{}
	c.ns.Store(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano())
	return c
}

func (c *fakeClock) now() time.Time                  { return time.Unix(0, c.ns.Load()).UTC() }
func (c *fakeClock) advance(d time.Duration)         { c.ns.Add(int64(d)) }
func (c *fakeClock) after(d time.Duration) time.Time { return c.now().Add(d) }

// testConfig mirrors the defaults with every duration in milliseconds. The
// window is 300 ms, so a bucket is exactly 10 ms wide.
func testConfig() Config {
	return Config{
		Window:              300 * time.Millisecond,
		MinRequests:         20,
		FailureRatio:        0.5,
		ConsecutiveFailures: 5,
		OpenBase:            50 * time.Millisecond,
		OpenMax:             400 * time.Millisecond,
		FatalOpen:           600 * time.Millisecond,
		ThrottleDefault:     50 * time.Millisecond,
		ThrottleMax:         400 * time.Millisecond,
		RetryAfterCap:       1000 * time.Millisecond,
		ProbeWait:           10 * time.Millisecond,
	}
}

const (
	success   = domain.OutcomeSuccess
	failure   = domain.OutcomeRetryable
	throttled = domain.OutcomeThrottled
	fatal     = domain.OutcomeProviderFatal
)

func newBreaker(t *testing.T, clk *fakeClock) *Breaker {
	t.Helper()
	b, err := New(testConfig(), clk.now, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return b
}

// send admits one send, which must be admitted, and records its outcome.
func send(t *testing.T, b *Breaker, outcome domain.Outcome, retryAfter time.Duration) time.Time {
	t.Helper()
	p, ok, until := b.Admit()
	if !ok {
		t.Fatalf("Admit refused (until %v) in state %s", until, b.State())
	}
	return p.Record(outcome, retryAfter)
}

// sendPattern sends one outcome per letter: S success, F failure (retryable),
// I ignored (permanent).
func sendPattern(t *testing.T, b *Breaker, pattern string) {
	t.Helper()
	for _, r := range pattern {
		send(t, b, letter(t, r), 0)
	}
}

func letter(t *testing.T, r rune) domain.Outcome {
	t.Helper()
	switch r {
	case 'S':
		return success
	case 'F':
		return failure
	case 'I':
		return domain.OutcomePermanent
	}
	t.Fatalf("unknown pattern letter %q", r)
	return ""
}

func wantState(t *testing.T, b *Breaker, want State) {
	t.Helper()
	if got := b.State(); got != want {
		t.Fatalf("state = %s, want %s", got, want)
	}
}

// openFromClosed opens a closed breaker through rule 2 and returns the end
// of the open period.
func openFromClosed(t *testing.T, b *Breaker) time.Time {
	t.Helper()
	sendPattern(t, b, "FFFF")
	until := send(t, b, failure, 0)
	wantState(t, b, Open)
	return until
}

// toHalfOpen advances the clock to the end of the open period.
func toHalfOpen(t *testing.T, b *Breaker, clk *fakeClock) {
	t.Helper()
	_, until := b.Ready()
	clk.advance(until.Sub(clk.now()))
	wantState(t, b, HalfOpen)
}

// TestBreaker_FiveConsecutiveFailuresOpen tests rule 2 alone: every case
// stays below MinRequests, so rule 1 cannot open the circuit.
func TestBreaker_FiveConsecutiveFailuresOpen(t *testing.T) {
	cases := []struct {
		name    string
		pattern string
		want    State
	}{
		{"five failures open", "FFFFF", Open},
		{"four failures do not open", "FFFF", Closed},
		{"a success in between resets the count", "FFFFSFFFF", Closed},
		{"five failures after a reset open", "FFFFSFFFFF", Open},
		{"many successes then five failures open", "SSSSSSSSFFFFF", Open},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if n := len(tc.pattern); n >= testConfig().MinRequests {
				t.Fatalf("pattern has %d outcomes; rule 1 could interfere", n)
			}
			clk := newClock()
			b := newBreaker(t, clk)
			for i, r := range tc.pattern {
				if i > 0 {
					wantState(t, b, Closed) // only the last outcome may open
				}
				send(t, b, letter(t, r), 0)
			}
			wantState(t, b, tc.want)
		})
	}
}

// TestBreaker_WindowRule tests rule 1 at its thresholds. No pattern holds five
// failures in a row, so rule 2 cannot open the circuit.
func TestBreaker_WindowRule(t *testing.T) {
	cases := []struct {
		name    string
		pattern string
		want    State
	}{
		{"19 counted outcomes never open, even at 79 % failures", "FFFFSFFFFSFFFFSFFFF", Closed},
		{"20 counted outcomes at exactly 50 % open", strings.Repeat("SF", 10), Open},
		{"20 counted outcomes at 45 % do not open", "SS" + strings.Repeat("SF", 9), Closed},
		{"21 counted outcomes at 48 % do not open", "SS" + strings.Repeat("SF", 9) + "F", Closed},
		{"22 counted outcomes at 50 % open", "SS" + strings.Repeat("SF", 9) + "FF", Open},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if strings.Contains(strings.ReplaceAll(tc.pattern, "I", ""), "FFFFF") {
				t.Fatal("pattern holds five failures in a row; rule 2 could interfere")
			}
			clk := newClock()
			b := newBreaker(t, clk)
			for i, r := range tc.pattern {
				if i > 0 {
					wantState(t, b, Closed)
				}
				send(t, b, letter(t, r), 0)
			}
			wantState(t, b, tc.want)
		})
	}
}

// TestBreaker_WindowExpiry checks that outcomes older than the window no longer
// count: 19 outcomes with 9 failures, then one failure after a delay. Counted
// together they are 20 at 50 %.
func TestBreaker_WindowExpiry(t *testing.T) {
	window := testConfig().Window
	cases := []struct {
		name  string
		delay time.Duration
		want  State
	}{
		{"same time", 0, Open},
		{"half the window later", window / 2, Open},
		{"just inside the window", window - time.Nanosecond, Open},
		{"exactly one window later", window, Closed},
		{"long after", 10 * window, Closed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clk := newClock()
			b := newBreaker(t, clk)
			sendPattern(t, b, strings.Repeat("SF", 9)+"S")
			clk.advance(tc.delay)
			send(t, b, failure, 0)
			wantState(t, b, tc.want)
		})
	}
}

func TestBreaker_WindowMemoryIsBounded(t *testing.T) {
	clk := newClock()
	b := newBreaker(t, clk)
	for range 10_000 {
		send(t, b, success, 0)
		clk.advance(time.Millisecond)
	}
	total, _ := b.win.counts(clk.now())
	// 300 ms of 1 ms steps; the bucket that the window is entering may be cut.
	if total < 290 || total > 300 {
		t.Fatalf("window counts %d outcomes, want 290..300", total)
	}
	if n := len(b.win.buckets); n != windowBuckets {
		t.Fatalf("window has %d buckets", n)
	}
}

func TestBreaker_OpenPeriodAndLazyHalfOpen(t *testing.T) {
	clk := newClock()
	b := newBreaker(t, clk)
	opened := clk.now()
	until := openFromClosed(t, b)
	if want := opened.Add(50 * time.Millisecond); !until.Equal(want) {
		t.Fatalf("open until %v, want %v", until, want)
	}

	clk.advance(49 * time.Millisecond)
	if ok, u := b.Ready(); ok || !u.Equal(until) {
		t.Fatalf("Ready while open = %v, %v; want false, %v", ok, u, until)
	}
	if _, ok, u := b.Admit(); ok || !u.Equal(until) {
		t.Fatalf("Admit while open = %v, %v; want false, %v", ok, u, until)
	}
	wantState(t, b, Open)

	clk.advance(time.Millisecond)
	wantState(t, b, HalfOpen)
	if ok, _ := b.Ready(); !ok {
		t.Fatal("Ready in half-open without a probe = false")
	}
}

func TestBreaker_OneProbeUnderConcurrentAdmits(t *testing.T) {
	clk := newClock()
	b := newBreaker(t, clk)
	openFromClosed(t, b)
	toHalfOpen(t, b, clk)

	const callers = 100
	start := make(chan struct{})
	var admitted atomic.Int32
	var wrongUntil atomic.Int32
	wantUntil := clk.after(testConfig().ProbeWait)
	var wg sync.WaitGroup
	for range callers {
		wg.Go(func() {
			<-start
			p, ok, until := b.Admit()
			if ok {
				admitted.Add(1)
				if !p.t.probe {
					t.Error("permit admitted in half-open is not the probe")
				}
				return
			}
			if !until.Equal(wantUntil) {
				wrongUntil.Add(1)
			}
		})
	}
	close(start)
	wg.Wait()
	if n := admitted.Load(); n != 1 {
		t.Fatalf("%d permits admitted, want exactly 1", n)
	}
	if n := wrongUntil.Load(); n != 0 {
		t.Fatalf("%d refusals did not say now + ProbeWait", n)
	}
}

func TestBreaker_OpenDurationDoublesAndResets(t *testing.T) {
	clk := newClock()
	b := newBreaker(t, clk)
	until := openFromClosed(t, b)
	if d := until.Sub(clk.now()); d != 50*time.Millisecond {
		t.Fatalf("open from closed for %s, want 50ms", d)
	}
	for _, want := range []time.Duration{100, 200, 400, 400, 400} {
		toHalfOpen(t, b, clk)
		until = send(t, b, domain.OutcomeUnknown, 0)
		if d := until.Sub(clk.now()); d != want*time.Millisecond {
			t.Fatalf("open after a failed probe for %s, want %dms", d, want)
		}
	}
	toHalfOpen(t, b, clk)
	if u := send(t, b, success, 0); !u.IsZero() {
		t.Fatalf("Record of a successful probe = %v, want zero", u)
	}
	wantState(t, b, Closed)

	// A success resets the doubling and the consecutive count.
	sendPattern(t, b, "FFFF")
	wantState(t, b, Closed)
	until = send(t, b, failure, 0)
	if d := until.Sub(clk.now()); d != 50*time.Millisecond {
		t.Fatalf("open after reset for %s, want 50ms", d)
	}
	toHalfOpen(t, b, clk)
	until = send(t, b, failure, 0)
	if d := until.Sub(clk.now()); d != 100*time.Millisecond {
		t.Fatalf("first failed probe after reset opens for %s, want 100ms", d)
	}
}

func TestBreaker_SuccessfulProbeResetsWindow(t *testing.T) {
	clk := newClock()
	b := newBreaker(t, clk)
	// 19 counted outcomes, 9 failures, in the window...
	sendPattern(t, b, strings.Repeat("SF", 9)+"S")
	b.mu.Lock()
	b.win.add(clk.now(), true) // ...then force an opening that leaves the window full.
	b.mu.Unlock()
	send(t, b, fatal, 0)
	toHalfOpen(t, b, clk)
	send(t, b, success, 0)
	wantState(t, b, Closed)
	// Without the reset, one failure would make 21 at 52 %.
	send(t, b, failure, 0)
	wantState(t, b, Closed)
}

func TestBreaker_ProviderFatal(t *testing.T) {
	t.Run("from closed", func(t *testing.T) {
		clk := newClock()
		b := newBreaker(t, clk)
		until := send(t, b, fatal, 0)
		wantState(t, b, Open)
		if d := until.Sub(clk.now()); d != 600*time.Millisecond {
			t.Fatalf("open for %s, want 600ms", d)
		}
	})
	t.Run("as a probe result", func(t *testing.T) {
		clk := newClock()
		b := newBreaker(t, clk)
		openFromClosed(t, b)
		toHalfOpen(t, b, clk)
		send(t, b, failure, 0) // n = 1: 100 ms
		toHalfOpen(t, b, clk)
		until := send(t, b, fatal, 0)
		if d := until.Sub(clk.now()); d != 600*time.Millisecond {
			t.Fatalf("open for %s, want 600ms", d)
		}
		// provider_fatal neither advances nor resets the probe doubling.
		toHalfOpen(t, b, clk)
		until = send(t, b, failure, 0)
		if d := until.Sub(clk.now()); d != 200*time.Millisecond {
			t.Fatalf("failed probe after a fatal probe opens for %s, want 200ms", d)
		}
	})
}

func TestBreaker_Throttled(t *testing.T) {
	t.Run("with a hint", func(t *testing.T) {
		clk := newClock()
		b := newBreaker(t, clk)
		until := send(t, b, throttled, 30*time.Millisecond)
		if d := until.Sub(clk.now()); d != 30*time.Millisecond {
			t.Fatalf("open for %s, want 30ms", d)
		}
	})
	t.Run("with a hint above the cap", func(t *testing.T) {
		clk := newClock()
		b := newBreaker(t, clk)
		until := send(t, b, throttled, 5*time.Second)
		if d := until.Sub(clk.now()); d != 1000*time.Millisecond {
			t.Fatalf("open for %s, want the cap 1000ms", d)
		}
	})
	t.Run("without a hint doubles per opening up to the maximum", func(t *testing.T) {
		clk := newClock()
		b := newBreaker(t, clk)
		until := send(t, b, throttled, 0)
		if d := until.Sub(clk.now()); d != 50*time.Millisecond {
			t.Fatalf("first opening for %s, want 50ms", d)
		}
		for _, want := range []time.Duration{100, 200, 400, 400} {
			toHalfOpen(t, b, clk)
			until = send(t, b, throttled, 0)
			if d := until.Sub(clk.now()); d != want*time.Millisecond {
				t.Fatalf("opening for %s, want %dms", d, want)
			}
		}
		// A success resets k.
		toHalfOpen(t, b, clk)
		send(t, b, success, 0)
		until = send(t, b, throttled, 0)
		if d := until.Sub(clk.now()); d != 50*time.Millisecond {
			t.Fatalf("opening after a success for %s, want 50ms", d)
		}
	})
	t.Run("a non-throttled opening neither advances nor resets k", func(t *testing.T) {
		clk := newClock()
		b := newBreaker(t, clk)
		send(t, b, throttled, 0) // 50 ms, k = 1
		toHalfOpen(t, b, clk)
		send(t, b, failure, 0)
		toHalfOpen(t, b, clk)
		send(t, b, fatal, 0)
		toHalfOpen(t, b, clk)
		until := send(t, b, throttled, 0)
		if d := until.Sub(clk.now()); d != 100*time.Millisecond {
			t.Fatalf("opening for %s, want 100ms", d)
		}
	})
	t.Run("a hint neither advances nor resets k", func(t *testing.T) {
		clk := newClock()
		b := newBreaker(t, clk)
		send(t, b, throttled, 0) // 50 ms, k = 1
		toHalfOpen(t, b, clk)
		send(t, b, throttled, 20*time.Millisecond)
		toHalfOpen(t, b, clk)
		until := send(t, b, throttled, 0)
		if d := until.Sub(clk.now()); d != 100*time.Millisecond {
			t.Fatalf("opening for %s, want 100ms", d)
		}
	})
	t.Run("many throttled answers in flight are one step", func(t *testing.T) {
		clk := newClock()
		b := newBreaker(t, clk)
		var permits []Permit
		for range 25 {
			p, ok, _ := b.Admit()
			if !ok {
				t.Fatal("Admit refused in closed")
			}
			permits = append(permits, p)
		}
		first := permits[0].Record(throttled, 0)
		for _, p := range permits[1:] {
			if u := p.Record(throttled, 0); !u.Equal(first) {
				t.Fatalf("stale record returned %v, want %v", u, first)
			}
		}
		if d := first.Sub(clk.now()); d != 50*time.Millisecond {
			t.Fatalf("open for %s, want 50ms", d)
		}
		toHalfOpen(t, b, clk)
		until := send(t, b, throttled, 0)
		if d := until.Sub(clk.now()); d != 100*time.Millisecond {
			t.Fatalf("second opening for %s, want 100ms (one step)", d)
		}
	})
}

func TestBreaker_IgnoredOutcomes(t *testing.T) {
	ignored := []domain.Outcome{domain.OutcomePermanent, domain.OutcomeAborted, domain.OutcomeInternalError}
	t.Run("in closed", func(t *testing.T) {
		clk := newClock()
		b := newBreaker(t, clk)
		for range 50 {
			for _, o := range ignored {
				if u := send(t, b, o, 0); !u.IsZero() {
					t.Fatalf("Record(%s) = %v, want zero", o, u)
				}
			}
		}
		wantState(t, b, Closed)
		if total, _ := b.win.counts(clk.now()); total != 0 {
			t.Fatalf("window holds %d outcomes, want 0", total)
		}
		// Ignored outcomes neither count nor break a run of failures.
		sendPattern(t, b, "FFFF")
		for _, o := range ignored {
			send(t, b, o, 0)
		}
		wantState(t, b, Closed)
		send(t, b, failure, 0)
		wantState(t, b, Open)
	})
	for _, o := range ignored {
		t.Run("as a probe result: "+string(o), func(t *testing.T) {
			clk := newClock()
			b := newBreaker(t, clk)
			openFromClosed(t, b)
			toHalfOpen(t, b, clk)
			p, ok, _ := b.Admit()
			if !ok || !p.t.probe {
				t.Fatal("no probe admitted")
			}
			if u := p.Record(o, 0); !u.IsZero() {
				t.Fatalf("Record = %v, want zero", u)
			}
			wantState(t, b, HalfOpen)
			if ok, _ := b.Ready(); !ok {
				t.Fatal("Ready after an undecided probe = false")
			}
			// The old permit cannot decide anymore; the next admitted send is the probe.
			p.Record(success, 0)
			wantState(t, b, HalfOpen)
			next, ok, _ := b.Admit()
			if !ok || !next.t.probe {
				t.Fatal("the next admitted send is not the probe")
			}
			next.Record(success, 0)
			wantState(t, b, Closed)
		})
	}
}

func TestBreaker_StalePermits(t *testing.T) {
	t.Run("admitted while closed, recorded while open", func(t *testing.T) {
		clk := newClock()
		b := newBreaker(t, clk)
		p, _, _ := b.Admit()
		until := openFromClosed(t, b)
		if u := p.Record(success, 0); !u.Equal(until) {
			t.Fatalf("Record = %v, want the open period's end %v", u, until)
		}
		wantState(t, b, Open)
	})
	t.Run("admitted while closed, recorded while half-open", func(t *testing.T) {
		for _, o := range []domain.Outcome{success, failure, fatal, throttled} {
			clk := newClock()
			b := newBreaker(t, clk)
			p, _, _ := b.Admit()
			openFromClosed(t, b)
			toHalfOpen(t, b, clk)
			if u := p.Record(o, 0); !u.IsZero() {
				t.Fatalf("%s: Record = %v, want zero", o, u)
			}
			wantState(t, b, HalfOpen)
			if ok, _ := b.Ready(); !ok {
				t.Fatalf("%s: a stale record took the probe slot", o)
			}
		}
	})
	t.Run("admitted while half-open, recorded after a reopening", func(t *testing.T) {
		clk := newClock()
		b := newBreaker(t, clk)
		openFromClosed(t, b)
		toHalfOpen(t, b, clk)
		p, _, _ := b.Admit()
		// The state cannot change while the probe is in flight; force it.
		b.mu.Lock()
		b.open(clk.now(), fatal, time.Second)
		b.mu.Unlock()
		until := p.Record(success, 0)
		wantState(t, b, Open)
		if want := clk.after(time.Second); !until.Equal(want) {
			t.Fatalf("Record = %v, want %v", until, want)
		}
	})
	t.Run("outlived a whole open period", func(t *testing.T) {
		clk := newClock()
		b := newBreaker(t, clk)
		p, _, _ := b.Admit()
		openFromClosed(t, b)
		toHalfOpen(t, b, clk)
		send(t, b, success, 0)
		sendPattern(t, b, "FFFF")
		if u := p.Record(failure, 0); !u.IsZero() {
			t.Fatalf("Record = %v, want zero", u)
		}
		wantState(t, b, Closed) // the stale failure was not the fifth
		send(t, b, failure, 0)
		wantState(t, b, Open)
	})
	t.Run("second record of the same permit", func(t *testing.T) {
		clk := newClock()
		b := newBreaker(t, clk)
		p, _, _ := b.Admit()
		for range 10 {
			p.Record(failure, 0)
		}
		sendPattern(t, b, "FFF")
		wantState(t, b, Closed) // four distinct failures
		send(t, b, failure, 0)
		wantState(t, b, Open)
	})
	t.Run("second record of the probe", func(t *testing.T) {
		clk := newClock()
		b := newBreaker(t, clk)
		openFromClosed(t, b)
		toHalfOpen(t, b, clk)
		p, _, _ := b.Admit()
		p.Record(success, 0)
		send(t, b, fatal, 0) // opens from closed
		until := p.Record(failure, 0)
		if want := clk.after(600 * time.Millisecond); !until.Equal(want) {
			t.Fatalf("Record = %v, want %v", until, want)
		}
	})
	t.Run("zero permit", func(t *testing.T) {
		if u := (Permit{}).Record(failure, 0); !u.IsZero() {
			t.Fatalf("Record = %v, want zero", u)
		}
	})
}

func TestBreaker_RecordReturnValue(t *testing.T) {
	clk := newClock()
	b := newBreaker(t, clk)
	if u := send(t, b, success, 0); !u.IsZero() {
		t.Fatalf("success in closed: %v, want zero", u)
	}
	if u := send(t, b, failure, 0); !u.IsZero() {
		t.Fatalf("failure that does not open: %v, want zero", u)
	}
	stale, _, _ := b.Admit()
	sendPattern(t, b, "FFF")
	until := send(t, b, failure, 0)
	if want := clk.after(50 * time.Millisecond); !until.Equal(want) {
		t.Fatalf("opening failure: %v, want %v", until, want)
	}
	clk.advance(10 * time.Millisecond)
	if u := stale.Record(failure, 0); !u.Equal(until) {
		t.Fatalf("stale record while open: %v, want %v", u, until)
	}
	toHalfOpen(t, b, clk)
	p, _, _ := b.Admit()
	until = p.Record(failure, 0)
	if want := clk.after(100 * time.Millisecond); !until.Equal(want) {
		t.Fatalf("failed probe: %v, want %v", until, want)
	}
	clk.advance(100 * time.Millisecond)
	if u := p.Record(failure, 0); !u.IsZero() {
		t.Fatalf("second record after the open period: %v, want zero", u)
	}
	if u := send(t, b, success, 0); !u.IsZero() {
		t.Fatalf("successful probe: %v, want zero", u)
	}
}

func TestBreaker_Ready(t *testing.T) {
	clk := newClock()
	b := newBreaker(t, clk)
	if ok, u := b.Ready(); !ok || !u.IsZero() {
		t.Fatalf("closed: %v, %v; want true, zero", ok, u)
	}
	until := openFromClosed(t, b)
	if ok, u := b.Ready(); ok || !u.Equal(until) {
		t.Fatalf("open: %v, %v; want false, %v", ok, u, until)
	}
	toHalfOpen(t, b, clk)
	for range 3 { // Ready consumes nothing.
		if ok, u := b.Ready(); !ok || !u.IsZero() {
			t.Fatalf("half-open without a probe: %v, %v; want true, zero", ok, u)
		}
	}
	p, ok, _ := b.Admit()
	if !ok {
		t.Fatal("Admit after Ready refused the probe")
	}
	if ok, u := b.Ready(); ok || !u.Equal(clk.after(10*time.Millisecond)) {
		t.Fatalf("half-open with a probe in flight: %v, %v; want false, now + ProbeWait", ok, u)
	}
	clk.advance(25 * time.Millisecond)
	if _, ok, u := b.Admit(); ok || !u.Equal(clk.after(10*time.Millisecond)) {
		t.Fatalf("Admit with a probe in flight: %v, %v; want false, now + ProbeWait", ok, u)
	}
	p.Record(success, 0)
	if ok, u := b.Ready(); !ok || !u.IsZero() {
		t.Fatalf("closed again: %v, %v; want true, zero", ok, u)
	}
}

func TestBreaker_OnChangeSequence(t *testing.T) {
	clk := newClock()
	var got []Change
	var b *Breaker
	b, err := New(testConfig(), clk.now, func(c Change) {
		got = append(got, c)
		b.State() // a call from inside onChange must not deadlock
		b.Ready()
	})
	if err != nil {
		t.Fatal(err)
	}
	t0 := clk.now()
	openFromClosed(t, b)                 // 1: closed -> open, 50 ms
	clk.advance(50 * time.Millisecond)   // 2: open -> half-open, lazily
	send(t, b, domain.OutcomeUnknown, 0) // 3: half-open -> open, 100 ms
	clk.advance(100 * time.Millisecond)
	send(t, b, success, 0) // 4: open -> half-open in Admit; 5: half-open -> closed
	send(t, b, fatal, 0)   // 6: closed -> open, 600 ms

	ms := func(n int) time.Time { return t0.Add(time.Duration(n) * time.Millisecond) }
	want := []Change{
		{From: Closed, To: Open, Outcome: failure, Until: ms(50)},
		{From: Open, To: HalfOpen},
		{From: HalfOpen, To: Open, Outcome: domain.OutcomeUnknown, Until: ms(150)},
		{From: Open, To: HalfOpen},
		{From: HalfOpen, To: Closed, Outcome: success},
		{From: Closed, To: Open, Outcome: fatal, Until: ms(750)},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d changes, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i].From != want[i].From || got[i].To != want[i].To ||
			got[i].Outcome != want[i].Outcome || !got[i].Until.Equal(want[i].Until) {
			t.Errorf("change %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestBreaker_OnChangeIsCalledOutsideTheLock(t *testing.T) {
	clk := newClock()
	var b *Breaker
	locked := true
	b, err := New(testConfig(), clk.now, func(Change) {
		if b.mu.TryLock() {
			locked = false
			b.mu.Unlock()
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	openFromClosed(t, b)
	if locked {
		t.Fatal("onChange ran while the breaker's lock was held")
	}
}

func TestNew_RejectsInvalidConfig(t *testing.T) {
	cases := []struct {
		name   string
		modify func(*Config)
	}{
		{"zero window", func(c *Config) { c.Window = 0 }},
		{"window shorter than its buckets", func(c *Config) { c.Window = windowBuckets - 1 }},
		{"zero min requests", func(c *Config) { c.MinRequests = 0 }},
		{"zero failure ratio", func(c *Config) { c.FailureRatio = 0 }},
		{"failure ratio above one", func(c *Config) { c.FailureRatio = 1.01 }},
		{"failure ratio NaN", func(c *Config) { c.FailureRatio = math.NaN() }},
		{"zero consecutive failures", func(c *Config) { c.ConsecutiveFailures = 0 }},
		{"zero open base", func(c *Config) { c.OpenBase = 0 }},
		{"open max below open base", func(c *Config) { c.OpenMax = c.OpenBase - 1 }},
		{"zero fatal open", func(c *Config) { c.FatalOpen = 0 }},
		{"zero throttle default", func(c *Config) { c.ThrottleDefault = 0 }},
		{"throttle max below default", func(c *Config) { c.ThrottleMax = c.ThrottleDefault - 1 }},
		{"zero retry-after cap", func(c *Config) { c.RetryAfterCap = 0 }},
		{"negative probe wait", func(c *Config) { c.ProbeWait = -time.Millisecond }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig()
			tc.modify(&cfg)
			if b, err := New(cfg, newClock().now, nil); err == nil || b != nil {
				t.Fatalf("New = %v, %v; want an error", b, err)
			}
		})
	}
	t.Run("nil clock", func(t *testing.T) {
		if _, err := New(testConfig(), nil, nil); err == nil {
			t.Fatal("New accepted a nil clock")
		}
	})
	t.Run("valid", func(t *testing.T) {
		cfg := testConfig()
		cfg.FailureRatio = 1
		cfg.OpenMax = cfg.OpenBase
		if _, err := New(cfg, newClock().now, nil); err != nil {
			t.Fatalf("New: %v", err)
		}
	})
}

func TestState_String(t *testing.T) {
	for s, want := range map[State]string{Closed: "closed", Open: "open", HalfOpen: "half_open", State(9): "State(9)"} {
		if got := s.String(); got != want {
			t.Errorf("String() = %q, want %q", got, want)
		}
	}
}

// TestBreaker_ConcurrentUse runs every method from many goroutines while the
// clock moves, for the race detector, and checks two invariants: never two
// probes in flight, and no send admitted while open. Every state change
// starts a new generation and onChange sees the changes in order, so the
// state of the generation a permit was admitted in is known afterwards.
func TestBreaker_ConcurrentUse(t *testing.T) {
	clk := newClock()
	var mu sync.Mutex
	var changes []Change
	b, err := New(testConfig(), clk.now, func(c Change) {
		mu.Lock()
		changes = append(changes, c)
		mu.Unlock()
	})
	if err != nil {
		t.Fatal(err)
	}
	outcomes := []domain.Outcome{
		success, success, success, success, success, success, success, success, success, success,
		success, success, success, success, success, success, failure, domain.OutcomeUnknown,
		domain.OutcomeUnreachable, throttled, fatal, domain.OutcomePermanent, domain.OutcomeAborted,
		domain.OutcomeInternalError,
	}
	const goroutines, steps = 32, 3000
	var probesInFlight atomic.Int32
	gens := make([][]uint64, goroutines)
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Go(func() {
			r := rand.New(rand.NewPCG(uint64(g), 7)) //nolint:gosec // deterministic test schedule
			var held []Permit
			record := func(i int) {
				p := held[i]
				held = append(held[:i], held[i+1:]...)
				if p.t.probe {
					probesInFlight.Add(-1)
				}
				p.Record(outcomes[r.IntN(len(outcomes))], time.Duration(r.IntN(3))*10*time.Millisecond)
			}
			for range steps {
				switch r.IntN(8) {
				case 0:
					b.Ready()
				case 1:
					b.State()
				case 2:
					clk.advance(time.Duration(r.IntN(4)) * time.Millisecond)
				case 3, 4:
					if len(held) > 0 {
						record(r.IntN(len(held)))
					}
				default:
					p, ok, _ := b.Admit()
					if !ok {
						continue
					}
					gens[g] = append(gens[g], p.t.gen)
					if p.t.probe && probesInFlight.Add(1) > 1 {
						t.Error("two probes in flight")
					}
					held = append(held, p)
				}
			}
			for len(held) > 0 {
				record(0)
			}
		})
	}
	wg.Wait()

	states := []State{Closed} // states[g] is the state of generation g
	for i, c := range changes {
		if c.From != states[i] || c.From == c.To {
			t.Fatalf("change %d = %+v does not follow state %s", i, c, states[i])
		}
		states = append(states, c.To)
	}
	b.mu.Lock()
	gen := b.gen
	b.mu.Unlock()
	if uint64(len(states)) != gen+1 {
		t.Fatalf("%d changes delivered, %d happened", len(states)-1, gen)
	}
	admitted := 0
	for _, list := range gens {
		for _, g := range list {
			admitted++
			if states[g] == Open {
				t.Fatalf("a send was admitted in generation %d, which is open", g)
			}
		}
	}
	opened := 0
	for _, c := range changes {
		if c.To == Open {
			opened++
		}
	}
	if admitted == 0 || opened == 0 {
		t.Fatalf("the schedule exercised too little: %d admitted, %d openings", admitted, opened)
	}
	t.Logf("%d sends admitted, %d state changes, %d openings", admitted, len(changes), opened)
}
