package domain

import (
	"fmt"
	"math"
	"testing"
	"time"
)

var (
	testNow = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	profHigh   = defaultProfile(time.Second, 30*time.Second)
	profNormal = defaultProfile(5*time.Second, 10*time.Minute)
	profLow    = defaultProfile(30*time.Second, time.Hour)
)

func defaultProfile(base, capDur time.Duration) Profile {
	return Profile{
		Base: base, Cap: capDur, MinDelay: time.Second,
		RetryAfterCap: 10 * time.Minute, ThrottleDefault: 5 * time.Second,
	}
}

func TestDecideRetry(t *testing.T) {
	prof := Profile{Base: 10 * time.Second, Cap: time.Minute, MinDelay: 2 * time.Second}
	tests := []struct {
		name     string
		outcome  Outcome
		failures int
		max      int
		profile  Profile
		jitter   float64
		want     Decision
	}{
		{
			"budget remaining", OutcomeRetryable, 0, 5, prof, 0.5,
			Decision{Action: ActionRetry, Delay: 5 * time.Second, ConsumesBudget: true},
		},
		{
			"min delay wins", OutcomeRetryable, 1, 5,
			Profile{Base: time.Second, Cap: time.Minute, MinDelay: 5 * time.Second},
			0.5,
			Decision{Action: ActionRetry, Delay: 5 * time.Second, ConsumesBudget: true},
		},
		{
			"last failure", OutcomeRetryable, 4, 5, prof, 0.5,
			Decision{Action: ActionFail, Reason: ReasonMaxFailures, ConsumesBudget: true},
		},
		{
			"last failure unknown", OutcomeUnknown, 4, 5, prof, 0.5,
			Decision{Action: ActionFail, Reason: ReasonMaxFailuresUnknownOutcome, ConsumesBudget: true},
		},
		{
			"max one fails first", OutcomeRetryable, 0, 1, prof, 0.5,
			Decision{Action: ActionFail, Reason: ReasonMaxFailures, ConsumesBudget: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DecideRetry(tt.outcome, tt.failures, tt.max, testNow.Add(time.Hour), 0, tt.profile, testNow, tt.jitter)
			if got != tt.want {
				t.Errorf("got %+v, want %+v", got, tt.want)
			}
			if again := DecideRetry(tt.outcome, tt.failures, tt.max, testNow.Add(time.Hour), 0, tt.profile, testNow, tt.jitter); again != got {
				t.Error("not deterministic")
			}
		})
	}
}

func TestDecideRetryBudget(t *testing.T) {
	far := testNow.Add(1000 * time.Hour)
	for _, maxF := range []int{1, 5, 8, 10} {
		for _, tc := range []struct {
			outcome Outcome
			reason  FailureReason
		}{
			{OutcomeRetryable, ReasonMaxFailures},
			{OutcomeUnknown, ReasonMaxFailuresUnknownOutcome},
			{OutcomeInternalError, ReasonInternalError},
		} {
			t.Run(fmt.Sprintf("%s/max%d", tc.outcome, maxF), func(t *testing.T) {
				last := DecideRetry(tc.outcome, maxF-1, maxF, far, 0, profNormal, testNow, 0.5)
				want := Decision{Action: ActionFail, Reason: tc.reason, ConsumesBudget: true}
				if last != want {
					t.Errorf("failure %d of %d: got %+v, want %+v", maxF, maxF, last, want)
				}
				if maxF > 1 {
					before := DecideRetry(tc.outcome, maxF-2, maxF, far, 0, profNormal, testNow, 0.5)
					if before.Action != ActionRetry || !before.ConsumesBudget {
						t.Errorf("failure %d of %d: got %+v, want a retry", maxF-1, maxF, before)
					}
				}
				over := DecideRetry(tc.outcome, maxF+3, maxF, far, 0, profNormal, testNow, 0.5)
				if over != want {
					t.Errorf("failures beyond the budget: got %+v, want %+v", over, want)
				}
			})
		}
	}
	for _, o := range []Outcome{OutcomeUnreachable, OutcomeThrottled, OutcomeProviderFatal} {
		for _, failures := range []int{0, 4, 5, 100} {
			got := DecideRetry(o, failures, 5, far, 0, profNormal, testNow, 0.5)
			if got.Action != ActionRetry || got.ConsumesBudget {
				t.Errorf("%s with failures=%d: got %+v, want a retry without budget", o, failures, got)
			}
		}
	}
	// max_failures 1: a non-consuming outcome still retries.
	if got := DecideRetry(OutcomeUnreachable, 0, 1, far, 0, profNormal, testNow, 0.5); got.Action != ActionRetry {
		t.Errorf("unreachable with max 1: got %+v", got)
	}
}

func TestDecideRetryScope(t *testing.T) {
	far := testNow.Add(1000 * time.Hour)
	tests := []struct {
		o    Outcome
		want bool
	}{
		{OutcomeRetryable, true},
		{OutcomeUnknown, true},
		{OutcomeInternalError, true},
		{OutcomePermanent, false},
		{OutcomeProviderFatal, false},
		{OutcomeThrottled, false},
		{OutcomeUnreachable, false},
		// Not defined for DecideRetry: handled like internal_error.
		{OutcomeSuccess, true},
		{OutcomeInFlight, true},
		{OutcomeAborted, true},
		{Outcome(""), true},
		{Outcome("bogus"), true},
	}
	for _, tt := range tests {
		got := DecideRetry(tt.o, 0, 5, far, 0, profNormal, testNow, 0.5)
		if got.ConsumesBudget != tt.want {
			t.Errorf("%q: retry ConsumesBudget = %v, want %v", tt.o, got.ConsumesBudget, tt.want)
		}
		last := DecideRetry(tt.o, 4, 5, far, 0, profNormal, testNow, 0.5)
		if last.ConsumesBudget != tt.want {
			t.Errorf("%q: final ConsumesBudget = %v, want %v", tt.o, last.ConsumesBudget, tt.want)
		}
	}
	// Undefined outcomes fail like internal_error when the budget is used up.
	for _, o := range []Outcome{OutcomeSuccess, OutcomeInFlight, OutcomeAborted, Outcome("")} {
		got := DecideRetry(o, 4, 5, far, 0, profNormal, testNow, 0.5)
		if got.Action != ActionFail || got.Reason != ReasonInternalError {
			t.Errorf("%q at the budget: got %+v, want internal_error", o, got)
		}
	}
}

// refBackoff is an independent reference for jitter x min(cap, base x 2^failures).
func refBackoff(p Profile, failures int, jitter float64) time.Duration {
	v := math.Ldexp(float64(p.Base), failures)
	v = math.Min(v, float64(p.Cap))
	return max(time.Duration(v*jitter), p.MinDelay)
}

func TestDecideRetryFixedJitter(t *testing.T) {
	far := testNow.Add(1000 * time.Hour)
	profiles := map[string]Profile{"high": profHigh, "normal": profNormal, "low": profLow}
	for name, p := range profiles {
		for _, j := range []float64{0, 0.25, 0.5, 0.999} {
			for f := 0; f <= 12; f++ {
				got := DecideRetry(OutcomeUnreachable, f, 100, far, 0, p, testNow, j)
				want := refBackoff(p, f, j)
				if got.Action != ActionRetry || got.Delay != want {
					t.Errorf("%s jitter %v failures %d: got %+v, want delay %v", name, j, f, got, want)
				}
			}
		}
	}
	// Literal spot checks.
	spot := []struct {
		p        Profile
		failures int
		jitter   float64
		want     time.Duration
	}{
		{profHigh, 0, 0.5, time.Second}, // 0.5 s raised to the floor
		{profHigh, 3, 0.5, 4 * time.Second},
		{profHigh, 12, 0.5, 15 * time.Second}, // capped at 30 s
		{profNormal, 0, 0.5, 2500 * time.Millisecond},
		{profNormal, 5, 0.25, 40 * time.Second},
		{profNormal, 12, 0.5, 5 * time.Minute},
		{profLow, 2, 0.5, time.Minute},
		{profLow, 12, 0.25, 15 * time.Minute},
		{profLow, 3, 0, time.Second},
	}
	for _, s := range spot {
		got := DecideRetry(OutcomeUnreachable, s.failures, 100, far, 0, s.p, testNow, s.jitter)
		if got.Delay != s.want {
			t.Errorf("%+v failures %d jitter %v: delay %v, want %v", s.p, s.failures, s.jitter, got.Delay, s.want)
		}
	}
}

func TestDecideRetryBounds(t *testing.T) {
	far := testNow.Add(100000 * time.Hour)
	jitters := []float64{-1, 0, 1e-9, 0.1, 0.25, 0.5, 0.75, 0.999, 0.9999999999999999, 1, 2, math.NaN(), math.Inf(1)}
	for name, p := range map[string]Profile{"high": profHigh, "normal": profNormal, "low": profLow} {
		hintCeiling := max(p.Cap, p.RetryAfterCap)
		for f := -3; f <= 200; f++ {
			for _, j := range jitters {
				d := DecideRetry(OutcomeUnreachable, f, 1000, far, 0, p, testNow, j)
				if d.Action != ActionRetry || d.Delay < p.MinDelay || d.Delay > p.Cap {
					t.Fatalf("%s failures %d jitter %v: %+v outside [%v, %v]", name, f, j, d, p.MinDelay, p.Cap)
				}
				for _, hint := range []time.Duration{time.Second, time.Minute, time.Hour, 1000 * time.Hour, time.Duration(math.MaxInt64)} {
					d = DecideRetry(OutcomeRetryable, f, 1000, far, hint, p, testNow, j)
					if d.Action != ActionRetry || d.Delay < p.MinDelay || d.Delay > hintCeiling {
						t.Fatalf("%s failures %d jitter %v hint %v: %+v above %v", name, f, j, hint, d, hintCeiling)
					}
				}
			}
		}
	}
	// A huge failure count must neither overflow nor panic.
	d := DecideRetry(OutcomeUnreachable, math.MaxInt32, math.MaxInt, far, 0, profLow, testNow, 0.5)
	if d.Delay != 30*time.Minute {
		t.Errorf("huge failures: delay %v, want 30m", d.Delay)
	}
	// A very large Base must not overflow either.
	big := Profile{Base: time.Duration(math.MaxInt64 / 3), Cap: time.Duration(math.MaxInt64 / 2), MinDelay: time.Second}
	d = DecideRetry(OutcomeUnreachable, 5, 100, testNow.AddDate(2000, 0, 0), 0, big, testNow, 0.5)
	if d.Action != ActionRetry || d.Delay <= 0 || d.Delay > big.Cap {
		t.Errorf("big base: delay %v", d.Delay)
	}
}

func TestDecideRetryRetryAfter(t *testing.T) {
	far := testNow.Add(1000 * time.Hour)
	p := profNormal // failures 0, jitter 0.5: back-off 2.5 s
	tests := []struct {
		name    string
		outcome Outcome
		hint    time.Duration
		want    time.Duration
	}{
		{"hint wins", OutcomeRetryable, 30 * time.Second, 30 * time.Second},
		{"hint loses", OutcomeRetryable, time.Second, 2500 * time.Millisecond},
		{"hint above cap is cut", OutcomeRetryable, time.Hour, 10 * time.Minute},
		{"hint at the cap", OutcomeRetryable, 10 * time.Minute, 10 * time.Minute},
		{"throttled without hint", OutcomeThrottled, 0, 5 * time.Second},
		{"throttled with negative hint", OutcomeThrottled, -time.Second, 5 * time.Second},
		{"throttled with hint", OutcomeThrottled, 20 * time.Second, 20 * time.Second},
		{"throttled with small hint", OutcomeThrottled, time.Second, 2500 * time.Millisecond},
		{"retryable without hint", OutcomeRetryable, 0, 2500 * time.Millisecond},
		{"unreachable without hint", OutcomeUnreachable, 0, 2500 * time.Millisecond},
	}
	for _, tt := range tests {
		got := DecideRetry(tt.outcome, 0, 8, far, tt.hint, p, testNow, 0.5)
		if got.Action != ActionRetry || got.Delay != tt.want {
			t.Errorf("%s: got %+v, want delay %v", tt.name, got, tt.want)
		}
	}
	// The cap on the hint applies to the default too, and a zero cap means no cap.
	small := p
	small.RetryAfterCap = 3 * time.Second
	if got := DecideRetry(OutcomeThrottled, 0, 8, far, 0, small, testNow, 0.5); got.Delay != 3*time.Second {
		t.Errorf("default above cap: %+v", got)
	}
	nocap := p
	nocap.RetryAfterCap = 0
	if got := DecideRetry(OutcomeRetryable, 0, 8, far, time.Hour, nocap, testNow, 0.5); got.Delay != time.Hour {
		t.Errorf("no cap: %+v", got)
	}
}

func TestDecideRetryExpiry(t *testing.T) {
	p := profNormal // failures 0, jitter 0.5: delay 2.5 s
	const delay = 2500 * time.Millisecond
	t.Run("exactly at expiry fails", func(t *testing.T) {
		got := DecideRetry(OutcomeRetryable, 0, 8, testNow.Add(delay), 0, p, testNow, 0.5)
		want := Decision{Action: ActionFail, Reason: ReasonExpired, ConsumesBudget: true}
		if got != want {
			t.Errorf("got %+v, want %+v", got, want)
		}
	})
	t.Run("one nanosecond earlier retries", func(t *testing.T) {
		got := DecideRetry(OutcomeRetryable, 0, 8, testNow.Add(delay+1), 0, p, testNow, 0.5)
		want := Decision{Action: ActionRetry, Delay: delay, ConsumesBudget: true}
		if got != want {
			t.Errorf("got %+v, want %+v", got, want)
		}
	})
	t.Run("already expired", func(t *testing.T) {
		got := DecideRetry(OutcomeRetryable, 0, 8, testNow.Add(-time.Hour), 0, p, testNow, 0.5)
		if got.Action != ActionFail || got.Reason != ReasonExpired {
			t.Errorf("got %+v", got)
		}
	})
	t.Run("hint is part of the delay", func(t *testing.T) {
		got := DecideRetry(OutcomeThrottled, 0, 8, testNow.Add(10*time.Second), 10*time.Second, p, testNow, 0.5)
		want := Decision{Action: ActionFail, Reason: ReasonExpired, ConsumesBudget: false}
		if got != want {
			t.Errorf("got %+v, want %+v", got, want)
		}
	})
	t.Run("budget before expiry", func(t *testing.T) {
		got := DecideRetry(OutcomeUnknown, 7, 8, testNow.Add(-time.Hour), 0, p, testNow, 0.5)
		want := Decision{Action: ActionFail, Reason: ReasonMaxFailuresUnknownOutcome, ConsumesBudget: true}
		if got != want {
			t.Errorf("got %+v, want %+v", got, want)
		}
	})
	t.Run("ConsumesBudget is kept", func(t *testing.T) {
		for _, o := range []Outcome{OutcomeRetryable, OutcomeUnknown, OutcomeUnreachable, OutcomeThrottled, OutcomeProviderFatal} {
			got := DecideRetry(o, 0, 8, testNow, 0, p, testNow, 0.5)
			if got.Action != ActionFail || got.Reason != ReasonExpired || got.ConsumesBudget != o.ConsumesBudget() {
				t.Errorf("%s: got %+v", o, got)
			}
		}
	})
}

func TestDecideRetryPermanent(t *testing.T) {
	want := Decision{Action: ActionFail, Reason: ReasonProviderRejected}
	for _, failures := range []int{-1, 0, 4, 100} {
		for _, maxF := range []int{0, 1, 5} {
			for _, expires := range []time.Time{{}, testNow.Add(-time.Hour), testNow.Add(time.Hour)} {
				for _, hint := range []time.Duration{0, time.Hour} {
					got := DecideRetry(OutcomePermanent, failures, maxF, expires, hint, profHigh, testNow, 0.5)
					if got != want {
						t.Fatalf("failures %d max %d expires %v hint %v: got %+v", failures, maxF, expires, hint, got)
					}
				}
			}
		}
	}
}

func TestActionString(t *testing.T) {
	if ActionRetry.String() != "retry" || ActionFail.String() != "fail" {
		t.Error("bad Action.String")
	}
}
