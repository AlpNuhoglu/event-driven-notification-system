package ratelimit

import (
	"testing"
	"time"
)

func TestIntervalMicros(t *testing.T) {
	for perSecond, want := range map[int]int64{98: 10205, 100: 10000, 3: 333334, 1: 1_000_000, 1_000_000: 1, 2_000_000: 1} {
		if got := intervalMicros(perSecond); got != want {
			t.Errorf("intervalMicros(%d) = %d, want %d", perSecond, got, want)
		}
	}
}

func TestNewValidation(t *testing.T) {
	if _, err := New(nil, 98, time.Second); err == nil {
		t.Error("nil client accepted")
	}
	// A non-nil Scripter is needed for the remaining cases; the validation
	// runs before it is used, so a typed nil is enough to reach it.
	for _, c := range []struct {
		name      string
		perSecond int
		horizon   time.Duration
	}{
		{"zero rate", 0, time.Second},
		{"negative rate", -1, time.Second},
		{"negative horizon", 98, -time.Millisecond},
	} {
		if _, err := New(stubScripter{}, c.perSecond, c.horizon); err == nil {
			t.Errorf("%s accepted", c.name)
		}
	}
	l, err := New(stubScripter{}, 98, 250*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if l.intervalUS != 10205 || l.horizonUS != 250_000 {
		t.Errorf("interval %d horizon %d", l.intervalUS, l.horizonUS)
	}
	if _, err := New(stubScripter{}, 98, 0); err != nil {
		t.Errorf("zero horizon rejected: %v", err)
	}
}
