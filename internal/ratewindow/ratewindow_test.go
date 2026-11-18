package ratewindow

import (
	"testing"
	"time"
)

func at(base time.Time, offsets ...time.Duration) []time.Time {
	out := make([]time.Time, len(offsets))
	for i, o := range offsets {
		out[i] = base.Add(o)
	}
	return out
}

func every(base time.Time, step time.Duration, n int) []time.Time {
	out := make([]time.Time, n)
	for i := range out {
		out[i] = base.Add(time.Duration(i) * step)
	}
	return out
}

func TestMaxInWindow(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ms := time.Millisecond
	tests := []struct {
		name   string
		ts     []time.Time
		window time.Duration
		want   int
	}{
		{"empty", nil, time.Second, 0},
		{"zero window", at(base, 0), 0, 0},
		{"one event", at(base, 0), time.Second, 1},
		{"the window is half-open: an event exactly one window later is outside", at(base, 0, 1000*ms), time.Second, 1},
		{"just inside", at(base, 0, 999*ms), time.Second, 2},
		{"unsorted input", at(base, 900*ms, 0, 500*ms, 2000*ms), time.Second, 3},
		{"identical timestamps all count", at(base, 5*ms, 5*ms, 5*ms), time.Second, 3},
		{"the densest window is found in the middle", at(base, 0, 1500*ms, 1600*ms, 1700*ms, 3000*ms), time.Second, 3},
		{"exactly 100 per second, evenly spaced", every(base, 10*ms, 1000), time.Second, 100},
		{"98 per second, evenly spaced", every(base, 10205*time.Microsecond, 1000), time.Second, 98},
		// The case of decision D77: three sends that missed their slots leave
		// together at 30 ms, next to slots used on time.
		{
			"three late sends at 30 ms push a window to 101",
			append(at(base, 30*ms, 30*ms, 30*ms), every(base.Add(30612*time.Microsecond), 10204*time.Microsecond, 98)...),
			time.Second, 101,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := MaxInWindow(tt.ts, tt.window); got != tt.want {
				t.Errorf("MaxInWindow = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestMaxInWindow_DoesNotModifyInput(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ts := at(base, 3*time.Second, 0, time.Second)
	MaxInWindow(ts, time.Second)
	if !ts[0].Equal(base.Add(3 * time.Second)) {
		t.Error("input was reordered")
	}
}
