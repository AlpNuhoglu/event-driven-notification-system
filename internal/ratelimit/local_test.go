package ratelimit

import (
	"context"
	"math"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/ratewindow"
)

// fakeClock is an injected clock. When step is set, every reading advances
// it by step(n), n being the number of the reading.
type fakeClock struct {
	mu   sync.Mutex
	t    time.Time
	n    int
	step func(n int) time.Duration
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.t
	if c.step != nil {
		c.t = c.t.Add(c.step(c.n))
		c.n++
	}
	return now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newLocal(t *testing.T, perSecond, replicas int, horizon time.Duration, clock *fakeClock) *Local {
	t.Helper()
	l, err := NewLocal(perSecond, replicas, horizon, clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func reserveLocal(t *testing.T, l *Local, ch domain.Channel) time.Duration {
	t.Helper()
	wait, ok, err := l.Reserve(context.Background(), ch)
	if err != nil || !ok {
		t.Fatalf("reserve %s: ok %v, error %v", ch, ok, err)
	}
	return wait
}

func TestLocalInterval_ReplicasDivisor(t *testing.T) {
	for _, c := range []struct {
		perSecond, replicas int
		want                time.Duration
	}{
		{98, 1, 10205 * time.Microsecond},  // 98/s
		{98, 2, 20409 * time.Microsecond},  // 49/s: 20408.16 rounded up
		{98, 3, 30613 * time.Microsecond},  // 32.67/s: 30612.24 rounded up
		{100, 3, 30000 * time.Microsecond}, // exact
		{1, 1, time.Second},
	} {
		l := newLocal(t, c.perSecond, c.replicas, time.Second, newFakeClock())
		if l.Interval() != c.want {
			t.Errorf("rate %d / %d replicas: interval %s, want %s", c.perSecond, c.replicas, l.Interval(), c.want)
		}
		// Rounding can only slow the pace: never more than perSecond/replicas per second.
		if got := float64(time.Second) / float64(l.Interval()); got > float64(c.perSecond)/float64(c.replicas) {
			t.Errorf("rate %d / %d replicas: pace %.4f/s exceeds the share", c.perSecond, c.replicas, got)
		}
	}
}

func TestNewLocalValidation(t *testing.T) {
	for _, c := range []struct {
		name                string
		perSecond, replicas int
		horizon             time.Duration
	}{
		{"zero rate", 0, 1, time.Second},
		{"zero replicas", 98, 0, time.Second},
		{"negative replicas", 98, -1, time.Second},
		{"negative horizon", 98, 1, -time.Millisecond},
	} {
		if _, err := NewLocal(c.perSecond, c.replicas, c.horizon, nil); err == nil {
			t.Errorf("%s accepted", c.name)
		}
	}
	if _, err := NewLocal(98, 1, 0, nil); err != nil {
		t.Errorf("zero horizon rejected: %v", err)
	}
}

// Concurrent callers on a clock that moves on irregularly (sometimes by more
// than the interval): the slots are never closer than one interval, and no
// one-second window holds more than the share of the rate, rounded up.
func TestLocal_ConcurrentSpacing(t *testing.T) {
	const (
		callers   = 8
		perCaller = 150
		rate      = 98
	)
	for _, replicas := range []int{1, 2, 3} {
		clock := newFakeClock()
		clock.step = func(n int) time.Duration { return time.Duration(n%7) * 4 * time.Millisecond }
		l := newLocal(t, rate, replicas, time.Hour, clock)

		var mu sync.Mutex
		var slots []time.Time
		var wg sync.WaitGroup
		for range callers {
			wg.Go(func() {
				for range perCaller {
					_, slot, ok, err := l.reserve(context.Background(), domain.ChannelSMS)
					if err != nil || !ok {
						t.Errorf("reserve: ok %v, error %v", ok, err)
						return
					}
					mu.Lock()
					slots = append(slots, slot)
					mu.Unlock()
				}
			})
		}
		wg.Wait()

		slices.SortFunc(slots, time.Time.Compare)
		for i := 1; i < len(slots); i++ {
			if d := slots[i].Sub(slots[i-1]); d < l.Interval() {
				t.Fatalf("replicas %d: slot %d is %s after the previous, below the interval %s", replicas, i, d, l.Interval())
			}
		}
		limit := int(math.Ceil(float64(rate) / float64(replicas)))
		if got := ratewindow.MaxInWindow(slots, time.Second); got > limit {
			t.Errorf("replicas %d: a one-second window holds %d slots, more than %d", replicas, got, limit)
		}
	}
}

// After an idle period the slots start at now and are spaced by the
// interval again: idle time builds up no burst.
func TestLocal_NoBurstAfterIdle(t *testing.T) {
	clock := newFakeClock()
	l := newLocal(t, 98, 1, time.Hour, clock)
	reserveLocal(t, l, domain.ChannelSMS)
	clock.Advance(10 * time.Second)
	for i := range 5 {
		if got, want := reserveLocal(t, l, domain.ChannelSMS), time.Duration(i)*l.Interval(); got != want {
			t.Errorf("reservation %d after idle: wait %s, want %s", i, got, want)
		}
	}
}

// A slot beyond the horizon is refused and nothing is reserved.
func TestLocal_Horizon(t *testing.T) {
	clock := newFakeClock()
	l := newLocal(t, 98, 1, 25*time.Millisecond, clock)
	for i := range 3 { // waits 0, 10.205 ms, 20.41 ms
		if got := reserveLocal(t, l, domain.ChannelSMS); got != time.Duration(i)*l.Interval() {
			t.Errorf("reservation %d: wait %s", i, got)
		}
	}
	for range 3 { // 30.615 ms > 25 ms: refused, and the refusals reserve nothing
		if wait, ok, err := l.Reserve(context.Background(), domain.ChannelSMS); ok || err != nil || wait != 0 {
			t.Fatalf("beyond the horizon: wait %s, ok %v, error %v", wait, ok, err)
		}
	}
	clock.Advance(l.Interval())
	if got := reserveLocal(t, l, domain.ChannelSMS); got != 2*l.Interval() {
		t.Errorf("after one interval: wait %s, want %s", got, 2*l.Interval())
	}

	zero := newLocal(t, 98, 1, 0, newFakeClock())
	reserveLocal(t, zero, domain.ChannelSMS)
	if _, ok, _ := zero.Reserve(context.Background(), domain.ChannelSMS); ok {
		t.Error("zero horizon: a second slot at the same instant was reserved")
	}
}

// Each channel has its own schedule.
func TestLocal_ChannelsAreIndependent(t *testing.T) {
	l := newLocal(t, 98, 1, time.Hour, newFakeClock())
	for range 5 {
		reserveLocal(t, l, domain.ChannelSMS)
	}
	for _, ch := range []domain.Channel{domain.ChannelEmail, domain.ChannelPush} {
		if got := reserveLocal(t, l, ch); got != 0 {
			t.Errorf("%s: first wait %s, want 0", ch, got)
		}
	}
	if got := reserveLocal(t, l, domain.ChannelSMS); got != 5*l.Interval() {
		t.Errorf("sms: sixth wait %s, want %s", got, 5*l.Interval())
	}
}

// A cancelled context reserves nothing and returns its error.
func TestLocal_CancelledContext(t *testing.T) {
	l := newLocal(t, 98, 1, time.Hour, newFakeClock())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, ok, err := l.Reserve(ctx, domain.ChannelSMS); ok || err == nil {
		t.Fatalf("cancelled: ok %v, error %v", ok, err)
	}
	if got := reserveLocal(t, l, domain.ChannelSMS); got != 0 {
		t.Errorf("after a cancelled call: wait %s, want 0", got)
	}
}
