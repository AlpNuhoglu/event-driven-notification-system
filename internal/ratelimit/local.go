package ratelimit

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

// Local is the in-process limiter of the Redis-down fallback (D25): GCRA with
// zero burst per channel, as the Lua script, on the process's own clock. Each
// replica paces at perSecond / replicas, so that the replicas together stay
// near the shared rate; they share no clock, so the limit is approximate.
type Local struct {
	now      func() time.Time
	interval time.Duration
	horizon  time.Duration

	mu  sync.Mutex
	tat map[domain.Channel]time.Time // the theoretical arrival time per channel
}

// NewLocal returns an in-process limiter pacing each channel at
// perSecond / replicas with the given reservation horizon. now is the clock;
// nil means time.Now, whose readings carry the monotonic clock, so a change
// of the wall clock does not move the schedule.
func NewLocal(perSecond, replicas int, horizon time.Duration, now func() time.Time) (*Local, error) {
	if perSecond <= 0 {
		return nil, fmt.Errorf("ratelimit: rate must be positive, got %d", perSecond)
	}
	if replicas <= 0 {
		return nil, fmt.Errorf("ratelimit: replicas must be positive, got %d", replicas)
	}
	if horizon < 0 {
		return nil, fmt.Errorf("ratelimit: horizon must not be negative, got %s", horizon)
	}
	if now == nil {
		now = time.Now
	}
	return &Local{
		now:      now,
		interval: time.Duration(localIntervalMicros(perSecond, replicas)) * time.Microsecond,
		horizon:  horizon,
		tat:      map[domain.Channel]time.Time{},
	}, nil
}

// localIntervalMicros is the emission interval at perSecond / replicas in
// microseconds, rounded up as in intervalMicros, so that rounding can only
// slow the pace down.
func localIntervalMicros(perSecond, replicas int) int64 {
	n, r := int64(perSecond), int64(replicas)
	return (1_000_000*r + n - 1) / n
}

// Interval returns the emission interval of each channel.
func (l *Local) Interval() time.Duration { return l.interval }

// Reserve has the contract of Limiter.Reserve: a relative wait until the
// reserved slot, or ok == false and nothing reserved when the next free slot
// starts beyond the horizon. It returns an error only when ctx has ended.
func (l *Local) Reserve(ctx context.Context, channel domain.Channel) (wait time.Duration, ok bool, err error) {
	wait, _, ok, err = l.reserve(ctx, channel)
	return wait, ok, err
}

// reserve is Reserve that also returns the slot's time on the limiter's clock.
func (l *Local) reserve(ctx context.Context, channel domain.Channel) (wait time.Duration, slot time.Time, ok bool, err error) {
	if err := ctx.Err(); err != nil {
		return 0, time.Time{}, false, fmt.Errorf("ratelimit: local reserve %s: %w", channel, err)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	// As in the Lua script: a slot is max(TAT, now), and taking it moves TAT
	// one interval past the slot, so two slots are never closer than one
	// interval and an idle channel accumulates no burst.
	now := l.now()
	tat, seen := l.tat[channel]
	if !seen || tat.Before(now) {
		tat = now
	}
	wait = tat.Sub(now)
	if wait > l.horizon {
		return 0, time.Time{}, false, nil
	}
	l.tat[channel] = tat.Add(l.interval)
	return wait, tat, true, nil
}
