// Package ratelimit paces sends per channel with GCRA in Redis.
package ratelimit

import (
	"context"
	_ "embed" // gcra.lua
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

//go:embed gcra.lua
var gcraSource string

// Limiter reserves send slots with the GCRA script (zero burst).
type Limiter struct {
	rdb        redis.Scripter
	script     *redis.Script
	intervalUS int64
	horizonUS  int64
	keyPrefix  string
}

// New returns a limiter pacing each channel at perSecond with the given reservation horizon.
func New(rdb redis.Scripter, perSecond int, horizon time.Duration) (*Limiter, error) {
	if rdb == nil {
		return nil, errors.New("ratelimit: nil redis client")
	}
	if perSecond <= 0 {
		return nil, fmt.Errorf("ratelimit: rate must be positive, got %d", perSecond)
	}
	if horizon < 0 {
		return nil, fmt.Errorf("ratelimit: horizon must not be negative, got %s", horizon)
	}
	return &Limiter{
		rdb:        rdb,
		script:     redis.NewScript(gcraSource),
		intervalUS: intervalMicros(perSecond),
		horizonUS:  horizon.Microseconds(),
		keyPrefix:  "rl:",
	}, nil
}

// intervalMicros is the emission interval in microseconds, rounded up so that
// rounding can only slow the pace down.
func intervalMicros(perSecond int) int64 {
	n := int64(perSecond)
	return (1_000_000 + n - 1) / n
}

func (l *Limiter) key(channel domain.Channel) string {
	return l.keyPrefix + string(channel)
}

// Reserve asks for the next slot of the channel.
//
//	ok == true:  a slot is reserved wait from now (a relative wait on the Redis clock).
//	ok == false: the next free slot starts beyond the horizon; nothing was reserved.
func (l *Limiter) Reserve(ctx context.Context, channel domain.Channel) (wait time.Duration, ok bool, err error) {
	wait, _, ok, err = l.reserve(ctx, channel)
	return wait, ok, err
}

// reserve is Reserve that also returns the slot in microseconds on the Redis clock.
func (l *Limiter) reserve(ctx context.Context, channel domain.Channel) (wait time.Duration, slotUS int64, ok bool, err error) {
	res, err := l.script.Run(ctx, l.rdb, []string{l.key(channel)}, l.intervalUS, l.horizonUS).Int64Slice()
	if err != nil {
		return 0, 0, false, fmt.Errorf("ratelimit: reserve %s: %w", channel, err)
	}
	if len(res) != 2 {
		return 0, 0, false, fmt.Errorf("ratelimit: script returned %d values, want 2", len(res))
	}
	if res[0] < 0 {
		return 0, 0, false, nil
	}
	return time.Duration(res[0]) * time.Microsecond, res[1], true, nil
}
