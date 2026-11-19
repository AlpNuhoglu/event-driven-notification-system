//go:build integration

package ratelimit

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

// Durations of the fallback tests, in milliseconds.
const (
	itTimeout = 100 * time.Millisecond
	itProbe   = 200 * time.Millisecond
	itWait    = 10_000 * time.Millisecond
)

// proxiedFallback is a Fallback over the Redis limiter of a client that
// reaches the test's Redis database through a proxy, built with the
// worker's client options. Both limiters have a long horizon, so that the
// bursts of reservations of the tests are never refused.
type proxiedFallback struct {
	rdb   *redis.Client // direct, for inspection
	proxy *testutil.Proxy
	reg   *prometheus.Registry
	f     *Fallback
}

func newProxiedFallback(t *testing.T) *proxiedFallback {
	t.Helper()
	rdb := testutil.NewRedis(t)
	proxy, url := testutil.NewRedisProxy(t, rdb)
	opts, err := RedisOptions(url)
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(opts)
	t.Cleanup(func() { _ = client.Close() })
	reg := prometheus.NewRegistry()
	f, err := NewFallback(newLimiter(t, client, 98, longHorizon), FallbackConfig{
		PerSecond: 98, Replicas: 2, Horizon: longHorizon,
		Timeout: itTimeout, ProbeInterval: itProbe,
		Registry: reg, Logger: slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	return &proxiedFallback{rdb: rdb, proxy: proxy, reg: reg, f: f}
}

// reserve reserves an SMS slot, which must succeed, and returns how long the
// call took.
func (p *proxiedFallback) reserve(t *testing.T) time.Duration {
	t.Helper()
	start := time.Now()
	_, ok, err := p.f.Reserve(context.Background(), domain.ChannelSMS)
	took := time.Since(start)
	if err != nil || !ok {
		t.Fatalf("reserve: ok %v, error %v", ok, err)
	}
	return took
}

func (p *proxiedFallback) gauge(t *testing.T) float64 {
	t.Helper()
	mfs, err := p.reg.Gather()
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

// tat reads the theoretical arrival time the script stored for SMS; 0 when
// there is none.
func (p *proxiedFallback) tat(t *testing.T) int64 {
	t.Helper()
	v, err := p.rdb.Get(context.Background(), "rl:sms").Result()
	if errors.Is(err, redis.Nil) {
		return 0
	}
	if err != nil {
		t.Fatal(err)
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// Redis becomes unreachable: reservations keep succeeding from the
// in-process limiter, and the gauge is 1. When Redis is back, the gauge
// returns to 0 and the slots come from Redis again.
func TestFallback_RedisUnreachableAndBack(t *testing.T) {
	p := newProxiedFallback(t)
	p.reserve(t)
	if p.tat(t) == 0 || p.gauge(t) != 0 {
		t.Fatalf("with Redis up: tat %d, gauge %v", p.tat(t), p.gauge(t))
	}

	p.proxy.Break()
	if err := p.rdb.Del(context.Background(), "rl:sms").Err(); err != nil {
		t.Fatal(err)
	}
	for i := range 20 {
		if took := p.reserve(t); took > itTimeout+100*time.Millisecond {
			t.Errorf("reservation %d with Redis unreachable took %s, more than the timeout %s", i, took, itTimeout)
		}
	}
	if !p.f.Active() || p.gauge(t) != 1 {
		t.Fatalf("Redis unreachable: active %v, gauge %v", p.f.Active(), p.gauge(t))
	}
	if p.tat(t) != 0 {
		t.Fatal("a slot was written to Redis while it was unreachable")
	}

	p.proxy.Restore(t)
	testutil.Eventually(t, func() bool { p.reserve(t); return !p.f.Active() }, itWait)
	if p.gauge(t) != 0 {
		t.Errorf("gauge %v after Redis came back, want 0", p.gauge(t))
	}
	first := p.tat(t)
	p.reserve(t)
	second := p.tat(t)
	if first == 0 || second-first < 10205 {
		t.Errorf("slots after Redis came back: tat %d then %d; want Redis to schedule them one interval (10205us) apart or more", first, second)
	}
}

// A Redis that accepts the connection but never answers costs the call that
// meets it at most the timeout; the next calls before the probe is due do
// not touch Redis at all.
func TestFallback_HangingRedisCostsOneTimeout(t *testing.T) {
	p := newProxiedFallback(t)
	p.reserve(t)
	p.proxy.Pause()
	t.Cleanup(p.proxy.Resume)

	took := p.reserve(t)
	if took < itTimeout || took > itTimeout+100*time.Millisecond {
		t.Errorf("the call that met the hanging Redis took %s, want about the timeout %s", took, itTimeout)
	}
	if p.gauge(t) != 1 {
		t.Fatalf("gauge %v, want 1", p.gauge(t))
	}
	start := time.Now()
	for range 10 {
		p.reserve(t)
	}
	if took := time.Since(start); took >= itProbe {
		t.Fatalf("ten local reservations took %s", took)
	} else if took > itTimeout/2 {
		t.Errorf("ten reservations before the probe took %s: they waited on Redis", took)
	}

	p.proxy.Resume()
	testutil.Eventually(t, func() bool { p.reserve(t); return !p.f.Active() }, itWait)
	if p.gauge(t) != 0 {
		t.Errorf("gauge %v after Redis answered again, want 0", p.gauge(t))
	}
}
