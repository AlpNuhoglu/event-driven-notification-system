package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

// Reserver reserves send slots; *Limiter (Redis) and *Local implement it,
// with the contract of Limiter.Reserve.
type Reserver interface {
	Reserve(ctx context.Context, channel domain.Channel) (wait time.Duration, ok bool, err error)
}

// FallbackMetric is the name of the gauge that is 1 while the fallback is
// active (ADR-0009 §1).
const FallbackMetric = "limiter_fallback_active"

// FallbackConfig configures NewFallback.
type FallbackConfig struct {
	// PerSecond, Replicas and Horizon configure the in-process limiter:
	// PerSecond / Replicas per channel, with the horizon of the Redis one.
	PerSecond int
	Replicas  int
	Horizon   time.Duration
	// Timeout bounds every call to the primary, so that an unreachable or
	// hanging Redis cannot stall the pacer longer than this.
	Timeout time.Duration
	// ProbeInterval is how often the primary is asked again while the
	// fallback is active.
	ProbeInterval time.Duration
	// Registry receives the gauge limiter_fallback_active.
	Registry prometheus.Registerer
	Logger   *slog.Logger
	// Now is the clock of the probe schedule and the in-process limiter;
	// nil means time.Now.
	Now func() time.Time
}

// Fallback is the rate limiter of the worker (D25): the shared Redis limiter
// while Redis answers, an in-process limiter while it does not, so that
// sending never stops because of Redis.
//
// A call to the primary that fails with an error other than the caller's own
// cancelled context switches to the fallback, and that same call is answered
// by the in-process limiter. While the fallback is active, the primary is
// asked again at most once per probe interval and by one caller at a time;
// every other call is answered locally. The first call the primary answers
// ends the fallback. The caller never sees the primary's error.
//
// The primary must honour the deadline of its context (for go-redis:
// Options.ContextTimeoutEnabled, which RedisOptions sets).
type Fallback struct {
	primary    Reserver
	local      *Local
	timeout    time.Duration
	probeEvery time.Duration
	now        func() time.Time
	gauge      prometheus.Gauge
	log        *slog.Logger

	mu        sync.Mutex
	active    bool
	probing   bool      // a caller is asking the primary while active
	nextProbe time.Time // while active: the earliest time of the next probe
}

// NewFallback returns a Fallback over primary and registers its gauge on
// cfg.Registry. A gauge of the same name that is already registered there is
// reused, so a process that builds the role twice keeps one series.
func NewFallback(primary Reserver, cfg FallbackConfig) (*Fallback, error) {
	switch {
	case primary == nil:
		return nil, errors.New("ratelimit: nil primary limiter")
	case cfg.Registry == nil, cfg.Logger == nil:
		return nil, errors.New("ratelimit: fallback needs a registry and a logger")
	case cfg.Timeout <= 0:
		return nil, fmt.Errorf("ratelimit: timeout must be positive, got %s", cfg.Timeout)
	case cfg.ProbeInterval <= 0:
		return nil, fmt.Errorf("ratelimit: probe interval must be positive, got %s", cfg.ProbeInterval)
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	local, err := NewLocal(cfg.PerSecond, cfg.Replicas, cfg.Horizon, now)
	if err != nil {
		return nil, err
	}
	gauge, err := registerGauge(cfg.Registry, prometheus.NewGauge(prometheus.GaugeOpts{
		Name: FallbackMetric,
		Help: "1 while the rate limit is applied by the in-process fallback because Redis is unavailable.",
	}))
	if err != nil {
		return nil, fmt.Errorf("ratelimit: register %s: %w", FallbackMetric, err)
	}
	gauge.Set(0)
	return &Fallback{
		primary:    primary,
		local:      local,
		timeout:    cfg.Timeout,
		probeEvery: cfg.ProbeInterval,
		now:        now,
		gauge:      gauge,
		log:        cfg.Logger,
	}, nil
}

func registerGauge(reg prometheus.Registerer, g prometheus.Gauge) (prometheus.Gauge, error) {
	err := reg.Register(g)
	var already prometheus.AlreadyRegisteredError
	if errors.As(err, &already) {
		if existing, ok := already.ExistingCollector.(prometheus.Gauge); ok {
			return existing, nil
		}
	}
	return g, err
}

// Active reports whether the fallback is active.
func (f *Fallback) Active() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.active
}

// Reserve has the contract of Limiter.Reserve. It returns an error only when
// ctx has ended.
func (f *Fallback) Reserve(ctx context.Context, channel domain.Channel) (time.Duration, bool, error) {
	f.mu.Lock()
	if f.active && (f.probing || f.now().Before(f.nextProbe)) {
		f.mu.Unlock()
		return f.local.Reserve(ctx, channel)
	}
	probe := f.active
	f.probing = probe
	f.mu.Unlock()

	callCtx, cancel := context.WithTimeout(ctx, f.timeout)
	wait, ok, err := f.primary.Reserve(callCtx, channel)
	cancel()

	f.mu.Lock()
	defer f.mu.Unlock()
	if probe {
		f.probing = false
	}
	switch {
	case err == nil:
		if f.active {
			f.active = false
			f.gauge.Set(0)
			f.log.Info("ratelimit: Redis answers again; the in-process fallback has ended")
		}
		return wait, ok, nil
	case ctx.Err() != nil:
		// The caller's own cancellation says nothing about Redis.
		return 0, false, err
	}
	f.nextProbe = f.now().Add(f.probeEvery)
	if !f.active {
		f.active = true
		f.gauge.Set(1)
		f.log.Warn("ratelimit: Redis unavailable; pacing with the in-process fallback limiter",
			slog.Any("error", err), slog.Duration("interval", f.local.Interval()))
	}
	return f.local.Reserve(ctx, channel)
}

// RedisOptions parses a Redis URL into client options for the limiter. It
// enables ContextTimeoutEnabled: without it go-redis ignores the deadline of
// the context on the socket and waits for its own read timeout (seconds),
// so a hanging Redis would stall the pacer beyond the fallback's timeout.
func RedisOptions(url string) (*redis.Options, error) {
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	opts.ContextTimeoutEnabled = true
	return opts, nil
}
