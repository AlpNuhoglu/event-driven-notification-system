//go:build integration

package integration

import (
	"context"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/bootstrap"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/ratelimit"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/ratewindow"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/worker"
)

// The rate of the fallback tests: 50/s shared by three replicas, so each
// replica's fallback paces at 16.67/s (one slot every 60 ms). No one-second
// window can then hold more than 17 sends, with 20 ms of the 60 ms interval
// to spare for jitter.
const (
	fallbackPerSecond = 50
	fallbackReplicas  = 3
	fallbackSends     = 40
)

// fallbackVars are the configuration of the fallback tests.
var fallbackVars = map[string]string{
	"RATE_LIMIT_PER_SECOND":              "50",
	"WORKER_REPLICAS":                    "3",
	"RATE_LIMIT_REDIS_TIMEOUT":           "100ms",
	"RATE_LIMIT_FALLBACK_PROBE_INTERVAL": "200ms",
}

// gaugeValue reads limiter_fallback_active from g.
func gaugeValue(t *testing.T, g prometheus.Gatherer) float64 {
	t.Helper()
	mfs, err := g.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() == ratelimit.FallbackMetric {
			return mf.GetMetric()[0].GetGauge().GetValue()
		}
	}
	t.Fatalf("%s is not registered", ratelimit.FallbackMetric)
	return -1
}

// D25: Redis becomes unreachable while the worker role runs. Notifications
// keep reaching sent, paced by the in-process limiter at the replica's share
// of the rate, and limiter_fallback_active is 1. When Redis is back the gauge
// returns to 0 and the slots come from Redis again.
func TestE2E_RedisDownSendingContinues(t *testing.T) {
	e := newEnv(t)
	cfg := e.config(fallbackVars)
	proxy, url := testutil.NewRedisProxy(t, e.rdb)
	opts, err := ratelimit.RedisOptions(url)
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(opts)
	t.Cleanup(func() { _ = rdb.Close() })
	reg := prometheus.NewRegistry()
	role, err := worker.NewRole(cfg, worker.Resources{
		Pool: e.rolePool(cfg), Conn: e.dial(), Redis: rdb, Registry: reg,
	}, e.log)
	if err != nil {
		t.Fatalf("worker role: %v", err)
	}
	start(t, role.Run)
	e.startRelay(cfg)

	// Redis up.
	e.create(3)
	testutil.Eventually(t, func() bool { return e.countStatus(domain.StatusSent) == 3 }, waitTimeout)
	if g := gaugeValue(t, reg); g != 0 {
		t.Fatalf("gauge %v with Redis up, want 0", g)
	}
	before := e.prov.count()

	// Redis unreachable.
	proxy.Break()
	e.create(fallbackSends)
	testutil.Eventually(t, func() bool { return e.countStatus(domain.StatusSent) == 3+fallbackSends }, waitTimeout)
	if g := gaugeValue(t, reg); g != 1 {
		t.Errorf("gauge %v with Redis unreachable, want 1", g)
	}
	reqs := e.prov.requests()[before:]
	arrivals := make([]time.Time, len(reqs))
	for i, r := range reqs {
		arrivals[i] = r.at
	}
	limit := int(math.Ceil(float64(fallbackPerSecond) / fallbackReplicas))
	maxWindow := ratewindow.MaxInWindow(arrivals, time.Second)
	t.Logf("Redis unreachable: %d sends; largest one-second window %d (fallback rate %.2f/s)",
		len(reqs), maxWindow, float64(fallbackPerSecond)/fallbackReplicas)
	if len(reqs) != fallbackSends {
		t.Errorf("provider received %d requests while Redis was unreachable, want %d", len(reqs), fallbackSends)
	}
	if maxWindow > limit {
		t.Errorf("a one-second window holds %d sends, more than the fallback's %d", maxWindow, limit)
	}

	// Redis back: the gauge returns to 0 at the first probe that Redis
	// answers. A probe needs a reservation, so notifications are sent one
	// after another until it has happened.
	proxy.Restore(t)
	sent := 3 + fallbackSends
	deadline := time.Now().Add(waitTimeout)
	for gaugeValue(t, reg) != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("gauge still %v %s after Redis came back", gaugeValue(t, reg), waitTimeout)
		}
		e.create(1)
		sent++
		testutil.Eventually(t, func() bool { return e.countStatus(domain.StatusSent) == sent }, waitTimeout)
	}
	e.create(1)
	sent++
	testutil.Eventually(t, func() bool { return e.countStatus(domain.StatusSent) == sent }, waitTimeout)
	n, err := e.rdb.Exists(context.Background(), "rl:"+string(domain.ChannelSMS)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Error("no slot was reserved in Redis after it came back")
	}
}

// D25: a worker started while Redis is unreachable starts (the failed ping
// is a warning) and sends, with the fallback active. worker.Service is the
// binary's own start-up path; its metrics are on the default registry, and
// the admin handler serves them.
func TestE2E_WorkerStartsWithRedisUnreachable(t *testing.T) {
	e := newEnv(t)
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	unreachable := "redis://" + ln.Addr().String() + "/0"
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	vars := map[string]string{"REDIS_URL": unreachable}
	for k, v := range fallbackVars {
		vars[k] = v
	}
	cfg := e.config(vars)

	start(t, func(ctx context.Context) error { return worker.Service(ctx, cfg, e.log) })
	e.startRelay(cfg)
	ids := e.create(3)
	for _, id := range ids {
		e.waitStatus(id, domain.StatusSent)
	}
	if g := gaugeValue(t, prometheus.DefaultGatherer); g != 1 {
		t.Errorf("gauge %v, want 1", g)
	}

	admin := httptest.NewServer(bootstrap.AdminHandler())
	t.Cleanup(admin.Close)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, admin.URL+"/metrics", http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := admin.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "\nlimiter_fallback_active 1\n") {
		t.Errorf("admin /metrics: status %d, limiter_fallback_active 1 missing", resp.StatusCode)
	}
	if strings.Contains(string(body), testRecipient) || strings.Contains(string(body), testContent) {
		t.Error("admin /metrics contains a recipient or content")
	}
}
