//go:build integration

package worker

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/config"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/health"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

func TestMain(m *testing.M) { testutil.Main(m) }

const readinessWait = 10_000 * time.Millisecond

// statuses runs the registry and returns the status of each check by name.
func statuses(reg *health.Readiness) map[string]string {
	out := map[string]string{}
	for _, c := range reg.Run(context.Background()).Checks {
		out[c.Name] = c.Status
	}
	return out
}

func allOK(reg *health.Readiness) bool {
	s := statuses(reg)
	return len(s) == 3 && s["postgres"] == health.StatusOK && s["rabbitmq"] == health.StatusOK && s["consumer"] == health.StatusOK
}

// Service registers its three checks on the registry of its context. With
// the broker connection broken, "rabbitmq" and "consumer" fail and
// "postgres" passes; restored, all pass again. With Redis unreachable all
// three still pass: the rate limit has a fallback.
func TestService_ReadinessFollowsPostgresBrokerAndConsumers(t *testing.T) {
	// Service registers its metrics on the default registry; a registry of
	// its own per test keeps repeated runs from colliding.
	prev := prometheus.DefaultRegisterer
	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	t.Cleanup(func() { prometheus.DefaultRegisterer = prev })

	broker := testutil.NewProxy(t, testutil.NewRabbitMQVhost(t))
	redisProxy, redisURL := testutil.NewRedisProxy(t, testutil.NewRedis(t))
	vars := map[string]string{
		"DATABASE_URL":                 testutil.NewMigratedDatabase(t),
		"RABBITMQ_URL":                 broker.URL(),
		"REDIS_URL":                    redisURL,
		"WORKER_CHANNELS":              "sms,email",
		"PROVIDER_URL":                 "http://127.0.0.1:1/",
		"AMQP_RECOVERY_RETRY_INTERVAL": "20ms",
		"AMQP_RECOVERY_MAX_RETRIES":    "1000",
	}
	cfg, err := config.LoadFrom(func(k string) (string, bool) { v, ok := vars[k]; return v, ok })
	if err != nil {
		t.Fatalf("config: %v", err)
	}

	reg := health.NewReadiness(readinessWait)
	ctx, cancel := context.WithCancel(health.WithReadiness(context.Background(), reg))
	done := make(chan error, 1)
	go func() { done <- Service(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil))) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("service: %v", err)
		}
	})

	testutil.Eventually(t, func() bool { return allOK(reg) }, readinessWait)

	broker.Break()
	testutil.Eventually(t, func() bool {
		s := statuses(reg)
		return s["rabbitmq"] == health.StatusFail && s["consumer"] == health.StatusFail
	}, readinessWait)
	if s := statuses(reg); s["postgres"] != health.StatusOK {
		t.Errorf("postgres %q while the broker is broken, want ok", s["postgres"])
	}

	broker.Restore(t)
	testutil.Eventually(t, func() bool { return allOK(reg) }, readinessWait)

	redisProxy.Break()
	if !allOK(reg) {
		t.Errorf("checks with Redis unreachable: %v, want all ok", statuses(reg))
	}
}
