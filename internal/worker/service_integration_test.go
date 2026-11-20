//go:build integration

package worker

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/config"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/health"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

// D5: when the RabbitMQ client's connection recovery gives up, the worker
// role ends with an error, so that its process exits with a non-zero code
// and the restart policy starts it again (bootstrap turns the error into
// the exit code, TestService_ErrorExitsNonZero). The broker connection is
// broken for good after the role is ready; with a budget of two attempts
// Service returns an error instead of serving on without a broker.
func TestService_RecoveryGiveUpEndsWithAnError(t *testing.T) {
	prev := prometheus.DefaultRegisterer
	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	t.Cleanup(func() { prometheus.DefaultRegisterer = prev })

	broker := testutil.NewProxy(t, testutil.NewRabbitMQVhost(t))
	_, redisURL := testutil.NewRedisProxy(t, testutil.NewRedis(t))
	vars := map[string]string{
		"DATABASE_URL":                 testutil.NewMigratedDatabase(t),
		"RABBITMQ_URL":                 broker.URL(),
		"REDIS_URL":                    redisURL,
		"WORKER_CHANNELS":              "sms",
		"PROVIDER_URL":                 "http://127.0.0.1:1/",
		"AMQP_RECOVERY_RETRY_INTERVAL": "20ms",
		"AMQP_RECOVERY_MAX_RETRIES":    "2",
	}
	cfg, err := config.LoadFrom(func(k string) (string, bool) { v, ok := vars[k]; return v, ok })
	if err != nil {
		t.Fatalf("config: %v", err)
	}

	reg := health.NewReadiness(readinessWait)
	ctx, cancel := context.WithCancel(health.WithReadiness(context.Background(), reg))
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Service(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil))) }()
	testutil.Eventually(t, func() bool {
		s := statuses(reg)
		return s["rabbitmq"] == health.StatusOK && s["consumer"] == health.StatusOK
	}, readinessWait)

	broker.Break() // never restored
	var runErr error
	ended := false
	testutil.Eventually(t, func() bool {
		select {
		case runErr = <-done:
			ended = true
		default:
		}
		return ended
	}, readinessWait)
	// Either signal of the give-up may end the role first: the connection's
	// (GaveUp) or a consumer's delivery stream, which the library closes
	// when it gives up (TestConsumer_ChannelClosedForGoodIsError). Both are
	// errors; which one is reported is not part of the decision.
	if runErr == nil {
		t.Fatal("Service returned nil after the connection recovery gave up, want an error")
	}
	t.Logf("Service ended with: %v", runErr)
}
