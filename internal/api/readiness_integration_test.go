//go:build integration

package api

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/config"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/health"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/queue"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

// The postgres check passes while the pool is open and fails once it is
// closed.
func TestRegisterReadiness_PostgresPassesWithOpenPoolAndFailsClosed(t *testing.T) {
	url := testutil.NewMigratedDatabase(t)
	pool, err := openPool(context.Background(), config.Config{Connections: config.Connections{DatabaseURL: url}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	reg := health.NewReadiness(roleWait)
	ready := registerReadiness(reg)
	if rep := reg.Run(context.Background()); rep.Status != health.StatusFail {
		t.Errorf("before the pool is set: %+v, want fail", rep)
	}
	ready.pool.Store(pool)
	rep := reg.Run(context.Background())
	if rep.Status != health.StatusOK || len(rep.Checks) != 1 || rep.Checks[0].Name != "postgres" {
		t.Errorf("open pool: %+v, want one passing check named postgres", rep)
	}
	pool.Close()
	if rep := reg.Run(context.Background()); rep.Status != health.StatusFail {
		t.Errorf("closed pool: %+v, want fail", rep)
	}
}

// The api's service registers exactly one check, "postgres", on the registry
// of its context, and it passes while RabbitMQ is unreachable: the status
// event publisher is optional.
func TestService_RegistersOnlyPostgresReadiness(t *testing.T) {
	vars := map[string]string{
		"DATABASE_URL": testutil.NewMigratedDatabase(t),
		"API_ADDR":     "127.0.0.1:0",
		"RABBITMQ_URL": "amqp://guest:guest@" + testutil.UnusedAddr(t) + "/",
	}
	cfg, err := config.LoadFrom(func(k string) (string, bool) { v, ok := vars[k]; return v, ok })
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ev, err := queue.NewEventPublisher(queue.EventPublisherConfigFrom(cfg), logger)
	if err != nil {
		t.Fatalf("event publisher: %v", err)
	}
	reg := health.NewReadiness(roleWait)
	ctx, cancel := context.WithCancel(health.WithReadiness(context.Background(), reg))
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg, logger, ev) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("service: %v", err)
		}
	})

	// A registry without checks is ok too, so wait for the check to exist.
	testutil.Eventually(t, func() bool {
		rep := reg.Run(ctx)
		return len(rep.Checks) > 0 && rep.Status == health.StatusOK
	}, roleWait)
	rep := reg.Run(ctx)
	if len(rep.Checks) != 1 || rep.Checks[0].Name != "postgres" {
		t.Errorf("checks %+v, want exactly postgres", rep.Checks)
	}
}
