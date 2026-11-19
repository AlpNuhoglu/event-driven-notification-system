//go:build integration

package relay

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

// The postgres check passes while the pool is open and fails once it is
// closed.
func TestRegisterReadiness_PostgresPassesWithOpenPoolAndFailsClosed(t *testing.T) {
	url := testutil.NewMigratedDatabase(t)
	pool, err := openPool(context.Background(), config.Config{Connections: config.Connections{DatabaseURL: url}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	reg := health.NewReadiness(waitTimeout)
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

// Service registers exactly one check, "postgres", on the registry of its
// context, and it passes once the service is connected.
func TestService_RegistersPostgresReadiness(t *testing.T) {
	prev := prometheus.DefaultRegisterer
	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	t.Cleanup(func() { prometheus.DefaultRegisterer = prev })

	vars := map[string]string{
		"DATABASE_URL": testutil.NewMigratedDatabase(t),
		"RABBITMQ_URL": testutil.NewRabbitMQVhost(t),
	}
	cfg, err := config.LoadFrom(func(k string) (string, bool) { v, ok := vars[k]; return v, ok })
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	reg := health.NewReadiness(waitTimeout)
	ctx, cancel := context.WithCancel(health.WithReadiness(context.Background(), reg))
	done := make(chan error, 1)
	go func() { done <- Service(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil))) }()
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
	}, waitTimeout)
	rep := reg.Run(ctx)
	if len(rep.Checks) != 1 || rep.Checks[0].Name != "postgres" {
		t.Errorf("checks %+v, want exactly postgres", rep.Checks)
	}
}
