package relay

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/events"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/config"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/queue"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
)

// Service is the relay role (a bootstrap.Service). It connects to Postgres
// and RabbitMQ, declares the topology and runs the publishing cycle and the
// lease sweeper until ctx is cancelled. It returns an error when a dependency
// cannot be reached at start and when the broker connection's recovery gives
// up, so that the process exits with a non-zero code (ADR-0005).
func Service(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	pool, err := pgxpool.New(ctx, cfg.Connections.DatabaseURL)
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("postgres: %w", err)
	}

	conn, err := queue.Dial(cfg.Connections.RabbitMQURL, cfg.Connections.AMQPRecoveryRetryInterval,
		cfg.Connections.AMQPRecoveryMaxRetries, logger)
	if err != nil {
		return err
	}
	defer func() {
		if err := conn.Close(); err != nil {
			logger.Warn("relay: close rabbitmq connection", "error", err)
		}
	}()
	if err := conn.DeclareTopology(cfg.Worker.ConsumerTimeout); err != nil {
		return err
	}
	pub, err := queue.NewPublisher(conn, cfg.Relay.BatchSize)
	if err != nil {
		return err
	}

	r := New(store.New(pool, store.ProfilesFrom(cfg.Retry)), pub, conn, events.Nop{}, logger, ConfigFrom(cfg.Relay), jitter)
	return run(ctx, r, conn.GaveUp(), conn.Err)
}

// run runs both loops until ctx is cancelled, gaveUp is closed or a loop
// ends with an error (the publisher's channel is closed), and waits for them
// to finish. It returns an error in the last two cases, so that the process
// exits with a non-zero code and is restarted.
func run(ctx context.Context, r *Relay, gaveUp <-chan struct{}, gaveUpErr func() error) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	loopErr := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Go(func() {
		if err := r.RunPublishing(runCtx); err != nil {
			loopErr <- err
		}
	})
	wg.Go(func() {
		if err := r.RunLeaseSweeper(runCtx); err != nil {
			loopErr <- err
		}
	})

	var err error
	select {
	case <-ctx.Done():
	case <-gaveUp:
		err = errors.Join(errors.New("rabbitmq connection recovery gave up"), gaveUpErr())
	case err = <-loopErr:
	}
	cancel()
	wg.Wait()
	return err
}

// ConfigFrom returns the relay settings of the loaded configuration.
func ConfigFrom(c config.Relay) Config {
	return Config{
		TickInterval:       c.TickInterval,
		BatchSize:          c.BatchSize,
		ConfirmTimeout:     c.ConfirmTimeout,
		HWMHigh:            c.LaneHWMHigh,
		HWMNormal:          c.LaneHWMNormal,
		HWMLow:             c.LaneHWMLow,
		LeaseSweepInterval: c.LeaseSweepEvery,
		LeaseSweepBatch:    c.LeaseSweepBatch,
	}
}

// jitter is the random factor of the retry back-off. It needs no
// cryptographic strength.
func jitter() float64 {
	return rand.Float64() //nolint:gosec // back-off jitter, not a secret
}
