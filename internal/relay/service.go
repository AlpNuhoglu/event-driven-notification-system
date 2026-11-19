package relay

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/config"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/queue"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
)

// Service is the relay role (a bootstrap.Service). It connects to Postgres
// and RabbitMQ, declares the topology and runs the publishing cycle with
// reconciliation, the lease sweeper, the expiry pass and the idempotency-key
// cleanup until ctx is cancelled. It returns an error when a dependency
// cannot be reached at start and when the broker connection's recovery gives
// up, so that the process exits with a non-zero code (ADR-0005).
func Service(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	pool, err := openPool(ctx, cfg)
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

	m, err := NewMetrics(prometheus.DefaultRegisterer)
	if err != nil {
		return err
	}
	ev, err := eventPublisher(cfg, logger)
	if err != nil {
		return err
	}
	// Closed after Run has returned (deferred calls run last-in first-out):
	// the events of the last transactions get their chance.
	defer ev.Close()
	r := New(store.New(pool, store.ProfilesFrom(cfg.Retry)), pub, conn, ev, logger, ConfigFrom(cfg.Relay), jitter, m)
	return r.Run(ctx, conn.GaveUp(), conn.Err)
}

// eventPublisher is the role's publisher of status events: a connection of
// its own, separate from the dispatch connection, opened lazily (ADR-0005,
// D11). The caller closes it.
func eventPublisher(cfg config.Config, logger *slog.Logger) (*queue.EventPublisher, error) {
	return queue.NewEventPublisher(queue.EventPublisherConfigFrom(cfg), logger)
}

// openPool opens the role's Postgres pool with the store's constructor, so
// that every connection plans with the current statistics (D79).
func openPool(ctx context.Context, cfg config.Config) (*pgxpool.Pool, error) {
	return store.OpenPool(ctx, cfg.Connections.DatabaseURL)
}

// Run runs the relay's four loops (the publishing cycle with
// reconciliation, the lease sweeper, the expiry pass and the idempotency-key
// cleanup) until ctx is cancelled, gaveUp is closed (the broker connection's
// recovery gave up; gaveUpErr returns its error) or a loop ends with an
// error (the publisher's channel is closed), and waits for them to finish.
// It returns an error in the last two cases, so that the process exits with
// a non-zero code and is restarted.
func (r *Relay) Run(ctx context.Context, gaveUp <-chan struct{}, gaveUpErr func() error) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	loops := []func(context.Context) error{r.RunPublishing, r.RunLeaseSweeper, r.RunExpiry, r.RunIdempotencyCleanup}
	loopErr := make(chan error, len(loops))
	var wg sync.WaitGroup
	for _, loop := range loops {
		wg.Go(func() {
			if err := loop(runCtx); err != nil {
				loopErr <- err
			}
		})
	}

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

		ExpiryInterval:             c.ExpiryInterval,
		ExpiryBatch:                c.ExpiryBatchSize,
		IdempotencyCleanupInterval: c.IdempotencyCleanupEvery,

		ReconcileMinInterval: c.ReconcileMinEvery,
	}
}

// jitter is the random factor of the retry back-off. It needs no
// cryptographic strength.
func jitter() float64 {
	return rand.Float64() //nolint:gosec // back-off jitter, not a secret
}
