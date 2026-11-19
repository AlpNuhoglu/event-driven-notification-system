package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/config"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/queue"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
)

// Server timeouts. No configuration variable exists for them yet; they are
// fixed here like the admin server's in internal/platform/bootstrap.
const (
	readHeaderTimeout = 5 * time.Second
	// readTimeout leaves room for a body of MAX_BODY_BYTES (5 MB by default)
	// on a slow connection.
	readTimeout     = 30 * time.Second
	writeTimeout    = 30 * time.Second
	idleTimeout     = 120 * time.Second
	shutdownTimeout = 10 * time.Second
)

// Service is the api role (a bootstrap.Service). It connects to Postgres
// (a failure at start is returned, so the process exits with a non-zero
// code), serves the API on API_ADDR and shuts the server down gracefully
// when ctx is cancelled. It does not connect to Redis. Its only use of
// RabbitMQ is the status-event publisher, which is optional: it connects
// lazily, so the api starts, serves and stays ready while the broker is
// unreachable (ADR-0005, D11, D39).
func Service(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	ev, err := queue.NewEventPublisher(queue.EventPublisherConfigFrom(cfg), logger)
	if err != nil {
		return err
	}
	return run(ctx, cfg, logger, ev)
}

// run is Service with its event publisher. It closes ev after the server has
// stopped, which gives the events of the last requests their chance.
func run(ctx context.Context, cfg config.Config, logger *slog.Logger, ev *queue.EventPublisher) error {
	defer ev.Close()
	pool, err := openPool(ctx, cfg)
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("postgres: %w", err)
	}

	handler := NewHandler(Options{
		Repository:           store.New(pool, store.ProfilesFrom(cfg.Retry)),
		DB:                   pool,
		Logger:               logger,
		Retry:                cfg.Retry,
		MaxBodyBytes:         cfg.API.MaxBodyBytes,
		IdempotencyKeyTTL:    cfg.API.IdempotencyKeyTTL,
		ExpiresAtMinLead:     cfg.API.ExpiresAtMinLead,
		ScheduledAtTolerance: cfg.API.SchedulePastTolerance,
		ScheduledAtMaxAhead:  cfg.API.ScheduleMaxHorizon,
		PhoneValidation:      domain.PhoneValidation(cfg.API.PhoneValidation),
		Events:               ev,
		SummaryWindow:        cfg.API.SummaryWindow,
		SummaryCacheTTL:      cfg.API.SummaryCacheTTL,
		SummaryBacklogTTL:    cfg.API.SummaryBacklogTTL,
		LaneHWM: map[domain.Priority]int{
			domain.PriorityHigh:   cfg.Relay.LaneHWMHigh,
			domain.PriorityNormal: cfg.Relay.LaneHWMNormal,
			domain.PriorityLow:    cfg.Relay.LaneHWMLow,
		},
	})
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.Ports.APIAddr)
	if err != nil {
		return fmt.Errorf("api listen: %w", err)
	}
	return serve(ctx, ln, handler, logger)
}

// openPool opens the role's Postgres pool with the store's constructor, so
// that every connection plans with the current statistics (D79).
func openPool(ctx context.Context, cfg config.Config) (*pgxpool.Pool, error) {
	return store.OpenPool(ctx, cfg.Connections.DatabaseURL)
}

// serve serves handler on ln until ctx is cancelled, then waits up to
// shutdownTimeout for the requests in flight.
func serve(ctx context.Context, ln net.Listener, handler http.Handler, logger *slog.Logger) error {
	srv := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	logger.Info("api listening", "api_addr", ln.Addr().String())

	select {
	case err := <-errc:
		return fmt.Errorf("api serve: %w", err)
	case <-ctx.Done():
	}
	shutCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		return fmt.Errorf("api shutdown: %w", err)
	}
	if err := <-errc; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("api serve: %w", err)
	}
	logger.Info("api stopped")
	return nil
}
