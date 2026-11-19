package gateway

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/config"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/queue"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
)

// readHeaderTimeout bounds the upgrade request's headers, like the admin
// server's in internal/platform/bootstrap.
const readHeaderTimeout = 5 * time.Second

// EventSource is the status-event consumer the role runs: *queue.EventConsumer.
type EventSource interface {
	// Run hands every event to handle until ctx is cancelled (nil) or the
	// consumer cannot go on (an error).
	Run(ctx context.Context, handle func(domain.StatusChanged)) error
}

// Service is the gateway role (a bootstrap.Service). It opens a read-only
// Postgres pool, connects to RabbitMQ with its own event queue, and serves
// GET /v1/ws on GATEWAY_ADDR until ctx is cancelled; then it closes every
// WebSocket connection with a close frame. It returns an error when a
// dependency cannot be reached at start and when the event consumer cannot
// go on (the broker connection's recovery gave up), so that the process
// exits with a non-zero code and is restarted (ADR-0005).
func Service(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	pool, err := store.OpenPoolWith(ctx, cfg.Connections.DatabaseURL, store.ReadOnly)
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("postgres: %w", err)
	}

	events, err := queue.DialEvents(cfg.Connections.RabbitMQURL, cfg.Connections.AMQPRecoveryRetryInterval,
		cfg.Connections.AMQPRecoveryMaxRetries,
		queue.EventQueueConfig{MaxLength: cfg.Events.QueueMaxLength, MessageTTL: cfg.Events.QueueMessageTTL}, logger)
	if err != nil {
		return err
	}
	defer func() {
		if err := events.Close(); err != nil {
			logger.Warn("gateway: close rabbitmq connection", "error", err)
		}
	}()

	g := New(OptionsFrom(cfg.Events, store.New(pool, nil), logger))
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.Ports.GatewayAddr)
	if err != nil {
		return fmt.Errorf("gateway listen: %w", err)
	}
	return Run(ctx, ln, g, events, cfg.Events.WSWriteTimeout, logger)
}

// OptionsFrom returns the gateway options of the loaded configuration.
func OptionsFrom(c config.Events, snapshots Snapshots, logger *slog.Logger) Options {
	return Options{
		Snapshots:        snapshots,
		Logger:           logger,
		OriginPatterns:   c.WSOriginPatterns,
		MaxSubscriptions: c.WSMaxSubscriptions,
		SendQueueSize:    c.WSSendQueueSize,
		PingInterval:     c.WSPingInterval,
		WriteTimeout:     c.WSWriteTimeout,
	}
}

// Run serves g on ln and dispatches the events of source to it until ctx is
// cancelled, the server fails or source ends with an error. Then it stops
// accepting, closes every WebSocket connection with a close frame (waiting
// at most shutdownTimeout) and stops source. It returns the error that ended
// it, or nil after ctx was cancelled.
func Run(ctx context.Context, ln net.Listener, g *Gateway, source EventSource, shutdownTimeout time.Duration, logger *slog.Logger) error {
	srv := &http.Server{
		Handler:           g.Handler(),
		ReadHeaderTimeout: readHeaderTimeout,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()

	srcCtx, stopSource := context.WithCancel(context.WithoutCancel(ctx))
	defer stopSource()
	srcErr := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Go(func() { srcErr <- source.Run(srcCtx, g.Dispatch) })
	logger.Info("gateway listening", "gateway_addr", ln.Addr().String())

	var err error
	select {
	case <-ctx.Done():
	case err = <-serveErr:
		err = fmt.Errorf("gateway serve: %w", err)
	case err = <-srcErr:
		if err == nil {
			err = errors.New("status events: the consumer ended")
		}
	}

	shutCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()
	// The server forgets a connection once it is upgraded (hijacked):
	// Shutdown closes the listener and waits for the plain requests only.
	if serr := srv.Shutdown(shutCtx); serr != nil && !errors.Is(serr, http.ErrServerClosed) {
		err = errors.Join(err, fmt.Errorf("gateway shutdown: %w", serr))
	}
	if gerr := g.Shutdown(shutCtx); gerr != nil {
		logger.Warn("gateway: connections still closing at the end of the shutdown time", "error", gerr)
	}
	stopSource()
	wg.Wait()
	logger.Info("gateway stopped")
	return err
}
