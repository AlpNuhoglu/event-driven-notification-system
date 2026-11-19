package gateway

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/config"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/health"
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

// Service is the gateway role (a bootstrap.Service). It registers its
// readiness checks on the admin port's registry, opens a read-only
// Postgres pool, connects to RabbitMQ with its own event queue, and serves
// GET /v1/ws on GATEWAY_ADDR until ctx is cancelled; then it closes every
// WebSocket connection with a close frame. It returns an error when a
// dependency cannot be reached at start and when the event consumer cannot
// go on (the broker connection's recovery gave up), so that the process
// exits with a non-zero code and is restarted (ADR-0005).
func Service(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	ready := registerReadiness(health.ReadinessFrom(ctx))
	pool, err := store.OpenPoolWith(ctx, cfg.Connections.DatabaseURL, store.ReadOnly)
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	ready.postgres.Store(&pinger{pool})

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
	ready.events.Store(&eventState{events})

	g := New(OptionsFrom(cfg.Events, store.New(pool, nil), logger))
	if err := RegisterMetrics(prometheus.DefaultRegisterer, g); err != nil {
		return fmt.Errorf("gateway metrics: %w", err)
	}
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.Ports.GatewayAddr)
	if err != nil {
		return fmt.Errorf("gateway listen: %w", err)
	}
	return Run(ctx, ln, g, events, cfg.Events.WSWriteTimeout, logger)
}

// Pinger checks that Postgres answers.
type Pinger interface {
	Ping(ctx context.Context) error
}

// EventState is what readiness asks of the event consumer:
// *queue.EventConsumer.
type EventState interface {
	// Connected reports whether the connection to the broker is open.
	Connected() bool
	// Active reports whether the consumer loop is consuming.
	Active() bool
}

// pinger and eventState let an interface value sit in an atomic.Pointer.
type (
	pinger     struct{ Pinger }
	eventState struct{ EventState }
)

// readiness holds what the gateway's readiness checks look at. The
// dependencies are set as the service connects to them; a check of one that
// is not set yet fails, so the role is not ready before it has started.
type readiness struct {
	postgres atomic.Pointer[pinger]
	events   atomic.Pointer[eventState]
}

// The names of the gateway's readiness checks.
const (
	checkPostgres      = "postgres"
	checkRabbitMQ      = "rabbitmq"
	checkEventConsumer = "event_consumer"
)

// registerReadiness registers the gateway's checks on r (ADR-0009 §7):
// Postgres answers (the snapshots are read from it), the connection to
// RabbitMQ is open, and the event consumer is active. The last two differ
// while the connection is back and the queue is not subscribed again yet: an
// open connection alone does not prove that events arrive.
func registerReadiness(r *health.Readiness) *readiness {
	ready := &readiness{}
	r.Add(checkPostgres, func(ctx context.Context) error {
		p := ready.postgres.Load()
		if p == nil {
			return errors.New("not connected yet")
		}
		return p.Ping(ctx)
	})
	r.Add(checkRabbitMQ, func(context.Context) error {
		if e := ready.events.Load(); e == nil || !e.Connected() {
			return errors.New("the connection to rabbitmq is not open")
		}
		return nil
	})
	r.Add(checkEventConsumer, func(context.Context) error {
		if e := ready.events.Load(); e == nil || !e.Active() {
			return errors.New("the event consumer is not active")
		}
		return nil
	})
	return ready
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
