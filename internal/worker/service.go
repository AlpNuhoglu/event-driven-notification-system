package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/events"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/config"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/provider"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/queue"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/ratelimit"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
)

// Service is the worker role (a bootstrap.Service). It connects to Postgres,
// RabbitMQ and Redis, wires the role with NewRole and runs it until ctx is
// cancelled. It returns an error when a dependency cannot be reached at
// start, when the broker connection's recovery gives up and when a
// consumer's delivery stream ends unexpectedly, so that the process exits
// with a non-zero code (ADR-0005). The connections are closed after the role
// has stopped.
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
			logger.Warn("worker: close rabbitmq connection", "error", err)
		}
	}()

	opts, err := redis.ParseURL(cfg.Connections.RedisURL)
	if err != nil {
		return fmt.Errorf("redis: %w", err)
	}
	rdb := redis.NewClient(opts)
	defer func() {
		if err := rdb.Close(); err != nil {
			logger.Warn("worker: close redis client", "error", err)
		}
	}()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis: %w", err)
	}

	role, err := NewRole(cfg, Resources{
		Pool:     pool,
		Conn:     conn,
		Redis:    rdb,
		Registry: prometheus.DefaultRegisterer,
	}, logger)
	if err != nil {
		return err
	}
	return role.Run(ctx)
}

// Resources are the connections the worker role runs on. Service opens them
// from the configuration; the integration tests pass their own (a database,
// a vhost and a Redis database per test), so that the binary and the tests
// share one wiring. The role does not close them.
type Resources struct {
	Pool  *pgxpool.Pool
	Conn  *queue.Connection
	Redis redis.Scripter
	// Registry receives the provider clients' counters; the clients of all
	// channels share them.
	Registry prometheus.Registerer
	// WriteMargin replaces DefaultWriteMargin when positive. It is not
	// configuration: only tests shorten it, like every other duration (D52).
	WriteMargin time.Duration
}

// Role is the wired worker role: for each channel of WORKER_CHANNELS one
// Worker with its own provider client and its own consumer, all sharing one
// store and one rate limiter. A Role runs once.
type Role struct {
	conn   *queue.Connection
	logger *slog.Logger
	units  []unit
}

// unit is the worker and the consumer of one channel.
type unit struct {
	channel  domain.Channel
	worker   *Worker
	consumer *queue.Consumer
}

// NewRole declares the topology and builds the workers and consumers of the
// channels in cfg.Worker.Channels on res. On an error, the consumer channels
// opened so far are closed.
func NewRole(cfg config.Config, res Resources, logger *slog.Logger) (*Role, error) {
	switch {
	case res.Pool == nil, res.Conn == nil, res.Redis == nil, res.Registry == nil, logger == nil:
		return nil, errors.New("worker role: every resource is required")
	case len(cfg.Worker.Channels) == 0:
		return nil, errors.New("worker role: WORKER_CHANNELS is empty")
	}
	if err := res.Conn.DeclareTopology(cfg.Worker.ConsumerTimeout); err != nil {
		return nil, err
	}
	limiter, err := ratelimit.New(res.Redis, cfg.RateLimit.PerSecond, cfg.RateLimit.Horizon)
	if err != nil {
		return nil, err
	}
	st := store.New(res.Pool, store.ProfilesFrom(cfg.Retry))
	margin := DefaultWriteMargin
	if res.WriteMargin > 0 {
		margin = res.WriteMargin
	}

	r := &Role{conn: res.Conn, logger: logger}
	for _, name := range cfg.Worker.Channels {
		u, err := newUnit(cfg, res, logger, st, limiter, margin, name)
		if err != nil {
			r.closeConsumers()
			return nil, err
		}
		r.units = append(r.units, u)
	}
	return r, nil
}

func newUnit(cfg config.Config, res Resources, logger *slog.Logger, st *store.Store, limiter *ratelimit.Limiter,
	margin time.Duration, name string,
) (unit, error) {
	ch, err := domain.ParseChannel(name)
	if err != nil {
		return unit{}, fmt.Errorf("worker role: %w", err)
	}
	client, err := provider.New(provider.Config{
		URL:                 cfg.Provider.URL,
		Name:                cfg.Provider.Name,
		IdempotencyHeader:   cfg.Provider.IdempotencyHeader,
		ConnectTimeout:      cfg.Provider.ConnectTimeout,
		Timeout:             cfg.Provider.Timeout,
		MaxIdleConnsPerHost: cfg.Worker.Concurrency,
	}, res.Registry)
	if err != nil {
		return unit{}, fmt.Errorf("worker role %s: %w", ch, err)
	}
	w, err := New(Config{
		Channel:       ch,
		Concurrency:   cfg.Worker.Concurrency,
		Lease:         cfg.Worker.LeaseDuration,
		HTTPTimeout:   cfg.Provider.Timeout,
		SlotTolerance: cfg.RateLimit.SlotTolerance,
		RetryInterval: cfg.RateLimit.RetryInterval,
		Drain:         cfg.Worker.ShutdownDrain,
		WriteMargin:   margin,
	}, Deps{
		Store:    st,
		Limiter:  limiter,
		Breaker:  AlwaysAllow{},
		Provider: providerClient{client},
		Events:   events.Nop{},
		Clock:    SystemClock{},
		Jitter:   jitter,
		Logger:   logger,
	})
	if err != nil {
		return unit{}, fmt.Errorf("worker role %s: %w", ch, err)
	}
	c, err := queue.NewConsumer(res.Conn, ch, cfg.Worker.Concurrency, logger)
	if err != nil {
		return unit{}, fmt.Errorf("worker role %s: %w", ch, err)
	}
	return unit{channel: ch, worker: w, consumer: c}, nil
}

// Worker returns the worker of channel c, or nil when the role does not
// serve c.
func (r *Role) Worker(c domain.Channel) *Worker {
	for _, u := range r.units {
		if u.channel == c {
			return u.worker
		}
	}
	return nil
}

// Run runs every worker and consumer until ctx is cancelled, the broker
// connection's recovery gives up or a consumer's delivery stream ends, then
// stops in the order of ADR-0005 "Graceful shutdown":
//
//  1. the consumers are cancelled, so nothing new arrives; deliveries already
//     on their way to a consumer are requeued;
//  2. each worker requeues its buffered messages and lets its running
//     dispatches finish within the drain timeout;
//  3. the consumers' channels are closed.
//
// It returns nil after a cancellation of ctx, and an error in the other two
// cases (after the same orderly stop).
func (r *Role) Run(ctx context.Context) error {
	consumeCtx, stopConsumers := context.WithCancel(ctx)
	defer stopConsumers()
	// The workers outlive ctx until the consumers have stopped.
	paceCtx, stopWorkers := context.WithCancel(context.WithoutCancel(ctx))
	defer stopWorkers()

	var workers sync.WaitGroup
	for _, u := range r.units {
		workers.Go(func() { u.worker.Run(paceCtx) })
	}
	consumerErrs := make(chan error, len(r.units))
	var consumers sync.WaitGroup
	for _, u := range r.units {
		consumers.Go(func() {
			push := func(d queue.Delivery) { u.worker.Push(delivery{d}) }
			if err := u.consumer.Run(consumeCtx, push); err != nil {
				consumerErrs <- fmt.Errorf("worker role %s: %w", u.channel, err)
			}
		})
	}

	var err error
	select {
	case <-ctx.Done():
	case <-r.conn.GaveUp():
		err = errors.Join(errors.New("rabbitmq connection recovery gave up"), r.conn.Err())
	case err = <-consumerErrs:
	}

	stopConsumers()
	consumers.Wait()
	close(consumerErrs)
	for cerr := range consumerErrs {
		// Errors while stopping lose nothing: closing the channel requeues
		// whatever is still unacknowledged.
		r.logger.Warn("worker: consumer stopped with an error", "error", cerr)
	}

	stopWorkers()
	workers.Wait()

	r.closeConsumers()
	return err
}

func (r *Role) closeConsumers() {
	for _, u := range r.units {
		if err := u.consumer.Close(); err != nil {
			r.logger.Warn("worker: close consumer channel", "channel", string(u.channel), "error", err)
		}
	}
}

// delivery adapts a queue.Delivery to Message.
type delivery struct{ d queue.Delivery }

// lowestPriority is the AMQP priority the broker gives a message without a
// priority property: such a message is sorted with the low ones (D7).
const lowestPriority uint8 = 4

func (m delivery) NotificationID() uuid.UUID { return m.d.ID }
func (m delivery) Epoch() uint64             { return m.d.Epoch }
func (m delivery) Ack() error                { return m.d.Ack() }
func (m delivery) Nack() error               { return m.d.Nack() }

func (m delivery) Priority() uint8 {
	if m.d.Priority == 0 {
		return lowestPriority
	}
	return m.d.Priority
}

// providerClient adapts a *provider.Client to Provider.
type providerClient struct{ c *provider.Client }

func (p providerClient) Name() string { return p.c.Name() }

func (p providerClient) Send(ctx context.Context, r Request) Response {
	res := p.c.Send(ctx, provider.Request{
		NotificationID: r.NotificationID,
		Channel:        r.Channel,
		To:             r.To,
		Content:        r.Content,
		TraceParent:    r.TraceParent,
	})
	return Response{
		StatusCode:   res.StatusCode,
		Err:          res.Err,
		MessageID:    res.MessageID,
		Latency:      res.Latency,
		WroteRequest: res.WroteRequest,
		RetryAfter:   res.RetryAfter,
	}
}

// jitter is the random factor of the retry back-off. It needs no
// cryptographic strength.
func jitter() float64 {
	return rand.Float64() //nolint:gosec // back-off jitter, not a secret
}
