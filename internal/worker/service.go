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

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/breaker"
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
// cancelled. It returns an error when Postgres or RabbitMQ cannot be reached
// at start, when the broker connection's recovery gives up and when a
// consumer's channel is closed for good (a server-side cancellation is
// followed by a new subscription instead, D8), so that the process exits
// with a non-zero code (ADR-0005). An unreachable Redis is only a warning:
// the rate limit then uses the in-process fallback (D25). The connections are
// closed after the role has stopped.
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
			logger.Warn("worker: close rabbitmq connection", "error", err)
		}
	}()

	opts, err := ratelimit.RedisOptions(cfg.Connections.RedisURL)
	if err != nil {
		return fmt.Errorf("redis: %w", err)
	}
	rdb := redis.NewClient(opts)
	defer func() {
		if err := rdb.Close(); err != nil {
			logger.Warn("worker: close redis client", "error", err)
		}
	}()
	// Redis holds only the shared rate limit. A worker that starts while it
	// is unreachable starts anyway and paces with the in-process fallback
	// from its first reservation (D25).
	pingCtx, cancelPing := context.WithTimeout(ctx, cfg.RateLimit.RedisTimeout)
	err = rdb.Ping(pingCtx).Err()
	cancelPing()
	if err != nil {
		logger.Warn("worker: redis unreachable at start; the rate limit falls back to the in-process limiter",
			"error", err)
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

// openPool opens the role's Postgres pool with the store's constructor, so
// that every connection plans with the current statistics (D79).
func openPool(ctx context.Context, cfg config.Config) (*pgxpool.Pool, error) {
	return store.OpenPool(ctx, cfg.Connections.DatabaseURL)
}

// Resources are the connections the worker role runs on. Service opens them
// from the configuration; the integration tests pass their own (a database,
// a vhost and a Redis database per test), so that the binary and the tests
// share one wiring. The role does not close them.
type Resources struct {
	Pool  *pgxpool.Pool
	Conn  *queue.Connection
	Redis redis.Scripter
	// Registry receives the provider clients' counters, which the clients of
	// all channels share, and the gauge limiter_fallback_active.
	Registry prometheus.Registerer
	// WriteMargin replaces DefaultWriteMargin when positive. It is not
	// configuration: only tests shorten it, like every other duration (D52).
	WriteMargin time.Duration
}

// Role is the wired worker role: for each channel of WORKER_CHANNELS one
// Worker with its own provider client and its own consumer, all sharing one
// store and one rate limiter (the Redis limiter with its in-process fallback,
// D25). A Role runs once.
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
	shared, err := ratelimit.New(res.Redis, cfg.RateLimit.PerSecond, cfg.RateLimit.Horizon)
	if err != nil {
		return nil, err
	}
	limiter, err := ratelimit.NewFallback(shared, ratelimit.FallbackConfig{
		PerSecond:     cfg.RateLimit.PerSecond,
		Replicas:      cfg.Worker.Replicas,
		Horizon:       cfg.RateLimit.Horizon,
		Timeout:       cfg.RateLimit.RedisTimeout,
		ProbeInterval: cfg.RateLimit.FallbackProbeInterval,
		Registry:      res.Registry,
		Logger:        logger,
	})
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

func newUnit(cfg config.Config, res Resources, logger *slog.Logger, st *store.Store, limiter Limiter,
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
	clock := SystemClock{}
	br, err := newCircuit(cfg, ch, clock, logger)
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

		ClaimRetryBase: cfg.Worker.ClaimRetryBase,
		ClaimRetryMax:  cfg.Worker.ClaimRetryMax,
	}, Deps{
		Store:    st,
		Limiter:  limiter,
		Breaker:  br,
		Provider: providerClient{client},
		Events:   events.Nop{},
		Clock:    clock,
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
			if err := u.consumer.Run(consumeCtx, push, u.worker.DropStale); err != nil {
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
func (m delivery) Stale() bool               { return m.d.Stale() }
func (m delivery) Ack() error                { return staleAs(m.d.Ack()) }
func (m delivery) Nack() error               { return staleAs(m.d.Nack()) }

// staleAs maps the queue's ErrStale to the worker's.
func staleAs(err error) error {
	if errors.Is(err, queue.ErrStale) {
		return fmt.Errorf("%w: %w", ErrStale, err)
	}
	return err
}

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

// circuit adapts a *breaker.Breaker to Breaker.
type circuit struct{ b *breaker.Breaker }

func (c circuit) Ready() (bool, time.Time) { return c.b.Ready() }

func (c circuit) Admit() (Permit, bool, time.Time) {
	p, ok, until := c.b.Admit()
	return p, ok, until
}

// newCircuit builds the circuit breaker of one (channel, provider) pair
// (D26) on the worker's clock. Its state lives in this process only; every
// state change is logged, and a circuit opened by a provider_fatal answer
// (wrong credentials, a wrong or expired endpoint) at error level, because
// only a person can fix its cause (ADR-0007 section 2).
func newCircuit(cfg config.Config, ch domain.Channel, clock Clock, logger *slog.Logger) (circuit, error) {
	log := logger.With(slog.String("channel", string(ch)), slog.String("provider", cfg.Provider.Name))
	b, err := breaker.New(breaker.Config{
		Window:              cfg.Breaker.Window,
		MinRequests:         cfg.Breaker.MinRequests,
		FailureRatio:        cfg.Breaker.FailureRatio,
		ConsecutiveFailures: cfg.Breaker.ConsecutiveFailures,
		OpenBase:            cfg.Breaker.OpenBase,
		OpenMax:             cfg.Breaker.OpenMax,
		FatalOpen:           cfg.Breaker.FatalOpen,
		ThrottleDefault:     cfg.Retry.ThrottleDefaultDelay,
		ThrottleMax:         cfg.Retry.ThrottleMaxDelay,
		RetryAfterCap:       cfg.Retry.AfterCap,
		// While a probe is in flight the pacer asks again at the pace at
		// which it asks the limiter again.
		ProbeWait: cfg.RateLimit.RetryInterval,
	}, clock.Now, func(c breaker.Change) { logCircuit(log, clock, c) })
	if err != nil {
		return circuit{}, err
	}
	return circuit{b}, nil
}

func logCircuit(log *slog.Logger, clock Clock, c breaker.Change) {
	attrs := []any{slog.String("from", c.From.String()), slog.String("to", c.To.String())}
	if c.Outcome != "" {
		attrs = append(attrs, slog.String("outcome", string(c.Outcome)))
	}
	switch c.To {
	case breaker.Open:
		attrs = append(attrs, slog.Duration("open_for", c.Until.Sub(clock.Now())))
		if c.Outcome == domain.OutcomeProviderFatal {
			log.Error("worker: circuit opened: the provider rejects the endpoint or the credentials; nothing is sent until a probe succeeds", attrs...)
			return
		}
		log.Warn("worker: circuit opened", attrs...)
	case breaker.Closed:
		log.Info("worker: circuit closed", attrs...)
	case breaker.HalfOpen:
		log.Debug("worker: circuit half-open", attrs...)
	}
}

// jitter is the random factor of the retry back-off. It needs no
// cryptographic strength.
func jitter() float64 {
	return rand.Float64() //nolint:gosec // back-off jitter, not a secret
}
