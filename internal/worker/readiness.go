package worker

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/health"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/queue"
)

// The names of the worker's readiness checks.
const (
	checkPostgres = "postgres"
	checkRabbitMQ = "rabbitmq"
	checkConsumer = "consumer"
)

// readiness holds what the worker's readiness checks look at. The
// dependencies are set as the service connects to them; a check of one that
// is not set yet fails, so the role is not ready before it has started.
type readiness struct {
	pool atomic.Pointer[pgxpool.Pool]
	conn atomic.Pointer[queue.Connection]
	role atomic.Pointer[Role]
}

// registerReadiness registers the worker's checks on r (ADR-0009 §7):
// Postgres answers, the dispatch connection to RabbitMQ is open, and every
// consumer of the role is active. An open connection alone does not prove
// that a consumer runs. Redis is not checked: the rate limit has a fallback.
func registerReadiness(r *health.Readiness) *readiness {
	ready := &readiness{}
	r.Add(checkPostgres, func(ctx context.Context) error {
		p := ready.pool.Load()
		if p == nil {
			return errors.New("not connected yet")
		}
		return p.Ping(ctx)
	})
	r.Add(checkRabbitMQ, func(context.Context) error {
		if c := ready.conn.Load(); c == nil || !c.IsOpen() {
			return errors.New("the connection to rabbitmq is not open")
		}
		return nil
	})
	r.Add(checkConsumer, func(context.Context) error {
		if role := ready.role.Load(); role == nil || !role.ConsumersActive() {
			return errors.New("not every consumer is active")
		}
		return nil
	})
	return ready
}

// ConsumersActive reports whether the consumer of every channel of the role
// is subscribed and consuming.
func (r *Role) ConsumersActive() bool {
	for _, u := range r.units {
		if !u.consumer.Active() {
			return false
		}
	}
	return true
}
