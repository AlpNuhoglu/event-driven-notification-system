package api

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/health"
)

// checkPostgres is the name of the role's only readiness check on the admin
// port.
const checkPostgres = "postgres"

// pgReadiness holds the pool the readiness check pings. It is set once the
// service has opened and pinged the pool; the check of a pool that is not set
// yet fails, so the role is not ready before it has started.
type pgReadiness struct {
	pool atomic.Pointer[pgxpool.Pool]
}

// registerReadiness registers the role's check on r (ADR-0009 §7): Postgres
// answers. Nothing else is checked: the status-event publisher is optional
// and the api does not use Redis.
func registerReadiness(r *health.Readiness) *pgReadiness {
	ready := &pgReadiness{}
	r.Add(checkPostgres, func(ctx context.Context) error {
		pool := ready.pool.Load()
		if pool == nil {
			return errors.New("not connected yet")
		}
		return pool.Ping(ctx)
	})
	return ready
}
