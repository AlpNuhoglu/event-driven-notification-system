package store

import (
	"context"
	"fmt"
	"strings"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/trace"
)

// PlanCacheMode is the plan_cache_mode of every connection the application
// opens (D79, ADR-0003 section 6): Postgres plans each execution of a
// prepared statement with its bound values and the table's current
// statistics, instead of reusing a plan cached while the table was small.
// It also reaches the foreign-key checks Postgres runs inside a statement,
// which the store cannot plan differently by changing how it sends the
// statement.
const PlanCacheMode = "force_custom_plan"

// PoolOption changes the configuration of a role's pool after PoolConfig
// has set what every pool has.
type PoolOption func(*pgxpool.Config)

// ReadOnly makes every session of the pool read-only by default
// (default_transaction_read_only): a statement that writes is refused with
// SQLSTATE 25006. The gateway opens its pool with it, because it only reads
// (ADR-0005 "Gateway"). It is a guard against a mistake in the role's code,
// not a permission: a session could still ask for a read-write transaction
// explicitly.
func ReadOnly(cfg *pgxpool.Config) {
	cfg.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
}

// WithTracerProvider makes the pool's statement spans use tp instead of the
// global tracer provider.
func WithTracerProvider(tp trace.TracerProvider) PoolOption {
	return func(cfg *pgxpool.Config) {
		cfg.ConnConfig.Tracer = queryTracer(tp)
	}
}

// queryTracer is the otelpgx tracer of every pool (ADR-0009 §6). It records
// a span for a statement only inside a span that is being recorded (a
// request, a delivery), so the relay's and the sweepers' polling statements
// start no traces of their own. A span is named after the sqlc query
// ("LockDueLane") and carries the statement text with its placeholders;
// the arguments are never recorded (otelpgx records them only when asked
// to, and the tracing package's exporter drops any that appear), and the
// acquisition of a connection from the pool gets no span. An error text is
// removed by the exporter too: a database error can quote a value.
func queryTracer(tp trace.TracerProvider) *otelpgx.Tracer {
	return otelpgx.NewTracer(
		otelpgx.WithTracerProvider(tp),
		otelpgx.WithSpanNameCtxFunc(querySpanName),
		otelpgx.WithDisableAcquireTracer(),
	)
}

// querySpanName is the name of the sqlc query a statement carries in its
// "-- name: X :kind" comment, or its SQL operation ("SELECT") without one.
func querySpanName(ctx context.Context, stmt string) string {
	if rest, ok := strings.CutPrefix(stmt, sqlcNamePrefix); ok {
		if name, _, ok := strings.Cut(rest, " "); ok && name != "" {
			return name
		}
	}
	return otelpgx.SQLOperationName(ctx, stmt)
}

const sqlcNamePrefix = "-- name: "

// PoolConfig parses databaseURL into the configuration of a role's pool:
// the URL's own settings, with plan_cache_mode set to PlanCacheMode as a
// startup parameter and statement tracing through the global tracer
// provider (WithTracerProvider replaces it), and then each option applied. The parameters are set in
// code, after the URL is parsed, so a DATABASE_URL works unchanged whatever
// it contains, and a URL (or PGOPTIONS) that sets plan_cache_mode itself
// does not decide: Postgres applies the startup parameters after the
// "options" string.
func PoolConfig(databaseURL string, opts ...PoolOption) (*pgxpool.Config, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	cfg.ConnConfig.RuntimeParams["plan_cache_mode"] = PlanCacheMode
	cfg.ConnConfig.Tracer = queryTracer(nil)
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg, nil
}

// OpenPool opens the Postgres pool of a role. It and OpenPoolWith are the
// only way the application opens one (a test enforces it). Like pgxpool.New
// it does not connect: the first connection is made by the first use, or by
// Ping.
func OpenPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	return OpenPoolWith(ctx, databaseURL)
}

// OpenPoolWith is OpenPool with options, for example ReadOnly.
func OpenPoolWith(ctx context.Context, databaseURL string, opts ...PoolOption) (*pgxpool.Pool, error) {
	cfg, err := PoolConfig(databaseURL, opts...)
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("open pool: %w", err)
	}
	return pool, nil
}
