package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
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

// PoolConfig parses databaseURL into the configuration of a role's pool:
// the URL's own settings, with plan_cache_mode set to PlanCacheMode as a
// startup parameter, and then each option applied. The parameters are set in
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
