package testutil

import (
	"context"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// CustomPlanMode is the plan_cache_mode every role's pool must run with
// (D79). It equals store.PlanCacheMode; this package cannot import the store.
const CustomPlanMode = "force_custom_plan"

// AutoPlanCacheOptions is a libpq "options" value that asks for Postgres's
// default plan_cache_mode, which the roles' pools must override (D79).
const AutoPlanCacheOptions = "-c plan_cache_mode=auto"

const planCheckTimeout = 30_000 * time.Millisecond

// RequireCustomPlanPool checks the pool that open builds, as a role builds
// it, on a new migrated database (D79): an acquired connection, a
// transaction and a connection the pool opens after Reset all run with
// plan_cache_mode = force_custom_plan, and so does a pool on a URL whose
// "options" ask for plan_cache_mode = auto (the code decides, not the URL).
func RequireCustomPlanPool(t *testing.T, open PoolOpener) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), planCheckTimeout)
	defer cancel()
	base := NewMigratedDatabase(t)

	pool := openFor(ctx, t, open, base)
	if got := planCacheMode(ctx, t, pool); got != CustomPlanMode {
		t.Errorf("pooled connection: plan_cache_mode = %q, want %q", got, CustomPlanMode)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	var inTx string
	if err := tx.QueryRow(ctx, "SHOW plan_cache_mode").Scan(&inTx); err != nil {
		t.Fatalf("show in transaction: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if inTx != CustomPlanMode {
		t.Errorf("in a transaction: plan_cache_mode = %q, want %q", inTx, CustomPlanMode)
	}

	before := backendPID(ctx, t, pool)
	pool.Reset()
	if after := backendPID(ctx, t, pool); after == before {
		t.Fatalf("after Reset the pool still uses backend %d: the check needs a new connection", before)
	}
	if got := planCacheMode(ctx, t, pool); got != CustomPlanMode {
		t.Errorf("connection opened after Reset: plan_cache_mode = %q, want %q", got, CustomPlanMode)
	}

	withOptions := openFor(ctx, t, open, WithURLParam(t, base, "options", AutoPlanCacheOptions))
	if got := planCacheMode(ctx, t, withOptions); got != CustomPlanMode {
		t.Errorf("URL with options %q: plan_cache_mode = %q, want %q", AutoPlanCacheOptions, got, CustomPlanMode)
	}
}

// WithURLParam returns databaseURL with the query parameter key set to
// value. The query is percent-encoded as libpq and pgx decode it: a space is
// %20, not "+" (which they keep as a plus sign), and "=" is %3D.
func WithURLParam(t testing.TB, databaseURL, key, value string) string {
	t.Helper()
	u, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatalf("parse database url: %v", err)
	}
	q := u.Query()
	q.Set(key, value)
	// Encode escapes a literal "+" as %2B, so every "+" left is a space.
	u.RawQuery = strings.ReplaceAll(q.Encode(), "+", "%20")
	return u.String()
}

func openFor(ctx context.Context, t *testing.T, open PoolOpener, databaseURL string) *pgxpool.Pool {
	t.Helper()
	pool, err := open(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func planCacheMode(ctx context.Context, t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	var mode string
	if err := pool.QueryRow(ctx, "SHOW plan_cache_mode").Scan(&mode); err != nil {
		t.Fatalf("show plan_cache_mode: %v", err)
	}
	return mode
}

func backendPID(ctx context.Context, t *testing.T, pool *pgxpool.Pool) int32 {
	t.Helper()
	var pid int32
	if err := pool.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid); err != nil {
		t.Fatalf("backend pid: %v", err)
	}
	return pid
}
