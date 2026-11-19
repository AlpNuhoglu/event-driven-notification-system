//go:build integration

package store

import (
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

// OpenPool plans every statement with the current statistics (D79): on a
// pooled connection, in a transaction, after Reset and whatever the URL asks.
func TestOpenPool_PlansWithCurrentStatistics(t *testing.T) {
	testutil.RequireCustomPlanPool(t, OpenPool)
}

// The code decides plan_cache_mode, not the environment: a URL or PGOPTIONS
// that asks for another mode, which a plain connection does get (the
// control), still ends with force_custom_plan on the pool. The cases ask for
// force_generic_plan rather than auto, Postgres's default, so that the
// control can tell the request reached Postgres.
func TestOpenPool_OverridesTheURLAndTheEnvironment(t *testing.T) {
	base := testutil.NewMigratedDatabase(t)
	const other = "force_generic_plan"
	cases := []struct {
		name, url, pgoptions string
	}{
		{"options in the URL", testutil.WithURLParam(t, base, "options", "-c plan_cache_mode="+other), ""},
		{"parameter in the URL", testutil.WithURLParam(t, base, "plan_cache_mode", other), ""},
		{"PGOPTIONS", base, "-c plan_cache_mode=" + other},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.pgoptions != "" {
				t.Setenv("PGOPTIONS", c.pgoptions)
			}
			ctx := ctxT(t)

			conn, err := pgx.Connect(ctx, c.url)
			if err != nil {
				t.Fatal(err)
			}
			var plain string
			err = conn.QueryRow(ctx, "SHOW plan_cache_mode").Scan(&plain)
			_ = conn.Close(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if plain != other {
				t.Fatalf("control: a plain connection runs with %q, want %q: the case does not reach Postgres", plain, other)
			}

			pool, err := OpenPool(ctx, c.url)
			if err != nil {
				t.Fatal(err)
			}
			defer pool.Close()
			var got string
			if err := pool.QueryRow(ctx, "SHOW plan_cache_mode").Scan(&got); err != nil {
				t.Fatal(err)
			}
			if got != PlanCacheMode {
				t.Errorf("plan_cache_mode = %q, want %q", got, PlanCacheMode)
			}
		})
	}
}
