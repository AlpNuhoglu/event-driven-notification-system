//go:build integration

package gateway

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

// The read-only pool Service opens plans every statement with the current
// statistics too (D79, "and the gateway when it arrives"): the read-only
// option keeps plan_cache_mode = force_custom_plan on a pooled connection, in
// a transaction, on a connection opened after Reset, and when DATABASE_URL
// asks for plan_cache_mode = auto. The opener is the call Service makes.
func TestService_PoolPlansWithCurrentStatistics(t *testing.T) {
	testutil.RequireCustomPlanPool(t, func(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
		return store.OpenPoolWith(ctx, databaseURL, store.ReadOnly)
	})
}
