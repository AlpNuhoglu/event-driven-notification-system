//go:build integration

package worker

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/config"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

// The pool Service opens plans every statement with the current statistics
// (D79): plan_cache_mode = force_custom_plan on a pooled connection, in a
// transaction, on a connection opened after Reset, and also when
// DATABASE_URL asks for plan_cache_mode = auto.
func TestService_PoolPlansWithCurrentStatistics(t *testing.T) {
	testutil.RequireCustomPlanPool(t, func(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
		return openPool(ctx, config.Config{Connections: config.Connections{DatabaseURL: databaseURL}})
	})
}
