//go:build integration

package migrations_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
	"github.com/AlpNuhoglu/event-driven-notification-system/migrations"
)

func TestMain(m *testing.M) { testutil.Main(m) }

func tableCount(t *testing.T, url string) int {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	var n int
	err = conn.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables
		WHERE table_schema = 'public'
		  AND table_name IN ('batches', 'notifications', 'notification_attempts', 'idempotency_keys',
		                     'templates', 'template_versions')`).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestMigrations_UpDownUp(t *testing.T) {
	url := testutil.NewDatabase(t)
	steps := []struct {
		name  string
		apply func(string) error
		want  int
	}{
		{"up", migrations.Up, 6},
		{"up again is a no-op", migrations.Up, 6},
		{"down", migrations.Down, 0},
		{"up after down", migrations.Up, 6},
	}
	for _, s := range steps {
		if err := s.apply(url); err != nil {
			t.Fatalf("%s: %v", s.name, err)
		}
		if got := tableCount(t, url); got != s.want {
			t.Fatalf("%s: %d tables, want %d", s.name, got, s.want)
		}
	}
}
