//go:build integration

package relay

import (
	"bytes"
	"context"
	"net"
	"net/url"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
)

// queueRows creates n SMS rows of normal priority, marks them queued through
// a lane transaction whose publish confirms them, and back-dates their
// queued_at by ageMillis milliseconds (test setup).
func queueRows(t *testing.T, st *store.Store, pool *pgxpool.Pool, n, ageMillis int) []uuid.UUID {
	t.Helper()
	ids := create(t, st, domain.ChannelSMS, domain.PriorityNormal, n)
	lane := store.Lane{Channel: domain.ChannelSMS, Priority: domain.PriorityNormal}
	// Only the new rows are confirmed; other pending rows of the lane stay
	// pending.
	_, err := st.PublishLane(ctxT(t), lane, 1000, func(context.Context, []store.DueRow) ([]uuid.UUID, error) {
		return ids, nil
	})
	if err != nil {
		t.Fatalf("publish lane: %v", err)
	}
	requireStatus(t, st, ids, domain.StatusQueued)
	if _, err := pool.Exec(ctxT(t), `UPDATE notifications SET queued_at = now() - make_interval(secs => $2::int / 1000.0) WHERE id = ANY($1::uuid[])`,
		ids, ageMillis); err != nil {
		t.Fatalf("back-date queued_at: %v", err)
	}
	return ids
}

// runRelayBinary runs the relay binary's entry point, as main does, with the
// given variables.
func runRelayBinary(t *testing.T, vars map[string]string, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var so, se bytes.Buffer
	code = Main(ctxT(t), args, func(k string) (string, bool) { v, ok := vars[k]; return v, ok }, &so, &se)
	return code, so.String(), se.String()
}

// requeue-stale through the relay binary's entry point against real
// Postgres (D64): by default only the rows queued longer than 5 minutes
// return to pending; --older-than=1ms takes the younger ones too; --all
// takes every queued row. It connects to Postgres only: RabbitMQ is
// unreachable and the admin port is taken, and the command still succeeds.
// A database error exits 1, a usage error 2.
func TestRequeueStaleCommand(t *testing.T) {
	st, pool := newStore(t)
	taken, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = taken.Close() })
	vars := map[string]string{
		"DATABASE_URL": pool.Config().ConnString(),
		"RABBITMQ_URL": "amqp://127.0.0.1:1/",
		"ADMIN_ADDR":   taken.Addr().String(),
	}

	stale := queueRows(t, st, pool, 3, 600_000) // 10 minutes
	young := queueRows(t, st, pool, 2, 299_000) // just under 5 minutes
	pending := create(t, st, domain.ChannelSMS, domain.PriorityNormal, 1)

	code, out, errOut := runRelayBinary(t, vars, "requeue-stale")
	if code != 0 || out != "requeue-stale: 3 notifications returned to pending\n" || errOut != "" {
		t.Fatalf("default: exit %d, stdout %q, stderr %q", code, out, errOut)
	}
	requireStatus(t, st, stale, domain.StatusPending)
	requireStatus(t, st, young, domain.StatusQueued)
	requireStatus(t, st, pending, domain.StatusPending)

	code, out, errOut = runRelayBinary(t, vars, "requeue-stale", "--older-than=1ms")
	if code != 0 || out != "requeue-stale: 2 notifications returned to pending\n" {
		t.Fatalf("--older-than=1ms: exit %d, stdout %q, stderr %q", code, out, errOut)
	}
	requireStatus(t, st, young, domain.StatusPending)

	// Queued just now: only --all takes them.
	fresh := queueRows(t, st, pool, 4, 0)
	code, out, _ = runRelayBinary(t, vars, "requeue-stale")
	if code != 0 || out != "requeue-stale: 0 notifications returned to pending\n" {
		t.Fatalf("default on fresh rows: exit %d, stdout %q", code, out)
	}
	code, out, errOut = runRelayBinary(t, vars, "requeue-stale", "--all")
	if code != 0 || out != "requeue-stale: 4 notifications returned to pending\n" {
		t.Fatalf("--all: exit %d, stdout %q, stderr %q", code, out, errOut)
	}
	requireStatus(t, st, fresh, domain.StatusPending)
	for _, id := range append(stale, fresh...) {
		n, err := st.GetNotification(ctxT(t), id)
		if err != nil {
			t.Fatal(err)
		}
		if n.QueuedAt != nil || n.Version < 3 {
			t.Errorf("row %s: queued_at set %v, version %d; want cleared and bumped by transition 8", id, n.QueuedAt != nil, n.Version)
		}
	}

	// A usage error exits 2 with the usage, and changes nothing.
	again := queueRows(t, st, pool, 1, 600_000)
	code, out, errOut = runRelayBinary(t, vars, "requeue-stale", "--all", "--older-than=1m")
	if code != 2 || out != "" || !strings.Contains(errOut, "cannot be used together") ||
		!strings.Contains(errOut, "usage: relay [healthcheck | requeue-stale [--older-than=5m] [--all]]") {
		t.Errorf("both flags: exit %d, stdout %q, stderr %q", code, out, errOut)
	}
	requireStatus(t, st, again, domain.StatusQueued)

	// A database error exits 1.
	downURL := url.URL{
		Scheme: "postgres", User: url.UserPassword("app", "secret"), Host: "127.0.0.1:1",
		Path: "/notifications", RawQuery: "sslmode=disable&connect_timeout=1",
	}
	down := map[string]string{"DATABASE_URL": downURL.String()}
	code, out, errOut = runRelayBinary(t, down, "requeue-stale", "--all")
	if code != 1 || out != "" || !strings.HasPrefix(errOut, "requeue-stale: ") {
		t.Errorf("database down: exit %d, stdout %q, stderr %q", code, out, errOut)
	}
	if strings.Contains(errOut, "secret") {
		t.Errorf("stderr leaks the password: %q", errOut)
	}
}
