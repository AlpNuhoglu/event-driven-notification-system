//go:build integration

package store

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	checkViolation  = "23514"
	uniqueViolation = "23505"
)

// The database itself rejects rows that break the invariants of ADR-0004 section 6.
func TestConstraints(t *testing.T) {
	cases := []struct {
		name    string
		claimed bool // the row has an in_flight attempt 1
		sql     string
		want    string
	}{
		{"failed without reason", false, `UPDATE notifications SET status = 'failed' WHERE id = $1`, checkViolation},
		{"reason without failed", false, `UPDATE notifications SET failure_reason = 'expired' WHERE id = $1`, checkViolation},
		{"sent without sent_at", false, `UPDATE notifications SET status = 'sent' WHERE id = $1`, checkViolation},
		{"sent_at without sent", false, `UPDATE notifications SET sent_at = now() WHERE id = $1`, checkViolation},
		{
			"second in_flight attempt", true,
			`INSERT INTO notification_attempts (notification_id, attempt_no, channel, provider, outcome, started_at)
			 VALUES ($1, 2, 'sms', 'mock', 'in_flight', now())`, uniqueViolation,
		},
		{
			"in_flight attempt with finished_at", false,
			`INSERT INTO notification_attempts (notification_id, attempt_no, channel, provider, outcome, started_at, finished_at)
			 VALUES ($1, 1, 'sms', 'mock', 'in_flight', now(), now())`, checkViolation,
		},
		{
			"finished attempt without finished_at", false,
			`INSERT INTO notification_attempts (notification_id, attempt_no, channel, provider, outcome, started_at)
			 VALUES ($1, 1, 'sms', 'mock', 'success', now())`, checkViolation,
		},
		{
			"finishing an attempt without finished_at", true,
			`UPDATE notification_attempts SET outcome = 'success' WHERE notification_id = $1`, checkViolation,
		},
	}
	s, pool := newTestStore(t, zeroDelay)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := create(t, s)
			if tc.claimed {
				claim(t, s, n.ID)
			}
			if got := execErr(t, pool, tc.sql, n.ID); got != tc.want {
				t.Errorf("SQLSTATE = %q, want %q", got, tc.want)
			}
		})
	}
	// The valid counterparts are accepted.
	n := create(t, s)
	if got := execErr(t, pool, `UPDATE notifications SET status = 'failed', failure_reason = 'expired' WHERE id = $1`, n.ID); got != "" {
		t.Errorf("valid failed row rejected: %s", got)
	}
}

func execErr(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) string {
	t.Helper()
	_, err := pool.Exec(ctxT(t), sql, args...)
	if err != nil && sqlState(err) == "" {
		t.Fatalf("%s: %v", sql, err)
	}
	return sqlState(err)
}
