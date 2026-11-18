//go:build integration

package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

// lockWaitTimeout is shorter than the test context, so a claim that never
// blocks fails the poll before the context expires.
const lockWaitTimeout = 5_000 * time.Millisecond

type claimResult struct {
	claim Claim
	event domain.StatusChanged
	err   error
}

// claimLockWaiters counts backends of this test's database that are blocked
// on a lock while running the claim statement.
func claimLockWaiters(ctx context.Context, t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	err := pool.QueryRow(ctx, `
		SELECT count(*) FROM pg_stat_activity
		WHERE datname = current_database()
		  AND pid <> pg_backend_pid()
		  AND wait_event_type = 'Lock'
		  AND query LIKE '%WITH claimed AS%'`).Scan(&n)
	if err != nil {
		t.Errorf("pg_stat_activity: %v", err)
		return -1
	}
	return n
}

// startClaim runs Claim in a goroutine; the result arrives on the channel.
func startClaim(ctx context.Context, s *Store, id uuid.UUID) <-chan claimResult {
	done := make(chan claimResult, 1)
	go func() {
		c, ev, err := s.Claim(ctx, id, testLease, testProvider)
		done <- claimResult{c, ev, err}
	}()
	return done
}

func receive(t *testing.T, done <-chan claimResult) claimResult {
	t.Helper()
	var r claimResult
	testutil.Eventually(t, func() bool {
		select {
		case r = <-done:
			return true
		default:
			return false
		}
	}, waitTimeout)
	return r
}

// A claim issued while the relay's lane transaction is open waits on the row
// lock and claims the row after the commit (ADR-0002, ADR-0004 transition 3).
// With a claim guard of status = 'queued' only, the claim would see the
// committed 'pending' version, match zero rows without waiting, and this test
// would fail at the pg_stat_activity check.
func TestClaim_WaitsForRelayCommit(t *testing.T) {
	s, pool := newTestStore(t, zeroDelay)
	n := create(t, s)
	lane := Lane{Channel: n.Channel, Priority: n.Priority}

	var done <-chan claimResult
	events, err := s.PublishLane(ctxT(t), lane, 10, func(ctx context.Context, rows []DueRow) ([]uuid.UUID, error) {
		if len(rows) != 1 || rows[0].ID != n.ID {
			return nil, fmt.Errorf("unexpected rows %v", rows)
		}
		// The relay holds the row lock now; a worker receives the message early.
		done = startClaim(ctx, s, n.ID)
		testutil.Eventually(t, func() bool { return claimLockWaiters(ctx, t, pool) == 1 }, lockWaitTimeout)
		select {
		case r := <-done:
			return nil, fmt.Errorf("claim returned before the relay committed: %v", r.err)
		default:
		}
		return []uuid.UUID{n.ID}, nil
	})
	if err != nil {
		t.Fatalf("publish lane: %v", err)
	}
	if len(events) != 1 || events[0].Status != domain.StatusQueued {
		t.Fatalf("relay events = %+v", events)
	}

	r := receive(t, done)
	if r.err != nil {
		t.Fatalf("claim after commit: %v", r.err)
	}
	got := get(t, s, n.ID)
	if got.Status != domain.StatusProcessing || r.claim.Expired {
		t.Errorf("status = %s, expired = %v", got.Status, r.claim.Expired)
	}
	if got.QueuedAt == nil {
		t.Error("queued_at is NULL: the claim did not run after the relay's commit")
	}
	if got.Version != 3 || r.event.Version != 3 || got.AttemptCount != 1 {
		t.Errorf("version = %d, event version = %d, attempt_count = %d", got.Version, r.event.Version, got.AttemptCount)
	}
}

// A relay rollback leaves a pending row that is claimed directly, with
// queued_at NULL.
func TestClaim_DirectAfterRelayRollback(t *testing.T) {
	t.Run("claim after rollback", func(t *testing.T) {
		s, _ := newTestStore(t, zeroDelay)
		n := create(t, s)
		errBroker := errors.New("broker unavailable")
		_, err := s.PublishLane(ctxT(t), Lane{n.Channel, n.Priority}, 10,
			func(context.Context, []DueRow) ([]uuid.UUID, error) { return nil, errBroker })
		if !errors.Is(err, errBroker) {
			t.Fatalf("publish lane error = %v, want %v", err, errBroker)
		}
		if got := get(t, s, n.ID); got.Status != domain.StatusPending || got.Version != n.Version {
			t.Fatalf("after rollback: status %s version %d", got.Status, got.Version)
		}
		c, ev, err := s.Claim(ctxT(t), n.ID, testLease, testProvider)
		if err != nil {
			t.Fatalf("direct claim: %v", err)
		}
		got := get(t, s, n.ID)
		checkTransition(t, n, got, ev, domain.StatusProcessing)
		if got.QueuedAt != nil || c.Notification.QueuedAt != nil {
			t.Error("queued_at must stay NULL for a direct claim")
		}
	})

	t.Run("claim waiting during the rollback", func(t *testing.T) {
		s, pool := newTestStore(t, zeroDelay)
		n := create(t, s)
		var done <-chan claimResult
		errBroker := errors.New("confirm timeout")
		_, err := s.PublishLane(ctxT(t), Lane{n.Channel, n.Priority}, 10,
			func(ctx context.Context, _ []DueRow) ([]uuid.UUID, error) {
				done = startClaim(ctx, s, n.ID)
				testutil.Eventually(t, func() bool { return claimLockWaiters(ctx, t, pool) == 1 }, lockWaitTimeout)
				return nil, errBroker
			})
		if !errors.Is(err, errBroker) {
			t.Fatalf("publish lane error = %v", err)
		}
		r := receive(t, done)
		if r.err != nil {
			t.Fatalf("claim after rollback: %v", r.err)
		}
		got := get(t, s, n.ID)
		if got.Status != domain.StatusProcessing || got.QueuedAt != nil || got.Version != n.Version+1 {
			t.Errorf("status %s queued_at %v version %d", got.Status, got.QueuedAt, got.Version)
		}
	})
}

// Two or more concurrent claims of one row: exactly one wins, the others get
// ErrNotApplied, and exactly one attempt row exists.
func TestClaim_TwoWorkersExactlyOne(t *testing.T) {
	const rounds, workers = 25, 8
	s, _ := newTestStore(t, zeroDelay)
	for round := range rounds {
		n := create(t, s)
		start := make(chan struct{})
		results := make([]error, workers)
		var wg sync.WaitGroup
		for w := range workers {
			wg.Go(func() {
				<-start
				_, _, results[w] = s.Claim(context.Background(), n.ID, testLease, testProvider)
			})
		}
		close(start)
		wg.Wait()
		wins := 0
		for _, err := range results {
			switch {
			case err == nil:
				wins++
			case errors.Is(err, ErrNotApplied):
			default:
				t.Fatalf("round %d: unexpected error %v", round, err)
			}
		}
		if wins != 1 {
			t.Fatalf("round %d: %d winners, want 1", round, wins)
		}
		a := attempts(t, s, n.ID)
		if len(a) != 1 || a[0].AttemptNo != 1 || a[0].Outcome != domain.OutcomeInFlight {
			t.Fatalf("round %d: attempts %+v", round, a)
		}
		if got := get(t, s, n.ID); got.AttemptCount != 1 || got.Version != 2 {
			t.Fatalf("round %d: attempt_count %d version %d", round, got.AttemptCount, got.Version)
		}
	}
}

func TestClaim_Fields(t *testing.T) {
	s, _ := newTestStore(t, zeroDelay)
	n := create(t, s)
	c, ev, err := s.Claim(ctxT(t), n.ID, testLease, testProvider)
	if err != nil {
		t.Fatal(err)
	}
	got := get(t, s, n.ID)
	checkTransition(t, n, got, ev, domain.StatusProcessing)
	if c.Expired || got.AttemptCount != 1 || got.FailureReason != nil {
		t.Errorf("claim %+v row %+v", c, got)
	}
	if got.LeaseExpiresAt == nil || got.LeaseExpiresAt.Sub(got.UpdatedAt) != testLease {
		t.Errorf("lease_expires_at = %v, want updated_at + %s", got.LeaseExpiresAt, testLease)
	}
	a := attempts(t, s, n.ID)
	if len(a) != 1 {
		t.Fatalf("attempts = %+v", a)
	}
	want := Attempt{
		NotificationID: n.ID, AttemptNo: 1, Channel: n.Channel, Provider: testProvider,
		Outcome: domain.OutcomeInFlight, StartedAt: got.UpdatedAt,
	}
	if a[0] != want {
		t.Errorf("attempt = %+v, want %+v", a[0], want)
	}
}

// Expired at claim: failed/expired, no attempt row, attempt_count unchanged.
func TestClaim_ExpiredAtClaim(t *testing.T) {
	for _, from := range []domain.Status{domain.StatusPending, domain.StatusQueued, domain.StatusRetrying} {
		t.Run(string(from), func(t *testing.T) {
			s, pool := newTestStore(t, zeroDelay)
			n := create(t, s)
			switch from {
			case domain.StatusQueued:
				if _, err := s.PublishLane(ctxT(t), Lane{n.Channel, n.Priority}, 10, publishAll); err != nil {
					t.Fatal(err)
				}
			case domain.StatusRetrying:
				claim(t, s, n.ID)
				if _, err := s.CompleteFailure(ctxT(t), n.ID, 1, AttemptResult{Outcome: domain.OutcomeRetryable}, 0); err != nil {
					t.Fatal(err)
				}
			}
			expire(t, pool, n.ID)
			before := get(t, s, n.ID)
			if before.Status != from {
				t.Fatalf("setup: status %s", before.Status)
			}
			nAttempts := len(attempts(t, s, n.ID))

			c, ev, err := s.Claim(ctxT(t), n.ID, testLease, testProvider)
			if err != nil {
				t.Fatal(err)
			}
			after := get(t, s, n.ID)
			checkTransition(t, before, after, ev, domain.StatusFailed)
			if !c.Expired || after.FailureReason == nil || *after.FailureReason != domain.ReasonExpired {
				t.Errorf("expired = %v, reason = %v", c.Expired, after.FailureReason)
			}
			if after.AttemptCount != before.AttemptCount || after.LeaseExpiresAt != nil {
				t.Errorf("attempt_count %d -> %d, lease %v", before.AttemptCount, after.AttemptCount, after.LeaseExpiresAt)
			}
			if got := len(attempts(t, s, n.ID)); got != nAttempts {
				t.Errorf("attempt rows %d -> %d", nAttempts, got)
			}
		})
	}
}

func TestClaim_NotClaimable(t *testing.T) {
	cases := []struct {
		name  string
		store Profiles
		setup func(t *testing.T, s *Store, pool *pgxpool.Pool, id uuid.UUID)
	}{
		{"retrying not yet due", delayed, func(t *testing.T, s *Store, _ *pgxpool.Pool, id uuid.UUID) {
			claim(t, s, id)
			if _, err := s.CompleteFailure(ctxT(t), id, 1, AttemptResult{Outcome: domain.OutcomeRetryable}, 0); err != nil {
				t.Fatal(err)
			}
		}},
		{"scheduled pending not yet due", zeroDelay, nil},
		{"processing", zeroDelay, func(t *testing.T, s *Store, _ *pgxpool.Pool, id uuid.UUID) {
			claim(t, s, id)
		}},
		{"sent", zeroDelay, func(t *testing.T, s *Store, _ *pgxpool.Pool, id uuid.UUID) {
			claim(t, s, id)
			if _, err := s.CompleteSent(ctxT(t), id, 1, testProvider, AttemptResult{Outcome: domain.OutcomeSuccess}); err != nil {
				t.Fatal(err)
			}
		}},
		{"failed", zeroDelay, func(t *testing.T, s *Store, _ *pgxpool.Pool, id uuid.UUID) {
			claim(t, s, id)
			if _, err := s.CompleteFailure(ctxT(t), id, 1, AttemptResult{Outcome: domain.OutcomePermanent}, 0); err != nil {
				t.Fatal(err)
			}
		}},
		// Transition 9 (cancel) is out of scope here, so the state is set directly.
		{"cancelled", zeroDelay, func(t *testing.T, _ *Store, pool *pgxpool.Pool, id uuid.UUID) {
			exec(t, pool, `UPDATE notifications SET status = 'cancelled' WHERE id = $1`, id)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, pool := newTestStore(t, tc.store)
			mods := []func(*NewNotification){withMaxFailures(1)}
			if tc.name == "retrying not yet due" {
				mods = []func(*NewNotification){withMaxFailures(5)}
			}
			if tc.setup == nil {
				mods = append(mods, func(n *NewNotification) { n.ScheduledAt = ptr(time.Now().Add(time.Hour)) })
			}
			n := create(t, s, mods...)
			if tc.setup != nil {
				tc.setup(t, s, pool, n.ID)
			}
			before := get(t, s, n.ID)
			beforeAttempts := attempts(t, s, n.ID)
			if _, _, err := s.Claim(ctxT(t), n.ID, testLease, testProvider); !errors.Is(err, ErrNotApplied) {
				t.Fatalf("claim of %s row: err = %v, want ErrNotApplied", before.Status, err)
			}
			if after := get(t, s, n.ID); after.Version != before.Version || after.Status != before.Status {
				t.Errorf("row changed: %+v -> %+v", before, after)
			}
			if got := attempts(t, s, n.ID); len(got) != len(beforeAttempts) {
				t.Errorf("attempt rows %d -> %d", len(beforeAttempts), len(got))
			}
		})
	}
}

func TestClaim_UnknownID(t *testing.T) {
	s, _ := newTestStore(t, zeroDelay)
	if _, _, err := s.Claim(ctxT(t), uuid.New(), testLease, testProvider); !errors.Is(err, ErrNotApplied) {
		t.Errorf("err = %v, want ErrNotApplied", err)
	}
}
