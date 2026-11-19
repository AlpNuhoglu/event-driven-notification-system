//go:build integration

package store

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

// lateResult is the successful result a late worker of attempt 1 writes.
func lateResult() AttemptResult {
	return AttemptResult{
		Outcome: domain.OutcomeSuccess, HTTPStatus: ptr(202), Latency: 25_000 * time.Millisecond,
		ProviderMessageID: ptr("late-pm-1"),
	}
}

// sweepOne runs the lease sweeper over a row whose lease was back-dated and
// returns the status it set.
func sweepOne(t *testing.T, s *Store, pool *pgxpool.Pool, id uuid.UUID) domain.Status {
	t.Helper()
	expireLease(t, pool, id)
	evs, err := s.SweepExpiredLeases(ctxT(t), 10, zeroJitter)
	if err != nil || len(evs) != 1 {
		t.Fatalf("sweep: %+v %v", evs, err)
	}
	return evs[0].Status
}

// claimAndSweep claims a row (attempt 1, whose worker stalls) and has the
// lease sweeper take it back; the attempt row is then unknown.
func claimAndSweep(t *testing.T, s *Store, pool *pgxpool.Pool, id uuid.UUID) domain.Status {
	t.Helper()
	claim(t, s, id)
	return sweepOne(t, s, pool, id)
}

// lateSuccess writes the late worker's success for attempt 1 and checks
// that transition 12 applied from the status the row had: the row is sent
// with the provider fields, failure_reason, queued_at and lease cleared,
// everything else as before, the event matches, and the attempt row holds
// the true outcome.
func lateSuccess(t *testing.T, s *Store, id uuid.UUID) {
	t.Helper()
	before := get(t, s, id)
	sent, ev, err := s.CompleteSent(ctxT(t), id, 1, testProvider, lateResult())
	if err != nil {
		t.Fatalf("late success from %s: %v", before.Status, err)
	}
	checkLateSuccess(t, s, before, sent, ev)
}

func checkLateSuccess(t *testing.T, s *Store, before Notification, sent Sent, ev domain.StatusChanged) {
	t.Helper()
	if want := (Sent{Late: true, From: before.Status}); transitionOf(sent) != want {
		t.Errorf("result %+v, want %+v", sent, want)
	}
	after := get(t, s, before.ID)
	checkTransition(t, before, after, ev, domain.StatusSent)
	if after.SentAt == nil || !after.SentAt.Equal(after.UpdatedAt) ||
		after.Provider == nil || *after.Provider != testProvider ||
		after.ProviderMessageID == nil || *after.ProviderMessageID != "late-pm-1" ||
		after.FailureReason != nil || after.QueuedAt != nil || after.LeaseExpiresAt != nil {
		t.Errorf("sent row = %+v", after)
	}
	// Nothing else changes: not the fencing token, the budget, due_at or
	// expires_at.
	b, a := before, after
	for _, n := range []*Notification{&b, &a} {
		n.Status, n.Version, n.UpdatedAt, n.SentAt = "", 0, time.Time{}, nil
		n.Provider, n.ProviderMessageID, n.FailureReason, n.QueuedAt, n.LeaseExpiresAt = nil, nil, nil, nil, nil
	}
	if !reflect.DeepEqual(b, a) {
		t.Errorf("transition 12 changed more than it sets:\nbefore %+v\nafter  %+v", before, after)
	}
	checkAttemptSuccess(t, s, before.ID, 1)
}

// checkAttemptSuccess asserts that attempt no holds the late worker's true
// outcome.
func checkAttemptSuccess(t *testing.T, s *Store, id uuid.UUID, no int) {
	t.Helper()
	a := attempts(t, s, id)
	if len(a) < no {
		t.Fatalf("attempts %+v", a)
	}
	got := a[no-1]
	if got.Outcome != domain.OutcomeSuccess || got.FinishedAt == nil || got.ProviderMessageID == nil ||
		*got.ProviderMessageID != "late-pm-1" || got.HTTPStatus == nil || *got.HTTPStatus != 202 {
		t.Errorf("attempt %d = %+v, want the true outcome success", no, got)
	}
}

// notApplied writes the late worker's success for attempt 1 and checks that
// it changed nothing on the row: ErrNotApplied, the row as before, and the
// attempt row with the true outcome.
func notApplied(t *testing.T, s *Store, id uuid.UUID) {
	t.Helper()
	before := get(t, s, id)
	sent, ev, err := s.CompleteSent(ctxT(t), id, 1, testProvider, lateResult())
	if !errors.Is(err, ErrNotApplied) {
		t.Fatalf("late success on a %s row (attempt_count %d): result %+v event %+v err %v, want ErrNotApplied",
			before.Status, before.AttemptCount, sent, ev, err)
	}
	if after := get(t, s, id); !reflect.DeepEqual(before, after) {
		t.Errorf("late success changed the row:\nbefore %+v\nafter  %+v", before, after)
	}
	checkAttemptSuccess(t, s, id, 1)
}

// The lease sweeper took the row back (transition 7) while the worker was
// stalled; its success then arrives: retrying -> sent, and failed
// (max_failures_unknown_outcome) -> sent.
func TestLateSuccess_AfterLeaseSweep(t *testing.T) {
	t.Run("swept to retrying", func(t *testing.T) {
		s, pool := newTestStore(t, delayed)
		n := create(t, s)
		if st := claimAndSweep(t, s, pool, n.ID); st != domain.StatusRetrying {
			t.Fatalf("swept to %s", st)
		}
		lateSuccess(t, s, n.ID)
	})
	t.Run("swept to failed", func(t *testing.T) {
		s, pool := newTestStore(t, delayed)
		n := create(t, s, withMaxFailures(1))
		if st := claimAndSweep(t, s, pool, n.ID); st != domain.StatusFailed {
			t.Fatalf("swept to %s", st)
		}
		if r := get(t, s, n.ID).FailureReason; r == nil || *r != domain.ReasonMaxFailuresUnknownOutcome {
			t.Fatalf("failure_reason %v", r)
		}
		lateSuccess(t, s, n.ID)
	})
}

// Swept, republished by the relay, then reconciled (transition 8): the row
// is pending with the same attempt_count, and the late success applies.
func TestLateSuccess_AfterReconcile(t *testing.T) {
	s, pool := newTestStore(t, zeroDelay)
	n := create(t, s)
	claimAndSweep(t, s, pool, n.ID)
	if evs, err := s.PublishLane(ctxT(t), smsNormal, 10, publishAll); err != nil || len(evs) != 1 {
		t.Fatalf("republish: %+v %v", evs, err)
	}
	if evs, err := s.ReconcileQueued(ctxT(t)); err != nil || len(evs) != 1 || evs[0].Status != domain.StatusPending {
		t.Fatalf("reconcile: %+v %v", evs, err)
	}
	lateSuccess(t, s, n.ID)
}

// The row expired while the worker was stalled: failed/expired -> sent, with
// failure_reason cleared. Both expiry paths: the relay's expiry pass
// (transition 10) and the claim of a redelivered copy (transition 3's
// expiry branch, which does not increment attempt_count).
func TestLateSuccess_AfterExpiry(t *testing.T) {
	t.Run("expiry pass", func(t *testing.T) {
		s, pool := newTestStore(t, zeroDelay)
		n := create(t, s)
		claimAndSweep(t, s, pool, n.ID)
		expire(t, pool, n.ID)
		if evs, err := s.ExpireDue(ctxT(t), 10); err != nil || len(evs) != 1 {
			t.Fatalf("expire: %+v %v", evs, err)
		}
		if r := get(t, s, n.ID).FailureReason; r == nil || *r != domain.ReasonExpired {
			t.Fatalf("failure_reason %v", r)
		}
		lateSuccess(t, s, n.ID)
	})
	t.Run("expired at claim", func(t *testing.T) {
		s, pool := newTestStore(t, zeroDelay)
		n := create(t, s)
		claimAndSweep(t, s, pool, n.ID)
		expire(t, pool, n.ID)
		if c := claim(t, s, n.ID); !c.Expired || c.Notification.AttemptCount != 1 {
			t.Fatalf("claim: %+v", c)
		}
		lateSuccess(t, s, n.ID)
	})
}

// The client cancelled while the worker was stalled: cancelled -> sent. The
// dedupe_key stays held (attempt_count > 0 before and after), and a second
// cancel answers as for a sent row.
func TestLateSuccess_AfterCancel(t *testing.T) {
	s, pool := newTestStore(t, zeroDelay)
	dedupe := func(n *NewNotification) { n.DedupeKey = ptr("order-42") }
	n := create(t, s, dedupe)
	claimAndSweep(t, s, pool, n.ID)
	if res, _, err := s.CancelNotification(ctxT(t), n.ID); err != nil || !res.Applied || !res.MayHaveBeenSent {
		t.Fatalf("cancel: %+v %v", res, err)
	}
	lateSuccess(t, s, n.ID)

	if _, err := s.CreateNotification(ctxT(t), func() NewNotification { m := baseNew(); dedupe(&m); return m }()); !errors.Is(err, ErrDuplicate) {
		t.Errorf("create with the same dedupe_key: err %v, want ErrDuplicate", err)
	}
	_, _, err := s.CancelNotification(ctxT(t), n.ID)
	if r, ok := refusal(err); !ok || r.Refusal != RefusalNotCancellable || r.Current != domain.StatusSent {
		t.Errorf("second cancel: err %v, want not_cancellable from sent", err)
	}
}

// Redrive (transition 11) keeps attempt_count: a late success of the last
// attempt still applies to the redriven pending row. Once the redriven row
// has been claimed again, the newer attempt owns it and the late success
// does not apply.
func TestLateSuccess_AfterRedrive(t *testing.T) {
	redriven := func(t *testing.T) (*Store, uuid.UUID) {
		t.Helper()
		s, pool := newTestStore(t, zeroDelay)
		n := create(t, s, withMaxFailures(1))
		if st := claimAndSweep(t, s, pool, n.ID); st != domain.StatusFailed {
			t.Fatalf("swept to %s", st)
		}
		r, _, err := s.RedriveNotification(ctxT(t), n.ID, nil, testMinLead)
		if err != nil || r.Status != domain.StatusPending || r.AttemptCount != 1 {
			t.Fatalf("redrive: %+v %v", r, err)
		}
		return s, n.ID
	}
	t.Run("redriven", func(t *testing.T) {
		s, id := redriven(t)
		lateSuccess(t, s, id)
	})
	t.Run("redriven and claimed again", func(t *testing.T) {
		s, id := redriven(t)
		if c := claim(t, s, id); c.Notification.AttemptCount != 2 {
			t.Fatalf("second claim: attempt_count %d", c.Notification.AttemptCount)
		}
		notApplied(t, s, id)
		if a := attempts(t, s, id); len(a) != 2 || a[1].Outcome != domain.OutcomeInFlight {
			t.Errorf("attempt 2 = %+v, want in_flight", a)
		}
	})
}

// A late success never applies after a newer claim, whatever the row's
// status has become since. This is the test of the attempt_count fence of
// transition 12: without it, every case except processing and sent moves
// the row to sent.
func TestLateSuccess_NewerAttemptWins(t *testing.T) {
	// Each setup starts with attempt 1 swept and attempt 2 claimed
	// (processing), and moves the row on from there.
	cases := []struct {
		name string
		then func(t *testing.T, s *Store, pool *pgxpool.Pool, id uuid.UUID)
		want domain.Status
	}{
		{"processing", nil, domain.StatusProcessing},
		{"retrying", func(t *testing.T, s *Store, _ *pgxpool.Pool, id uuid.UUID) {
			failAttempt(t, s, id, 2, domain.OutcomeRetryable)
		}, domain.StatusRetrying},
		{"retrying, swept", func(t *testing.T, s *Store, pool *pgxpool.Pool, id uuid.UUID) {
			sweepOne(t, s, pool, id)
		}, domain.StatusRetrying},
		{"queued", func(t *testing.T, s *Store, _ *pgxpool.Pool, id uuid.UUID) {
			failAttempt(t, s, id, 2, domain.OutcomeRetryable)
			if _, err := s.PublishLane(ctxT(t), smsNormal, 10, publishAll); err != nil {
				t.Fatal(err)
			}
		}, domain.StatusQueued},
		{"pending", func(t *testing.T, s *Store, _ *pgxpool.Pool, id uuid.UUID) {
			failAttempt(t, s, id, 2, domain.OutcomeRetryable)
			if _, err := s.PublishLane(ctxT(t), smsNormal, 10, publishAll); err != nil {
				t.Fatal(err)
			}
			if _, err := s.ReconcileQueued(ctxT(t)); err != nil {
				t.Fatal(err)
			}
		}, domain.StatusPending},
		{"failed", func(t *testing.T, s *Store, _ *pgxpool.Pool, id uuid.UUID) {
			failAttempt(t, s, id, 2, domain.OutcomePermanent)
		}, domain.StatusFailed},
		{"cancelled", func(t *testing.T, s *Store, _ *pgxpool.Pool, id uuid.UUID) {
			failAttempt(t, s, id, 2, domain.OutcomeRetryable)
			if _, _, err := s.CancelNotification(ctxT(t), id); err != nil {
				t.Fatal(err)
			}
		}, domain.StatusCancelled},
		{"sent", func(t *testing.T, s *Store, _ *pgxpool.Pool, id uuid.UUID) {
			if _, _, err := s.CompleteSent(ctxT(t), id, 2, testProvider, AttemptResult{Outcome: domain.OutcomeSuccess, ProviderMessageID: ptr("pm-2")}); err != nil {
				t.Fatal(err)
			}
		}, domain.StatusSent},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, pool := newTestStore(t, zeroDelay)
			n := create(t, s)
			claimAndSweep(t, s, pool, n.ID)
			if c := claim(t, s, n.ID); c.Notification.AttemptCount != 2 {
				t.Fatalf("second claim: attempt_count %d", c.Notification.AttemptCount)
			}
			if tc.then != nil {
				tc.then(t, s, pool, n.ID)
			}
			if got := get(t, s, n.ID); got.Status != tc.want || got.AttemptCount != 2 {
				t.Fatalf("setup: status %s attempt_count %d, want %s and 2", got.Status, got.AttemptCount, tc.want)
			}
			attempt2 := attempts(t, s, n.ID)[1]
			notApplied(t, s, n.ID)
			if got := attempts(t, s, n.ID)[1]; !reflect.DeepEqual(got, attempt2) {
				t.Errorf("attempt 2 changed: %+v -> %+v", attempt2, got)
			}
		})
	}
}

// failAttempt ends attempt no with a failure outcome through the worker's
// final write (transition 5 or 6).
func failAttempt(t *testing.T, s *Store, id uuid.UUID, no int, o domain.Outcome) {
	t.Helper()
	if _, err := s.CompleteFailure(ctxT(t), id, no, AttemptResult{Outcome: o}, 0); err != nil {
		t.Fatalf("complete failure: %v", err)
	}
}

// A success written again for a row that is already sent (the final-write
// retry of D18 after a commit whose answer was lost) changes nothing.
func TestLateSuccess_AlreadySentChangesNothing(t *testing.T) {
	s, _ := newTestStore(t, zeroDelay)
	n := create(t, s)
	claim(t, s, n.ID)
	sent, _, err := s.CompleteSent(ctxT(t), n.ID, 1, testProvider, lateResult())
	if err != nil || sent.Late {
		t.Fatalf("first write: %+v %v", sent, err)
	}
	notApplied(t, s, n.ID)
}

// sentLockWaiters counts backends of this test's database that are blocked
// on a lock while running the worker's transition 4 (MarkSent).
func sentLockWaiters(ctx context.Context, t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	err := pool.QueryRow(ctx, `
		SELECT count(*) FROM pg_stat_activity
		WHERE datname = current_database()
		  AND pid <> pg_backend_pid()
		  AND wait_event_type = 'Lock'
		  AND query LIKE '%-- name: MarkSent :one%'`).Scan(&n)
	if err != nil {
		t.Errorf("pg_stat_activity: %v", err)
		return -1
	}
	return n
}

type sentResult struct {
	sent Sent
	ev   domain.StatusChanged
	err  error
}

func startCompleteSent(ctx context.Context, s *Store, id uuid.UUID) <-chan sentResult {
	done := make(chan sentResult, 1)
	go func() {
		sent, ev, err := s.CompleteSent(ctx, id, 1, testProvider, lateResult())
		done <- sentResult{sent, ev, err}
	}()
	return done
}

func receiveSent(t *testing.T, done <-chan sentResult) sentResult {
	t.Helper()
	var r sentResult
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

// checkSentWithSuccess asserts the end state every order of the lease
// sweeper and the final write must reach: sent with the successful attempt
// row, never retrying (or failed) with a successful attempt.
func checkSentWithSuccess(t *testing.T, s *Store, id uuid.UUID) {
	t.Helper()
	got := get(t, s, id)
	if got.Status != domain.StatusSent || got.SentAt == nil || got.FailureReason != nil || got.AttemptCount != 1 {
		t.Errorf("row %+v, want sent", got)
	}
	checkAttemptSuccess(t, s, id, 1)
}

// The lease sweeper and the worker's final write on the same row (the lease
// expired, but the worker is slow, not dead). The row always ends sent, by
// transition 4 or by transition 12, and the attempt row holds success.
func TestLeaseSweeper_VersusFinalWrite(t *testing.T) {
	setup := func(t *testing.T) (*Store, *pgxpool.Pool, Notification) {
		t.Helper()
		s, pool := newTestStore(t, delayed)
		n := create(t, s)
		claim(t, s, n.ID)
		expireLease(t, pool, n.ID)
		return s, pool, get(t, s, n.ID)
	}

	t.Run("sweep first", func(t *testing.T) {
		s, _, n := setup(t)
		if evs, err := s.SweepExpiredLeases(ctxT(t), 10, zeroJitter); err != nil || len(evs) != 1 {
			t.Fatalf("sweep: %+v %v", evs, err)
		}
		lateSuccess(t, s, n.ID)
		checkSentWithSuccess(t, s, n.ID)
	})

	t.Run("final write first", func(t *testing.T) {
		s, _, n := setup(t)
		sent, _, err := s.CompleteSent(ctxT(t), n.ID, 1, testProvider, lateResult())
		if err != nil || transitionOf(sent) != (Sent{From: domain.StatusProcessing}) {
			t.Fatalf("final write: %+v %v", sent, err)
		}
		if evs, err := s.SweepExpiredLeases(ctxT(t), 10, zeroJitter); err != nil || len(evs) != 0 {
			t.Fatalf("sweep after the final write: %+v %v", evs, err)
		}
		checkSentWithSuccess(t, s, n.ID)
	})

	// The sweeper holds the row (inside its transaction: the attempt is
	// already unknown, the decision not yet applied) when the final write
	// starts. The final write's transition 4 waits on the row lock and,
	// after the sweeper's commit, finds retrying: transition 12 applies.
	t.Run("sweeper holds the row, final write waits", func(t *testing.T) {
		s, pool, n := setup(t)
		var done <-chan sentResult
		evs, err := s.SweepExpiredLeases(ctxT(t), 10, func() float64 {
			done = startCompleteSent(ctxT(t), s, n.ID)
			testutil.Eventually(t, func() bool { return sentLockWaiters(ctxT(t), t, pool) == 1 }, lockWaitTimeout)
			return 0
		})
		if err != nil || len(evs) != 1 || evs[0].Status != domain.StatusRetrying {
			t.Fatalf("sweep: %+v %v", evs, err)
		}
		r := receiveSent(t, done)
		if r.err != nil || transitionOf(r.sent) != (Sent{Late: true, From: domain.StatusRetrying}) || r.ev.Version != evs[0].Version+1 {
			t.Fatalf("final write after the sweep: %+v (sweep event %+v)", r, evs[0])
		}
		checkSentWithSuccess(t, s, n.ID)
	})

	// Another transaction holds the row when both start: the sweeper skips
	// it (SKIP LOCKED, it never waits) and the final write waits for the
	// lock, then applies transition 4.
	t.Run("row held, sweeper skips, final write waits", func(t *testing.T) {
		s, pool, n := setup(t)
		ctx := ctxT(t)
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(context.Background()) }()
		if _, err := tx.Exec(ctx, `SELECT 1 FROM notifications WHERE id = $1 FOR UPDATE`, n.ID); err != nil {
			t.Fatal(err)
		}
		done := startCompleteSent(ctx, s, n.ID)
		testutil.Eventually(t, func() bool { return sentLockWaiters(ctx, t, pool) == 1 }, lockWaitTimeout)
		if evs, err := s.SweepExpiredLeases(ctx, 10, zeroJitter); err != nil || len(evs) != 0 {
			t.Fatalf("sweep of a held row: %+v %v", evs, err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		r := receiveSent(t, done)
		if r.err != nil || transitionOf(r.sent) != (Sent{From: domain.StatusProcessing}) {
			t.Fatalf("final write: %+v", r)
		}
		checkSentWithSuccess(t, s, n.ID)
	})

	// Started together, many rounds: whichever order the database picks,
	// the row ends sent with a successful attempt.
	t.Run("concurrent", func(t *testing.T) {
		const rounds = 100
		s, pool := newTestStore(t, delayed)
		var direct, late int
		for round := range rounds {
			n := create(t, s)
			claim(t, s, n.ID)
			expireLease(t, pool, n.ID)
			start := make(chan struct{})
			var (
				wg       sync.WaitGroup
				r        sentResult
				swept    []domain.StatusChanged
				sweepErr error
			)
			wg.Go(func() {
				<-start
				r.sent, r.ev, r.err = s.CompleteSent(context.Background(), n.ID, 1, testProvider, lateResult())
			})
			wg.Go(func() {
				<-start
				swept, sweepErr = s.SweepExpiredLeases(context.Background(), 10, zeroJitter)
			})
			close(start)
			wg.Wait()
			if r.err != nil || sweepErr != nil {
				t.Fatalf("round %d: final write %v, sweep %v", round, r.err, sweepErr)
			}
			switch {
			case !r.sent.Late && r.sent.From == domain.StatusProcessing && len(swept) == 0:
				direct++
			case r.sent.Late && r.sent.From == domain.StatusRetrying && len(swept) == 1:
				late++
			default:
				t.Fatalf("round %d: final write %+v, sweep %+v", round, r.sent, swept)
			}
			checkSentWithSuccess(t, s, n.ID)
		}
		t.Logf("%d rounds: transition 4 %d, transition 12 after the sweep %d", rounds, direct, late)
	})
}
