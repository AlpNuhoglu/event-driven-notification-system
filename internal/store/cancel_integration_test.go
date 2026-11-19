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

// raceRounds is the number of rounds of the concurrent cancel/claim race.
const raceRounds = 200

// testMinLead is the minimum lead of a new expires_at in these tests.
const testMinLead = 30_000 * time.Millisecond

type cancelResult struct {
	res   Cancelled
	event domain.StatusChanged
	err   error
}

func startCancel(ctx context.Context, s *Store, id uuid.UUID) <-chan cancelResult {
	done := make(chan cancelResult, 1)
	go func() {
		r, ev, err := s.CancelNotification(ctx, id)
		done <- cancelResult{r, ev, err}
	}()
	return done
}

func receiveCancel(t *testing.T, done <-chan cancelResult) cancelResult {
	t.Helper()
	var r cancelResult
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

// cancelLockWaiters counts backends of this test's database that are blocked
// on a lock while running the cancel statement.
func cancelLockWaiters(ctx context.Context, t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	err := pool.QueryRow(ctx, `
		SELECT count(*) FROM pg_stat_activity
		WHERE datname = current_database()
		  AND pid <> pg_backend_pid()
		  AND wait_event_type = 'Lock'
		  AND query LIKE '%SET status = ''cancelled''%'`).Scan(&n)
	if err != nil {
		t.Errorf("pg_stat_activity: %v", err)
		return -1
	}
	return n
}

func refusal(err error) (*RefusedError, bool) {
	var r *RefusedError
	ok := errors.As(err, &r)
	return r, ok
}

// raceWinner checks one cancel/claim race on a due pending row: exactly one
// of "the cancel applied" and "the claim returned the row", and the final
// row agrees with the winner. It returns true when the claim won.
func raceWinner(t *testing.T, s *Store, id uuid.UUID, c cancelResult, cl claimResult) (claimWon bool) {
	t.Helper()
	cancelled := c.err == nil && c.res.Applied
	claimed := cl.err == nil
	if cancelled == claimed {
		t.Fatalf("cancel applied %v (err %v), claim returned the row %v (err %v): want exactly one",
			cancelled, c.err, claimed, cl.err)
	}
	got := get(t, s, id)
	a := attempts(t, s, id)
	if cancelled {
		if !errors.Is(cl.err, ErrNotApplied) {
			t.Fatalf("claim after the cancel: err %v, want ErrNotApplied", cl.err)
		}
		if got.Status != domain.StatusCancelled || got.AttemptCount != 0 || len(a) != 0 {
			t.Fatalf("cancel won, row status %s attempt_count %d attempts %d", got.Status, got.AttemptCount, len(a))
		}
		if c.event.Status != domain.StatusCancelled || c.event.Version != got.Version {
			t.Fatalf("cancel event %+v, row version %d", c.event, got.Version)
		}
		return false
	}
	r, ok := refusal(c.err)
	if !ok || r.Refusal != RefusalNotCancellable || r.Current != domain.StatusProcessing {
		t.Fatalf("cancel after the claim: err %v, want not_cancellable from processing", c.err)
	}
	if got.Status != domain.StatusProcessing || got.AttemptCount != 1 || len(a) != 1 {
		t.Fatalf("claim won, row status %s attempt_count %d attempts %d", got.Status, got.AttemptCount, len(a))
	}
	return true
}

// Cancel versus claim is decided by the guards of the two statements alone
// (ADR-0004 transition 9): never both. Without the status guard of
// CancelNotification, the cancel also applies to a row the claim took, and
// every case where the claim wins fails.
func TestCancel_RacesWithClaim(t *testing.T) {
	t.Run("claim first", func(t *testing.T) {
		s, _ := newTestStore(t, zeroDelay)
		n := create(t, s)
		c, ev, err := s.Claim(ctxT(t), n.ID, testLease, testProvider)
		cl := claimResult{c, ev, err}
		r, cev, cerr := s.CancelNotification(ctxT(t), n.ID)
		if !raceWinner(t, s, n.ID, cancelResult{r, cev, cerr}, cl) {
			t.Fatal("the cancel won although the claim ran first")
		}
	})

	t.Run("cancel first", func(t *testing.T) {
		s, _ := newTestStore(t, zeroDelay)
		n := create(t, s)
		r, cev, cerr := s.CancelNotification(ctxT(t), n.ID)
		c, ev, err := s.Claim(ctxT(t), n.ID, testLease, testProvider)
		if raceWinner(t, s, n.ID, cancelResult{r, cev, cerr}, claimResult{c, ev, err}) {
			t.Fatal("the claim won although the cancel ran first")
		}
	})

	// Both statements wait on a row lock held by another transaction, in a
	// known order; Postgres grants the row to the first waiter. The second
	// re-evaluates its guard after the first has committed.
	for _, claimFirst := range []bool{true, false} {
		name := "both waiting, cancel queued first"
		if claimFirst {
			name = "both waiting, claim queued first"
		}
		t.Run(name, func(t *testing.T) {
			s, pool := newTestStore(t, zeroDelay)
			n := create(t, s)
			ctx := ctxT(t)
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback(context.Background()) }()
			if _, err := tx.Exec(ctx, `SELECT 1 FROM notifications WHERE id = $1 FOR UPDATE`, n.ID); err != nil {
				t.Fatal(err)
			}
			var (
				claimDone  <-chan claimResult
				cancelDone <-chan cancelResult
			)
			startClaimWaiting := func() {
				claimDone = startClaim(ctx, s, n.ID)
				testutil.Eventually(t, func() bool { return claimLockWaiters(ctx, t, pool) == 1 }, lockWaitTimeout)
			}
			startCancelWaiting := func() {
				cancelDone = startCancel(ctx, s, n.ID)
				testutil.Eventually(t, func() bool { return cancelLockWaiters(ctx, t, pool) == 1 }, lockWaitTimeout)
			}
			if claimFirst {
				startClaimWaiting()
				startCancelWaiting()
			} else {
				startCancelWaiting()
				startClaimWaiting()
			}
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			if got := raceWinner(t, s, n.ID, receiveCancel(t, cancelDone), receive(t, claimDone)); got != claimFirst {
				t.Fatalf("claim won %v, want %v (the first waiter wins)", got, claimFirst)
			}
		})
	}

	// Started together, many rounds. Both orders must occur, so the test
	// shows the guards deciding a real race, not one fixed order. The cancel
	// opens a transaction first (one more round trip than the one-statement
	// claim), so left alone the claim nearly always wins; in two rounds out
	// of three the claim first makes one or two round trips of its own
	// (SELECT 1), which moves the start of the two statements across each
	// other.
	t.Run("concurrent", func(t *testing.T) {
		s, pool := newTestStore(t, zeroDelay)
		var claimWins, cancelWins int
		for round := range raceRounds {
			n := create(t, s)
			start := make(chan struct{})
			var (
				wg       sync.WaitGroup
				c        cancelResult
				cl       claimResult
				delayErr error
			)
			wg.Go(func() {
				<-start
				r, ev, err := s.CancelNotification(context.Background(), n.ID)
				c = cancelResult{r, ev, err}
			})
			wg.Go(func() {
				<-start
				for range round % 3 {
					if _, err := pool.Exec(context.Background(), `SELECT 1`); err != nil {
						delayErr = err
					}
				}
				r, ev, err := s.Claim(context.Background(), n.ID, testLease, testProvider)
				cl = claimResult{r, ev, err}
			})
			close(start)
			wg.Wait()
			if delayErr != nil {
				t.Fatal(delayErr)
			}
			if raceWinner(t, s, n.ID, c, cl) {
				claimWins++
			} else {
				cancelWins++
			}
		}
		t.Logf("%d rounds: the claim won %d, the cancel won %d", raceRounds, claimWins, cancelWins)
		if claimWins == 0 || cancelWins == 0 {
			t.Errorf("only one order occurred (claim %d, cancel %d)", claimWins, cancelWins)
		}
	})
}

func TestCancel_FromEachState(t *testing.T) {
	cases := []struct {
		name    string
		setup   func(t *testing.T, s *Store, id uuid.UUID)
		applied bool
		refused domain.Status
	}{
		{"pending", nil, true, ""},
		{"queued", func(t *testing.T, s *Store, _ uuid.UUID) {
			if _, err := s.PublishLane(ctxT(t), smsNormal, 10, publishAll); err != nil {
				t.Fatal(err)
			}
		}, true, ""},
		{"retrying", func(t *testing.T, s *Store, id uuid.UUID) {
			claim(t, s, id)
			if _, err := s.CompleteFailure(ctxT(t), id, 1, AttemptResult{Outcome: domain.OutcomeRetryable}, 0); err != nil {
				t.Fatal(err)
			}
		}, true, ""},
		{"processing", func(t *testing.T, s *Store, id uuid.UUID) { claim(t, s, id) }, false, domain.StatusProcessing},
		{"sent", func(t *testing.T, s *Store, id uuid.UUID) {
			claim(t, s, id)
			if _, _, err := s.CompleteSent(ctxT(t), id, 1, testProvider, AttemptResult{Outcome: domain.OutcomeSuccess}); err != nil {
				t.Fatal(err)
			}
		}, false, domain.StatusSent},
		{"failed", failOnce, false, domain.StatusFailed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := newTestStore(t, zeroDelay)
			maxFailures := 5
			if tc.name == "failed" {
				maxFailures = 1
			}
			n := create(t, s, withMaxFailures(maxFailures))
			if tc.setup != nil {
				tc.setup(t, s, n.ID)
			}
			before := get(t, s, n.ID)
			res, ev, err := s.CancelNotification(ctxT(t), n.ID)
			after := get(t, s, n.ID)
			if !tc.applied {
				r, ok := refusal(err)
				if !ok || r.Refusal != RefusalNotCancellable || r.Current != tc.refused {
					t.Fatalf("err %v, want not_cancellable from %s", err, tc.refused)
				}
				if !reflect.DeepEqual(before, after) {
					t.Errorf("row changed:\n%+v\n%+v", before, after)
				}
				return
			}
			if err != nil || !res.Applied || res.MayHaveBeenSent {
				t.Fatalf("result %+v err %v", res, err)
			}
			if !reflect.DeepEqual(res.Notification, after) {
				t.Errorf("returned row %+v, stored %+v", res.Notification, after)
			}
			checkTransition(t, before, after, ev, domain.StatusCancelled)

			// Repeated: the current row, no event, nothing changed.
			res2, ev2, err := s.CancelNotification(ctxT(t), n.ID)
			if err != nil || res2.Applied || ev2 != (domain.StatusChanged{}) {
				t.Fatalf("repeat: result %+v event %+v err %v", res2, ev2, err)
			}
			if !reflect.DeepEqual(res2.Notification, get(t, s, n.ID)) || res2.Notification.Version != after.Version {
				t.Errorf("repeat changed the row: %+v", res2.Notification)
			}
		})
	}
}

func TestCancel_UnknownID(t *testing.T) {
	s, _ := newTestStore(t, zeroDelay)
	if _, _, err := s.CancelNotification(ctxT(t), uuid.New()); !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

// The flag reads the attempts: an unknown outcome produced by the real lease
// sweeper sets it, and the cancel leaves the attempt as it was.
func TestCancel_MayHaveBeenSent(t *testing.T) {
	s, pool := newTestStore(t, zeroDelay)
	n := create(t, s)
	claim(t, s, n.ID)
	expireLease(t, pool, n.ID)
	if evs, err := s.SweepExpiredLeases(ctxT(t), 10, zeroJitter); err != nil || len(evs) != 1 {
		t.Fatalf("sweep: %+v %v", evs, err)
	}
	beforeAttempts := attempts(t, s, n.ID)
	if len(beforeAttempts) != 1 || beforeAttempts[0].Outcome != domain.OutcomeUnknown {
		t.Fatalf("attempts %+v", beforeAttempts)
	}
	res, _, err := s.CancelNotification(ctxT(t), n.ID)
	if err != nil || !res.Applied || !res.MayHaveBeenSent {
		t.Fatalf("result %+v err %v", res, err)
	}
	if got := attempts(t, s, n.ID); !reflect.DeepEqual(got, beforeAttempts) {
		t.Errorf("attempts changed: %+v -> %+v", beforeAttempts, got)
	}
	res, _, err = s.CancelNotification(ctxT(t), n.ID)
	if err != nil || res.Applied || !res.MayHaveBeenSent {
		t.Errorf("repeat: result %+v err %v", res, err)
	}
}

// insertBatch writes a batches row for notifications created in the test.
func insertBatch(t *testing.T, pool *pgxpool.Pool) uuid.UUID {
	t.Helper()
	id := uuid.Must(uuid.NewV7())
	exec(t, pool, `INSERT INTO batches (id, total, accepted, rejected, correlation_id) VALUES ($1, 1, 1, 0, 'c')`, id)
	return id
}

func TestCancelBatch(t *testing.T) {
	s, pool := newTestStore(t, zeroDelay)
	batchID := insertBatch(t, pool)
	inBatch := func(n *NewNotification) { n.BatchID = &batchID }
	pending := create(t, s, inBatch)
	processing := create(t, s, inBatch)
	claim(t, s, processing.ID)
	sent := create(t, s, inBatch)
	claim(t, s, sent.ID)
	if _, _, err := s.CompleteSent(ctxT(t), sent.ID, 1, testProvider, AttemptResult{Outcome: domain.OutcomeSuccess}); err != nil {
		t.Fatal(err)
	}
	unknown := create(t, s, inBatch)
	claim(t, s, unknown.ID)
	expireLease(t, pool, unknown.ID)
	if evs, err := s.SweepExpiredLeases(ctxT(t), 10, zeroJitter); err != nil || len(evs) != 1 {
		t.Fatalf("sweep: %+v %v", evs, err)
	}
	other := create(t, s) // not in the batch

	got, evs, err := s.CancelBatch(ctxT(t), batchID)
	if err != nil {
		t.Fatal(err)
	}
	want := CancelledBatch{Cancelled: 2, NotCancellable: 2, MayHaveBeenSent: 1}
	if got != want {
		t.Errorf("counts %+v, want %+v", got, want)
	}
	ids := map[uuid.UUID]bool{}
	for _, ev := range evs {
		ids[ev.ID] = true
		if ev.Status != domain.StatusCancelled || !reflect.DeepEqual(ev, get(t, s, ev.ID).Event()) {
			t.Errorf("event %+v", ev)
		}
	}
	if len(evs) != 2 || !ids[pending.ID] || !ids[unknown.ID] {
		t.Errorf("events for %v, want the pending and the unknown one", ids)
	}
	if get(t, s, other.ID).Status != domain.StatusPending {
		t.Error("a notification outside the batch was cancelled")
	}

	again, evs, err := s.CancelBatch(ctxT(t), batchID)
	if err != nil || again != want || len(evs) != 0 {
		t.Errorf("repeat: counts %+v events %d err %v", again, len(evs), err)
	}

	if _, _, err := s.CancelBatch(ctxT(t), uuid.New()); !errors.Is(err, ErrBatchNotFound) {
		t.Errorf("unknown batch: err %v", err)
	}
	empty := insertBatch(t, pool)
	if got, evs, err := s.CancelBatch(ctxT(t), empty); err != nil || got != (CancelledBatch{}) || len(evs) != 0 {
		t.Errorf("batch without notifications: %+v %d %v", got, len(evs), err)
	}
}

// failExpired makes n failed/expired through the claim's expiry branch,
// after one real attempt, so attempt_count is 1.
func failExpired(t *testing.T, s *Store, pool *pgxpool.Pool, id uuid.UUID) {
	t.Helper()
	claim(t, s, id)
	if _, err := s.CompleteFailure(ctxT(t), id, 1, AttemptResult{Outcome: domain.OutcomeRetryable}, 0); err != nil {
		t.Fatal(err)
	}
	expire(t, pool, id)
	if c := claim(t, s, id); !c.Expired {
		t.Fatalf("claim did not expire the row: %+v", c.Notification.Status)
	}
}

func TestRedrive_Refusals(t *testing.T) {
	s, pool := newTestStore(t, zeroDelay)
	soon := time.Now().Add(1000 * time.Millisecond)
	later := time.Now().Add(3_600_000 * time.Millisecond)

	expired := create(t, s)
	failExpired(t, s, pool, expired.ID)
	rejected := create(t, s, withMaxFailures(1))
	failOnce(t, s, rejected.ID)
	pending := create(t, s)

	cases := []struct {
		name    string
		id      uuid.UUID
		exp     *time.Time
		refusal Refusal
		current domain.Status
	}{
		{"expired without expires_at", expired.ID, nil, RefusalExpiresAtRequired, domain.StatusFailed},
		{"expired, new one too soon", expired.ID, &soon, RefusalTooSoon, domain.StatusFailed},
		{"not expired, expires_at given", rejected.ID, &later, RefusalExpiresAtNotAllowed, domain.StatusFailed},
		{"pending", pending.ID, nil, RefusalNotFailed, domain.StatusPending},
		{"pending with expires_at", pending.ID, &later, RefusalNotFailed, domain.StatusPending},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := get(t, s, tc.id)
			_, _, err := s.RedriveNotification(ctxT(t), tc.id, tc.exp, testMinLead)
			r, ok := refusal(err)
			if !ok || r.Refusal != tc.refusal || r.Current != tc.current {
				t.Fatalf("err %v, want %s from %s", err, tc.refusal, tc.current)
			}
			// D16: only the refusal of an unneeded expires_at carries the
			// stored value.
			wantStored := time.Time{}
			if tc.refusal == RefusalExpiresAtNotAllowed {
				wantStored = before.ExpiresAt
			}
			if !r.StoredExpiresAt.Equal(wantStored) {
				t.Errorf("stored expires_at %v, want %v", r.StoredExpiresAt, wantStored)
			}
			if after := get(t, s, tc.id); !reflect.DeepEqual(before, after) {
				t.Errorf("row changed:\n%+v\n%+v", before, after)
			}
		})
	}
	if _, _, err := s.RedriveNotification(ctxT(t), uuid.New(), nil, testMinLead); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown id: err %v", err)
	}
}

// Transitions 9 and 11 obey the rules of every transition (version + 1,
// updated_at, the event matches the row, no immutable column changes), and a
// cancel changes nothing but status, version and updated_at.
func TestCancelRedrive_TransitionInvariants(t *testing.T) {
	newExp := time.Now().Add(3_600_000 * time.Millisecond)
	cases := []struct {
		name  string
		setup func(t *testing.T, s *Store, pool *pgxpool.Pool, id uuid.UUID)
		act   func(t *testing.T, s *Store, id uuid.UUID) domain.StatusChanged
		want  domain.Status
	}{
		{"9 pending to cancelled", nil, cancelAct, domain.StatusCancelled},
		{"9 queued to cancelled", func(t *testing.T, s *Store, _ *pgxpool.Pool, _ uuid.UUID) {
			if _, err := s.PublishLane(ctxT(t), smsNormal, 10, publishAll); err != nil {
				t.Fatal(err)
			}
		}, cancelAct, domain.StatusCancelled},
		{"9 retrying to cancelled", func(t *testing.T, s *Store, _ *pgxpool.Pool, id uuid.UUID) {
			claim(t, s, id)
			if _, err := s.CompleteFailure(ctxT(t), id, 1, AttemptResult{Outcome: domain.OutcomeRetryable}, 0); err != nil {
				t.Fatal(err)
			}
		}, cancelAct, domain.StatusCancelled},
		{"11 failed (expired) to pending", failExpired, func(t *testing.T, s *Store, id uuid.UUID) domain.StatusChanged {
			n, ev, err := s.RedriveNotification(ctxT(t), id, &newExp, testMinLead)
			if err != nil {
				t.Fatal(err)
			}
			if !n.ExpiresAt.Equal(newExp.Truncate(time.Microsecond)) {
				t.Errorf("expires_at %v, want %v", n.ExpiresAt, newExp)
			}
			return ev
		}, domain.StatusPending},
		{"11 failed (budget) to pending", func(t *testing.T, s *Store, _ *pgxpool.Pool, id uuid.UUID) {
			failOnce(t, s, id)
		}, func(t *testing.T, s *Store, id uuid.UUID) domain.StatusChanged {
			_, ev, err := s.RedriveNotification(ctxT(t), id, nil, testMinLead)
			if err != nil {
				t.Fatal(err)
			}
			return ev
		}, domain.StatusPending},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, pool := newTestStore(t, zeroDelay)
			batchID := insertBatch(t, pool)
			maxFailures := 5
			if tc.name == "11 failed (budget) to pending" {
				maxFailures = 1
			}
			n := create(t, s, withMaxFailures(maxFailures), func(n *NewNotification) {
				n.BatchID = &batchID
				n.DedupeKey = ptr("dk-" + tc.name)
				n.TemplateName = ptr("otp")
				n.TemplateVersion = ptr(int32(2))
				n.TemplateVars = []byte(`{"code": "9"}`)
				n.ScheduledAt = ptr(time.Now().Add(-1000 * time.Millisecond)) // already due
			})
			if tc.setup != nil {
				tc.setup(t, s, pool, n.ID)
			}
			before := get(t, s, n.ID)
			beforeAttempts := attempts(t, s, n.ID)
			ev := tc.act(t, s, n.ID)
			after := get(t, s, n.ID)
			checkTransition(t, before, after, ev, tc.want)
			if got := attempts(t, s, n.ID); !reflect.DeepEqual(got, beforeAttempts) {
				t.Errorf("attempts changed: %+v -> %+v", beforeAttempts, got)
			}
			switch tc.want {
			case domain.StatusCancelled:
				b, a := before, after
				b.Status, b.Version, b.UpdatedAt = "", 0, time.Time{}
				a.Status, a.Version, a.UpdatedAt = "", 0, time.Time{}
				if !reflect.DeepEqual(b, a) {
					t.Errorf("cancel changed more than status, version, updated_at:\n%+v\n%+v", before, after)
				}
			case domain.StatusPending:
				if after.Failures != 0 || after.FailureReason != nil || after.QueuedAt != nil ||
					!after.DueAt.Equal(after.UpdatedAt) || after.AttemptCount != before.AttemptCount ||
					after.MaxFailures != before.MaxFailures || after.LeaseExpiresAt != nil {
					t.Errorf("redriven row %+v (before %+v)", after, before)
				}
				if tc.name == "11 failed (budget) to pending" && !after.ExpiresAt.Equal(before.ExpiresAt) {
					t.Errorf("expires_at %v, want the stored %v", after.ExpiresAt, before.ExpiresAt)
				}
			}
		})
	}
}

// failOnce fails a notification created with a failure budget of 1: one
// attempt that fails, with the stored expires_at still in the future.
func failOnce(t *testing.T, s *Store, id uuid.UUID) {
	t.Helper()
	claim(t, s, id)
	ev, err := s.CompleteFailure(ctxT(t), id, 1, AttemptResult{Outcome: domain.OutcomePermanent}, 0)
	if err != nil || ev.Status != domain.StatusFailed {
		t.Fatalf("fail once: %+v %v", ev, err)
	}
}

func cancelAct(t *testing.T, s *Store, id uuid.UUID) domain.StatusChanged {
	t.Helper()
	res, ev, err := s.CancelNotification(ctxT(t), id)
	if err != nil || !res.Applied {
		t.Fatalf("cancel: %+v %v", res, err)
	}
	return ev
}

// After a redrive the next claim opens attempt attempt_count + 1; after a
// redrive whose expiry then passes, the claim fails the row again without an
// attempt.
func TestRedrive_NextClaim(t *testing.T) {
	s, pool := newTestStore(t, zeroDelay)
	newExp := time.Now().Add(3_600_000 * time.Millisecond)

	n := create(t, s)
	failExpired(t, s, pool, n.ID)
	if _, _, err := s.RedriveNotification(ctxT(t), n.ID, &newExp, testMinLead); err != nil {
		t.Fatal(err)
	}
	c := claim(t, s, n.ID)
	if c.Expired || c.Notification.AttemptCount != 2 {
		t.Fatalf("claim after redrive: expired %v attempt_count %d", c.Expired, c.Notification.AttemptCount)
	}
	if a := attempts(t, s, n.ID); len(a) != 2 || a[1].AttemptNo != 2 || a[1].Outcome != domain.OutcomeInFlight {
		t.Errorf("attempts %+v", a)
	}

	m := create(t, s)
	failExpired(t, s, pool, m.ID)
	if _, _, err := s.RedriveNotification(ctxT(t), m.ID, &newExp, testMinLead); err != nil {
		t.Fatal(err)
	}
	expire(t, pool, m.ID)
	c = claim(t, s, m.ID)
	got := get(t, s, m.ID)
	if !c.Expired || got.Status != domain.StatusFailed || got.FailureReason == nil ||
		*got.FailureReason != domain.ReasonExpired || got.AttemptCount != 1 {
		t.Errorf("claim after redrive and expiry: expired %v row %+v", c.Expired, got)
	}
	if a := attempts(t, s, m.ID); len(a) != 1 {
		t.Errorf("attempts %+v, want the first one only", a)
	}
}
