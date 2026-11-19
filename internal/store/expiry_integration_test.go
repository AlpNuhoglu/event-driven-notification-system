//go:build integration

package store

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store/db"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

// expireBy back-dates expires_at to d before now.
func expireBy(t *testing.T, pool *pgxpool.Pool, id uuid.UUID, d time.Duration) {
	t.Helper()
	exec(t, pool, `UPDATE notifications SET expires_at = now() - $2::interval WHERE id = $1`, id, interval(d))
}

// requireExpiredOnce asserts that the row is failed/expired, that it changed
// exactly once since before, and that its attempt count, failures and
// attempt rows are unchanged.
func requireExpiredOnce(t *testing.T, s *Store, before Notification, beforeAttempts []Attempt) {
	t.Helper()
	after := get(t, s, before.ID)
	if after.Status != domain.StatusFailed || after.FailureReason == nil || *after.FailureReason != domain.ReasonExpired {
		t.Fatalf("row is %s/%v, want failed/expired", after.Status, after.FailureReason)
	}
	if after.Version != before.Version+1 {
		t.Errorf("version %d -> %d, want one transition", before.Version, after.Version)
	}
	if after.AttemptCount != before.AttemptCount || after.Failures != before.Failures {
		t.Errorf("attempt_count %d -> %d, failures %d -> %d", before.AttemptCount, after.AttemptCount, before.Failures, after.Failures)
	}
	if got := attempts(t, s, before.ID); len(got) != len(beforeAttempts) ||
		(len(got) > 0 && !reflect.DeepEqual(got, beforeAttempts)) {
		t.Errorf("attempts changed: %+v -> %+v", beforeAttempts, got)
	}
}

// failedEventsOf counts the events of id that report failed.
func failedEventsOf(id uuid.UUID, evs ...domain.StatusChanged) int {
	n := 0
	for _, e := range evs {
		if e.ID == id && e.Status == domain.StatusFailed {
			n++
		}
	}
	return n
}

// TestExpiry_VersusClaim: the relay's expiry pass (transition 10) and a
// worker's claim (transition 3, whose expiry branch fails the row itself) on
// the same expired row. Exactly one of them fails the row, which ends
// failed/expired with attempt_count unchanged and no attempt row, and
// exactly one event exists for the transition.
func TestExpiry_VersusClaim(t *testing.T) {
	t.Run("pass then claim", func(t *testing.T) {
		s, pool := newTestStore(t, zeroDelay)
		n := create(t, s)
		expire(t, pool, n.ID)
		before := get(t, s, n.ID)

		evs, err := s.ExpireDue(ctxT(t), 10)
		if err != nil || len(evs) != 1 {
			t.Fatalf("pass: %+v %v", evs, err)
		}
		if _, _, err := s.Claim(ctxT(t), n.ID, testLease, testProvider); !errors.Is(err, ErrNotApplied) {
			t.Errorf("claim after the pass: %v, want ErrNotApplied", err)
		}
		requireExpiredOnce(t, s, before, nil)
		if !reflect.DeepEqual(evs[0], get(t, s, n.ID).Event()) {
			t.Errorf("event %+v does not match the row", evs[0])
		}
	})

	t.Run("claim then pass", func(t *testing.T) {
		s, pool := newTestStore(t, zeroDelay)
		n := create(t, s)
		expire(t, pool, n.ID)
		before := get(t, s, n.ID)

		c, ev, err := s.Claim(ctxT(t), n.ID, testLease, testProvider)
		if err != nil || !c.Expired {
			t.Fatalf("claim: expired %v err %v", c.Expired, err)
		}
		evs, err := s.ExpireDue(ctxT(t), 10)
		if err != nil || len(evs) != 0 {
			t.Errorf("pass after the claim: %+v %v, want no event", evs, err)
		}
		requireExpiredOnce(t, s, before, nil)
		if failedEventsOf(n.ID, append(evs, ev)...) != 1 {
			t.Errorf("events %+v %+v, want exactly one", ev, evs)
		}
	})

	t.Run("concurrently", func(t *testing.T) {
		const rounds = 25
		s, pool := newTestStore(t, zeroDelay)
		for round := range rounds {
			n := create(t, s)
			if round%3 == 1 {
				if _, err := s.PublishLane(ctxT(t), smsNormal, 10, publishAll); err != nil {
					t.Fatal(err)
				}
			}
			expire(t, pool, n.ID)
			before := get(t, s, n.ID)

			start := make(chan struct{})
			var passEvs []domain.StatusChanged
			var passErr, claimErr error
			var claimEv domain.StatusChanged
			var wg sync.WaitGroup
			wg.Go(func() {
				<-start
				passEvs, passErr = s.ExpireDue(context.Background(), 10)
			})
			wg.Go(func() {
				<-start
				_, claimEv, claimErr = s.Claim(context.Background(), n.ID, testLease, testProvider)
			})
			close(start)
			wg.Wait()

			if passErr != nil {
				t.Fatalf("round %d: pass: %v", round, passErr)
			}
			evs := passEvs
			switch {
			case claimErr == nil:
				evs = append(evs, claimEv)
			case errors.Is(claimErr, ErrNotApplied):
			default:
				t.Fatalf("round %d: claim: %v", round, claimErr)
			}
			if got := failedEventsOf(n.ID, evs...); got != 1 || len(evs) != 1 {
				t.Fatalf("round %d: events %+v, want exactly one failed event", round, evs)
			}
			requireExpiredOnce(t, s, before, nil)
			if !reflect.DeepEqual(evs[0], get(t, s, n.ID).Event()) {
				t.Fatalf("round %d: event %+v does not match the row", round, evs[0])
			}
		}
	})

	// The pass holds the row lock in an open transaction: the claim waits for
	// it and, after the commit, finds failed and affects zero rows.
	t.Run("claim waits for the pass", func(t *testing.T) {
		s, pool := newTestStore(t, zeroDelay)
		n := create(t, s)
		expire(t, pool, n.ID)
		before := get(t, s, n.ID)
		ctx := ctxT(t)

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(context.Background()) }()
		rows, err := db.New(tx).ExpireDue(ctx, 10)
		if err != nil || len(rows) != 1 {
			t.Fatalf("pass in a transaction: %d rows %v", len(rows), err)
		}
		done := startClaim(ctx, s, n.ID)
		testutil.Eventually(t, func() bool { return claimLockWaiters(ctx, t, pool) == 1 }, lockWaitTimeout)
		select {
		case r := <-done:
			t.Fatalf("claim returned before the pass committed: %v", r.err)
		default:
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if r := receive(t, done); !errors.Is(r.err, ErrNotApplied) {
			t.Errorf("claim after the pass: %v, want ErrNotApplied", r.err)
		}
		requireExpiredOnce(t, s, before, nil)
	})

	// A claim holds the row lock in an open transaction: the pass skips the
	// row without waiting and fails the unlocked one; the claim's expiry
	// branch then fails the locked row itself.
	t.Run("pass skips a row a claim holds", func(t *testing.T) {
		s, pool := newTestStore(t, zeroDelay)
		held := create(t, s)
		free := create(t, s)
		expire(t, pool, held.ID)
		expireBy(t, pool, free.ID, 1000*time.Millisecond) // older: first in order
		before := get(t, s, held.ID)
		ctx := ctxT(t)

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(context.Background()) }()
		row, err := db.New(tx).ClaimNotification(ctx, db.ClaimNotificationParams{
			Lease: interval(testLease), ID: held.ID, Provider: testProvider,
		})
		if err != nil || row.Status != string(domain.StatusFailed) {
			t.Fatalf("claim in a transaction: %s %v", row.Status, err)
		}
		evs, err := s.ExpireDue(ctx, 10)
		if err != nil {
			t.Fatalf("pass while a claim holds a row: %v", err)
		}
		if len(evs) != 1 || evs[0].ID != free.ID {
			t.Fatalf("pass events %+v, want only the unlocked row", evs)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		requireExpiredOnce(t, s, before, nil)
		if evs, err := s.ExpireDue(ctx, 10); err != nil || len(evs) != 0 {
			t.Errorf("pass after the claim's commit: %+v %v, want nothing", evs, err)
		}
	})

	// The relay's lane transaction holds the row while it publishes: the pass
	// skips it; once it is queued, the next pass fails it.
	t.Run("pass skips a row the relay is publishing", func(t *testing.T) {
		s, pool := newTestStore(t, zeroDelay)
		n := create(t, s)
		expire(t, pool, n.ID)
		var during []domain.StatusChanged
		_, err := s.PublishLane(ctxT(t), smsNormal, 10, func(ctx context.Context, rows []DueRow) ([]uuid.UUID, error) {
			evs, err := s.ExpireDue(ctx, 10)
			during = evs
			if err != nil {
				return nil, err
			}
			return publishAll(ctx, rows)
		})
		if err != nil {
			t.Fatalf("publish lane: %v", err)
		}
		if len(during) != 0 {
			t.Errorf("pass during the lane transaction: %+v, want nothing", during)
		}
		before := get(t, s, n.ID)
		if before.Status != domain.StatusQueued {
			t.Fatalf("row is %s, want queued", before.Status)
		}
		evs, err := s.ExpireDue(ctxT(t), 10)
		if err != nil || len(evs) != 1 {
			t.Fatalf("pass after the commit: %+v %v", evs, err)
		}
		requireExpiredOnce(t, s, before, nil)
	})

	// A row a worker holds in processing is not the pass's: its expiry is
	// decided by the worker's final write or by the lease sweeper.
	t.Run("processing neighbour untouched", func(t *testing.T) {
		s, pool := newTestStore(t, zeroDelay)
		processing := create(t, s)
		claim(t, s, processing.ID)
		expire(t, pool, processing.ID)
		neighbour := create(t, s)
		expire(t, pool, neighbour.ID)
		before := get(t, s, processing.ID)
		beforeAttempts := attempts(t, s, processing.ID)

		evs, err := s.ExpireDue(ctxT(t), 10)
		if err != nil || len(evs) != 1 || evs[0].ID != neighbour.ID {
			t.Fatalf("pass: %+v %v, want the neighbour only", evs, err)
		}
		if after := get(t, s, processing.ID); !reflect.DeepEqual(after, before) {
			t.Errorf("processing row changed:\n%+v\n%+v", before, after)
		}
		if got := attempts(t, s, processing.ID); !reflect.DeepEqual(got, beforeAttempts) {
			t.Errorf("attempts changed")
		}
	})
}

// The pass takes at most limit rows, oldest expires_at first, and leaves
// rows that have not expired.
func TestExpireDue_LimitAndOrder(t *testing.T) {
	s, pool := newTestStore(t, zeroDelay)
	// Created in one order, expired in the reverse one.
	var byAge []uuid.UUID // oldest expires_at first
	ids := make([]uuid.UUID, 5)
	for i := range ids {
		ids[i] = create(t, s).ID
	}
	for i, id := range ids {
		expireBy(t, pool, id, time.Duration(i+1)*1000*time.Millisecond)
	}
	for i := len(ids) - 1; i >= 0; i-- {
		byAge = append(byAge, ids[i])
	}
	live := create(t, s)
	scheduled := create(t, s, func(n *NewNotification) { n.ScheduledAt = ptr(time.Now().Add(time.Hour)) })

	var got []uuid.UUID
	for _, want := range []int{2, 2, 1, 0} {
		evs, err := s.ExpireDue(ctxT(t), 2)
		if err != nil || len(evs) != want {
			t.Fatalf("pass: %d events %v, want %d", len(evs), err, want)
		}
		batch := make(map[uuid.UUID]bool, len(evs))
		for _, e := range evs {
			batch[e.ID] = true
		}
		for _, id := range byAge[len(got) : len(got)+want] {
			if !batch[id] {
				t.Errorf("batch %v misses %s: not the oldest expires_at first", evs, id)
			}
		}
		for _, e := range evs {
			got = append(got, e.ID)
		}
	}
	for _, n := range []Notification{live, scheduled} {
		if after := get(t, s, n.ID); after.Status != domain.StatusPending || after.Version != n.Version {
			t.Errorf("unexpired row %s became %s", n.ID, after.Status)
		}
	}
}

// Rows in processing, sent, failed and cancelled are never touched, however
// long ago they expired.
func TestExpireDue_OtherStatusesUntouched(t *testing.T) {
	s, pool := newTestStore(t, zeroDelay)
	processing := create(t, s)
	claim(t, s, processing.ID)
	sent := create(t, s)
	claim(t, s, sent.ID)
	if _, err := s.CompleteSent(ctxT(t), sent.ID, 1, testProvider, AttemptResult{Outcome: domain.OutcomeSuccess}); err != nil {
		t.Fatal(err)
	}
	failed := create(t, s, withMaxFailures(1))
	claim(t, s, failed.ID)
	if _, err := s.CompleteFailure(ctxT(t), failed.ID, 1, AttemptResult{Outcome: domain.OutcomePermanent}, 0); err != nil {
		t.Fatal(err)
	}
	cancelled := create(t, s)
	if res, _, err := s.CancelNotification(ctxT(t), cancelled.ID); err != nil || !res.Applied {
		t.Fatalf("cancel: %+v %v", res, err)
	}
	others := []uuid.UUID{processing.ID, sent.ID, failed.ID, cancelled.ID}
	before := make(map[uuid.UUID]Notification, len(others))
	for _, id := range others {
		expireBy(t, pool, id, 3_600_000*time.Millisecond)
		before[id] = get(t, s, id)
	}

	evs, err := s.ExpireDue(ctxT(t), 100)
	if err != nil || len(evs) != 0 {
		t.Fatalf("pass: %+v %v, want nothing", evs, err)
	}
	for _, id := range others {
		if after := get(t, s, id); !reflect.DeepEqual(after, before[id]) {
			t.Errorf("%s row changed:\n%+v\n%+v", before[id].Status, before[id], after)
		}
	}
}

func TestExpireDue_InvalidLimit(t *testing.T) {
	s, _ := newTestStore(t, zeroDelay)
	for _, limit := range []int{0, -1, 1 << 40} {
		if _, err := s.ExpireDue(ctxT(t), limit); err == nil {
			t.Errorf("limit %d accepted", limit)
		}
	}
}

// expireKey back-dates an idempotency key's expires_at to d before now.
func expireKey(t *testing.T, pool *pgxpool.Pool, key string, d time.Duration) {
	t.Helper()
	exec(t, pool, `UPDATE idempotency_keys SET expires_at = now() - $2::interval WHERE key = $1`, key, interval(d))
}

func keyExists(t *testing.T, pool *pgxpool.Pool, key string) bool {
	t.Helper()
	return count(t, pool, `SELECT count(*) FROM idempotency_keys WHERE key = $1`, key) == 1
}

// D78: an expired key is deleted, a live key is kept; at most
// IdempotencyCleanupBatch keys per call, oldest expires_at first.
func TestDeleteExpiredIdempotencyKeys(t *testing.T) {
	s, pool := newTestStore(t, zeroDelay)
	createSingle(t, s, idemKey("key-expired", "hash-a"), baseNew())
	createSingle(t, s, idemKey("key-live", "hash-b"), baseNew())
	expireKey(t, pool, "key-expired", 1*time.Millisecond)

	n, err := s.DeleteExpiredIdempotencyKeys(ctxT(t))
	if err != nil || n != 1 {
		t.Fatalf("delete: %d %v, want 1", n, err)
	}
	if keyExists(t, pool, "key-expired") || !keyExists(t, pool, "key-live") {
		t.Errorf("expired key kept or live key deleted")
	}

	// More than one batch: the oldest IdempotencyCleanupBatch first.
	extra := 7
	exec(t, pool, `
		INSERT INTO idempotency_keys (key, request_hash, created_at, expires_at)
		SELECT 'bulk-' || g, 'h', now() - interval '2 days', now() - g * interval '1 second'
		FROM generate_series(1, $1::int) AS g`, IdempotencyCleanupBatch+extra)
	n, err = s.DeleteExpiredIdempotencyKeys(ctxT(t))
	if err != nil || n != IdempotencyCleanupBatch {
		t.Fatalf("first batch: %d %v, want %d", n, err, IdempotencyCleanupBatch)
	}
	// g = 1..extra are the youngest and remain.
	if got := count(t, pool, `SELECT count(*) FROM idempotency_keys WHERE key LIKE 'bulk-%'`); got != extra {
		t.Errorf("%d expired keys left, want %d", got, extra)
	}
	for g := 1; g <= extra; g++ {
		if !keyExists(t, pool, fmt.Sprintf("bulk-%d", g)) {
			t.Errorf("bulk-%d deleted before older keys", g)
		}
	}
	n, err = s.DeleteExpiredIdempotencyKeys(ctxT(t))
	if err != nil || n != extra {
		t.Fatalf("second batch: %d %v, want %d", n, err, extra)
	}
	if !keyExists(t, pool, "key-live") {
		t.Error("live key deleted")
	}
}

// D78: a concurrent take-over of an expired key succeeds whether the delete
// runs before or after it.
func TestDeleteExpiredIdempotencyKeys_VersusTakeOver(t *testing.T) {
	// setup stores a key with hash-old, back-dates it, and adds a second
	// expired key that nothing holds.
	setup := func(t *testing.T) (*Store, *pgxpool.Pool, *IdempotencyKey) {
		t.Helper()
		s, pool := newTestStore(t, zeroDelay)
		createSingle(t, s, idemKey("key-taken", "hash-old"), baseNew())
		createSingle(t, s, idemKey("key-other", "hash-other"), baseNew())
		expireKey(t, pool, "key-taken", 1*time.Millisecond)
		expireKey(t, pool, "key-other", 1*time.Millisecond)
		return s, pool, idemKey("key-taken", "hash-new")
	}
	requireTakenOver := func(t *testing.T, pool *pgxpool.Pool, r SingleResult) {
		t.Helper()
		if r.Outcome != OutcomeCreated || r.Response == nil {
			t.Fatalf("take-over = %+v, want created", r)
		}
		k := readKey(t, pool, "key-taken")
		if k.hash != "hash-new" || k.status == nil || *k.status != 202 || !jsonEqual(t, k.body, r.Response.Body) {
			t.Errorf("key after the take-over: %+v", k)
		}
		if !k.expiresAt.After(k.createdAt) {
			t.Errorf("key not live: created %v expires %v", k.createdAt, k.expiresAt)
		}
		if got := count(t, pool, `SELECT count(*) FROM notifications`); got != 3 {
			t.Errorf("%d notifications, want 3", got)
		}
	}

	// The delete's transaction holds the key: the take-over waits for it in
	// its claim statement, then inserts the key as new.
	t.Run("delete first, take-over waits", func(t *testing.T) {
		s, pool, key := setup(t)
		ctx := ctxT(t)
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(context.Background()) }()
		n, err := db.New(tx).DeleteExpiredIdempotencyKeys(ctx, IdempotencyCleanupBatch)
		if err != nil || n != 2 {
			t.Fatalf("delete in a transaction: %d %v", n, err)
		}
		done := startCreateSingle(ctx, s, key, baseNew())
		testutil.Eventually(t, func() bool { return stmtLockWaiters(ctx, t, pool, claimKeyStmt) == 1 }, lockWaitTimeout)
		select {
		case r := <-done:
			t.Fatalf("take-over returned before the delete committed: %+v %v", r.res, r.err)
		default:
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		r := receiveSingle(t, done)
		if r.err != nil {
			t.Fatalf("take-over: %v", r.err)
		}
		requireTakenOver(t, pool, r.res)
		if keyExists(t, pool, "key-other") {
			t.Error("the unheld expired key was not deleted")
		}
	})

	t.Run("delete committed, then take-over", func(t *testing.T) {
		s, pool, key := setup(t)
		if n, err := s.DeleteExpiredIdempotencyKeys(ctxT(t)); err != nil || n != 2 {
			t.Fatalf("delete: %d %v", n, err)
		}
		requireTakenOver(t, pool, createSingle(t, s, key, baseNew()))
	})

	t.Run("take-over committed, then delete", func(t *testing.T) {
		s, pool, key := setup(t)
		r := createSingle(t, s, key, baseNew())
		if n, err := s.DeleteExpiredIdempotencyKeys(ctxT(t)); err != nil || n != 1 {
			t.Fatalf("delete: %d %v, want only the other key", n, err)
		}
		requireTakenOver(t, pool, r)
	})

	// The take-over's transaction is open when the delete runs: the delete
	// skips the locked key without waiting, deletes the other one, and the
	// request completes normally.
	t.Run("take-over open, delete skips it", func(t *testing.T) {
		s, pool, key := setup(t)
		ctx := ctxT(t)
		r, err := s.CreateSingle(ctx, key, baseNew(), func(n Notification) (StoredResponse, error) {
			deleted, err := s.DeleteExpiredIdempotencyKeys(ctx)
			if err != nil {
				return StoredResponse{}, fmt.Errorf("delete during the take-over: %w", err)
			}
			if deleted != 1 {
				return StoredResponse{}, fmt.Errorf("delete during the take-over: %d keys, want 1", deleted)
			}
			return respond202(n)
		})
		if err != nil {
			t.Fatalf("take-over: %v", err)
		}
		requireTakenOver(t, pool, r)
		if keyExists(t, pool, "key-other") {
			t.Error("the unheld expired key was not deleted")
		}
		if n, err := s.DeleteExpiredIdempotencyKeys(ctx); err != nil || n != 0 {
			t.Errorf("delete after the take-over: %d %v, want 0", n, err)
		}
	})
}

// The expiry pass and the idempotency-key cleanup must never run as cached
// prepared statements: after many passes that found nothing, Postgres would
// switch to a generic plan that joins each batch against every expired row
// (design review log, section 6). The check is the listing's
// (TestList_RunsWithCustomPlans): neither statement appears in
// pg_prepared_statements, with GetNotification as the control that the check
// can see cached statements.
func TestExpiry_RunsWithCustomPlans(t *testing.T) {
	url := testutil.NewMigratedDatabase(t)
	pool, err := pgxpool.New(context.Background(), url+"&pool_max_conns=1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	s := New(pool, zeroDelay)
	n := create(t, s)

	for range 10 {
		if _, err := s.ExpireDue(ctxT(t), 10); err != nil {
			t.Fatal(err)
		}
		if _, err := s.DeleteExpiredIdempotencyKeys(ctxT(t)); err != nil {
			t.Fatal(err)
		}
	}
	count := func(like string) int {
		var c int
		if err := pool.QueryRow(ctxT(t), `SELECT count(*) FROM pg_prepared_statements WHERE statement LIKE $1`, like).Scan(&c); err != nil {
			t.Fatal(err)
		}
		return c
	}
	if c := count(`%WITH due AS%`); c != 0 {
		t.Errorf("the expiry statement is prepared on the connection (%d), want 0", c)
	}
	if c := count(`%WITH doomed AS%`); c != 0 {
		t.Errorf("the key cleanup statement is prepared on the connection (%d), want 0", c)
	}

	// Control.
	for range 3 {
		get(t, s, n.ID)
	}
	if c := count(`%FROM notifications WHERE id = $1%`); c == 0 {
		t.Error("the control statement (GetNotification) is not in pg_prepared_statements: the check cannot see cached statements")
	}
}
