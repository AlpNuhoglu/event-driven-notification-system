//go:build integration

package store

import (
	"context"
	"maps"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store/db"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

// allLanes are the nine lanes (channel x priority).
func allLanes() []Lane {
	var out []Lane
	for _, c := range []domain.Channel{domain.ChannelSMS, domain.ChannelEmail, domain.ChannelPush} {
		for _, p := range []domain.Priority{domain.PriorityHigh, domain.PriorityNormal, domain.PriorityLow} {
			out = append(out, Lane{Channel: c, Priority: p})
		}
	}
	return out
}

// publishEveryLane runs PublishLane on every lane and returns the ids that
// became queued.
func publishEveryLane(t *testing.T, s *Store) map[uuid.UUID]bool {
	t.Helper()
	out := map[uuid.UUID]bool{}
	for _, l := range allLanes() {
		evs, err := s.PublishLane(ctxT(t), l, 1000, publishAll)
		if err != nil {
			t.Fatalf("publish %v: %v", l, err)
		}
		for _, e := range evs {
			if e.Status != domain.StatusQueued || e.Channel != l.Channel || e.Priority != l.Priority {
				t.Fatalf("publish event %+v on lane %v", e, l)
			}
			out[e.ID] = true
		}
	}
	return out
}

// reconcileLockWaiters counts backends of this test's database that are
// blocked on a lock while running the reconciliation statement.
func reconcileLockWaiters(ctx context.Context, t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	err := pool.QueryRow(ctx, `
		SELECT count(*) FROM pg_stat_activity
		WHERE datname = current_database()
		  AND pid <> pg_backend_pid()
		  AND wait_event_type = 'Lock'
		  AND query LIKE '%-- name: ReconcileQueued :many%'`).Scan(&n)
	if err != nil {
		t.Errorf("pg_stat_activity: %v", err)
		return -1
	}
	return n
}

type reconcileResult struct {
	evs []domain.StatusChanged
	err error
}

func startReconcile(ctx context.Context, s *Store) <-chan reconcileResult {
	done := make(chan reconcileResult, 1)
	go func() {
		evs, err := s.ReconcileQueued(ctx)
		done <- reconcileResult{evs, err}
	}()
	return done
}

func receiveReconcile(t *testing.T, done <-chan reconcileResult) reconcileResult {
	t.Helper()
	var r reconcileResult
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

// otherStatuses creates one row in each status other than queued, on the
// sms/normal lane, and returns them by status. Nothing is queued afterwards.
func otherStatuses(t *testing.T, s *Store, pool *pgxpool.Pool) map[domain.Status]uuid.UUID {
	t.Helper()
	out := map[domain.Status]uuid.UUID{}
	scheduled := func(n *NewNotification) { n.ScheduledAt = ptr(time.Now().Add(testDelay)) }

	pending := create(t, s, scheduled) // not due: never published
	out[domain.StatusPending] = pending.ID

	processing := create(t, s)
	claim(t, s, processing.ID)
	out[domain.StatusProcessing] = processing.ID

	retrying := create(t, s)
	claim(t, s, retrying.ID)
	if _, err := s.AbortAttempt(ctxT(t), retrying.ID, 1, testDelay); err != nil { // retrying, not due
		t.Fatal(err)
	}
	out[domain.StatusRetrying] = retrying.ID

	sent := create(t, s)
	claim(t, s, sent.ID)
	if _, _, err := s.CompleteSent(ctxT(t), sent.ID, 1, testProvider, AttemptResult{Outcome: domain.OutcomeSuccess}); err != nil {
		t.Fatal(err)
	}
	out[domain.StatusSent] = sent.ID

	failed := create(t, s)
	expire(t, pool, failed.ID)
	if _, err := s.ExpireDue(ctxT(t), 10); err != nil {
		t.Fatal(err)
	}
	out[domain.StatusFailed] = failed.ID

	cancelled := create(t, s, scheduled)
	if _, _, err := s.CancelNotification(ctxT(t), cancelled.ID); err != nil {
		t.Fatal(err)
	}
	out[domain.StatusCancelled] = cancelled.ID

	for st, id := range out {
		if got := get(t, s, id); got.Status != st {
			t.Fatalf("setup: row %s is %s, want %s", id, got.Status, st)
		}
	}
	return out
}

// Reconciliation returns exactly every queued row to pending: several
// hundred rows across all nine lanes, compared as an exact set, one event
// each, while the rows in every other status are untouched. The relay then
// publishes every one of them again. A reconciliation that skips a single
// queued row fails here.
func TestReconcile_EveryQueuedRowReturnsToPending(t *testing.T) {
	const perLane = 40
	s, pool := newTestStore(t, zeroDelay)
	others := otherStatuses(t, s, pool)
	othersBefore := map[uuid.UUID]Notification{}
	for _, id := range others {
		othersBefore[id] = get(t, s, id)
	}
	for _, l := range allLanes() {
		for range perLane {
			create(t, s, withLane(l.Channel, l.Priority))
		}
	}
	queued := publishEveryLane(t, s)
	if len(queued) != perLane*len(allLanes()) {
		t.Fatalf("setup: %d queued rows, want %d", len(queued), perLane*len(allLanes()))
	}
	before := map[uuid.UUID]Notification{}
	for id := range queued {
		before[id] = get(t, s, id)
	}

	evs, err := s.ReconcileQueued(ctxT(t))
	if err != nil {
		t.Fatal(err)
	}
	got := map[uuid.UUID]bool{}
	for _, ev := range evs {
		if got[ev.ID] {
			t.Errorf("row %s: two events", ev.ID)
		}
		got[ev.ID] = true
		if b, ok := before[ev.ID]; !ok || ev.Status != domain.StatusPending || ev.Version != b.Version+1 {
			t.Errorf("event %+v (queued before: %v)", ev, ok)
		}
	}
	if !maps.Equal(got, queued) {
		missing, extra := 0, 0
		for id := range queued {
			if !got[id] {
				missing++
			}
		}
		for id := range got {
			if !queued[id] {
				extra++
			}
		}
		t.Fatalf("reconciled %d rows, want exactly the %d queued rows: %d missing, %d not queued",
			len(got), len(queued), missing, extra)
	}
	for id, b := range before {
		after := get(t, s, id)
		checkTransition(t, b, after, after.Event(), domain.StatusPending)
		if after.QueuedAt != nil || after.AttemptCount != b.AttemptCount || after.Failures != b.Failures ||
			!after.DueAt.Equal(b.DueAt) || !after.ExpiresAt.Equal(b.ExpiresAt) {
			t.Errorf("reconciled row %+v (before %+v)", after, b)
		}
	}
	for id, b := range othersBefore {
		if after := get(t, s, id); !reflect.DeepEqual(b, after) {
			t.Errorf("%s row changed:\nbefore %+v\nafter  %+v", b.Status, b, after)
		}
	}
	if again := publishEveryLane(t, s); !maps.Equal(again, queued) {
		t.Errorf("republished %d rows, want the %d reconciled rows", len(again), len(queued))
	}
}

// With nothing queued, reconciliation changes nothing and returns no event.
func TestReconcile_OtherStatusesUntouched(t *testing.T) {
	s, pool := newTestStore(t, zeroDelay)
	others := otherStatuses(t, s, pool)
	before := map[uuid.UUID]Notification{}
	for _, id := range others {
		before[id] = get(t, s, id)
	}
	if evs, err := s.ReconcileQueued(ctxT(t)); err != nil || len(evs) != 0 {
		t.Fatalf("reconcile: %+v %v", evs, err)
	}
	if evs, err := s.RequeueStale(ctxT(t), 0); err != nil || len(evs) != 0 {
		t.Fatalf("requeue stale: %+v %v", evs, err)
	}
	for id, b := range before {
		if after := get(t, s, id); !reflect.DeepEqual(b, after) {
			t.Errorf("%s row changed", b.Status)
		}
	}
}

// A claim holds a queued row (its transaction is open) when reconciliation
// starts: reconciliation waits for the row lock, then leaves the processing
// row alone and returns the other queued rows to pending.
func TestReconcile_WaitsForClaim(t *testing.T) {
	s, pool := newTestStore(t, zeroDelay)
	held, other := create(t, s), create(t, s)
	publishEveryLane(t, s)
	ctx := ctxT(t)
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := s.q.WithTx(tx).ClaimNotification(ctx, claimParams(held.ID)); err != nil {
		t.Fatal(err)
	}

	done := startReconcile(ctx, s)
	testutil.Eventually(t, func() bool { return reconcileLockWaiters(ctx, t, pool) == 1 }, lockWaitTimeout)
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	r := receiveReconcile(t, done)
	if r.err != nil || len(r.evs) != 1 || r.evs[0].ID != other.ID {
		t.Fatalf("reconcile: %+v %v, want the other row only", r.evs, r.err)
	}
	got := get(t, s, held.ID)
	if got.Status != domain.StatusProcessing || got.Version != held.Version+2 || got.QueuedAt == nil {
		t.Errorf("claimed row %+v, want processing, untouched by reconciliation", got)
	}
}

// A cancel holds a queued row when reconciliation starts. If the cancel
// rolls back, the waiting reconciliation takes the row (it was waited for,
// not skipped); if the cancel commits, reconciliation leaves the cancelled
// row alone.
func TestReconcile_WaitsForCancel(t *testing.T) {
	for _, commit := range []bool{false, true} {
		name := "cancel rolls back"
		if commit {
			name = "cancel commits"
		}
		t.Run(name, func(t *testing.T) {
			s, pool := newTestStore(t, zeroDelay)
			n := create(t, s)
			publishEveryLane(t, s)
			queuedVersion := get(t, s, n.ID).Version
			ctx := ctxT(t)
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback(context.Background()) }()
			if _, err := s.q.WithTx(tx).CancelNotification(ctx, n.ID); err != nil {
				t.Fatal(err)
			}

			done := startReconcile(ctx, s)
			testutil.Eventually(t, func() bool { return reconcileLockWaiters(ctx, t, pool) == 1 }, lockWaitTimeout)
			if commit {
				err = tx.Commit(ctx)
			} else {
				err = tx.Rollback(ctx)
			}
			if err != nil {
				t.Fatal(err)
			}
			r := receiveReconcile(t, done)
			if r.err != nil {
				t.Fatal(r.err)
			}
			got := get(t, s, n.ID)
			if commit {
				if len(r.evs) != 0 || got.Status != domain.StatusCancelled || got.Version != queuedVersion+1 {
					t.Errorf("events %+v, row %s version %d: want the cancelled row left alone", r.evs, got.Status, got.Version)
				}
				return
			}
			if len(r.evs) != 1 || r.evs[0].ID != n.ID || got.Status != domain.StatusPending ||
				got.Version != queuedVersion+1 || r.evs[0].Version != got.Version {
				t.Errorf("events %+v, row %s version %d: want the row reconciled", r.evs, got.Status, got.Version)
			}
		})
	}
}

// claimParams are the parameters of the claim statement for a row.
func claimParams(id uuid.UUID) db.ClaimNotificationParams {
	return db.ClaimNotificationParams{Lease: interval(testLease), ID: id, Provider: testProvider}
}

// Reconciliation against a batch cancel of the same queued rows, started
// together, many rounds. The two sessions scan the rows in different
// orders (as in TestCancelBatch_ConcurrentSameBatch), so only the common id
// order of their locks keeps them from deadlocking. No call fails, no row
// stays queued, every row ends pending or cancelled, and each row's
// version went up once per event it got: one event when only one statement
// changed it, two when reconciliation ran first and the batch cancel then
// cancelled the pending row (both transitions are allowed).
func TestReconcile_VersusBatchCancel(t *testing.T) {
	const (
		rounds    = 10
		batchSize = 1000
	)
	s, pool := newTestStore(t, zeroDelay)
	exec(t, pool, `ALTER TABLE notifications SET (fillfactor = 50)`)
	seq, _ := storeWith(t, pool, map[string]string{
		"enable_indexscan": "off", "enable_bitmapscan": "off", "enable_indexonlyscan": "off",
	})
	idx, _ := storeWith(t, pool, map[string]string{
		"enable_seqscan": "off", "enable_bitmapscan": "off",
	})
	var orders [3]int // only reconciled, only cancelled, both
	for round := range rounds {
		batchID, ids := createBatchOf(t, s, batchSize)
		if evs, err := s.PublishLane(ctxT(t), smsNormal, batchSize, publishAll); err != nil || len(evs) != batchSize {
			t.Fatalf("round %d: publish %d rows, err %v", round, len(evs), err)
		}
		// Every other row by id moves within its page (see
		// TestCancelBatch_ConcurrentSameBatch).
		exec(t, pool, `UPDATE notifications SET updated_at = updated_at WHERE id = ANY($1::uuid[])`, evenIDs(ids))
		exec(t, pool, `ANALYZE notifications`)
		before := batchRows(t, pool, batchID)

		// Alternate which session reconciles and which cancels.
		rec, can := seq, idx
		if round%2 == 1 {
			rec, can = idx, seq
		}
		start := make(chan struct{})
		var (
			wg        sync.WaitGroup
			recEvs    []domain.StatusChanged
			canEvs    []domain.StatusChanged
			recErr    error
			cancelErr error
		)
		wg.Go(func() {
			<-start
			recEvs, recErr = rec.ReconcileQueued(context.Background())
		})
		wg.Go(func() {
			<-start
			_, canEvs, cancelErr = can.CancelBatch(context.Background(), batchID)
		})
		close(start)
		wg.Wait()
		if recErr != nil || cancelErr != nil {
			t.Fatalf("round %d: reconcile SQLSTATE %q %v; batch cancel SQLSTATE %q %v",
				round, sqlState(recErr), recErr, sqlState(cancelErr), cancelErr)
		}

		events := map[uuid.UUID][]domain.StatusChanged{}
		for _, ev := range slices.Concat(recEvs, canEvs) {
			events[ev.ID] = append(events[ev.ID], ev)
		}
		after := batchRows(t, pool, batchID)
		for id, a := range after {
			evs := events[id]
			slices.SortFunc(evs, func(x, y domain.StatusChanged) int { return int(x.Version - y.Version) })
			switch {
			case a.status != domain.StatusPending && a.status != domain.StatusCancelled:
				t.Fatalf("round %d: row %s ends %s", round, id, a.status)
			case len(evs) == 0 || len(evs) > 2 || a.version != before[id].version+int64(len(evs)):
				t.Fatalf("round %d: row %s version %d -> %d with %d events", round, id, before[id].version, a.version, len(evs))
			case evs[len(evs)-1].Status != a.status || evs[len(evs)-1].Version != a.version:
				t.Fatalf("round %d: row %s last event %+v, row %s %d", round, id, evs[len(evs)-1], a.status, a.version)
			case len(evs) == 2 && (evs[0].Status != domain.StatusPending || evs[1].Status != domain.StatusCancelled):
				t.Fatalf("round %d: row %s events %+v", round, id, evs)
			}
			switch {
			case len(evs) == 2:
				orders[2]++
			case a.status == domain.StatusPending:
				orders[0]++
			default:
				orders[1]++
			}
		}
		if len(events) != len(after) {
			t.Fatalf("round %d: events for %d rows, batch has %d", round, len(events), len(after))
		}
	}
	t.Logf("%d rows: reconciled only %d, cancelled only %d, reconciled then cancelled %d",
		rounds*batchSize, orders[0], orders[1], orders[2])
}

// requeue-stale by default takes only the queued rows whose queued_at is
// older than the threshold; --all (ReconcileQueued) then takes every one.
func TestRequeueStale_OlderThan(t *testing.T) {
	const threshold = 300_000 * time.Millisecond // the command's default, 5m
	s, pool := newTestStore(t, zeroDelay)
	for range 6 {
		create(t, s)
	}
	queued := publishEveryLane(t, s)
	stale := map[uuid.UUID]bool{}
	for id := range queued {
		if len(stale) == 3 {
			break
		}
		stale[id] = true
	}
	exec(t, pool, `UPDATE notifications SET queued_at = now() - interval '600000 milliseconds' WHERE id = ANY($1::uuid[])`,
		slices.Collect(maps.Keys(stale)))
	// Just inside the threshold: not stale yet.
	var fresh uuid.UUID
	for id := range queued {
		if !stale[id] {
			fresh = id
			break
		}
	}
	exec(t, pool, `UPDATE notifications SET queued_at = now() - interval '299000 milliseconds' WHERE id = $1`, fresh)

	evs, err := s.RequeueStale(ctxT(t), threshold)
	if err != nil {
		t.Fatal(err)
	}
	got := map[uuid.UUID]bool{}
	for _, ev := range evs {
		got[ev.ID] = true
		if ev.Status != domain.StatusPending {
			t.Errorf("event %+v", ev)
		}
	}
	if !maps.Equal(got, stale) {
		t.Fatalf("requeued %d rows %v, want the %d stale rows", len(got), got, len(stale))
	}

	all, err := s.ReconcileQueued(ctxT(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != len(queued)-len(stale) {
		t.Fatalf("--all took %d rows, want the remaining %d", len(all), len(queued)-len(stale))
	}
	for id := range queued {
		if st := get(t, s, id).Status; st != domain.StatusPending {
			t.Errorf("row %s is %s after --all", id, st)
		}
	}

	if _, err := s.RequeueStale(ctxT(t), -1*time.Millisecond); err == nil {
		t.Errorf("negative threshold: err %v", err)
	}
}
