//go:build integration

package store

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

const (
	// raceBatchSize is the number of notifications of the batch two cancels
	// race on: the largest batch the API accepts.
	raceBatchSize = 1000
	// batchRaceRounds is the number of rounds of "same plans".
	batchRaceRounds = 12
	// scanOrderRounds is the number of rounds of "different scan orders".
	// The earlier unordered statement deadlocked in 43 of 600 rounds there,
	// and each of 20 runs of 30 rounds failed.
	scanOrderRounds = 30
)

// createBatchOf inserts a batch of n due SMS notifications with
// CreateBatch, the insert the API uses, and returns the batch id and the
// notification ids.
func createBatchOf(t *testing.T, s *Store, n int) (uuid.UUID, []uuid.UUID) {
	t.Helper()
	items := make([]NewNotification, n)
	for i := range items {
		items[i] = baseNew()
	}
	res, err := s.CreateBatch(ctxT(t), nil, NewBatch{Total: n, CorrelationID: "corr-batch", Items: items},
		func(BatchResult) (StoredResponse, error) { return StoredResponse{Status: 202, Body: []byte(`{}`)}, nil })
	if err != nil || res.Outcome != OutcomeCreated || res.Accepted != n {
		t.Fatalf("create batch: %+v %v", res.Outcome, err)
	}
	ids := make([]uuid.UUID, n)
	for i, it := range res.Items {
		ids[i] = it.ID
	}
	return res.BatchID, ids
}

// batchRows returns the status and the version of every notification of the
// batch.
func batchRows(t *testing.T, pool *pgxpool.Pool, batchID uuid.UUID) map[uuid.UUID]struct {
	status  domain.Status
	version int64
} {
	t.Helper()
	rows, err := pool.Query(ctxT(t), `SELECT id, status, version FROM notifications WHERE batch_id = $1`, batchID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[uuid.UUID]struct {
		status  domain.Status
		version int64
	}{}
	for rows.Next() {
		var (
			id uuid.UUID
			st string
			v  int64
		)
		if err := rows.Scan(&id, &st, &v); err != nil {
			t.Fatal(err)
		}
		out[id] = struct {
			status  domain.Status
			version int64
		}{domain.Status(st), v}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

type batchCall struct {
	res CancelledBatch
	evs []domain.StatusChanged
	err error
}

// batchRace runs one round: CancelBatch on a and on b, started together,
// and with activity also a few single claims and single cancels of rows of
// the batch at the same time. It checks that no call fails, that both
// answers are consistent, and that every cancelled row was cancelled by
// exactly one call, its version up by exactly one. It returns false when a
// batch cancel failed (the error is reported).
func batchRace(t *testing.T, a, b *Store, pool *pgxpool.Pool, batchID uuid.UUID, ids []uuid.UUID, activity bool) bool {
	t.Helper()
	before := batchRows(t, pool, batchID)
	var (
		start         = make(chan struct{})
		wg            sync.WaitGroup
		ca, cb        batchCall
		mu            sync.Mutex
		singleEvents  []domain.StatusChanged
		activityError error
	)
	wg.Go(func() {
		<-start
		ca.res, ca.evs, ca.err = a.CancelBatch(context.Background(), batchID)
	})
	wg.Go(func() {
		<-start
		cb.res, cb.evs, cb.err = b.CancelBatch(context.Background(), batchID)
	})
	if activity {
		// Rows spread over the batch: some claimed, some cancelled singly.
		for i := 7; i < len(ids); i += len(ids) / 6 {
			claimID, cancelID := ids[i], ids[i-1]
			wg.Go(func() {
				<-start
				_, _, err := a.Claim(context.Background(), claimID, testLease, testProvider)
				if err != nil && !errors.Is(err, ErrNotApplied) {
					mu.Lock()
					activityError = errors.Join(activityError, err)
					mu.Unlock()
				}
			})
			wg.Go(func() {
				<-start
				res, ev, err := b.CancelNotification(context.Background(), cancelID)
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					activityError = errors.Join(activityError, err)
				} else if res.Applied {
					singleEvents = append(singleEvents, ev)
				}
			})
		}
	}
	close(start)
	wg.Wait()

	if activityError != nil {
		t.Errorf("single claim or cancel: %v", activityError)
	}
	ok := true
	for name, c := range map[string]batchCall{"first": ca, "second": cb} {
		if c.err != nil {
			t.Errorf("%s batch cancel: SQLSTATE %q: %v", name, sqlState(c.err), c.err)
			ok = false
			continue
		}
		if c.res.Cancelled+c.res.NotCancellable != len(ids) {
			t.Errorf("%s answer %+v does not add up to %d", name, c.res, len(ids))
		}
	}
	if !ok {
		return false
	}

	after := batchRows(t, pool, batchID)
	cancelled := map[uuid.UUID]bool{}
	for id, r := range after {
		switch r.status {
		case domain.StatusPending, domain.StatusQueued, domain.StatusRetrying:
			t.Errorf("row %s is still %s", id, r.status)
		case domain.StatusCancelled:
			cancelled[id] = true
			if r.version != before[id].version+1 {
				t.Errorf("row %s: version %d -> %d, want one step", id, before[id].version, r.version)
			}
		}
	}
	if got := max(ca.res.Cancelled, cb.res.Cancelled); got != len(cancelled) {
		t.Errorf("answers %+v and %+v; the later must count all %d cancelled rows", ca.res, cb.res, len(cancelled))
	}
	byCall := map[uuid.UUID]int{}
	for _, evs := range [][]domain.StatusChanged{ca.evs, cb.evs, singleEvents} {
		for _, ev := range evs {
			byCall[ev.ID]++
			if ev.Status != domain.StatusCancelled || ev.Version != after[ev.ID].version {
				t.Errorf("event %+v, row version %d", ev, after[ev.ID].version)
			}
		}
	}
	for id, n := range byCall {
		if n != 1 || !cancelled[id] {
			t.Errorf("row %s: %d events, cancelled %v", id, n, cancelled[id])
		}
	}
	if len(byCall) != len(cancelled) {
		t.Errorf("%d rows with an event, %d cancelled rows", len(byCall), len(cancelled))
	}
	return true
}

// storeWith returns a Store on its own pool to the test's database, whose
// connections run with the given settings.
func storeWith(t *testing.T, base *pgxpool.Pool, settings map[string]string) (*Store, *pgxpool.Pool) {
	t.Helper()
	cfg, err := PoolConfig(base.Config().ConnString())
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range settings {
		cfg.ConnConfig.RuntimeParams[k] = v
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return New(pool, zeroDelay), pool
}

// scanOrder returns the ids of the batch's cancellable rows in the order a
// plain scan of the session meets them.
func scanOrder(t *testing.T, pool *pgxpool.Pool, batchID uuid.UUID) []uuid.UUID {
	t.Helper()
	rows, err := pool.Query(ctxT(t), `
		SELECT id FROM notifications
		WHERE batch_id = $1 AND status IN ('pending', 'retrying', 'queued')`, batchID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		out = append(out, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// Two cancels of the same 1,000-row batch, started together: no deadlock,
// consistent answers, and every row cancelled by exactly one call
// (CancelBatchNotifications locks the rows in id order).
//
// "different scan orders" gives the two sessions scans that meet the rows
// in different orders: one session may only use a sequential scan, the
// other only an index scan, and half the rows of every page are moved
// within their page by a HOT update before the round, so that the heap
// order (sequential scan) differs from the order of the index entries (an
// index scan follows them to the moved row). A batch cancel that locks rows
// in the order its scan meets them (the earlier plain UPDATE) deadlocks
// there (SQLSTATE 40P01) in about one round in fourteen; under "same
// plans" it did not deadlock in 360 rounds.
func TestCancelBatch_ConcurrentSameBatch(t *testing.T) {
	t.Run("same plans", func(t *testing.T) {
		s, pool := newTestStore(t, zeroDelay)
		for round := range batchRaceRounds {
			batchID, ids := createBatchOf(t, s, raceBatchSize)
			batchRace(t, s, s, pool, batchID, ids, round%2 == 1)
		}
	})

	t.Run("different scan orders", func(t *testing.T) {
		s, pool := newTestStore(t, zeroDelay)
		// Room on every page, so that an update of a non-indexed column is
		// a HOT update that stays on its page.
		exec(t, pool, `ALTER TABLE notifications SET (fillfactor = 50)`)
		// One session may only scan sequentially, the other only through an
		// index.
		seq, seqPool := storeWith(t, pool, map[string]string{
			"enable_indexscan": "off", "enable_bitmapscan": "off", "enable_indexonlyscan": "off",
		})
		idx, idxPool := storeWith(t, pool, map[string]string{
			"enable_seqscan": "off", "enable_bitmapscan": "off",
		})
		for round := range scanOrderRounds {
			batchID, ids := createBatchOf(t, s, raceBatchSize)
			// Every other row by id gets a new tuple at the end of its page;
			// updated_at is not indexed, so the index entries keep pointing
			// at the old place.
			exec(t, pool, `UPDATE notifications SET updated_at = updated_at WHERE id = ANY($1::uuid[])`, evenIDs(ids))
			// Fresh statistics keep the join of the sequential-scan session
			// from rescanning the growing table once per row.
			exec(t, pool, `ANALYZE notifications`)
			// Precondition: the two sessions meet the rows in different orders.
			seqOrder, idxOrder := scanOrder(t, seqPool, batchID), scanOrder(t, idxPool, batchID)
			if len(seqOrder) != raceBatchSize || slices.Equal(seqOrder, idxOrder) {
				t.Fatalf("round %d: the two sessions scan the rows in the same order", round)
			}
			batchRace(t, seq, idx, pool, batchID, ids, round%2 == 1)
		}
	})
}

// evenIDs returns ids[0], ids[2], ...
func evenIDs(ids []uuid.UUID) []uuid.UUID {
	out := make([]uuid.UUID, 0, len(ids)/2+1)
	for i := 0; i < len(ids); i += 2 {
		out = append(out, ids[i])
	}
	return out
}
