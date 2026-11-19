//go:build integration

package store

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

// The snapshot by ids is the current state of each row with its version,
// the same value the row's last transition returned; an id without a row has
// no element.
func TestSnapshotByIDs_ReturnsTheCurrentStateAndVersionOfEachRow(t *testing.T) {
	s, _ := newTestStore(t, zeroDelay)
	pending := create(t, s)
	other := create(t, s)
	_, cancelled, err := s.CancelNotification(ctxT(t), other.ID)
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}

	got, err := s.SnapshotByIDs(ctxT(t), []uuid.UUID{pending.ID, other.ID, uuid.New()})
	if err != nil {
		t.Fatal(err)
	}
	byID := map[uuid.UUID]domain.StatusChanged{}
	for _, e := range got {
		byID[e.ID] = e
	}
	if len(got) != 2 || len(byID) != 2 {
		t.Fatalf("snapshot has %d elements for %d ids, want 2 (one per existing row)", len(got), len(byID))
	}
	if e := byID[pending.ID]; e.Status != domain.StatusPending || e.Version != pending.Version ||
		!e.UpdatedAt.Equal(pending.UpdatedAt) || e.Channel != pending.Channel || e.Priority != pending.Priority {
		t.Errorf("pending row: %+v, want status pending, version %d, updated_at %s", e, pending.Version, pending.UpdatedAt)
	}
	e := byID[other.ID]
	if e.Status != domain.StatusCancelled || e.Version != cancelled.Version || !e.UpdatedAt.Equal(cancelled.UpdatedAt) {
		t.Errorf("cancelled row: %+v, want status cancelled, version %d (the cancel's event)", e, cancelled.Version)
	}
	if e.Version <= pending.Version {
		t.Errorf("the cancelled row's version %d did not grow past its creation's", e.Version)
	}

	none, err := s.SnapshotByIDs(ctxT(t), nil)
	if err != nil || len(none) != 0 {
		t.Errorf("snapshot of no ids: %d elements, error %v", len(none), err)
	}
}

// The snapshot of a batch has one element per notification of the batch,
// each carrying the batch id.
func TestSnapshotByBatch_ReturnsEveryNotificationOfTheBatch(t *testing.T) {
	s, _ := newTestStore(t, zeroDelay)
	batchID, ids := createBatchOf(t, s, 5)
	create(t, s) // a notification outside the batch

	got, err := s.SnapshotByBatch(ctxT(t), batchID)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(ids) {
		t.Fatalf("snapshot has %d elements, want %d", len(got), len(ids))
	}
	seen := map[uuid.UUID]bool{}
	for _, e := range got {
		seen[e.ID] = true
		if e.BatchID == nil || *e.BatchID != batchID || e.Status != domain.StatusPending || e.Version < 1 {
			t.Errorf("element %+v: want the batch id, status pending and a version", e)
		}
	}
	for _, id := range ids {
		if !seen[id] {
			t.Errorf("notification %s of the batch is missing", id)
		}
	}

	none, err := s.SnapshotByBatch(ctxT(t), uuid.New())
	if err != nil || len(none) != 0 {
		t.Errorf("snapshot of an unknown batch: %d elements, error %v", len(none), err)
	}
}

// A pool opened with ReadOnly reads, and refuses a write (SQLSTATE 25006,
// read_only_sql_transaction): in a plain statement, in an explicit
// transaction and through a transition of the store.
func TestOpenPoolWith_ReadOnlyRefusesWrites(t *testing.T) {
	url := testutil.NewMigratedDatabase(t)
	rw, err := OpenPool(ctxT(t), url)
	if err != nil {
		t.Fatal(err)
	}
	defer rw.Close()
	n := create(t, New(rw, zeroDelay))

	ro, err := OpenPoolWith(ctxT(t), url, ReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	if _, err := New(ro, zeroDelay).SnapshotByIDs(ctxT(t), []uuid.UUID{n.ID}); err != nil {
		t.Fatalf("read through the read-only pool: %v", err)
	}
	var mode, planMode string
	if err := ro.QueryRow(ctxT(t), "SELECT current_setting('default_transaction_read_only'), current_setting('plan_cache_mode')").
		Scan(&mode, &planMode); err != nil {
		t.Fatal(err)
	}
	if mode != "on" || planMode != PlanCacheMode {
		t.Errorf("default_transaction_read_only = %q, plan_cache_mode = %q; want on and %s", mode, planMode, PlanCacheMode)
	}

	readOnly := func(what string, err error) {
		t.Helper()
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "25006" {
			t.Errorf("%s through the read-only pool: error %v, want SQLSTATE 25006", what, err)
		}
	}
	_, err = ro.Exec(ctxT(t), "UPDATE notifications SET content = 'changed' WHERE id = $1", n.ID)
	readOnly("an UPDATE", err)

	tx, err := ro.Begin(ctxT(t))
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctxT(t), "DELETE FROM notifications WHERE id = $1", n.ID)
	readOnly("a DELETE in a transaction", err)
	_ = tx.Rollback(ctxT(t))

	_, _, err = New(ro, zeroDelay).CancelNotification(ctxT(t), n.ID)
	readOnly("a cancel", err)

	got, err := New(rw, zeroDelay).GetNotification(ctxT(t), n.ID)
	if err != nil || got.Status != domain.StatusPending || got.Version != n.Version || got.Content != n.Content {
		t.Errorf("the row changed: status %s, version %d, error %v", got.Status, got.Version, err)
	}
}
