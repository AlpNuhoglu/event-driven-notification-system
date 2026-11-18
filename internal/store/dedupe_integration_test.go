//go:build integration

package store

import (
	"context"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

// holderStates are the states of a row that holds a dedupe_key, set up with
// raw SQL (cancel, transition 9, does not exist yet). Only a row cancelled
// before any attempt leaves the index (ADR-0006 section 11).
var holderStates = []struct {
	name     string
	set      string
	released bool
}{
	{"pending", `status = 'pending'`, false},
	{"cancelled before any attempt", `status = 'cancelled'`, true},
	{"cancelled after an attempt", `status = 'cancelled', attempt_count = 1`, false},
	{"failed before any attempt", `status = 'failed', failure_reason = 'expired'`, false},
	{"failed after attempts", `status = 'failed', failure_reason = 'max_failures_unknown_outcome', attempt_count = 3, failures = 3`, false},
	{"sent", `status = 'sent', sent_at = now(), attempt_count = 1`, false},
}

func TestCreateSingle_Dedupe(t *testing.T) {
	for _, st := range holderStates {
		t.Run(st.name, func(t *testing.T) {
			s, pool := newTestStore(t, zeroDelay)
			held := create(t, s, withDedupe("order-7"))
			setStatus(t, pool, held.ID, st.set)

			n := baseNew()
			n.DedupeKey = ptr("order-7")
			r := createSingle(t, s, nil, n)
			if st.released {
				if r.Outcome != OutcomeCreated || r.Notification.ID == held.ID ||
					r.Notification.DedupeKey == nil || *r.Notification.DedupeKey != "order-7" {
					t.Fatalf("result = %+v, want a new notification", r)
				}
				// The new row holds the key now.
				again := createSingle(t, s, nil, n)
				if again.Outcome != OutcomeDuplicate || again.ExistingID == nil || *again.ExistingID != r.Notification.ID {
					t.Errorf("again = %+v, want a duplicate of %s", again, r.Notification.ID)
				}
				return
			}
			if r.Outcome != OutcomeDuplicate || r.ExistingID == nil || *r.ExistingID != held.ID {
				t.Fatalf("result = %+v, want a duplicate of %s", r, held.ID)
			}
			if r.Response != nil || r.Notification.ID != uuid.Nil {
				t.Errorf("duplicate carries a response or a row: %+v", r)
			}
			if c := count(t, pool, `SELECT count(*) FROM notifications`); c != 1 {
				t.Errorf("%d notifications, want 1", c)
			}
		})
	}

	t.Run("no dedupe key never conflicts", func(t *testing.T) {
		s, pool := newTestStore(t, zeroDelay)
		for range 3 {
			if r := createSingle(t, s, nil, baseNew()); r.Outcome != OutcomeCreated {
				t.Fatalf("result = %+v", r)
			}
		}
		if c := count(t, pool, `SELECT count(*) FROM notifications`); c != 3 {
			t.Errorf("%d notifications, want 3", c)
		}
	})

	t.Run("without respond", func(t *testing.T) {
		s, _ := newTestStore(t, zeroDelay)
		r, err := s.CreateSingle(ctxT(t), nil, baseNew(), nil)
		if err != nil || r.Outcome != OutcomeCreated || r.Response != nil {
			t.Fatalf("result = %+v, err = %v", r, err)
		}
		if got := get(t, s, r.Notification.ID); got.ID != r.Notification.ID {
			t.Errorf("row not stored")
		}
	})
}

// Two single creates with the same dedupe_key and no idempotency key: the
// second waits on the first's uncommitted index entry, and after the commit
// reports the first's id.
func TestCreateSingle_ConcurrentSameDedupeKey(t *testing.T) {
	s, pool := newTestStore(t, zeroDelay)
	ctx := ctxT(t)
	n := baseNew()
	n.DedupeKey = ptr("order-race")

	var second <-chan singleResult
	first, err := s.CreateSingle(ctx, nil, n, func(row Notification) (StoredResponse, error) {
		second = startCreateSingle(ctx, s, nil, n)
		testutil.Eventually(t, func() bool {
			return stmtLockWaiters(ctx, t, pool, dedupeInsertStmt) == 1
		}, lockWaitTimeout)
		return respond202(row)
	})
	if err != nil || first.Outcome != OutcomeCreated {
		t.Fatalf("first = %+v, err = %v", first, err)
	}
	r := receiveSingle(t, second)
	if r.err != nil {
		t.Fatalf("second: %v", r.err)
	}
	if r.res.Outcome != OutcomeDuplicate || r.res.ExistingID == nil || *r.res.ExistingID != first.Notification.ID {
		t.Fatalf("second = %+v, want a duplicate of %s", r.res, first.Notification.ID)
	}
	if c := count(t, pool, `SELECT count(*) FROM notifications`); c != 1 {
		t.Errorf("%d notifications, want 1", c)
	}
}

// Many single creates with the same dedupe_key at once: exactly one is
// created and every other one reports its id.
func TestCreateSingle_ConcurrentSameDedupeKeyBurst(t *testing.T) {
	s, pool := newTestStore(t, zeroDelay)
	const callers = 16
	n := baseNew()
	n.DedupeKey = ptr("order-burst")

	results := make([]SingleResult, callers)
	errs := make([]error, callers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range callers {
		wg.Go(func() {
			<-start
			results[i], errs[i] = s.CreateSingle(context.Background(), nil, n, nil)
		})
	}
	close(start)
	wg.Wait()

	var created []uuid.UUID
	for i, r := range results {
		if errs[i] != nil {
			t.Fatalf("caller %d: %v", i, errs[i])
		}
		if r.Outcome == OutcomeCreated {
			created = append(created, r.Notification.ID)
		}
	}
	if len(created) != 1 {
		t.Fatalf("%d created, want 1", len(created))
	}
	for i, r := range results {
		if r.Outcome == OutcomeDuplicate && (r.ExistingID == nil || *r.ExistingID != created[0]) {
			t.Errorf("caller %d: %+v, want a duplicate of %s", i, r, created[0])
		}
	}
	if c := count(t, pool, `SELECT count(*) FROM notifications`); c != 1 {
		t.Errorf("%d notifications, want 1", c)
	}
}
