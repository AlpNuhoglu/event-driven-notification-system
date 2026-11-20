//go:build integration

package store

import (
	"context"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

// requireNotCancellable asserts that a cancel was refused because the row
// was processing.
func requireNotCancellable(t *testing.T, c cancelResult) {
	t.Helper()
	r, ok := refusal(c.err)
	if !ok || r.Refusal != RefusalNotCancellable || r.Current != domain.StatusProcessing {
		t.Fatalf("cancel: result %+v err %v, want not_cancellable from processing", c.res, c.err)
	}
}

// requireCancelledAfterSweep asserts the end state of a cancel that applied
// to the row the lease sweeper had returned to retrying: cancelled one
// version after the sweep's event, may_have_been_sent (the swept attempt is
// unknown), and the attempt row left as the sweeper wrote it.
func requireCancelledAfterSweep(t *testing.T, s *Store, id uuid.UUID, sweep domain.StatusChanged, c cancelResult) {
	t.Helper()
	if c.err != nil || !c.res.Applied || !c.res.MayHaveBeenSent {
		t.Fatalf("cancel after the sweep: result %+v err %v, want applied with may_have_been_sent", c.res, c.err)
	}
	if sweep.Status != domain.StatusRetrying {
		t.Fatalf("sweep event %+v, want retrying", sweep)
	}
	got := get(t, s, id)
	if got.Status != domain.StatusCancelled || got.Version != sweep.Version+1 || c.event.Version != got.Version {
		t.Errorf("row %s version %d, cancel event version %d; sweep event version %d",
			got.Status, got.Version, c.event.Version, sweep.Version)
	}
	if a := attempts(t, s, id); len(a) != 1 || a[0].Outcome != domain.OutcomeUnknown || a[0].FinishedAt == nil {
		t.Errorf("attempts %+v, want the swept attempt unknown", a)
	}
}

// The api's cancel and the lease sweeper on the same notification (the cell
// api × lease sweeper of the actor-pair matrix, ADR-0010 §1): a processing
// row whose lease expired. A cancel never applies to the row while it is
// processing, also not while the sweeper holds it inside its transaction,
// and it does not wait for the sweeper there: the row is not cancellable in
// the cancel's snapshot. Once the sweeper has committed the row is retrying,
// and a cancel applies to it with may_have_been_sent. Whatever the order,
// the sweeper's transition is never lost and the row never ends cancelled
// while it was still processing.
func TestCancel_VersusLeaseSweep(t *testing.T) {
	setup := func(t *testing.T) (*Store, Notification) {
		t.Helper()
		s, pool := newTestStore(t, delayed)
		n := create(t, s)
		claim(t, s, n.ID)
		expireLease(t, pool, n.ID)
		return s, n
	}
	sweep := func(t *testing.T, s *Store) domain.StatusChanged {
		t.Helper()
		evs, err := s.SweepExpiredLeases(ctxT(t), 10, zeroJitter)
		if err != nil || len(evs) != 1 {
			t.Fatalf("sweep: %+v %v", evs, err)
		}
		return evs[0]
	}
	cancel := func(ctx context.Context, s *Store, id uuid.UUID) cancelResult {
		r, ev, err := s.CancelNotification(ctx, id)
		return cancelResult{r, ev, err}
	}

	t.Run("cancel first", func(t *testing.T) {
		s, n := setup(t)
		before := get(t, s, n.ID)
		requireNotCancellable(t, cancel(ctxT(t), s, n.ID))
		if after := get(t, s, n.ID); after.Version != before.Version || after.Status != domain.StatusProcessing {
			t.Fatalf("a refused cancel changed the row: %s version %d", after.Status, after.Version)
		}
		ev := sweep(t, s)
		if ev.Status != domain.StatusRetrying || ev.Version != before.Version+1 {
			t.Errorf("sweep after the refused cancel: %+v", ev)
		}
	})

	t.Run("sweep first", func(t *testing.T) {
		s, n := setup(t)
		ev := sweep(t, s)
		requireCancelledAfterSweep(t, s, n.ID, ev, cancel(ctxT(t), s, n.ID))
		if evs, err := s.SweepExpiredLeases(ctxT(t), 10, zeroJitter); err != nil || len(evs) != 0 {
			t.Errorf("sweep of the cancelled row: %+v %v, want nothing", evs, err)
		}
	})

	// The sweeper holds the row (inside its transaction, from its jitter
	// callback, the attempt already unknown and the decision not yet
	// applied) when the cancel runs. The cancel returns at once, refused;
	// if it waited for the row lock, it would wait for this callback, which
	// waits for it, and its context would end the test.
	t.Run("sweeper holds the row, cancel is refused without waiting", func(t *testing.T) {
		s, n := setup(t)
		var during cancelResult
		evs, err := s.SweepExpiredLeases(ctxT(t), 10, func() float64 {
			ctx, stop := context.WithTimeout(context.Background(), lockWaitTimeout)
			defer stop()
			during = cancel(ctx, s, n.ID)
			return 0
		})
		if err != nil || len(evs) != 1 {
			t.Fatalf("sweep: %+v %v", evs, err)
		}
		requireNotCancellable(t, during)
		requireCancelledAfterSweep(t, s, n.ID, evs[0], cancel(ctxT(t), s, n.ID))
	})

	// Started together, many rounds: the sweep always applies, and the
	// cancel is either refused (it read the row while it was processing) or
	// applies after the sweep.
	t.Run("concurrent", func(t *testing.T) {
		const rounds = 100
		s, pool := newTestStore(t, delayed)
		var refused, applied int
		for round := range rounds {
			n := create(t, s)
			claim(t, s, n.ID)
			expireLease(t, pool, n.ID)
			start := make(chan struct{})
			var (
				wg       sync.WaitGroup
				c        cancelResult
				swept    []domain.StatusChanged
				sweepErr error
			)
			wg.Go(func() {
				<-start
				c = cancel(context.Background(), s, n.ID)
			})
			wg.Go(func() {
				<-start
				swept, sweepErr = s.SweepExpiredLeases(context.Background(), 10, zeroJitter)
			})
			close(start)
			wg.Wait()
			if sweepErr != nil || len(swept) != 1 {
				t.Fatalf("round %d: sweep %+v %v, want the row swept", round, swept, sweepErr)
			}
			if c.err == nil && c.res.Applied {
				applied++
				requireCancelledAfterSweep(t, s, n.ID, swept[0], c)
				continue
			}
			refused++
			requireNotCancellable(t, c)
			if got := get(t, s, n.ID); got.Status != domain.StatusRetrying || got.Version != swept[0].Version {
				t.Fatalf("round %d: row %s version %d after a refused cancel, want the sweep's retrying version %d",
					round, got.Status, got.Version, swept[0].Version)
			}
		}
		t.Logf("%d rounds: the cancel was refused %d, applied after the sweep %d", rounds, refused, applied)
	})
}
