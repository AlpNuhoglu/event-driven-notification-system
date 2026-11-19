package relay

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/config"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

// failedEvents returns n failed/expired events.
func failedEvents(n int) []domain.StatusChanged {
	reason := domain.ReasonExpired
	out := make([]domain.StatusChanged, n)
	for i := range out {
		out[i] = domain.StatusChanged{ID: uuid.New(), Status: domain.StatusFailed, FailureReason: &reason}
	}
	return out
}

func repeat(v, n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = v
	}
	return out
}

// One tick of the expiry pass runs the next batch at once while a batch
// comes back full, and stops at the first batch that is not full; every
// event is published, in order.
func TestExpire_RepeatsWhileBatchFull(t *testing.T) {
	for _, tc := range []struct {
		name    string
		batches []int
		calls   int
	}{
		{"backlog then a partial batch", []int{3, 3, 3, 1}, 4},
		{"backlog ending on a full batch", []int{3, 3}, 3}, // the third call finds nothing
		{"nothing expired", nil, 1},
		{"one partial batch", []int{2}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := &fakeStore{}
			var want []domain.StatusChanged
			for _, n := range tc.batches {
				evs := failedEvents(n)
				st.expireBatches = append(st.expireBatches, evs)
				want = append(want, evs...)
			}
			r, _, _, rec := newUnit(st, unitConfig)
			r.expire(context.Background())

			if got := st.expireCalls(); !slices.Equal(got, repeat(unitConfig.ExpiryBatch, tc.calls)) {
				t.Errorf("batch limits %v, want %d calls with limit %d", got, tc.calls, unitConfig.ExpiryBatch)
			}
			if got := rec.events(); !slices.EqualFunc(got, want, func(a, b domain.StatusChanged) bool { return a.ID == b.ID }) {
				t.Errorf("%d events published, want %d in order", len(got), len(want))
			}
		})
	}
}

// The expiry pass does not need the broker connection.
func TestExpire_RunsWhileConnectionDown(t *testing.T) {
	st := &fakeStore{expireBatches: [][]domain.StatusChanged{failedEvents(1)}}
	r, _, conn, rec := newUnit(st, unitConfig)
	conn.open.Store(false)
	r.expire(context.Background())
	if len(st.expireCalls()) != 1 || len(rec.events()) != 1 {
		t.Errorf("connection down: %d expiry calls, %d events; want 1 and 1", len(st.expireCalls()), len(rec.events()))
	}
}

// A failing batch is logged and ends the tick; the loop goes on and tries
// again at the next tick.
func TestRunExpiry_ErrorIsRetriedAtNextTick(t *testing.T) {
	st := &fakeStore{expireErr: errors.New("database unavailable")}
	r, _, _, rec := newUnit(st, unitConfig)
	r.expire(context.Background())
	if got := len(st.expireCalls()); got != 1 {
		t.Fatalf("%d calls in a failing tick, want 1", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.RunExpiry(ctx) }()
	testutil.Eventually(t, func() bool { return len(st.expireCalls()) >= 3 }, 5*time.Second)
	cancel()
	if err := <-done; err != nil {
		t.Errorf("RunExpiry: %v", err)
	}
	if len(rec.events()) != 0 {
		t.Errorf("events from a failing pass: %v", rec.events())
	}
}

// On shutdown the current batch finishes on a context that is not
// cancelled, its events are published, and no further batch starts even
// though the batch was full.
func TestExpire_ShutdownFinishesCurrentBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var batchCtxErr error
	st := &fakeStore{
		expireBatches: [][]domain.StatusChanged{failedEvents(3), failedEvents(3)},
		onExpire: func(batchCtx context.Context) {
			cancel() // SIGTERM arrives during the first batch
			batchCtxErr = batchCtx.Err()
		},
	}
	r, _, _, rec := newUnit(st, unitConfig)
	r.expire(ctx)
	if got := len(st.expireCalls()); got != 1 {
		t.Errorf("%d batches, want 1", got)
	}
	if batchCtxErr != nil {
		t.Errorf("the batch's context was cancelled by shutdown: %v", batchCtxErr)
	}
	if got := len(rec.events()); got != 3 {
		t.Errorf("%d events, want the 3 of the finished batch", got)
	}

	st2 := &fakeStore{}
	r2, _, _, _ := newUnit(st2, unitConfig)
	r2.expire(ctx) // already cancelled
	if len(st2.expireCalls()) != 0 {
		t.Error("a batch started after shutdown")
	}
}

// One tick of the cleanup runs the next batch at once while a batch of
// store.IdempotencyCleanupBatch keys comes back full.
func TestCleanup_RepeatsWhileBatchFull(t *testing.T) {
	full := store.IdempotencyCleanupBatch
	for _, tc := range []struct {
		name   string
		counts []int
		calls  int
	}{
		{"backlog then a partial batch", []int{full, full, 5}, 3},
		{"backlog ending on a full batch", []int{full}, 2},
		{"nothing expired", nil, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := &fakeStore{cleanupCounts: tc.counts}
			r, _, _, _ := newUnit(st, unitConfig)
			r.cleanup(context.Background())
			if got := st.cleanupCalls(); got != tc.calls {
				t.Errorf("%d cleanup calls, want %d", got, tc.calls)
			}
		})
	}
}

func TestCleanup_ErrorAndShutdown(t *testing.T) {
	st := &fakeStore{cleanupCounts: []int{store.IdempotencyCleanupBatch}, cleanupErr: errors.New("database unavailable")}
	r, _, _, _ := newUnit(st, unitConfig)
	r.cleanup(context.Background())
	if got := st.cleanupCalls(); got != 1 {
		t.Errorf("%d calls in a failing tick, want 1", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var batchCtxErr error
	st2 := &fakeStore{
		cleanupCounts: []int{store.IdempotencyCleanupBatch, store.IdempotencyCleanupBatch},
		onCleanup: func(batchCtx context.Context) {
			cancel()
			batchCtxErr = batchCtx.Err()
		},
	}
	r2, _, _, _ := newUnit(st2, unitConfig)
	r2.cleanup(ctx)
	if got := st2.cleanupCalls(); got != 1 {
		t.Errorf("%d batches after shutdown during the first, want 1", got)
	}
	if batchCtxErr != nil {
		t.Errorf("the batch's context was cancelled by shutdown: %v", batchCtxErr)
	}
	r2.cleanup(ctx)
	if got := st2.cleanupCalls(); got != 1 {
		t.Error("a batch started after shutdown")
	}

	// The loop survives failing ticks and stops on shutdown.
	ctx3, cancel3 := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.RunIdempotencyCleanup(ctx3) }()
	testutil.Eventually(t, func() bool { return st.cleanupCalls() >= 3 }, 5*time.Second)
	cancel3()
	if err := <-done; err != nil {
		t.Errorf("RunIdempotencyCleanup: %v", err)
	}
}

// Run starts the expiry pass and the cleanup next to the other loops, and
// every loop stops when ctx is cancelled.
func TestRun_StartsAndStopsTheHousekeepingLoops(t *testing.T) {
	st := &fakeStore{}
	r, _, _, _ := newUnit(st, unitConfig)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx, make(chan struct{}), func() error { return nil }) }()
	testutil.Eventually(t, func() bool {
		return len(st.expireCalls()) >= 2 && st.cleanupCalls() >= 2 && st.sweeps.Load() >= 2 && len(st.laneCalls()) >= 9
	}, 5*time.Second)
	cancel()
	if err := <-done; err != nil {
		t.Errorf("run: %v", err)
	}
	// Run has waited for every loop: no call starts after it returned.
	expiries, cleanups := len(st.expireCalls()), st.cleanupCalls()
	r.expire(ctx)
	r.cleanup(ctx)
	if len(st.expireCalls()) != expiries || st.cleanupCalls() != cleanups {
		t.Error("a housekeeping batch started after shutdown")
	}
}

func TestConfigFrom(t *testing.T) {
	c := config.Relay{
		TickInterval: 1 * time.Millisecond, BatchSize: 2, ConfirmTimeout: 3 * time.Millisecond,
		LaneHWMHigh: 4, LaneHWMNormal: 5, LaneHWMLow: 6,
		ExpiryInterval: 7 * time.Millisecond, ExpiryBatchSize: 8, IdempotencyCleanupEvery: 9 * time.Millisecond,
		LeaseSweepEvery: 10 * time.Millisecond, LeaseSweepBatch: 11,
	}
	want := Config{
		TickInterval: 1 * time.Millisecond, BatchSize: 2, ConfirmTimeout: 3 * time.Millisecond,
		HWMHigh: 4, HWMNormal: 5, HWMLow: 6,
		LeaseSweepInterval: 10 * time.Millisecond, LeaseSweepBatch: 11,
		ExpiryInterval: 7 * time.Millisecond, ExpiryBatch: 8, IdempotencyCleanupInterval: 9 * time.Millisecond,
	}
	if got := ConfigFrom(c); got != want {
		t.Errorf("ConfigFrom:\n got %+v\nwant %+v", got, want)
	}
}
