//go:build integration

package store

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

var smsNormal = Lane{Channel: domain.ChannelSMS, Priority: domain.PriorityNormal}

func ids(rows []DueRow) []uuid.UUID {
	out := make([]uuid.UUID, len(rows))
	for i, r := range rows {
		out[i] = r.ID
	}
	return out
}

func TestPublishLane_OnlyPublishedBecomeQueued(t *testing.T) {
	s, _ := newTestStore(t, zeroDelay)
	rows := []Notification{create(t, s), create(t, s), create(t, s)}
	events, err := s.PublishLane(ctxT(t), smsNormal, 10, func(_ context.Context, due []DueRow) ([]uuid.UUID, error) {
		if len(due) != 3 {
			t.Errorf("selected %d rows, want 3", len(due))
		}
		for _, d := range due {
			if d.TraceParent != rows[0].TraceParent {
				t.Errorf("trace_parent = %q", d.TraceParent)
			}
		}
		return []uuid.UUID{rows[0].ID, rows[2].ID, rows[0].ID}, nil // a duplicate is harmless
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("events = %+v", events)
	}
	byID := map[uuid.UUID]domain.StatusChanged{}
	for _, e := range events {
		byID[e.ID] = e
	}
	for i, n := range rows {
		got := get(t, s, n.ID)
		if i == 1 {
			if got.Status != domain.StatusPending || got.Version != n.Version || got.QueuedAt != nil {
				t.Errorf("unpublished row changed: %+v", got)
			}
			continue
		}
		checkTransition(t, n, got, byID[n.ID], domain.StatusQueued)
		if got.QueuedAt == nil || !got.QueuedAt.Equal(got.UpdatedAt) {
			t.Errorf("queued_at = %v, updated_at = %v", got.QueuedAt, got.UpdatedAt)
		}
	}
}

func TestPublishLane_ErrorRollsBack(t *testing.T) {
	s, _ := newTestStore(t, zeroDelay)
	n := create(t, s)
	errConfirm := errors.New("confirm timeout")
	events, err := s.PublishLane(ctxT(t), smsNormal, 10, func(context.Context, []DueRow) ([]uuid.UUID, error) {
		return nil, errConfirm
	})
	if !errors.Is(err, errConfirm) || events != nil {
		t.Fatalf("events %v, err %v", events, err)
	}
	if got := get(t, s, n.ID); !reflect.DeepEqual(got, n) {
		t.Errorf("row changed after rollback:\n%+v\n%+v", n, got)
	}
}

func TestPublishLane_RefusesUnselectedIDs(t *testing.T) {
	s, _ := newTestStore(t, zeroDelay)
	n := create(t, s)
	other := create(t, s, withLane(domain.ChannelEmail, domain.PriorityNormal))
	for name, extra := range map[string]uuid.UUID{"random id": uuid.New(), "row of another lane": other.ID} {
		t.Run(name, func(t *testing.T) {
			_, err := s.PublishLane(ctxT(t), smsNormal, 10, func(context.Context, []DueRow) ([]uuid.UUID, error) {
				return []uuid.UUID{n.ID, extra}, nil
			})
			if err == nil {
				t.Fatal("expected an error for an id that was not selected")
			}
			for _, id := range []uuid.UUID{n.ID, other.ID} {
				if got := get(t, s, id); got.Status != domain.StatusPending || got.Version != 1 {
					t.Errorf("row %s changed: %s v%d", id, got.Status, got.Version)
				}
			}
		})
	}
}

func TestPublishLane_LimitAndDueOrder(t *testing.T) {
	s, _ := newTestStore(t, zeroDelay)
	now := time.Now()
	// Created out of due_at order; scheduled in the past so all are due.
	offsets := []time.Duration{-3000, -5000, -1000, -4000, -2000} // milliseconds
	byOffset := map[time.Duration]uuid.UUID{}
	for _, o := range offsets {
		n := create(t, s, func(n *NewNotification) { n.ScheduledAt = ptr(now.Add(o * time.Millisecond)) })
		byOffset[o] = n.ID
	}
	var got []uuid.UUID
	_, err := s.PublishLane(ctxT(t), smsNormal, 3, func(_ context.Context, due []DueRow) ([]uuid.UUID, error) {
		got = ids(due)
		return got, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []uuid.UUID{byOffset[-5000], byOffset[-4000], byOffset[-3000]}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("selected %v, want %v (oldest due_at first, limit 3)", got, want)
	}
}

func TestPublishLane_TakesDueRetryingRows(t *testing.T) {
	s, _ := newTestStore(t, zeroDelay)
	n := create(t, s)
	claim(t, s, n.ID)
	if _, err := s.CompleteFailure(ctxT(t), n.ID, 1, AttemptResult{Outcome: domain.OutcomeRetryable}, 0); err != nil {
		t.Fatal(err)
	}
	before := get(t, s, n.ID)
	if before.Status != domain.StatusRetrying {
		t.Fatalf("setup: %s", before.Status)
	}
	events, err := s.PublishLane(ctxT(t), smsNormal, 10, publishAll)
	if err != nil || len(events) != 1 {
		t.Fatalf("events %+v err %v", events, err)
	}
	checkTransition(t, before, get(t, s, n.ID), events[0], domain.StatusQueued)
}

func TestPublishLane_SkipsRowsNotDueAndOtherLanes(t *testing.T) {
	s, _ := newTestStore(t, delayed)
	create(t, s, func(n *NewNotification) { n.ScheduledAt = ptr(time.Now().Add(time.Hour)) })
	create(t, s, withLane(domain.ChannelSMS, domain.PriorityHigh))
	create(t, s, withLane(domain.ChannelPush, domain.PriorityNormal))
	retrying := create(t, s)
	claim(t, s, retrying.ID)
	if _, err := s.CompleteFailure(ctxT(t), retrying.ID, 1, AttemptResult{Outcome: domain.OutcomeRetryable}, 0); err != nil {
		t.Fatal(err)
	}
	called := false
	events, err := s.PublishLane(ctxT(t), smsNormal, 10, func(context.Context, []DueRow) ([]uuid.UUID, error) {
		called = true
		return nil, nil
	})
	if err != nil || events != nil || called {
		t.Errorf("events %v err %v publish called %v; want nothing selected", events, err, called)
	}
}

func TestPublishLane_EmptyPublishedChangesNothing(t *testing.T) {
	s, _ := newTestStore(t, zeroDelay)
	n := create(t, s)
	events, err := s.PublishLane(ctxT(t), smsNormal, 10, func(context.Context, []DueRow) ([]uuid.UUID, error) {
		return nil, nil
	})
	if err != nil || len(events) != 0 {
		t.Fatalf("events %v err %v", events, err)
	}
	if got := get(t, s, n.ID); !reflect.DeepEqual(got, n) {
		t.Error("row changed")
	}
}

// A second lane transaction running while the first holds its row locks
// skips the locked rows (FOR UPDATE SKIP LOCKED).
func TestPublishLane_ConcurrentLaneTransactionSkipsLockedRows(t *testing.T) {
	s, _ := newTestStore(t, zeroDelay)
	all := map[uuid.UUID]bool{}
	for range 5 {
		all[create(t, s).ID] = true
	}
	var first, second []uuid.UUID
	_, err := s.PublishLane(ctxT(t), smsNormal, 2, func(ctx context.Context, due []DueRow) ([]uuid.UUID, error) {
		first = ids(due)
		_, err := s.PublishLane(ctx, smsNormal, 10, func(_ context.Context, due2 []DueRow) ([]uuid.UUID, error) {
			second = ids(due2)
			return second, nil
		})
		return first, err
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 || len(second) != 3 {
		t.Fatalf("first %d rows, second %d rows; want 2 and 3", len(first), len(second))
	}
	seen := map[uuid.UUID]bool{}
	for _, id := range append(append([]uuid.UUID{}, first...), second...) {
		if seen[id] || !all[id] {
			t.Errorf("id %s selected twice or unknown", id)
		}
		seen[id] = true
		if got := get(t, s, id); got.Status != domain.StatusQueued {
			t.Errorf("row %s is %s", id, got.Status)
		}
	}
}

func TestPublishLane_InvalidLimit(t *testing.T) {
	s, _ := newTestStore(t, zeroDelay)
	if _, err := s.PublishLane(ctxT(t), smsNormal, 0, publishAll); err == nil {
		t.Error("limit 0 must be refused")
	}
}

func TestQueuedCounts(t *testing.T) {
	s, _ := newTestStore(t, zeroDelay)
	lanes := map[Lane]int{
		{domain.ChannelSMS, domain.PriorityHigh}:     3,
		{domain.ChannelSMS, domain.PriorityLow}:      1,
		{domain.ChannelEmail, domain.PriorityNormal}: 2,
		{domain.ChannelPush, domain.PriorityHigh}:    0, // pending only
		{domain.ChannelEmail, domain.PriorityHigh}:   0,
		{domain.ChannelPush, domain.PriorityNormal}:  0,
		{domain.ChannelEmail, domain.PriorityLow}:    0,
		{domain.ChannelSMS, domain.PriorityNormal}:   0,
		{domain.ChannelPush, domain.PriorityLow}:     0,
	}
	for lane, queued := range lanes {
		for range queued + 1 { // one extra row per lane stays pending
			create(t, s, withLane(lane.Channel, lane.Priority))
		}
		if queued == 0 {
			continue
		}
		_, err := s.PublishLane(ctxT(t), lane, queued, publishAll)
		if err != nil {
			t.Fatal(err)
		}
	}
	// A processing row is not queued.
	p := create(t, s, withLane(domain.ChannelSMS, domain.PriorityHigh))
	claim(t, s, p.ID)

	got, err := s.QueuedCounts(ctxT(t))
	if err != nil {
		t.Fatal(err)
	}
	want := map[Lane]int{}
	for lane, q := range lanes {
		if q > 0 {
			want[lane] = q
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("QueuedCounts = %v, want %v", got, want)
	}
}
