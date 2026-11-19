package store

import (
	"context"
	"fmt"
	"time"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store/db"
)

// ReconcileQueued performs transition 8 for every queued notification, in
// one statement (ADR-0003 section 3, D5): each one returns to pending
// (version + 1, updated_at, queued_at cleared), so that the relay's normal
// cycle publishes it again. It never publishes. dedupe_key, attempt_count,
// failures, due_at and expires_at do not change.
//
// Rows another transaction holds (a claim, a cancel, a batch cancel) are
// waited for, not skipped; after the wait, a row that is no longer queued is
// left alone. The rows are locked in id order, the order of the batch
// cancel, so the two cannot deadlock; see the query for the other writers.
//
// One event per row is returned after the statement has committed. It is
// also requeue-stale --all (D64).
func (s *Store) ReconcileQueued(ctx context.Context) ([]domain.StatusChanged, error) {
	rows, err := s.q.ReconcileQueued(ctx)
	if err != nil {
		return nil, fmt.Errorf("reconcile queued: %w", err)
	}
	events := make([]domain.StatusChanged, len(rows))
	for i, r := range rows {
		events[i] = reconciledEvent(r)
	}
	return events, nil
}

// RequeueStale performs transition 8 for the manual requeue-stale command
// (ADR-0003 section 3, D64): only the queued notifications whose queued_at
// is older than olderThan return to pending, otherwise exactly as
// ReconcileQueued. The number of rows is len of the returned events, one
// per row, returned after the statement has committed.
func (s *Store) RequeueStale(ctx context.Context, olderThan time.Duration) ([]domain.StatusChanged, error) {
	if olderThan < 0 {
		return nil, fmt.Errorf("requeue stale: negative threshold %s", olderThan)
	}
	rows, err := s.q.RequeueStale(ctx, interval(olderThan))
	if err != nil {
		return nil, fmt.Errorf("requeue stale: %w", err)
	}
	events := make([]domain.StatusChanged, len(rows))
	for i, r := range rows {
		events[i] = reconciledEvent(db.ReconcileQueuedRow(r))
	}
	return events, nil
}

// reconciledEvent builds the event of transition 8 from the columns the
// statement returns, which are exactly those of the event.
func reconciledEvent(r db.ReconcileQueuedRow) domain.StatusChanged {
	var reason *domain.FailureReason
	if r.FailureReason != nil {
		fr := domain.FailureReason(*r.FailureReason)
		reason = &fr
	}
	return domain.StatusChanged{
		ID:            r.ID,
		BatchID:       r.BatchID,
		Channel:       domain.Channel(r.Channel),
		Priority:      domain.Priority(r.Priority),
		Status:        domain.Status(r.Status),
		Version:       r.Version,
		FailureReason: reason,
		AttemptCount:  int(r.AttemptCount),
		UpdatedAt:     r.UpdatedAt,
	}
}
