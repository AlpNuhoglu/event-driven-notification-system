package store

import (
	"context"
	"fmt"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store/db"
)

// SweepExpiredLeases performs transition 7 (ADR-0003 section 4) in one
// transaction: it locks up to limit processing rows whose lease has expired
// (FOR UPDATE SKIP LOCKED, so concurrent sweeps never take the same row), and
// for each one marks the open attempt unknown, asks domain.DecideRetry with
// OutcomeUnknown and the database now(), and applies MarkRetrying or
// MarkFailed. jitter is called once per row and returns a number in [0, 1).
// The events are returned after COMMIT.
//
// Lock order: the notifications row is locked by the select before its
// attempt row is touched, as in the worker's transactions.
func (s *Store) SweepExpiredLeases(ctx context.Context, limit int, jitter func() float64) ([]domain.StatusChanged, error) {
	batchLimit, err := toInt32(limit)
	if err != nil || batchLimit <= 0 {
		return nil, fmt.Errorf("sweep leases: invalid limit %d", limit)
	}
	var events []domain.StatusChanged
	err = s.inTx(ctx, func(q *db.Queries) error {
		rows, err := q.LockExpiredLeases(ctx, batchLimit)
		if err != nil {
			return fmt.Errorf("lock expired leases: %w", err)
		}
		events = make([]domain.StatusChanged, 0, len(rows))
		for _, r := range rows {
			// Zero rows here would mean a processing row without an in_flight
			// attempt; the row is still recovered, so the count is not checked.
			if _, err := q.MarkAttemptUnknown(ctx, db.MarkAttemptUnknownParams{
				NotificationID: r.ID,
				AttemptNo:      r.AttemptCount,
			}); err != nil {
				return fmt.Errorf("mark attempt unknown: %w", err)
			}
			prof, err := s.profile(domain.Priority(r.Priority))
			if err != nil {
				return err
			}
			decision := s.decide(domain.OutcomeUnknown, int(r.Failures), int(r.MaxFailures), r.ExpiresAt,
				0, prof, r.DbNow, jitter())
			row, err := applyDecision(ctx, q, r.ID, r.AttemptCount, decision)
			if err != nil {
				return err
			}
			events = append(events, fromRow(row).Event())
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("sweep leases: %w", err)
	}
	return events, nil
}
