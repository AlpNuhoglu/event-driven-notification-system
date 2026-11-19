package store

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

// SnapshotByIDs returns the current state of each notification in ids as
// the StatusChanged value of its last transition: the gateway's snapshot on
// subscribe (ADR-0005 "Gateway"). An id that names no notification has no
// element; the order is unspecified.
func (s *Store) SnapshotByIDs(ctx context.Context, ids []uuid.UUID) ([]domain.StatusChanged, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := s.q.SnapshotByIDs(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("snapshot by ids: %w", err)
	}
	out := make([]domain.StatusChanged, 0, len(rows))
	for _, r := range rows {
		out = append(out, snapshotEvent(r.ID, r.BatchID, r.Channel, r.Priority, r.Status, r.Version,
			r.FailureReason, r.AttemptCount, r.UpdatedAt))
	}
	return out, nil
}

// SnapshotByBatch returns the current state of every notification of the
// batch, like SnapshotByIDs. A batch id that names no batch has none.
func (s *Store) SnapshotByBatch(ctx context.Context, batchID uuid.UUID) ([]domain.StatusChanged, error) {
	rows, err := s.q.SnapshotByBatch(ctx, batchID)
	if err != nil {
		return nil, fmt.Errorf("snapshot by batch: %w", err)
	}
	out := make([]domain.StatusChanged, 0, len(rows))
	for _, r := range rows {
		out = append(out, snapshotEvent(r.ID, r.BatchID, r.Channel, r.Priority, r.Status, r.Version,
			r.FailureReason, r.AttemptCount, r.UpdatedAt))
	}
	return out, nil
}

// snapshotEvent builds the StatusChanged value of one snapshot row. The two
// generated row types have the same columns but are distinct types.
func snapshotEvent(id uuid.UUID, batchID *uuid.UUID, channel string, priority int16, status string,
	version int64, failureReason *string, attemptCount int32, updatedAt time.Time,
) domain.StatusChanged {
	var reason *domain.FailureReason
	if failureReason != nil {
		fr := domain.FailureReason(*failureReason)
		reason = &fr
	}
	return domain.StatusChanged{
		ID:            id,
		BatchID:       batchID,
		Channel:       domain.Channel(channel),
		Priority:      domain.Priority(priority),
		Status:        domain.Status(status),
		Version:       version,
		FailureReason: reason,
		AttemptCount:  int(attemptCount),
		UpdatedAt:     updatedAt,
	}
}
