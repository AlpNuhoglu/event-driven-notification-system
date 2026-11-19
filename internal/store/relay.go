package store

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store/db"
)

// Lane is a channel × priority pair, the unit of the relay's high-water mark.
type Lane struct {
	Channel  domain.Channel
	Priority domain.Priority
}

// QueuedCounts returns the number of queued notifications per lane. Lanes
// without queued rows are absent from the map.
//
// The count runs as the unnamed statement (customPlanDB): it has no
// parameters, and Postgres reuses the one plan of such a prepared statement
// under every plan_cache_mode, so a plan cached while the table was small
// would outlive a burst until the next ANALYZE (D79).
func (s *Store) QueuedCounts(ctx context.Context) (map[Lane]int, error) {
	rows, err := s.custom.CountQueuedByLane(ctx)
	if err != nil {
		return nil, fmt.Errorf("count queued: %w", err)
	}
	out := make(map[Lane]int, len(rows))
	for _, r := range rows {
		out[Lane{Channel: domain.Channel(r.Channel), Priority: domain.Priority(r.Priority)}] = int(r.Queued)
	}
	return out, nil
}

// DueRow is a notification selected by the relay for publishing.
type DueRow struct {
	ID          uuid.UUID
	TraceParent string
}

// PublishFunc publishes the rows and returns the ids that were confirmed and
// not returned by the broker.
type PublishFunc func(ctx context.Context, rows []DueRow) (published []uuid.UUID, err error)

// PublishLane runs one lane transaction of the relay cycle (transitions 1
// and 2, ADR-0002): BEGIN; lock up to limit due rows of the lane with
// FOR UPDATE SKIP LOCKED; publish(rows); mark the published ids queued; COMMIT.
//
//   - No due rows: publish is not called and (nil, nil) is returned.
//   - publish returns an error: the transaction rolls back, the error is
//     returned and every row keeps its status.
//   - published must be a subset (possibly empty) of the selected ids; an id
//     that was not selected in this transaction is an error and rolls back.
//     Rows not in published keep their status.
//   - The events are returned only after a successful COMMIT.
//
// The row locks are held while publish runs. That is deliberate (ADR-0002):
// a worker that receives a message before the commit waits on the lock in its
// claim instead of dropping the message.
func (s *Store) PublishLane(ctx context.Context, lane Lane, limit int, publish PublishFunc) ([]domain.StatusChanged, error) {
	batchLimit, err := toInt32(limit)
	if err != nil || batchLimit <= 0 {
		return nil, fmt.Errorf("publish lane: invalid limit %d", limit)
	}
	var events []domain.StatusChanged
	err = s.inTx(ctx, func(q *db.Queries) error {
		due, err := q.LockDueLane(ctx, db.LockDueLaneParams{
			Channel:    string(lane.Channel),
			Priority:   int16(lane.Priority),
			BatchLimit: batchLimit,
		})
		if err != nil {
			return fmt.Errorf("lock due lane: %w", err)
		}
		if len(due) == 0 {
			return nil
		}
		rows := make([]DueRow, len(due))
		selected := make(map[uuid.UUID]bool, len(due))
		for i, d := range due {
			rows[i] = DueRow{ID: d.ID, TraceParent: d.TraceParent}
			selected[d.ID] = true
		}

		published, err := publish(ctx, rows)
		if err != nil {
			return fmt.Errorf("publish: %w", err)
		}

		ids := make([]uuid.UUID, 0, len(published))
		seen := make(map[uuid.UUID]bool, len(published))
		for _, id := range published {
			if !selected[id] {
				return fmt.Errorf("publish lane: id %s was not selected in this transaction", id)
			}
			if !seen[id] {
				seen[id] = true
				ids = append(ids, id)
			}
		}
		if len(ids) == 0 {
			return nil
		}
		marked, err := q.MarkQueued(ctx, ids)
		if err != nil {
			return fmt.Errorf("mark queued: %w", err)
		}
		// The rows are locked by this transaction and were pending or
		// retrying when selected, so every id must have been updated.
		if len(marked) != len(ids) {
			return fmt.Errorf("mark queued: updated %d of %d locked rows", len(marked), len(ids))
		}
		events = make([]domain.StatusChanged, len(marked))
		for i, r := range marked {
			events[i] = fromRow(r).Event()
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return events, nil
}
