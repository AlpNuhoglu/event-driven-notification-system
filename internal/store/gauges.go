package store

import (
	"context"
	"fmt"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

// LaneGauge is what the relay's gauges show of one lane.
type LaneGauge struct {
	// Queued is the number of queued notifications (lane_queued).
	Queued int
	// OldestQueuedAge is the age of the oldest queued notification in
	// seconds on the database clock, 0 when nothing is queued
	// (lane_oldest_queued_age_seconds).
	OldestQueuedAge float64
	// BacklogDue is the number of pending and retrying notifications that
	// are due (backlog_due).
	BacklogDue int
}

// Gauges is one measurement of the relay's gauges.
type Gauges struct {
	// Lanes holds the lanes that have queued, due or future rows; a lane
	// that is absent has none.
	Lanes map[Lane]LaneGauge
	// ScheduledFuture is the number of pending and retrying notifications
	// that are due later: scheduled ones and retries waiting for their
	// back-off (scheduled_future).
	ScheduledFuture int
}

// Gauges measures the relay's gauges with two statements outside any
// transaction. Each reads only unfinished rows, through one of the partial
// indexes that hold them (the lane's own, or the expiry index), never the
// finished rows that make up most of the table. Like QueuedCounts, both run
// as the unnamed statement (customPlanDB): they have no parameters, and a
// plan cached while the table was small would outlive a burst (D79).
func (s *Store) Gauges(ctx context.Context) (Gauges, error) {
	queued, err := s.custom.QueuedLaneGauges(ctx)
	if err != nil {
		return Gauges{}, fmt.Errorf("queued gauges: %w", err)
	}
	backlog, err := s.custom.BacklogLaneGauges(ctx)
	if err != nil {
		return Gauges{}, fmt.Errorf("backlog gauges: %w", err)
	}
	out := Gauges{Lanes: make(map[Lane]LaneGauge, len(queued))}
	for _, r := range queued {
		lane := Lane{Channel: domain.Channel(r.Channel), Priority: domain.Priority(r.Priority)}
		g := out.Lanes[lane]
		g.Queued, g.OldestQueuedAge = int(r.Queued), r.OldestAgeSeconds
		out.Lanes[lane] = g
	}
	for _, r := range backlog {
		lane := Lane{Channel: domain.Channel(r.Channel), Priority: domain.Priority(r.Priority)}
		g := out.Lanes[lane]
		g.BacklogDue = int(r.Due)
		out.Lanes[lane] = g
		out.ScheduledFuture += int(r.Future)
	}
	return out, nil
}
