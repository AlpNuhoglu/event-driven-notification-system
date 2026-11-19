package store

import (
	"context"
	"fmt"
	"time"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store/db"
)

// LaneCount is a number of notifications of one lane. A lane with none has
// no element in the result of the function that returns it.
type LaneCount struct {
	Channel  domain.Channel
	Priority domain.Priority
	Count    int64
}

// Percentiles is the median and the 95th percentile of a set of durations in
// milliseconds. Both are nil when the set is empty.
type Percentiles struct {
	P50, P95 *float64
}

// ChannelWindow is what the attempts of one channel that finished within a
// window add up to.
type ChannelWindow struct {
	// Outcomes counts the finished attempts by outcome; an outcome that did
	// not occur is absent.
	Outcomes map[domain.Outcome]int64
	// ProviderLatency covers the attempts that recorded a latency.
	ProviderLatency Percentiles
	// EndToEnd covers the notifications that have a successful attempt in
	// the window: sent_at minus scheduled_at, or created_at when the
	// notification was not scheduled.
	EndToEnd Percentiles
}

// SummaryQueued returns the number of notifications in state queued of each
// lane that has any.
func (s *Store) SummaryQueued(ctx context.Context) ([]LaneCount, error) {
	rows, err := s.q.SummaryQueuedByLane(ctx)
	if err != nil {
		return nil, fmt.Errorf("summary: queued by lane: %w", err)
	}
	out := make([]LaneCount, len(rows))
	for i, r := range rows {
		out[i] = LaneCount{Channel: domain.Channel(r.Channel), Priority: domain.Priority(r.Priority), Count: r.N}
	}
	return out, nil
}

// SummaryBacklogDue returns, for each lane that has any, the number of
// notifications in state pending or retrying whose due_at is not after now.
func (s *Store) SummaryBacklogDue(ctx context.Context, now time.Time) ([]LaneCount, error) {
	rows, err := s.q.SummaryBacklogDueByLane(ctx, now)
	if err != nil {
		return nil, fmt.Errorf("summary: backlog by lane: %w", err)
	}
	out := make([]LaneCount, len(rows))
	for i, r := range rows {
		out[i] = LaneCount{Channel: domain.Channel(r.Channel), Priority: domain.Priority(r.Priority), Count: r.N}
	}
	return out, nil
}

// SummaryWindow adds up, for each of the three channels, the attempts that
// finished at or after since. Every channel has an entry.
func (s *Store) SummaryWindow(ctx context.Context, since time.Time) (map[domain.Channel]ChannelWindow, error) {
	channels := make([]string, 0, len(domain.Channels()))
	out := make(map[domain.Channel]ChannelWindow, len(domain.Channels()))
	for _, c := range domain.Channels() {
		channels = append(channels, string(c))
		out[c] = ChannelWindow{Outcomes: map[domain.Outcome]int64{}}
	}
	counts, err := s.q.SummaryAttemptsByOutcome(ctx, db.SummaryAttemptsByOutcomeParams{Channels: channels, Since: since})
	if err != nil {
		return nil, fmt.Errorf("summary: attempts by outcome: %w", err)
	}
	for _, r := range counts {
		if w, ok := out[domain.Channel(r.Channel)]; ok {
			w.Outcomes[domain.Outcome(r.Outcome)] = r.N
		}
	}
	prov, err := s.q.SummaryProviderLatency(ctx, db.SummaryProviderLatencyParams{Channels: channels, Since: since})
	if err != nil {
		return nil, fmt.Errorf("summary: provider latency: %w", err)
	}
	for _, r := range prov {
		if w, ok := out[domain.Channel(r.Channel)]; ok {
			w.ProviderLatency = Percentiles{P50: &r.P50, P95: &r.P95}
			out[domain.Channel(r.Channel)] = w
		}
	}
	e2e, err := s.q.SummaryEndToEndLatency(ctx, db.SummaryEndToEndLatencyParams{Channels: channels, Since: since})
	if err != nil {
		return nil, fmt.Errorf("summary: end-to-end latency: %w", err)
	}
	for _, r := range e2e {
		if w, ok := out[domain.Channel(r.Channel)]; ok {
			w.EndToEnd = Percentiles{P50: &r.P50, P95: &r.P95}
			out[domain.Channel(r.Channel)] = w
		}
	}
	return out, nil
}
