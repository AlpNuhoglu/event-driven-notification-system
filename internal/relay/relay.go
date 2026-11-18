// Package relay is the outbox relay (ADR-0002, ADR-0003): it publishes due
// notification rows from Postgres to RabbitMQ lane by lane, below each lane's
// high-water mark, and recovers notifications whose lease has expired.
package relay

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/events"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/queue"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
)

// Store is the part of the store the relay uses.
type Store interface {
	QueuedCounts(ctx context.Context) (map[store.Lane]int, error)
	PublishLane(ctx context.Context, lane store.Lane, limit int, publish store.PublishFunc) ([]domain.StatusChanged, error)
	SweepExpiredLeases(ctx context.Context, limit int, jitter func() float64) ([]domain.StatusChanged, error)
}

// Publisher publishes one batch and returns the ids that were confirmed and
// not returned (queue.Publisher).
type Publisher interface {
	PublishBatch(ctx context.Context, msgs []queue.Message, confirmTimeout time.Duration) ([]uuid.UUID, error)
}

// Connection tells whether the broker connection is open (queue.Connection).
type Connection interface {
	IsOpen() bool
}

// Config holds the relay's settings; every duration comes from configuration.
type Config struct {
	TickInterval   time.Duration // RELAY_TICK_INTERVAL
	BatchSize      int           // RELAY_BATCH_SIZE
	ConfirmTimeout time.Duration // RELAY_CONFIRM_TIMEOUT
	HWMHigh        int           // LANE_HWM_HIGH
	HWMNormal      int           // LANE_HWM_NORMAL
	HWMLow         int           // LANE_HWM_LOW

	LeaseSweepInterval time.Duration // LEASE_SWEEP_INTERVAL
	LeaseSweepBatch    int           // LEASE_SWEEP_BATCH_SIZE
}

func (c Config) hwm(p domain.Priority) int {
	switch p {
	case domain.PriorityHigh:
		return c.HWMHigh
	case domain.PriorityNormal:
		return c.HWMNormal
	default:
		return c.HWMLow
	}
}

// Relay runs the publishing cycle and the lease sweeper.
type Relay struct {
	store  Store
	pub    Publisher
	conn   Connection
	events events.Publisher
	logger *slog.Logger
	cfg    Config
	jitter func() float64
}

// New returns a Relay. jitter returns a number in [0, 1) for the retry
// decision of the lease sweeper.
func New(st Store, pub Publisher, conn Connection, ev events.Publisher, logger *slog.Logger, cfg Config, jitter func() float64) *Relay {
	return &Relay{store: st, pub: pub, conn: conn, events: ev, logger: logger, cfg: cfg, jitter: jitter}
}

// RunPublishing runs the publishing cycle every TickInterval until ctx is
// cancelled. A cycle in progress finishes its current lane transaction and
// then stops.
func (r *Relay) RunPublishing(ctx context.Context) error {
	t := time.NewTicker(r.cfg.TickInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			r.cycle(ctx)
		}
	}
}

// cycle is one tick of the publishing cycle (ADR-0002, ADR-0003 §1): one
// count query, then every channel and, within a channel, the priorities
// high, normal, low. A lane's limit is min(BatchSize, HWM - queued); a lane
// without headroom is skipped. A lane's error is logged and the cycle goes
// on; nothing is retried within the tick.
//
// While the broker connection is not open no transaction is started. The
// lane transaction runs on a context that shutdown does not cancel, so a
// lane that has started is finished; no further lane starts after ctx is
// cancelled.
func (r *Relay) cycle(ctx context.Context) {
	if ctx.Err() != nil || !r.conn.IsOpen() {
		return
	}
	queued, err := r.store.QueuedCounts(ctx)
	if err != nil {
		r.logger.Warn("relay: count queued rows failed", "error", err)
		return
	}
	for _, c := range domain.Channels() {
		for _, p := range domain.Priorities() {
			if ctx.Err() != nil || !r.conn.IsOpen() {
				return
			}
			lane := store.Lane{Channel: c, Priority: p}
			limit := min(r.cfg.BatchSize, r.cfg.hwm(p)-queued[lane])
			if limit <= 0 {
				continue
			}
			evs, err := r.store.PublishLane(context.WithoutCancel(ctx), lane, limit, r.publishFunc(lane))
			if err != nil {
				r.logger.Warn("relay: lane failed", "channel", string(c), "priority", p.String(), "error", err)
				continue
			}
			for _, e := range evs {
				r.events.Publish(e)
			}
		}
	}
}

// publishFunc publishes the rows of one lane with the lane's channel and
// priority.
func (r *Relay) publishFunc(lane store.Lane) store.PublishFunc {
	return func(ctx context.Context, rows []store.DueRow) ([]uuid.UUID, error) {
		msgs := make([]queue.Message, len(rows))
		for i, row := range rows {
			msgs[i] = queue.Message{ID: row.ID, Channel: lane.Channel, Priority: lane.Priority, TraceParent: row.TraceParent}
		}
		return r.pub.PublishBatch(ctx, msgs, r.cfg.ConfirmTimeout)
	}
}

// RunLeaseSweeper runs the lease sweeper (ADR-0003 §4, transition 7) every
// LeaseSweepInterval until ctx is cancelled.
func (r *Relay) RunLeaseSweeper(ctx context.Context) error {
	t := time.NewTicker(r.cfg.LeaseSweepInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			r.sweep(ctx)
		}
	}
}

func (r *Relay) sweep(ctx context.Context) {
	evs, err := r.store.SweepExpiredLeases(ctx, r.cfg.LeaseSweepBatch, r.jitter)
	if err != nil {
		if ctx.Err() == nil {
			r.logger.Warn("relay: lease sweep failed", "error", err)
		}
		return
	}
	for _, e := range evs {
		r.events.Publish(e)
	}
}
