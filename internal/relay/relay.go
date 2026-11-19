// Package relay is the outbox relay (ADR-0002, ADR-0003): it publishes due
// notification rows from Postgres to RabbitMQ lane by lane, below each lane's
// high-water mark, recovers notifications whose lease has expired, fails
// notifications whose expires_at has passed (transition 10), returns every
// queued notification to pending on each (re)connection to RabbitMQ
// (reconciliation, transition 8), deletes expired idempotency keys (D78) and
// measures the gauges of the lanes and of the backlog (ADR-0009 §1).
package relay

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/events"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/logging"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/tracing"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/queue"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
)

// Store is the part of the store the relay uses.
type Store interface {
	QueuedCounts(ctx context.Context) (map[store.Lane]int, error)
	PublishLane(ctx context.Context, lane store.Lane, limit int, publish store.PublishFunc) ([]domain.StatusChanged, error)
	SweepExpiredLeases(ctx context.Context, limit int, jitter func() float64) ([]domain.StatusChanged, error)
	ExpireDue(ctx context.Context, limit int) ([]domain.StatusChanged, error)
	DeleteExpiredIdempotencyKeys(ctx context.Context) (int, error)
	ReconcileQueued(ctx context.Context) ([]domain.StatusChanged, error)
	Gauges(ctx context.Context) (store.Gauges, error)
}

// Publisher publishes one batch and returns the ids that were confirmed and
// not returned (queue.Publisher).
type Publisher interface {
	PublishBatch(ctx context.Context, msgs []queue.Message, confirmTimeout time.Duration) ([]uuid.UUID, error)
}

// Connection tells whether the broker connection is open and counts its
// (re)connections (queue.Connection).
type Connection interface {
	IsOpen() bool
	Opens() (uint64, <-chan struct{})
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

	ExpiryInterval             time.Duration // EXPIRY_INTERVAL
	ExpiryBatch                int           // EXPIRY_BATCH_SIZE
	IdempotencyCleanupInterval time.Duration // IDEMPOTENCY_CLEANUP_INTERVAL

	ReconcileMinInterval time.Duration // RECONCILE_MIN_INTERVAL

	// GaugeInterval is the cadence of the gauge measurement
	// (BACKLOG_GAUGE_INTERVAL); zero or less runs no measurement.
	GaugeInterval time.Duration

	// TracerProvider records the publish spans; the global provider when
	// nil. Not configuration: tests pass their own.
	TracerProvider trace.TracerProvider
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

// Relay runs the publishing cycle with reconciliation, the lease sweeper,
// the expiry pass and the idempotency-key cleanup.
type Relay struct {
	store   Store
	pub     Publisher
	conn    Connection
	events  events.Publisher
	logger  *slog.Logger
	cfg     Config
	jitter  func() float64
	metrics *Metrics
	now     func() time.Time // the clock of the reconciliation window
	tracer  trace.Tracer
}

// New returns a Relay. jitter returns a number in [0, 1) for the retry
// decision of the lease sweeper. m receives the relay's metrics; nil means
// metrics that are not registered anywhere. The final transitions the relay
// hands to ev are counted on their way (notifications_finished_total).
func New(st Store, pub Publisher, conn Connection, ev events.Publisher, logger *slog.Logger, cfg Config, jitter func() float64, m *Metrics) *Relay {
	if m == nil {
		m, _ = NewMetrics(nil) // cannot fail without a registry
	}
	return &Relay{
		store: st, pub: pub, conn: conn, events: m.finished.Publisher(ev), logger: logger,
		cfg: cfg, jitter: jitter, metrics: m, now: time.Now,
		tracer: tracing.Tracer(cfg.TracerProvider, "internal/relay"),
	}
}

// RunPublishing runs the publishing cycle every TickInterval until ctx is
// cancelled, with reconciliation (transition 8) before a cycle when it is
// due. A cycle in progress finishes its current lane transaction and then
// stops. It returns an error only when the relay cannot go on publishing:
// the publisher's channel is closed (the role then exits and is restarted,
// like after a recovery that gave up).
func (r *Relay) RunPublishing(ctx context.Context) error {
	t := time.NewTicker(r.cfg.TickInterval)
	defer t.Stop()
	var rec reconciliation
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			// Between two cycles, in this goroutine: see reconcileIfDue.
			r.reconcileIfDue(ctx, &rec)
			if err := r.cycle(ctx); err != nil {
				return err
			}
		}
	}
}

// reconciliation is the publishing loop's state of transition 8 (ADR-0003
// §3, D5).
type reconciliation struct {
	handled uint64    // the (re)connections already taken into account (Connection.Opens)
	pending bool      // a trigger that no successful run has served yet
	last    time.Time // when the last successful run started; zero before the first
}

// reconcileIfDue returns every queued notification to pending (transition
// 8) when a (re)connection to RabbitMQ has not been served yet and the last
// run started at least ReconcileMinInterval ago. It never publishes: the
// next cycles publish the rows again, and a copy still in the broker is
// dropped by the claim guard.
//
// Invariants:
//   - Every (re)connection triggers a run, the first one after start-up
//     included: the count of Opens only grows and is compared with the count
//     already handled, so a reconnection between two ticks is not missed,
//     and several are one trigger.
//   - A trigger inside the window is deferred, not dropped: pending stays
//     set until a run succeeds, which happens at the first tick after the
//     window ends.
//   - A failed run leaves pending set and last unchanged: it is retried at
//     the next tick and does not count as a run for the window.
//   - It runs in the publishing loop's goroutine, so never while a lane
//     transaction is open, and no lane starts while it runs. This is what
//     makes it correct: the statement reconciles the rows that are queued in
//     its snapshot, and a lane that committed queued just after that
//     snapshot was taken would be missed; if that lane had published on the
//     lost connection, its row would stay queued with no copy in the broker.
//     Serialised with the lanes (and with a single relay process, D4), every
//     lane has committed or rolled back before the statement starts.
//   - Like a lane, a run that has started is finished on shutdown (its
//     context is not cancelled); no run starts after ctx is cancelled.
func (r *Relay) reconcileIfDue(ctx context.Context, s *reconciliation) {
	if n, _ := r.conn.Opens(); n > s.handled {
		s.handled, s.pending = n, true
	}
	if !s.pending || ctx.Err() != nil {
		return
	}
	now := r.now()
	if !s.last.IsZero() && now.Sub(s.last) < r.cfg.ReconcileMinInterval {
		return // deferred to the end of the window
	}
	reason := "reconnection"
	if s.last.IsZero() {
		reason = "first connection"
	}
	evs, err := r.store.ReconcileQueued(context.WithoutCancel(ctx))
	if err != nil {
		r.logger.Warn("relay: reconciliation failed; retrying at the next tick", "reason", reason, "error", err)
		return
	}
	s.pending, s.last = false, now
	r.metrics.reconciliationRuns.Inc()
	r.metrics.reconciledRows.Add(float64(len(evs)))
	r.logger.Info("relay: reconciliation ran", "rows", len(evs), "reason", reason)
	for _, e := range evs { // after the commit, as for every transition
		r.events.Publish(e)
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
//
// The only error cycle returns is a closed publisher channel: no later batch
// could be checked for unroutable messages, so the relay stops.
func (r *Relay) cycle(ctx context.Context) error {
	if ctx.Err() != nil || !r.conn.IsOpen() {
		return nil
	}
	queued, err := r.store.QueuedCounts(ctx)
	if err != nil {
		r.logger.Warn("relay: count queued rows failed", "error", err)
		return nil
	}
	for _, c := range domain.Channels() {
		for _, p := range domain.Priorities() {
			if ctx.Err() != nil || !r.conn.IsOpen() {
				return nil
			}
			lane := store.Lane{Channel: c, Priority: p}
			limit := min(r.cfg.BatchSize, r.cfg.hwm(p)-queued[lane])
			if limit <= 0 {
				continue
			}
			evs, err := r.store.PublishLane(context.WithoutCancel(ctx), lane, limit, r.publishFunc(lane))
			if errors.Is(err, queue.ErrPublisherChannelClosed) {
				return fmt.Errorf("relay: lane %s/%s: %w", c, p, err)
			}
			if err != nil {
				r.logger.Warn("relay: lane failed", "channel", string(c), "priority", p.String(), "error", err)
				continue
			}
			r.metrics.publishedLane(lane, len(evs))
			for _, e := range evs {
				r.publish(ctx, e)
			}
		}
	}
	return nil
}

// publishFunc publishes the rows of one lane with the lane's channel and
// priority. Each row gets its relay.publish span (startPublish, the trace
// rule D13), whose context the message carries as its traceparent; the
// spans end with the batch's outcome. The spans are in-memory work around
// the publish call and change nothing of the lane transaction.
func (r *Relay) publishFunc(lane store.Lane) store.PublishFunc {
	return func(ctx context.Context, rows []store.DueRow) ([]uuid.UUID, error) {
		msgs := make([]queue.Message, len(rows))
		spans := make([]trace.Span, len(rows))
		for i, row := range rows {
			spans[i] = r.startPublish(ctx, lane, row)
			msgs[i] = queue.Message{
				ID: row.ID, Channel: lane.Channel, Priority: lane.Priority,
				TraceParent: tracing.FormatTraceParent(spans[i].SpanContext()),
			}
		}
		published, err := r.pub.PublishBatch(ctx, msgs, r.cfg.ConfirmTimeout)
		endPublish(spans, rows, published, err)
		return published, err
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
	r.metrics.recoveredBySweeper(evs)
	for _, e := range evs {
		r.publish(ctx, e)
	}
}

// publish hands the event of a transition to the event publisher and logs
// it (logging.Transition: info for a final outcome, debug otherwise). The
// relay's contexts carry no correlation id, so the line carries the
// notification's own, from its row (ADR-0009 §5).
func (r *Relay) publish(ctx context.Context, e domain.StatusChanged) {
	r.events.Publish(e)
	logging.Transition(ctx, r.logger, e)
}

// RunExpiry runs the expiry pass (transition 10, ADR-0002 "Expiry", D12)
// every ExpiryInterval until ctx is cancelled. It does not depend on the
// broker connection: a client sees an expired notification as failed when it
// expires, not when the broker or the provider recovers.
func (r *Relay) RunExpiry(ctx context.Context) error {
	t := time.NewTicker(r.cfg.ExpiryInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			r.expire(ctx)
		}
	}
}

// expire is one tick of the expiry pass: batches of ExpiryBatch, the next one
// at once while a batch comes back full. The events of a batch are published
// after its statement has committed. A failing batch is logged and the pass
// ends; the next tick tries again.
//
// A batch runs on a context that shutdown does not cancel, so a batch that
// has started is finished; no further batch starts after ctx is cancelled.
func (r *Relay) expire(ctx context.Context) {
	for ctx.Err() == nil {
		evs, err := r.store.ExpireDue(context.WithoutCancel(ctx), r.cfg.ExpiryBatch)
		if err != nil {
			r.logger.Warn("relay: expiry pass failed", "error", err)
			return
		}
		r.metrics.expiredByPass(evs)
		for _, e := range evs {
			r.publish(ctx, e)
		}
		if len(evs) < r.cfg.ExpiryBatch {
			return
		}
	}
}

// RunIdempotencyCleanup deletes expired idempotency keys (D78, ADR-0006
// section 11) every IdempotencyCleanupInterval until ctx is cancelled.
func (r *Relay) RunIdempotencyCleanup(ctx context.Context) error {
	t := time.NewTicker(r.cfg.IdempotencyCleanupInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			r.cleanup(ctx)
		}
	}
}

// cleanup is one tick of the idempotency-key cleanup: batches of
// store.IdempotencyCleanupBatch keys, the next one at once while a batch
// comes back full. Errors and shutdown are handled as in expire.
func (r *Relay) cleanup(ctx context.Context) {
	for ctx.Err() == nil {
		n, err := r.store.DeleteExpiredIdempotencyKeys(context.WithoutCancel(ctx))
		if err != nil {
			r.logger.Warn("relay: idempotency-key cleanup failed", "error", err)
			return
		}
		if n < store.IdempotencyCleanupBatch {
			return
		}
	}
}

// RunGauges measures the gauges of the lanes and of the backlog
// (lane_queued, lane_oldest_queued_age_seconds, backlog_due,
// scheduled_future) at once and then every GaugeInterval until ctx is
// cancelled. It is a loop of its own: the two statements run outside any
// lane transaction and never delay the publishing cycle. A failed
// measurement is logged and leaves the gauges as they were. With a
// GaugeInterval of zero or less it returns at once.
func (r *Relay) RunGauges(ctx context.Context) error {
	if r.cfg.GaugeInterval <= 0 {
		return nil
	}
	r.measure(ctx)
	t := time.NewTicker(r.cfg.GaugeInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			r.measure(ctx)
		}
	}
}

func (r *Relay) measure(ctx context.Context) {
	g, err := r.store.Gauges(ctx)
	if err != nil {
		if ctx.Err() == nil {
			r.logger.Warn("relay: gauge measurement failed", "error", err)
		}
		return
	}
	r.metrics.setGauges(g)
}
