package relay

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/metrics"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
)

// The names of the relay's metrics (ADR-0009 §1).
const (
	ReconciliationRunsMetric = "reconciliation_runs_total"
	ReconciledRowsMetric     = "reconciled_rows_total"
	PublishedMetric          = "relay_published_total"
	SweeperRecoveredMetric   = "sweeper_recovered_total"
	ExpiredMetric            = metrics.ExpiredMetric
	LaneQueuedMetric         = "lane_queued"
	LaneOldestAgeMetric      = "lane_oldest_queued_age_seconds"
	BacklogDueMetric         = "backlog_due"
	ScheduledFutureMetric    = "scheduled_future"
)

// The kinds of sweeper_recovered_total: the status the lease sweeper moved a
// notification to (transition 7).
const (
	KindRetrying = "retrying"
	KindFailed   = "failed"
)

// Metrics are the relay's metrics.
type Metrics struct {
	reconciliationRuns prometheus.Counter
	reconciledRows     prometheus.Counter
	published          *prometheus.CounterVec // channel, priority
	recovered          *prometheus.CounterVec // kind
	expired            *metrics.Expired
	finished           *metrics.Finished

	laneQueued      *prometheus.GaugeVec // channel, priority
	laneOldestAge   *prometheus.GaugeVec // channel, priority
	backlogDue      *prometheus.GaugeVec // channel, priority
	scheduledFuture prometheus.Gauge
}

// NewMetrics returns the relay's metrics registered on reg (the role
// registers them on the default registry, which its admin port serves),
// with every series of a fixed label set created at zero. A metric of the
// same name that is already registered there is reused, so a process that
// builds the relay twice keeps one series. A nil reg leaves the metrics
// unregistered.
func NewMetrics(reg prometheus.Registerer) (*Metrics, error) {
	lane := []string{"channel", "priority"}
	m := &Metrics{}
	var err error
	register := func(name string, f func() error) {
		if err == nil {
			if ferr := f(); ferr != nil {
				err = fmt.Errorf("relay: register %s: %w", name, ferr)
			}
		}
	}
	register(ReconciliationRunsMetric, func() (e error) {
		m.reconciliationRuns, e = metrics.Register(reg, prometheus.NewCounter(prometheus.CounterOpts{
			Name: ReconciliationRunsMetric,
			Help: "Reconciliation runs (transition 8 on a (re)connection to RabbitMQ) that completed.",
		}))
		return e
	})
	register(ReconciledRowsMetric, func() (e error) {
		m.reconciledRows, e = metrics.Register(reg, prometheus.NewCounter(prometheus.CounterOpts{
			Name: ReconciledRowsMetric,
			Help: "Queued notifications that reconciliation returned to pending.",
		}))
		return e
	})
	register(PublishedMetric, func() (e error) {
		m.published, e = metrics.Register(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: PublishedMetric,
			Help: "Notifications published to RabbitMQ, confirmed and marked queued (transitions 1 and 2).",
		}, lane))
		return e
	})
	register(SweeperRecoveredMetric, func() (e error) {
		m.recovered, e = metrics.Register(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: SweeperRecoveredMetric,
			Help: "Notifications whose lease had expired, recovered by the lease sweeper (transition 7), by the status they moved to.",
		}, []string{"kind"}))
		return e
	})
	register(ExpiredMetric, func() (e error) {
		m.expired, e = metrics.NewExpired(reg, domain.Channels())
		return e
	})
	register(metrics.FinishedMetric, func() (e error) {
		m.finished, e = metrics.NewFinished(reg, domain.Channels(), domain.StatusFailed)
		return e
	})
	register(LaneQueuedMetric, func() (e error) {
		m.laneQueued, e = metrics.Register(reg, prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: LaneQueuedMetric,
			Help: "Queued notifications of the lane: its occupancy against the high-water mark.",
		}, lane))
		return e
	})
	register(LaneOldestAgeMetric, func() (e error) {
		m.laneOldestAge, e = metrics.Register(reg, prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: LaneOldestAgeMetric,
			Help: "Age of the lane's oldest queued notification in seconds; 0 when nothing is queued.",
		}, lane))
		return e
	})
	register(BacklogDueMetric, func() (e error) {
		m.backlogDue, e = metrics.Register(reg, prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: BacklogDueMetric,
			Help: "Pending and retrying notifications of the lane that are due and wait in Postgres.",
		}, lane))
		return e
	})
	register(ScheduledFutureMetric, func() (e error) {
		m.scheduledFuture, e = metrics.Register(reg, prometheus.NewGauge(prometheus.GaugeOpts{
			Name: ScheduledFutureMetric,
			Help: "Pending and retrying notifications due later: scheduled ones and retries waiting for their back-off.",
		}))
		return e
	})
	if err != nil {
		return nil, err
	}
	for _, c := range domain.Channels() {
		for _, p := range domain.Priorities() {
			m.published.WithLabelValues(string(c), p.String())
			for _, v := range []*prometheus.GaugeVec{m.laneQueued, m.laneOldestAge, m.backlogDue} {
				v.WithLabelValues(string(c), p.String())
			}
		}
	}
	m.recovered.WithLabelValues(KindRetrying)
	m.recovered.WithLabelValues(KindFailed)
	return m, nil
}

// publishedLane counts the notifications one lane transaction has published.
func (m *Metrics) publishedLane(lane store.Lane, n int) {
	if n > 0 {
		m.published.WithLabelValues(metrics.ChannelLabel(lane.Channel), metrics.PriorityLabel(lane.Priority)).Add(float64(n))
	}
}

// recoveredBySweeper counts the notifications of one sweep by the status
// they moved to.
func (m *Metrics) recoveredBySweeper(evs []domain.StatusChanged) {
	for _, e := range evs {
		switch e.Status {
		case domain.StatusRetrying:
			m.recovered.WithLabelValues(KindRetrying).Inc()
		case domain.StatusFailed:
			m.recovered.WithLabelValues(KindFailed).Inc()
		}
	}
}

// expiredByPass counts the notifications one batch of the expiry pass
// failed with reason expired.
func (m *Metrics) expiredByPass(evs []domain.StatusChanged) {
	for _, e := range evs {
		m.expired.Observe(e)
	}
}

// setGauges sets the lane gauges of every lane and scheduled_future from
// one measurement; a lane absent from it is set to zero.
func (m *Metrics) setGauges(g store.Gauges) {
	for _, c := range domain.Channels() {
		for _, p := range domain.Priorities() {
			lg := g.Lanes[store.Lane{Channel: c, Priority: p}]
			m.laneQueued.WithLabelValues(string(c), p.String()).Set(float64(lg.Queued))
			m.laneOldestAge.WithLabelValues(string(c), p.String()).Set(lg.OldestQueuedAge)
			m.backlogDue.WithLabelValues(string(c), p.String()).Set(float64(lg.BacklogDue))
		}
	}
	m.scheduledFuture.Set(float64(g.ScheduledFuture))
}
