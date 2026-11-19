package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/metrics"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
)

// The names of the worker's metrics (ADR-0009 §1). limiter_fallback_active
// belongs to internal/ratelimit, the provider anomalies to internal/provider.
const (
	AttemptsMetric         = "delivery_attempts_total"
	ProviderDurationMetric = "provider_request_duration_seconds"
	EndToEndMetric         = "end_to_end_latency_seconds"
	QueueWaitMetric        = "queue_wait_seconds"
	RateLimitWaitMetric    = "ratelimit_wait_seconds"
	CircuitStateMetric     = "circuit_state"
	InflightMetric         = "inflight"
	SlotsMissedMetric      = "pacer_slots_missed_total"
)

// The values of circuit_state.
const (
	circuitClosed   = 0
	circuitHalfOpen = 1
	circuitOpen     = 2
)

// AttemptOutcomes are the outcomes delivery_attempts_total counts: every
// final outcome of ADR-0007 §2.
var AttemptOutcomes = []domain.Outcome{
	domain.OutcomeSuccess, domain.OutcomeRetryable, domain.OutcomePermanent, domain.OutcomeProviderFatal,
	domain.OutcomeThrottled, domain.OutcomeUnknown, domain.OutcomeUnreachable, domain.OutcomeAborted,
	domain.OutcomeInternalError,
}

// Buckets of the worker's histograms other than the two latencies of
// metrics.LatencyBuckets.
var (
	// providerBuckets cover one provider request up to its timeout
	// (PROVIDER_TIMEOUT, 10 s by default).
	providerBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}
	// rateLimitBuckets cover the wait for a reserved slot, which the
	// reservation horizon bounds (RATE_LIMIT_HORIZON, 250 ms by default).
	rateLimitBuckets = []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1}
)

// Metrics are the metrics of a worker role, shared by the workers of its
// channels. Each worker creates the series of its channel at zero when it
// is made (New).
type Metrics struct {
	attempts         *prometheus.CounterVec   // channel, provider, outcome
	providerDuration *prometheus.HistogramVec // channel, provider
	endToEnd         *prometheus.HistogramVec // channel, priority
	queueWait        *prometheus.HistogramVec // channel, priority
	rateLimitWait    *prometheus.HistogramVec // channel
	circuit          *prometheus.GaugeVec     // channel, provider
	inflight         *prometheus.GaugeVec     // channel
	finished         *metrics.Finished
	// expired counts the notifications the claim found expired and failed.
	expired *metrics.Expired
	missed  *missedCollector
}

// NewMetrics returns the worker's metrics registered on reg; a metric that
// is already registered there is reused (metrics.Register). A nil reg
// leaves them unregistered.
func NewMetrics(reg prometheus.Registerer) (*Metrics, error) {
	m := &Metrics{}
	var err error
	register := func(name string, f func() error) {
		if err == nil {
			if ferr := f(); ferr != nil {
				err = fmt.Errorf("worker: register %s: %w", name, ferr)
			}
		}
	}
	register(AttemptsMetric, func() (e error) {
		m.attempts, e = metrics.Register(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: AttemptsMetric,
			Help: "Delivery attempts by outcome (ADR-0007 section 2); aborted attempts sent nothing.",
		}, []string{"channel", "provider", "outcome"}))
		return e
	})
	register(ProviderDurationMetric, func() (e error) {
		m.providerDuration, e = metrics.Register(reg, prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    ProviderDurationMetric,
			Help:    "Duration of one provider request, until its answer was read or it failed.",
			Buckets: providerBuckets,
		}, []string{"channel", "provider"}))
		return e
	})
	register(EndToEndMetric, func() (e error) {
		m.endToEnd, e = metrics.Register(reg, prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    EndToEndMetric,
			Help:    "From COALESCE(scheduled_at, created_at) to sent_at, on the database clock; observed when a notification becomes sent.",
			Buckets: metrics.LatencyBuckets,
		}, []string{"channel", "priority"}))
		return e
	})
	register(QueueWaitMetric, func() (e error) {
		m.queueWait, e = metrics.Register(reg, prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    QueueWaitMetric,
			Help:    "From queued_at to the claim, on the database clock; not observed for a row claimed without queued_at.",
			Buckets: metrics.LatencyBuckets,
		}, []string{"channel", "priority"}))
		return e
	})
	register(RateLimitWaitMetric, func() (e error) {
		m.rateLimitWait, e = metrics.Register(reg, prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    RateLimitWaitMetric,
			Help:    "Wait until a reserved rate-limit slot, as the limiter returned it.",
			Buckets: rateLimitBuckets,
		}, []string{"channel"}))
		return e
	})
	register(CircuitStateMetric, func() (e error) {
		m.circuit, e = metrics.Register(reg, prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: CircuitStateMetric,
			Help: "State of the circuit breaker: 0 closed, 1 half-open, 2 open.",
		}, []string{"channel", "provider"}))
		return e
	})
	register(InflightMetric, func() (e error) {
		m.inflight, e = metrics.Register(reg, prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: InflightMetric,
			Help: "Dispatches running (claim, slot wait, send, final write), each holding one of WORKER_CONCURRENCY slots.",
		}, []string{"channel"}))
		return e
	})
	register(SlotsMissedMetric, func() (e error) {
		m.missed, e = metrics.Register(reg, &missedCollector{byChannel: map[domain.Channel]func() uint64{}})
		return e
	})
	register(metrics.FinishedMetric, func() (e error) {
		m.finished, e = metrics.NewFinished(reg, nil)
		return e
	})
	register(metrics.ExpiredMetric, func() (e error) {
		m.expired, e = metrics.NewExpired(reg, nil)
		return e
	})
	if err != nil {
		return nil, err
	}
	return m, nil
}

// channelMetrics are the metrics of one worker: its channel and provider.
type channelMetrics struct {
	m        *Metrics
	channel  string
	provider string
	inflight prometheus.Gauge
}

// forChannel creates the series of the worker of channel c with its
// provider at zero, makes pacer_slots_missed_total{channel} read missed,
// and returns the worker's view of the metrics.
func (m *Metrics) forChannel(c domain.Channel, provider string, missed func() uint64) *channelMetrics {
	ch := string(c)
	for _, o := range AttemptOutcomes {
		m.attempts.WithLabelValues(ch, provider, string(o))
	}
	m.providerDuration.WithLabelValues(ch, provider)
	for _, p := range domain.Priorities() {
		m.endToEnd.WithLabelValues(ch, p.String())
		m.queueWait.WithLabelValues(ch, p.String())
	}
	m.rateLimitWait.WithLabelValues(ch)
	m.circuit.WithLabelValues(ch, provider)
	m.finished.Init([]domain.Channel{c}, domain.StatusSent, domain.StatusFailed)
	m.expired.Init([]domain.Channel{c})
	m.missed.set(c, missed)
	return &channelMetrics{m: m, channel: ch, provider: provider, inflight: m.inflight.WithLabelValues(ch)}
}

// setCircuit sets circuit_state{channel,provider} to the value of the
// breaker's state s (circuitClosed, circuitHalfOpen or circuitOpen).
func (m *Metrics) setCircuit(c domain.Channel, provider string, s int) {
	m.circuit.WithLabelValues(string(c), provider).Set(float64(s))
}

// attempted counts an attempt that reached the provider and observes the
// duration of its request. An internal_error comes from a panic around the
// send, not from the provider, and has no request duration.
func (cm *channelMetrics) attempted(r store.AttemptResult) {
	cm.m.attempts.WithLabelValues(cm.channel, cm.provider, string(r.Outcome)).Inc()
	if r.Outcome != domain.OutcomeInternalError {
		cm.m.providerDuration.WithLabelValues(cm.channel, cm.provider).Observe(r.Latency.Seconds())
	}
}

// aborted counts an attempt closed without a send.
func (cm *channelMetrics) aborted() {
	cm.m.attempts.WithLabelValues(cm.channel, cm.provider, string(domain.OutcomeAborted)).Inc()
}

// The decorators below observe the worker's collaborators without changing
// what they do; New wraps the dependencies with them, so the pacer and the
// dispatch code carry no metric code beyond the attempt counts and the
// in-flight gauge.

// measuredStore observes queue_wait_seconds on every claim that applied and
// end_to_end_latency_seconds on every transition to sent (4 and 12), both
// from the times of the row on the database clock. A write that is retried
// applies at most once (the repeat returns store.ErrNotApplied), so each
// transition is observed once.
type measuredStore struct {
	Store
	m *Metrics
}

func (s measuredStore) Claim(ctx context.Context, id uuid.UUID, lease time.Duration, provider string) (store.Claim, domain.StatusChanged, error) {
	c, ev, err := s.Store.Claim(ctx, id, lease, provider)
	if err == nil && c.Notification.QueuedAt != nil {
		n := c.Notification
		s.m.queueWait.WithLabelValues(metrics.ChannelLabel(n.Channel), metrics.PriorityLabel(n.Priority)).
			Observe(n.UpdatedAt.Sub(*n.QueuedAt).Seconds())
	}
	return c, ev, err
}

func (s measuredStore) CompleteSent(ctx context.Context, id uuid.UUID, attemptNo int, provider string, r store.AttemptResult) (store.Sent, domain.StatusChanged, error) {
	sent, ev, err := s.Store.CompleteSent(ctx, id, attemptNo, provider, r)
	if err == nil {
		s.m.endToEnd.WithLabelValues(metrics.ChannelLabel(ev.Channel), metrics.PriorityLabel(ev.Priority)).
			Observe(sent.EndToEnd.Seconds())
	}
	return sent, ev, err
}

// measuredLimiter observes ratelimit_wait_seconds on every reservation.
type measuredLimiter struct {
	Limiter
	wait prometheus.Observer
}

func (l measuredLimiter) Reserve(ctx context.Context, channel domain.Channel) (time.Duration, bool, error) {
	wait, ok, err := l.Limiter.Reserve(ctx, channel)
	if err == nil && ok {
		l.wait.Observe(wait.Seconds())
	}
	return wait, ok, err
}

// instrument wraps the dependencies of the worker of channel c with the
// decorators of m: the store, the limiter and the publisher of status
// events, which counts the final transitions (notifications_finished_total).
func (m *Metrics) instrument(c domain.Channel, d Deps) Deps {
	d.Store = measuredStore{Store: d.Store, m: m}
	d.Limiter = measuredLimiter{Limiter: d.Limiter, wait: m.rateLimitWait.WithLabelValues(string(c))}
	d.Events = m.finished.Publisher(m.expired.Publisher(d.Events))
	return d
}

// missedCollector serves pacer_slots_missed_total{channel} from the count
// each worker keeps (Worker.MissedSlots), so the pacer keeps counting with
// its atomic alone.
type missedCollector struct {
	mu        sync.Mutex
	byChannel map[domain.Channel]func() uint64
}

var missedDesc = prometheus.NewDesc(SlotsMissedMetric,
	"Rate-limit slots given up because the claim returned more than PACER_SLOT_TOLERANCE after them, or a kept slot was stale (D77).",
	[]string{"channel"}, nil)

// set makes the series of channel c read missed; a later worker of the
// same channel in the process replaces an earlier one.
func (c *missedCollector) set(ch domain.Channel, missed func() uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.byChannel[ch] = missed
}

func (c *missedCollector) Describe(ch chan<- *prometheus.Desc) { ch <- missedDesc }

func (c *missedCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for channel, missed := range c.byChannel {
		ch <- prometheus.MustNewConstMetric(missedDesc, prometheus.CounterValue, float64(missed()), string(channel))
	}
}
