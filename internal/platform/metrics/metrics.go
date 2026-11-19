// Package metrics is the metric code the roles share (ADR-0009 §1): the
// registration rules, the buckets of the latency histograms and the counter
// of final transitions, notifications_finished_total, which api, relay and
// worker each move for the transitions they perform.
//
// Every role registers its metrics on the registry it is given (the default
// registry in the binaries, which the admin port serves; a registry of their
// own in the tests), and creates every series of a fixed label set at zero
// when it starts, so that a dashboard query has data before the first event.
package metrics

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/events"
)

// LatencyBuckets are the buckets of end_to_end_latency_seconds and
// queue_wait_seconds: from 100 ms to 3 hours, so that a burst backlog stays
// visible instead of collapsing into the last bucket (ADR-0009 §1).
var LatencyBuckets = []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1800, 3600, 10800}

// Register registers c on reg and returns the collector to use. A collector
// with the same descriptors that is already registered there is returned
// instead, so that a process that builds a role twice (the tests of the
// binaries' services do) keeps one series. A nil reg leaves c unregistered.
func Register[T prometheus.Collector](reg prometheus.Registerer, c T) (T, error) {
	if reg == nil {
		return c, nil
	}
	err := reg.Register(c)
	var already prometheus.AlreadyRegisteredError
	if errors.As(err, &already) {
		if existing, ok := already.ExistingCollector.(T); ok {
			return existing, nil
		}
	}
	return c, err
}

// Replace registers c on reg, replacing a collector with the same
// descriptors that is already registered. It is for collectors that read the
// state of one object when they are scraped (an event publisher, a
// gateway): the newest object of a process is the one that is reported. A
// nil reg does nothing.
func Replace(reg prometheus.Registerer, c prometheus.Collector) error {
	if reg == nil {
		return nil
	}
	err := reg.Register(c)
	var already prometheus.AlreadyRegisteredError
	if errors.As(err, &already) {
		reg.Unregister(already.ExistingCollector)
		err = reg.Register(c)
	}
	return err
}

// FinishedMetric is the name of the counter of final transitions.
const FinishedMetric = "notifications_finished_total"

// Finished counts the final transitions a role performs:
// notifications_finished_total{channel,priority,status}, status being sent,
// failed or cancelled. It counts transitions, not notifications: a
// notification that is redriven, or that becomes sent again through a late
// success, is counted again (D75).
type Finished struct {
	vec *prometheus.CounterVec
}

// NewFinished returns the counter registered on reg (nil: unregistered),
// with the series of every channel of channels, every priority and each of
// statuses created at zero: the final statuses the role can reach on the
// channels it serves (Init adds more later).
func NewFinished(reg prometheus.Registerer, channels []domain.Channel, statuses ...domain.Status) (*Finished, error) {
	vec, err := Register(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: FinishedMetric,
		Help: "Final transitions (sent, failed, cancelled) performed by this role; a redriven notification or a late success counts again.",
	}, []string{"channel", "priority", "status"}))
	if err != nil {
		return nil, err
	}
	f := &Finished{vec: vec}
	f.Init(channels, statuses...)
	return f, nil
}

// Init creates the series of every channel of channels, every priority and
// each of statuses at zero.
func (f *Finished) Init(channels []domain.Channel, statuses ...domain.Status) {
	for _, c := range channels {
		for _, p := range domain.Priorities() {
			for _, s := range statuses {
				f.vec.WithLabelValues(string(c), p.String(), string(s))
			}
		}
	}
}

// Observe counts e when it is a final transition.
func (f *Finished) Observe(e domain.StatusChanged) {
	if !IsFinal(e.Status) {
		return
	}
	f.vec.WithLabelValues(ChannelLabel(e.Channel), PriorityLabel(e.Priority), string(e.Status)).Inc()
}

// Publisher returns a publisher that counts every final transition and then
// hands the event to next. Every transition of a role reaches its publisher
// exactly once, after its transaction has committed, so this is the one
// place where a role counts them.
func (f *Finished) Publisher(next events.Publisher) events.Publisher {
	return finishedPublisher{f: f, next: next}
}

type finishedPublisher struct {
	f    *Finished
	next events.Publisher
}

func (p finishedPublisher) Publish(e domain.StatusChanged) {
	p.f.Observe(e)
	p.next.Publish(e)
}

// ExpiredMetric is the name of the counter of expiries.
const ExpiredMetric = "expired_total"

// Expired counts the notifications a role fails with reason expired:
// expired_total{channel,priority} (D75). The relay's expiry pass fails most
// of them; the worker fails one whose expires_at has passed when it claims
// it. Both count, so that the expiry alert sees every expiry.
type Expired struct {
	vec *prometheus.CounterVec
}

// NewExpired returns the counter registered on reg (nil: unregistered),
// with the series of every channel of channels and every priority created
// at zero (Init adds more later).
func NewExpired(reg prometheus.Registerer, channels []domain.Channel) (*Expired, error) {
	vec, err := Register(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: ExpiredMetric,
		Help: "Notifications failed with reason expired by this role: the relay's expiry pass (transition 10), or the worker's claim of a notification whose expires_at has passed.",
	}, []string{"channel", "priority"}))
	if err != nil {
		return nil, err
	}
	x := &Expired{vec: vec}
	x.Init(channels)
	return x, nil
}

// Init creates the series of every channel of channels and every priority
// at zero.
func (x *Expired) Init(channels []domain.Channel) {
	for _, c := range channels {
		for _, p := range domain.Priorities() {
			x.vec.WithLabelValues(string(c), p.String())
		}
	}
}

// Observe counts e when it is a transition to failed with reason expired.
func (x *Expired) Observe(e domain.StatusChanged) {
	if e.Status != domain.StatusFailed || e.FailureReason == nil || *e.FailureReason != domain.ReasonExpired {
		return
	}
	x.vec.WithLabelValues(ChannelLabel(e.Channel), PriorityLabel(e.Priority)).Inc()
}

// Publisher returns a publisher that counts every expiry and then hands the
// event to next, like Finished.Publisher.
func (x *Expired) Publisher(next events.Publisher) events.Publisher {
	return expiredPublisher{x: x, next: next}
}

type expiredPublisher struct {
	x    *Expired
	next events.Publisher
}

func (p expiredPublisher) Publish(e domain.StatusChanged) {
	p.x.Observe(e)
	p.next.Publish(e)
}

// IsFinal reports whether s is a final status.
func IsFinal(s domain.Status) bool {
	return s == domain.StatusSent || s == domain.StatusFailed || s == domain.StatusCancelled
}

// Unknown is the label value of a value outside its fixed set, for example
// a channel a client sent that is not a channel.
const Unknown = "unknown"

// ChannelLabel returns the label value of c: its name, or Unknown when c is
// not one of the channels.
func ChannelLabel(c domain.Channel) string {
	if _, err := domain.ParseChannel(string(c)); err != nil {
		return Unknown
	}
	return string(c)
}

// PriorityLabel returns the label value of p: high, normal, low, or Unknown.
func PriorityLabel(p domain.Priority) string {
	if !p.Valid() {
		return Unknown
	}
	return p.String()
}
