package queue

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/metrics"
)

// The names of the counters of the status-event publisher, served by every
// role that publishes status events (api, relay, worker).
const (
	EventsPublishedMetric = "status_events_published_total"
	EventsDroppedMetric   = "status_events_dropped_total"
)

// The reasons of status_events_dropped_total, one per counter of EventStats.
const (
	DropBufferFull   = "buffer_full"
	DropNotConnected = "not_connected"
	DropPublishError = "publish_error"
)

var (
	eventsPublishedDesc = prometheus.NewDesc(EventsPublishedMetric,
		"Status events written to the broker connection (published without confirms).", nil, nil)
	eventsDroppedDesc = prometheus.NewDesc(EventsDroppedMetric,
		"Status events dropped, by reason: buffer_full, not_connected, publish_error.", []string{"reason"}, nil)
)

// eventStatsCollector reads the counters of one publisher when it is
// scraped, so the publisher keeps counting with its atomics alone.
type eventStatsCollector struct {
	stats func() EventStats
}

func (c eventStatsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- eventsPublishedDesc
	ch <- eventsDroppedDesc
}

func (c eventStatsCollector) Collect(ch chan<- prometheus.Metric) {
	s := c.stats()
	ch <- prometheus.MustNewConstMetric(eventsPublishedDesc, prometheus.CounterValue, float64(s.Published))
	ch <- prometheus.MustNewConstMetric(eventsDroppedDesc, prometheus.CounterValue, float64(s.DroppedBufferFull), DropBufferFull)
	ch <- prometheus.MustNewConstMetric(eventsDroppedDesc, prometheus.CounterValue, float64(s.DroppedNotConnected), DropNotConnected)
	ch <- prometheus.MustNewConstMetric(eventsDroppedDesc, prometheus.CounterValue, float64(s.DroppedPublishError), DropPublishError)
}

// RegisterEventMetrics registers status_events_published_total and
// status_events_dropped_total{reason} of p on reg; every reason is served
// from the start. A collector of an earlier publisher of the process is
// replaced (metrics.Replace). A nil reg does nothing.
func RegisterEventMetrics(reg prometheus.Registerer, p *EventPublisher) error {
	return metrics.Replace(reg, eventStatsCollector{stats: p.Stats})
}
