package queue

import (
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

// Every counter of the publisher's stats is served under its name and
// reason, and every reason exists from the start.
func TestEventMetrics_ServeEveryCounterOfTheStats(t *testing.T) {
	reg := prometheus.NewRegistry()
	stats := EventStats{}
	if err := reg.Register(eventStatsCollector{stats: func() EventStats { return stats }}); err != nil {
		t.Fatal(err)
	}
	for _, reason := range []string{DropBufferFull, DropNotConnected, DropPublishError} {
		if v := testutil.MetricValue(t, reg, EventsDroppedMetric, testutil.Labels("reason", reason)); v != 0 {
			t.Errorf("%s = %v before any event, want 0", reason, v)
		}
	}
	stats = EventStats{Published: 5, DroppedBufferFull: 4, DroppedNotConnected: 3, DroppedPublishError: 2}
	if v := testutil.MetricValue(t, reg, EventsPublishedMetric, nil); v != 5 {
		t.Errorf("published = %v, want 5", v)
	}
	want := map[string]float64{DropBufferFull: 4, DropNotConnected: 3, DropPublishError: 2}
	for reason, n := range want {
		if v := testutil.MetricValue(t, reg, EventsDroppedMetric, testutil.Labels("reason", reason)); v != n {
			t.Errorf("dropped %s = %v, want %v", reason, v, n)
		}
	}
}

// RegisterEventMetrics serves the counters of the publisher it is given: an
// event published after Close is dropped as not_connected. A second
// publisher of the process replaces the first.
func TestRegisterEventMetrics_ReadsThePublisher(t *testing.T) {
	cfg := EventPublisherConfig{URL: "amqp://localhost:1/", BufferSize: 1, RetryInterval: time.Millisecond, FlushTimeout: time.Millisecond}
	reg := prometheus.NewRegistry()
	first, err := NewEventPublisher(cfg, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewEventPublisher(cfg, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	if err := RegisterEventMetrics(reg, first); err != nil {
		t.Fatal(err)
	}
	if err := RegisterEventMetrics(reg, p); err != nil {
		t.Fatal(err)
	}
	p.Close()
	p.Publish(domain.StatusChanged{})
	if v := testutil.MetricValue(t, reg, EventsDroppedMetric, testutil.Labels("reason", DropNotConnected)); v != 1 {
		t.Errorf("not_connected = %v, want 1", v)
	}
	first.Close()
}
