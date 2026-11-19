package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

type recorder struct{ evs []domain.StatusChanged }

func (r *recorder) Publish(e domain.StatusChanged) { r.evs = append(r.evs, e) }

// The publisher counts every final transition by channel, priority and
// status, ignores the other transitions, and hands every event on.
func TestFinished_CountsFinalTransitionsOnTheirWayToThePublisher(t *testing.T) {
	reg := prometheus.NewRegistry()
	f, err := NewFinished(reg, []domain.Channel{domain.ChannelSMS}, domain.StatusSent)
	if err != nil {
		t.Fatal(err)
	}
	next := &recorder{}
	p := f.Publisher(next)
	for _, e := range []domain.StatusChanged{
		{Channel: domain.ChannelSMS, Priority: domain.PriorityHigh, Status: domain.StatusSent},
		{Channel: domain.ChannelSMS, Priority: domain.PriorityHigh, Status: domain.StatusSent},
		{Channel: domain.ChannelEmail, Priority: domain.PriorityLow, Status: domain.StatusFailed},
		{Channel: domain.ChannelPush, Priority: domain.PriorityNormal, Status: domain.StatusCancelled},
		{Channel: domain.ChannelSMS, Priority: domain.PriorityHigh, Status: domain.StatusQueued},
		{Channel: domain.ChannelSMS, Priority: domain.PriorityHigh, Status: domain.StatusRetrying},
		{Channel: domain.ChannelSMS, Priority: domain.PriorityHigh, Status: domain.StatusProcessing},
		{Channel: domain.ChannelSMS, Priority: domain.PriorityHigh, Status: domain.StatusPending},
	} {
		p.Publish(e)
	}
	if len(next.evs) != 8 {
		t.Errorf("%d events handed on, want 8", len(next.evs))
	}
	for _, c := range []struct {
		channel, priority, status string
		want                      float64
	}{
		{"sms", "high", "sent", 2},
		{"email", "low", "failed", 1},
		{"push", "normal", "cancelled", 1},
		{"sms", "normal", "sent", 0}, // created at zero
	} {
		got := testutil.MetricValue(t, reg, FinishedMetric, testutil.Labels("channel", c.channel, "priority", c.priority, "status", c.status))
		if got != c.want {
			t.Errorf("%s/%s/%s = %v, want %v", c.channel, c.priority, c.status, got, c.want)
		}
	}
	// 3 priorities of sms/sent created at zero, plus email/failed and
	// push/cancelled: nothing for the non-final statuses.
	if n := testutil.SeriesCount(t, reg, FinishedMetric); n != 5 {
		t.Errorf("%d series, want 5", n)
	}
}

// A collector that is registered twice is shared, so a role built twice in
// one process keeps one series; Replace swaps a collector for a new one.
func TestRegisterReusesAndReplaceSwaps(t *testing.T) {
	reg := prometheus.NewRegistry()
	opts := prometheus.CounterOpts{Name: "x_total", Help: "x"}
	a, err := Register(reg, prometheus.NewCounter(opts))
	if err != nil {
		t.Fatal(err)
	}
	b, err := Register(reg, prometheus.NewCounter(opts))
	if err != nil {
		t.Fatal(err)
	}
	b.Inc()
	if a != b || testutil.MetricValue(t, reg, "x_total", nil) != 1 {
		t.Error("the second registration did not reuse the first counter")
	}

	g := prometheus.GaugeOpts{Name: "y", Help: "y"}
	if err := Replace(reg, prometheus.NewGaugeFunc(g, func() float64 { return 1 })); err != nil {
		t.Fatal(err)
	}
	if err := Replace(reg, prometheus.NewGaugeFunc(g, func() float64 { return 2 })); err != nil {
		t.Fatal(err)
	}
	if v := testutil.MetricValue(t, reg, "y", nil); v != 2 {
		t.Errorf("y = %v after the replacement, want 2", v)
	}
	if _, err := Register(nil, prometheus.NewCounter(opts)); err != nil || Replace(nil, a) != nil {
		t.Error("a nil registry is not a no-op")
	}
}

// Label values come from fixed sets: a value outside them is "unknown".
func TestLabelsOutsideTheirSetAreUnknown(t *testing.T) {
	if ChannelLabel("fax") != Unknown || ChannelLabel(domain.ChannelEmail) != "email" {
		t.Error("channel label")
	}
	if PriorityLabel(domain.Priority(9)) != Unknown || PriorityLabel(domain.PriorityLow) != "low" {
		t.Error("priority label")
	}
}
