package relay

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/metrics"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/queue"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

// newMeasuredUnit is newUnit with the relay's metrics on a registry of the
// test's own.
func newMeasuredUnit(t *testing.T, st *fakeStore, cfg Config) (*Relay, *prometheus.Registry, *recorder) {
	t.Helper()
	reg := prometheus.NewRegistry()
	m, err := NewMetrics(reg)
	if err != nil {
		t.Fatal(err)
	}
	conn := &fakeConn{}
	conn.open.Store(true)
	conn.opens.Store(1)
	rec := &recorder{}
	r := New(st, &fakePublisher{}, conn, rec, slog.New(slog.DiscardHandler), cfg, func() float64 { return 0.5 }, m)
	return r, reg, rec
}

func laneLabels(c domain.Channel, p domain.Priority) map[string]string {
	return testutil.Labels("channel", string(c), "priority", p.String())
}

func finishedLabels(c domain.Channel, p domain.Priority, s domain.Status) map[string]string {
	return testutil.Labels("channel", string(c), "priority", p.String(), "status", string(s))
}

// Before any work, the relay's registry serves every metric ADR-0009 gives
// the relay, with every series of their fixed label sets, and the counters
// of its event publisher.
func TestMetrics_RelayServesEveryMetricBeforeTraffic(t *testing.T) {
	reg := prometheus.NewRegistry()
	ev, err := queue.NewEventPublisher(queue.EventPublisherConfig{
		URL: "amqp://localhost:1/", BufferSize: 1, RetryInterval: time.Millisecond, FlushTimeout: time.Millisecond,
	}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer ev.Close()
	if _, err := registerMetrics(reg, ev); err != nil {
		t.Fatal(err)
	}

	want := []string{
		BacklogDueMetric, ExpiredMetric, LaneQueuedMetric, LaneOldestAgeMetric, metrics.FinishedMetric,
		ReconciledRowsMetric, ReconciliationRunsMetric, PublishedMetric, ScheduledFutureMetric,
		queue.EventsDroppedMetric, queue.EventsPublishedMetric, SweeperRecoveredMetric,
	}
	slices.Sort(want)
	if got := testutil.MetricNames(t, reg); !slices.Equal(got, want) {
		t.Errorf("metric names:\n got %v\nwant %v", got, want)
	}
	var series []testutil.Series
	for _, c := range domain.Channels() {
		for _, p := range domain.Priorities() {
			for _, name := range []string{PublishedMetric, ExpiredMetric, LaneQueuedMetric, LaneOldestAgeMetric, BacklogDueMetric} {
				series = append(series, testutil.Series{Name: name, Labels: laneLabels(c, p)})
			}
			series = append(series, testutil.Series{Name: metrics.FinishedMetric, Labels: finishedLabels(c, p, domain.StatusFailed)})
		}
	}
	series = append(series,
		testutil.Series{Name: SweeperRecoveredMetric, Labels: testutil.Labels("kind", KindRetrying)},
		testutil.Series{Name: SweeperRecoveredMetric, Labels: testutil.Labels("kind", KindFailed)},
		testutil.Series{Name: ScheduledFutureMetric},
		testutil.Series{Name: ReconciliationRunsMetric},
		testutil.Series{Name: ReconciledRowsMetric},
	)
	for _, reason := range []string{queue.DropBufferFull, queue.DropNotConnected, queue.DropPublishError} {
		series = append(series, testutil.Series{Name: queue.EventsDroppedMetric, Labels: testutil.Labels("reason", reason)})
	}
	testutil.RequireSeries(t, reg, series...)
}

// A lane transaction counts the notifications it published in its lane; a
// lane that failed counts nothing. Publishing is not a final transition.
func TestMetrics_CycleCountsPublishedPerLane(t *testing.T) {
	failed := store.Lane{Channel: domain.ChannelEmail, Priority: domain.PriorityLow}
	st := &fakeStore{laneErr: map[store.Lane]error{failed: errors.New("lane failed")}}
	r, reg, _ := newMeasuredUnit(t, st, unitConfig)
	runCycle(t, r, context.Background())
	runCycle(t, r, context.Background())

	for _, c := range domain.Channels() {
		for _, p := range domain.Priorities() {
			want := 2.0 // the fake publishes one row per lane and cycle
			if (store.Lane{Channel: c, Priority: p}) == failed {
				want = 0
			}
			if v := testutil.MetricValue(t, reg, PublishedMetric, laneLabels(c, p)); v != want {
				t.Errorf("%s/%s published = %v, want %v", c, p, v, want)
			}
			if v := testutil.MetricValue(t, reg, metrics.FinishedMetric, finishedLabels(c, p, domain.StatusFailed)); v != 0 {
				t.Errorf("%s/%s finished = %v after publishing, want 0", c, p, v)
			}
		}
	}
}

// The lease sweeper's recoveries are counted by the status they moved to,
// and its failures are final transitions of the relay.
func TestMetrics_SweepCountsRecoveredByKindAndFinished(t *testing.T) {
	ev := func(s domain.Status) domain.StatusChanged {
		return domain.StatusChanged{ID: uuid.New(), Channel: domain.ChannelPush, Priority: domain.PriorityHigh, Status: s}
	}
	st := &fakeStore{sweepEvents: []domain.StatusChanged{ev(domain.StatusRetrying), ev(domain.StatusFailed), ev(domain.StatusFailed)}}
	r, reg, rec := newMeasuredUnit(t, st, unitConfig)
	r.sweep(context.Background())

	if v := testutil.MetricValue(t, reg, SweeperRecoveredMetric, testutil.Labels("kind", KindRetrying)); v != 1 {
		t.Errorf("recovered retrying = %v, want 1", v)
	}
	if v := testutil.MetricValue(t, reg, SweeperRecoveredMetric, testutil.Labels("kind", KindFailed)); v != 2 {
		t.Errorf("recovered failed = %v, want 2", v)
	}
	if v := testutil.MetricValue(t, reg, metrics.FinishedMetric, finishedLabels(domain.ChannelPush, domain.PriorityHigh, domain.StatusFailed)); v != 2 {
		t.Errorf("finished push/high/failed = %v, want 2", v)
	}
	if n := len(rec.events()); n != 3 {
		t.Errorf("%d events reached the publisher, want 3", n)
	}
}

// The expiry pass counts every notification it failed as expired with its
// channel and priority, and as a final transition.
func TestMetrics_ExpiryCountsExpiredWithPriority(t *testing.T) {
	reason := domain.ReasonExpired
	ev := func(c domain.Channel, p domain.Priority) domain.StatusChanged {
		return domain.StatusChanged{ID: uuid.New(), Channel: c, Priority: p, Status: domain.StatusFailed, FailureReason: &reason}
	}
	st := &fakeStore{expireBatches: [][]domain.StatusChanged{
		{ev(domain.ChannelSMS, domain.PriorityHigh), ev(domain.ChannelSMS, domain.PriorityNormal), ev(domain.ChannelSMS, domain.PriorityNormal)},
		{ev(domain.ChannelEmail, domain.PriorityLow)},
	}}
	r, reg, _ := newMeasuredUnit(t, st, unitConfig)
	r.expire(context.Background())

	want := map[store.Lane]float64{
		{Channel: domain.ChannelSMS, Priority: domain.PriorityHigh}:   1,
		{Channel: domain.ChannelSMS, Priority: domain.PriorityNormal}: 2,
		{Channel: domain.ChannelEmail, Priority: domain.PriorityLow}:  1,
	}
	for _, c := range domain.Channels() {
		for _, p := range domain.Priorities() {
			w := want[store.Lane{Channel: c, Priority: p}]
			if v := testutil.MetricValue(t, reg, ExpiredMetric, laneLabels(c, p)); v != w {
				t.Errorf("expired %s/%s = %v, want %v", c, p, v, w)
			}
			if v := testutil.MetricValue(t, reg, metrics.FinishedMetric, finishedLabels(c, p, domain.StatusFailed)); v != w {
				t.Errorf("finished %s/%s/failed = %v, want %v", c, p, v, w)
			}
		}
	}
}

// The gauge loop measures at once and then every GaugeInterval; every lane
// is set from each measurement, a lane absent from it to zero; a failed
// measurement leaves the gauges as they were.
func TestMetrics_GaugeLoopSetsEveryLaneFromEachMeasurement(t *testing.T) {
	smsHigh := store.Lane{Channel: domain.ChannelSMS, Priority: domain.PriorityHigh}
	pushLow := store.Lane{Channel: domain.ChannelPush, Priority: domain.PriorityLow}
	st := &fakeStore{gauges: store.Gauges{
		Lanes: map[store.Lane]store.LaneGauge{
			smsHigh: {Queued: 7, OldestQueuedAge: 12.5, BacklogDue: 3},
			pushLow: {BacklogDue: 40},
		},
		ScheduledFuture: 9,
	}}
	cfg := unitConfig
	cfg.GaugeInterval = 5 * time.Millisecond
	r, reg, _ := newMeasuredUnit(t, st, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.RunGauges(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("RunGauges: %v", err)
		}
	}()

	value := func(name string, l store.Lane) float64 {
		return testutil.MetricValue(t, reg, name, laneLabels(l.Channel, l.Priority))
	}
	// The loop sets the gauges of one measurement one after the other, so
	// the test waits for all of them: reading the rest as soon as the first
	// is set can see a measurement half applied (seen in CI in P6: "backlog
	// push/low = 0, want 40").
	if !testutil.Within(func() bool {
		return value(LaneQueuedMetric, smsHigh) == 7 && value(LaneOldestAgeMetric, smsHigh) == 12.5 &&
			value(BacklogDueMetric, smsHigh) == 3 && value(BacklogDueMetric, pushLow) == 40 &&
			testutil.MetricValue(t, reg, ScheduledFutureMetric, nil) == 9
	}, 5*time.Second) {
		t.Fatalf("after 5s: queued sms/high = %v (want 7), oldest age sms/high = %v (want 12.5), backlog sms/high = %v (want 3), backlog push/low = %v (want 40), scheduled_future = %v (want 9)",
			value(LaneQueuedMetric, smsHigh), value(LaneOldestAgeMetric, smsHigh), value(BacklogDueMetric, smsHigh),
			value(BacklogDueMetric, pushLow), testutil.MetricValue(t, reg, ScheduledFutureMetric, nil))
	}

	// The queue drains: the lane is absent from the next measurement.
	st.mu.Lock()
	st.gauges = store.Gauges{Lanes: map[store.Lane]store.LaneGauge{pushLow: {BacklogDue: 1}}}
	st.mu.Unlock()
	testutil.Eventually(t, func() bool {
		return value(LaneQueuedMetric, smsHigh) == 0 && value(LaneOldestAgeMetric, smsHigh) == 0 &&
			value(BacklogDueMetric, smsHigh) == 0 && value(BacklogDueMetric, pushLow) == 1 &&
			testutil.MetricValue(t, reg, ScheduledFutureMetric, nil) == 0
	}, 5*time.Second)

	// A failing measurement changes nothing.
	st.mu.Lock()
	st.gaugesErr = errors.New("database unavailable")
	st.gauges = store.Gauges{}
	calls := st.measures
	st.mu.Unlock()
	testutil.Eventually(t, func() bool { return st.measureCalls() >= calls+2 }, 5*time.Second)
	if v := value(BacklogDueMetric, pushLow); v != 1 {
		t.Errorf("backlog push/low = %v after a failed measurement, want 1", v)
	}
}

// Without a gauge interval the loop measures nothing and returns.
func TestRunGauges_DisabledWithoutInterval(t *testing.T) {
	st := &fakeStore{}
	r, _, _ := newMeasuredUnit(t, st, unitConfig)
	if err := r.RunGauges(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := st.measureCalls(); n != 0 {
		t.Errorf("%d measurements, want 0", n)
	}
}
