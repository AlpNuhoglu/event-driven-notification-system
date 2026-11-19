//go:build integration

package integration

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/api"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/config"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/metrics"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/queue"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/relay"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/worker"
)

// startMeasuredWorker is startWorker with the role's metrics on reg.
func (e *env) startMeasuredWorker(cfg config.Config, reg *prometheus.Registry) *worker.Role {
	e.t.Helper()
	role, err := worker.NewRole(cfg, worker.Resources{
		Pool:     e.rolePool(cfg),
		Conn:     e.dial(),
		Redis:    e.rdb,
		Registry: reg,
	}, e.log)
	if err != nil {
		e.t.Fatalf("worker role: %v", err)
	}
	start(e.t, role.Run)
	return role
}

// createWith inserts one notification of the lane with the given TTL and
// scheduled_at (nil: due at once).
func (e *env) createWith(c domain.Channel, p domain.Priority, ttl time.Duration, scheduledAt *time.Time) uuid.UUID {
	e.t.Helper()
	ctx, cancel := e.ctx()
	defer cancel()
	row, err := e.store.CreateNotification(ctx, store.NewNotification{
		Channel: c, Priority: p, Recipient: domain.Recipient(testRecipient), Content: testContent,
		TTL: ttl, MaxFailures: 5, CorrelationID: "corr-metrics", TraceParent: testTraceParent, ScheduledAt: scheduledAt,
	})
	if err != nil {
		e.t.Fatalf("create: %v", err)
	}
	return row.ID
}

func finished(c domain.Channel, p domain.Priority, s domain.Status) map[string]string {
	return testutil.Labels("channel", string(c), "priority", p.String(), "status", string(s))
}

// total sums every series of notifications_finished_total of a registry.
func total(t *testing.T, g prometheus.Gatherer) float64 {
	t.Helper()
	var sum float64
	for _, v := range testutil.SeriesValues(t, g, metrics.FinishedMetric, "status") {
		sum += v
	}
	return sum
}

// Before any message, the wired worker role serves every metric ADR-0009
// gives the worker, the provider client's anomaly counters, the limiter's
// fallback gauge and the counters of its event publisher, each with the
// series of the role's channels.
func TestMetrics_WorkerRoleServesEveryMetricBeforeTraffic(t *testing.T) {
	e := newEnv(t)
	cfg := e.config(map[string]string{"WORKER_CHANNELS": "sms,push"})
	reg := prometheus.NewRegistry()
	role, err := worker.NewRole(cfg, worker.Resources{Pool: e.rolePool(cfg), Conn: e.dial(), Redis: e.rdb, Registry: reg}, e.log)
	if err != nil {
		t.Fatal(err)
	}
	start(t, role.Run)

	want := []string{
		worker.AttemptsMetric, worker.CircuitStateMetric, worker.EndToEndMetric, metrics.ExpiredMetric, worker.InflightMetric,
		"limiter_fallback_active", metrics.FinishedMetric, worker.SlotsMissedMetric, "provider_malformed_response_total",
		worker.ProviderDurationMetric, "provider_unexpected_2xx_total", worker.QueueWaitMetric, worker.RateLimitWaitMetric,
		queue.EventsDroppedMetric, queue.EventsPublishedMetric,
	}
	slices.Sort(want)
	if got := testutil.MetricNames(t, reg); !slices.Equal(got, want) {
		t.Errorf("metric names:\n got %v\nwant %v", got, want)
	}
	var series []testutil.Series
	for _, ch := range []string{"sms", "push"} {
		cp := testutil.Labels("channel", ch, "provider", "mock")
		for _, o := range worker.AttemptOutcomes {
			series = append(series, testutil.Series{Name: worker.AttemptsMetric, Labels: testutil.Labels("channel", ch, "provider", "mock", "outcome", string(o))})
		}
		series = append(series,
			testutil.Series{Name: worker.CircuitStateMetric, Labels: cp},
			testutil.Series{Name: worker.ProviderDurationMetric, Labels: cp},
			testutil.Series{Name: worker.InflightMetric, Labels: testutil.Labels("channel", ch)},
			testutil.Series{Name: worker.SlotsMissedMetric, Labels: testutil.Labels("channel", ch)},
			testutil.Series{Name: worker.RateLimitWaitMetric, Labels: testutil.Labels("channel", ch)},
			testutil.Series{Name: "provider_unexpected_2xx_total", Labels: testutil.Labels("channel", ch, "provider", "mock", "code", "200")},
		)
		for _, p := range domain.Priorities() {
			l := testutil.Labels("channel", ch, "priority", p.String())
			series = append(series,
				testutil.Series{Name: worker.EndToEndMetric, Labels: l},
				testutil.Series{Name: worker.QueueWaitMetric, Labels: l},
				testutil.Series{Name: metrics.FinishedMetric, Labels: finished(domain.Channel(ch), p, domain.StatusSent)},
				testutil.Series{Name: metrics.FinishedMetric, Labels: finished(domain.Channel(ch), p, domain.StatusFailed)},
			)
		}
	}
	series = append(series,
		testutil.Series{Name: "limiter_fallback_active"},
		testutil.Series{Name: "provider_malformed_response_total", Labels: testutil.Labels("provider", "mock")},
		testutil.Series{Name: queue.EventsDroppedMetric, Labels: testutil.Labels("reason", queue.DropNotConnected)},
	)
	testutil.RequireSeries(t, reg, series...)
	if n := testutil.SeriesCount(t, reg, worker.CircuitStateMetric); n != 2 {
		t.Errorf("%d circuit series, want one per channel the role serves (2)", n)
	}
}

// notifications_finished_total counts each final transition once, in the
// role that performed it: the worker's sent and failed, the api's cancel,
// the relay's expiry (also counted in expired_total with its priority). The
// other roles count nothing for them.
func TestMetrics_FinishedCountedOnceByTheRoleThatFinished(t *testing.T) {
	e := newEnv(t)
	cfg := e.config(map[string]string{"WORKER_CHANNELS": "sms", "EXPIRY_INTERVAL": "20ms"})
	workerReg := prometheus.NewRegistry()
	e.startMeasuredWorker(cfg, workerReg)
	_, relayReg := e.startRelayWith(cfg, nil)

	apiReg := prometheus.NewRegistry()
	m, err := api.NewMetrics(apiReg)
	if err != nil {
		t.Fatal(err)
	}
	pool := e.rolePool(cfg)
	srv := httptest.NewServer(api.NewHandler(api.Options{
		Repository: store.New(pool, store.ProfilesFrom(cfg.Retry)), DB: pool, Logger: e.log,
		Retry: cfg.Retry, MaxBodyBytes: cfg.API.MaxBodyBytes, Metrics: m,
	}))
	t.Cleanup(srv.Close)

	// Sent by the worker.
	sent := e.createWith(domain.ChannelSMS, domain.PriorityNormal, testTTL, nil)
	e.waitStatus(sent, domain.StatusSent)
	// Failed by the worker: a permanent rejection.
	e.prov.answer(http.StatusBadRequest)
	failed := e.createWith(domain.ChannelSMS, domain.PriorityLow, testTTL, nil)
	e.waitStatus(failed, domain.StatusFailed)
	// Cancelled through the api before it is due.
	later := time.Now().Add(time.Hour)
	cancelled := e.createWith(domain.ChannelSMS, domain.PriorityNormal, testTTL, &later)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL+"/v1/notifications/"+cancelled.String()+"/cancel", http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("cancel: %d", resp.StatusCode)
	}
	// Expired by the relay: an email notification no worker consumes.
	expired := e.createWith(domain.ChannelEmail, domain.PriorityHigh, 300*time.Millisecond, nil)
	e.waitStatus(expired, domain.StatusFailed)
	if r := e.get(expired).FailureReason; r == nil || *r != domain.ReasonExpired {
		t.Fatalf("failure reason %v, want expired", r)
	}

	checks := []struct {
		name   string
		reg    *prometheus.Registry
		labels map[string]string
	}{
		{"worker sent", workerReg, finished(domain.ChannelSMS, domain.PriorityNormal, domain.StatusSent)},
		{"worker failed", workerReg, finished(domain.ChannelSMS, domain.PriorityLow, domain.StatusFailed)},
		{"api cancelled", apiReg, finished(domain.ChannelSMS, domain.PriorityNormal, domain.StatusCancelled)},
		{"relay expired", relayReg, finished(domain.ChannelEmail, domain.PriorityHigh, domain.StatusFailed)},
	}
	for _, c := range checks {
		if v := testutil.MetricValue(t, c.reg, metrics.FinishedMetric, c.labels); v != 1 {
			t.Errorf("%s: %v, want 1", c.name, v)
		}
	}
	for name, want := range map[string]struct {
		reg *prometheus.Registry
		n   float64
	}{"worker": {workerReg, 2}, "api": {apiReg, 1}, "relay": {relayReg, 1}} {
		if got := total(t, want.reg); got != want.n {
			t.Errorf("%s counted %v final transitions, want %v", name, got, want.n)
		}
	}
	if v := testutil.MetricValue(t, relayReg, relay.ExpiredMetric, testutil.Labels("channel", "email", "priority", "high")); v != 1 {
		t.Errorf("expired_total{email,high} = %v, want 1", v)
	}
	if v := testutil.SeriesValues(t, relayReg, relay.ExpiredMetric, "priority"); v["high"] != 1 || v["normal"] != 0 || v["low"] != 0 {
		t.Errorf("expired_total by priority %v, want only one high", v)
	}
	// The sent notification was published by the relay: its queue wait was
	// observed once.
	if n, _ := testutil.HistogramSample(t, workerReg, worker.QueueWaitMetric, testutil.Labels("channel", "sms", "priority", "normal")); n != 1 {
		t.Errorf("%d queue waits of sms/normal, want 1", n)
	}
	if v := testutil.MetricValue(t, relayReg, relay.PublishedMetric, testutil.Labels("channel", "sms", "priority", "normal")); v != 1 {
		t.Errorf("relay_published_total{sms,normal} = %v, want 1", v)
	}
}

// The wired worker measures on the database's times of the row: a row
// claimed without queued_at (its message published while it was pending,
// so claimed directly) observes no queue wait, and the end-to-end latency
// runs from scheduled_at when the notification has one.
func TestMetrics_DirectClaimAndScheduledLatency(t *testing.T) {
	e := newEnv(t)
	cfg := e.config(map[string]string{"WORKER_CHANNELS": "sms"})
	reg := prometheus.NewRegistry()
	e.startMeasuredWorker(cfg, reg)
	pub := e.publisher()
	publish := func(id uuid.UUID) {
		t.Helper()
		ctx, cancel := e.ctx()
		defer cancel()
		msg := queue.Message{ID: id, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh, TraceParent: testTraceParent}
		if published, err := pub.PublishBatch(ctx, []queue.Message{msg}, confirmTimeout); err != nil || len(published) != 1 {
			t.Fatalf("publish: %v, %d confirmed", err, len(published))
		}
	}
	labels := testutil.Labels("channel", "sms", "priority", "high")

	plain := e.createWith(domain.ChannelSMS, domain.PriorityHigh, testTTL, nil)
	publish(plain)
	e.waitStatus(plain, domain.StatusSent)
	n := e.get(plain)
	if n.QueuedAt != nil {
		t.Fatalf("queued_at %v: the row was not claimed directly", n.QueuedAt)
	}
	if count, _ := testutil.HistogramSample(t, reg, worker.QueueWaitMetric, labels); count != 0 {
		t.Errorf("%d queue waits observed for a row without queued_at, want 0", count)
	}
	count, sum := testutil.HistogramSample(t, reg, worker.EndToEndMetric, labels)
	if want := n.SentAt.Sub(n.CreatedAt).Seconds(); count != 1 || !near(sum, want) {
		t.Errorf("end-to-end count %d sum %v, want 1 and sent_at - created_at = %v", count, sum, want)
	}

	// Scheduled ten minutes before it was created, so it is due at once.
	at := time.Now().Add(-10 * time.Minute)
	scheduled := e.createWith(domain.ChannelSMS, domain.PriorityHigh, testTTL, &at)
	publish(scheduled)
	e.waitStatus(scheduled, domain.StatusSent)
	s := e.get(scheduled)
	count, sum2 := testutil.HistogramSample(t, reg, worker.EndToEndMetric, labels)
	if want := s.SentAt.Sub(*s.ScheduledAt).Seconds(); count != 2 || !near(sum2-sum, want) || want < 9*60 {
		t.Errorf("end-to-end count %d, observed %v, want 2 and sent_at - scheduled_at = %v", count, sum2-sum, want)
	}
}

// near compares two durations in seconds to the microsecond.
func near(a, b float64) bool { return a-b < 1e-6 && b-a < 1e-6 }
