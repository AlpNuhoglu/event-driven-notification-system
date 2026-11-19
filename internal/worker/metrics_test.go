package worker

import (
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/breaker"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/config"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/metrics"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

// The harness's channel and provider, and the priority of the fake's rows.
var (
	smsFake  = testutil.Labels("channel", "sms", "provider", "fake")
	smsHigh  = testutil.Labels("channel", "sms", "priority", "high")
	smsLabel = testutil.Labels("channel", "sms")
)

func outcomeLabels(o domain.Outcome) map[string]string {
	return testutil.Labels("channel", "sms", "provider", "fake", "outcome", string(o))
}

func finishedLabels(p domain.Priority, s domain.Status) map[string]string {
	return testutil.Labels("channel", "sms", "priority", p.String(), "status", string(s))
}

// A worker creates every series of its channel and provider at zero when it
// is made, before it has seen a message.
func TestMetrics_WorkerSeriesExistAtZero(t *testing.T) {
	h := newHarness(t)
	want := []string{
		AttemptsMetric, CircuitStateMetric, EndToEndMetric, metrics.ExpiredMetric, InflightMetric, metrics.FinishedMetric,
		SlotsMissedMetric, ProviderDurationMetric, QueueWaitMetric, RateLimitWaitMetric,
	}
	slices.Sort(want)
	if got := testutil.MetricNames(t, h.reg); !slices.Equal(got, want) {
		t.Errorf("metric names:\n got %v\nwant %v", got, want)
	}
	var series []testutil.Series
	for _, o := range AttemptOutcomes {
		series = append(series, testutil.Series{Name: AttemptsMetric, Labels: outcomeLabels(o)})
	}
	for _, p := range domain.Priorities() {
		l := testutil.Labels("channel", "sms", "priority", p.String())
		series = append(series,
			testutil.Series{Name: EndToEndMetric, Labels: l},
			testutil.Series{Name: QueueWaitMetric, Labels: l},
			testutil.Series{Name: metrics.FinishedMetric, Labels: finishedLabels(p, domain.StatusSent)},
			testutil.Series{Name: metrics.FinishedMetric, Labels: finishedLabels(p, domain.StatusFailed)},
		)
	}
	series = append(series,
		testutil.Series{Name: ProviderDurationMetric, Labels: smsFake},
		testutil.Series{Name: CircuitStateMetric, Labels: smsFake},
		testutil.Series{Name: RateLimitWaitMetric, Labels: smsLabel},
		testutil.Series{Name: InflightMetric, Labels: smsLabel},
		testutil.Series{Name: SlotsMissedMetric, Labels: smsLabel},
	)
	testutil.RequireSeries(t, h.reg, series...)
	if n := testutil.SeriesCount(t, h.reg, AttemptsMetric); n != 9 {
		t.Errorf("%d attempt series, want one per final outcome (9)", n)
	}
}

// A sent attempt: the success is counted, the provider request's duration,
// the rate-limit wait, the queue wait (from queued_at to the claim) and the
// end-to-end latency are observed, the dispatch is in flight while it runs,
// and the transition to sent is counted once.
func TestMetrics_SentAttempt(t *testing.T) {
	h := newHarness(t)
	h.lim.setScript(reserveAnswer{wait: 20 * ms, ok: true})
	h.prov.latency = 120 * ms
	h.st.queuedFor = 2 * time.Second
	h.st.endToEnd = 3 * time.Second
	h.push(prioHigh)
	h.start()

	h.waitBlocked(1) // the dispatch waits for its slot
	if v := testutil.MetricValue(t, h.reg, InflightMetric, smsLabel); v != 1 {
		t.Errorf("inflight = %v while the dispatch waits for its slot, want 1", v)
	}
	h.clk.Advance(20 * ms)
	h.waitBlocked(1) // the provider request
	h.clk.Advance(120 * ms)
	h.eventually(func() bool { return h.st.finals() == 1 })
	h.eventually(func() bool { return testutil.MetricValue(t, h.reg, InflightMetric, smsLabel) == 0 })

	if v := testutil.MetricValue(t, h.reg, AttemptsMetric, outcomeLabels(domain.OutcomeSuccess)); v != 1 {
		t.Errorf("success attempts = %v, want 1", v)
	}
	checkHistogram(t, h.reg, ProviderDurationMetric, smsFake, 1, 0.12)
	checkHistogram(t, h.reg, RateLimitWaitMetric, smsLabel, 1, 0.02)
	checkHistogram(t, h.reg, QueueWaitMetric, smsHigh, 1, 2)
	checkHistogram(t, h.reg, EndToEndMetric, smsHigh, 1, 3)
	if v := testutil.MetricValue(t, h.reg, metrics.FinishedMetric, finishedLabels(domain.PriorityHigh, domain.StatusSent)); v != 1 {
		t.Errorf("finished sent = %v, want 1", v)
	}
}

// A late success (transition 12) is a transition to sent too: its
// end-to-end latency is observed and it is counted as sent.
func TestMetrics_LateSuccessObservesEndToEnd(t *testing.T) {
	h := newHarness(t)
	h.st.late = store.Sent{Late: true, From: domain.StatusRetrying, EndToEnd: 4 * time.Second}
	h.push(prioNormal)
	h.start()
	h.eventually(func() bool { return h.st.finals() == 1 })

	checkHistogram(t, h.reg, EndToEndMetric, smsHigh, 1, 4)
	if v := testutil.MetricValue(t, h.reg, metrics.FinishedMetric, finishedLabels(domain.PriorityHigh, domain.StatusSent)); v != 1 {
		t.Errorf("finished sent = %v, want 1", v)
	}
}

// A failed attempt is counted with its outcome; a row claimed without
// queued_at (a direct claim) observes no queue wait; a failure that ends
// the notification is a final transition; nothing is observed as sent.
func TestMetrics_FailedAttemptAndDirectClaim(t *testing.T) {
	h := newHarness(t)
	h.prov.respond = func(Request) Response { return Response{StatusCode: 400, Latency: 30 * ms} }
	h.st.failStatus = domain.StatusFailed
	h.push(prioNormal)
	h.start()
	h.eventually(func() bool { return h.st.finals() == 1 })

	if v := testutil.MetricValue(t, h.reg, AttemptsMetric, outcomeLabels(domain.OutcomePermanent)); v != 1 {
		t.Errorf("permanent attempts = %v, want 1", v)
	}
	checkHistogram(t, h.reg, ProviderDurationMetric, smsFake, 1, 0.03)
	checkHistogram(t, h.reg, QueueWaitMetric, smsHigh, 0, 0)
	checkHistogram(t, h.reg, EndToEndMetric, smsHigh, 0, 0)
	if v := testutil.MetricValue(t, h.reg, metrics.FinishedMetric, finishedLabels(domain.PriorityHigh, domain.StatusFailed)); v != 1 {
		t.Errorf("finished failed = %v, want 1", v)
	}
	if v := testutil.MetricValue(t, h.reg, metrics.FinishedMetric, finishedLabels(domain.PriorityHigh, domain.StatusSent)); v != 0 {
		t.Errorf("finished sent = %v, want 0", v)
	}
}

// A notification whose expires_at has passed when the worker claims it is
// failed by the claim itself, with reason expired. The worker counts it in
// expired_total with its priority, like the relay's expiry pass, so that the
// expiry alert sees it, and as a final transition; no attempt is counted.
func TestMetrics_ClaimOfAnExpiredNotificationCountsAnExpiry(t *testing.T) {
	h := newHarness(t)
	h.st.expired = func(uuid.UUID) bool { return true }
	h.push(prioNormal)
	h.start()
	h.eventually(func() bool {
		return testutil.MetricValue(t, h.reg, metrics.ExpiredMetric, smsHigh) == 1
	})

	if v := testutil.MetricValue(t, h.reg, metrics.FinishedMetric, finishedLabels(domain.PriorityHigh, domain.StatusFailed)); v != 1 {
		t.Errorf("finished failed = %v, want 1", v)
	}
	for _, o := range AttemptOutcomes {
		if v := testutil.MetricValue(t, h.reg, AttemptsMetric, outcomeLabels(o)); v != 0 {
			t.Errorf("%s attempts = %v, want 0: nothing was attempted", o, v)
		}
	}
	if n := len(h.prov.sent()); n != 0 {
		t.Errorf("%d provider requests for an expired notification", n)
	}
}

// An attempt closed without a send (here the send deadline passed during
// the claim) is counted as aborted and has no provider request.
func TestMetrics_AbortedAttempt(t *testing.T) {
	h := newHarness(t)
	h.lim.setScript(reserveAnswer{wait: 20 * ms, ok: true})
	h.st.claimDelay = delayFor(16 * time.Second)
	h.push(prioNormal)
	h.start()
	h.waitBlocked(1) // the claim
	h.clk.Advance(16 * time.Second)
	h.eventually(func() bool { return h.st.finals() == 1 })

	if v := testutil.MetricValue(t, h.reg, AttemptsMetric, outcomeLabels(domain.OutcomeAborted)); v != 1 {
		t.Errorf("aborted attempts = %v, want 1", v)
	}
	checkHistogram(t, h.reg, ProviderDurationMetric, smsFake, 0, 0)
	if v := testutil.MetricValue(t, h.reg, metrics.FinishedMetric, finishedLabels(domain.PriorityHigh, domain.StatusFailed)); v != 0 {
		t.Errorf("finished failed = %v after an abort, want 0", v)
	}
}

// pacer_slots_missed_total reads the pacer's own count of slots given up.
func TestMetrics_MissedSlotsServedFromThePacerCount(t *testing.T) {
	h := newHarness(t)
	h.lim.setScript(reserveAnswer{wait: 20 * ms, ok: true}, reserveAnswer{wait: 7 * ms, ok: true})
	h.st.claimDelay = delayFor(30 * ms)
	h.push(prioNormal)
	h.start()
	h.waitBlocked(1) // the claim
	h.clk.Advance(30 * ms)
	h.eventually(func() bool { return h.w.MissedSlots() == 1 })

	if v := testutil.MetricValue(t, h.reg, SlotsMissedMetric, smsLabel); v != 1 {
		t.Errorf("pacer_slots_missed_total = %v, want 1", v)
	}
	// Both reservations were observed: the slot given up and the new one.
	checkHistogram(t, h.reg, RateLimitWaitMetric, smsLabel, 2, 0.027)
}

// circuit_state follows every state change of the breaker the role wires:
// 2 when it opens, 1 when the open period ends, 0 when the probe succeeds.
func TestMetrics_CircuitStateFollowsTheBreaker(t *testing.T) {
	cfg, err := config.LoadFrom(func(k string) (string, bool) {
		return map[string]string{"PROVIDER_NAME": "mock"}[k], k == "PROVIDER_NAME"
	})
	if err != nil {
		t.Fatal(err)
	}
	reg := prometheus.NewRegistry()
	m, err := NewMetrics(reg)
	if err != nil {
		t.Fatal(err)
	}
	m.forChannel(domain.ChannelEmail, "mock", func() uint64 { return 0 })
	clk := newFakeClock()
	c, err := newCircuit(cfg, domain.ChannelEmail, clk, slog.New(slog.DiscardHandler), m)
	if err != nil {
		t.Fatal(err)
	}
	labels := testutil.Labels("channel", "email", "provider", "mock")
	state := func() float64 { return testutil.MetricValue(t, reg, CircuitStateMetric, labels) }
	if s := state(); s != circuitClosed {
		t.Fatalf("state %v at start, want closed (0)", s)
	}

	p, ok, _ := c.Admit()
	if !ok {
		t.Fatal("a closed circuit refused a send")
	}
	p.Record(domain.OutcomeProviderFatal, 0)
	if s := state(); s != circuitOpen || c.b.State() != breaker.Open {
		t.Fatalf("state %v after provider_fatal, want open (2)", s)
	}

	clk.Advance(cfg.Breaker.FatalOpen)
	if ok, _ := c.Ready(); !ok {
		t.Fatal("not ready after the open period")
	}
	if s := state(); s != circuitHalfOpen {
		t.Fatalf("state %v after the open period, want half-open (1)", s)
	}

	p, ok, _ = c.Admit()
	if !ok {
		t.Fatal("the probe was not admitted")
	}
	p.Record(domain.OutcomeSuccess, 0)
	if s := state(); s != circuitClosed {
		t.Errorf("state %v after a successful probe, want closed (0)", s)
	}
}

// checkHistogram compares the sample count and sum (within a microsecond
// per sample) of a histogram series.
func checkHistogram(t *testing.T, g prometheus.Gatherer, name string, labels map[string]string, count uint64, sum float64) {
	t.Helper()
	gotCount, gotSum := testutil.HistogramSample(t, g, name, labels)
	if gotCount != count || gotSum < sum-1e-6*float64(count+1) || gotSum > sum+1e-6*float64(count+1) {
		t.Errorf("%s%v: count %d sum %v, want count %d sum %v", name, labels, gotCount, gotSum, count, sum)
	}
}
