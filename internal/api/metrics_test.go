package api

import (
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	spec "github.com/AlpNuhoglu/event-driven-notification-system/api"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/metrics"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/queue"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

// newMeasuredFakeAPI is newFakeAPI with the api's metrics on a registry of
// the test's own.
func newMeasuredFakeAPI(t *testing.T, repo *fakeRepo) (*apiClient, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	m, err := NewMetrics(reg)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(Options{
		Repository:   repo,
		DB:           repo,
		Logger:       slog.New(slog.DiscardHandler),
		Retry:        testRetry(),
		MaxBodyBytes: 1024,
		DBTimeout:    1000 * time.Millisecond,
		Metrics:      m,
	})
	return newAPIClient(t, h), reg
}

func requestLabels(route, method, code string) map[string]string {
	return testutil.Labels("route", route, "method", method, "code", code)
}

func rejectedLabels(channel, reason string) map[string]string {
	return testutil.Labels("channel", channel, "reason", reason)
}

// Before any request, the api's registry serves every metric ADR-0009 gives
// the api, with every series of their fixed label sets: each operation of
// the specification with each status code it documents, each creation and
// rejection label set, the cancellation of every lane, and the counters of
// the event publisher.
func TestMetrics_APIServesEveryMetricBeforeTraffic(t *testing.T) {
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
		RequestDurationMetric, RequestsMetric, ReplaysMetric, CreatedMetric, metrics.FinishedMetric,
		RejectedMetric, queue.EventsDroppedMetric, queue.EventsPublishedMetric,
	}
	slices.Sort(want)
	if got := testutil.MetricNames(t, reg); !slices.Equal(got, want) {
		t.Errorf("metric names:\n got %v\nwant %v", got, want)
	}

	ops, err := documentedOperations(spec.OpenAPI())
	if err != nil {
		t.Fatal(err)
	}
	var series []testutil.Series
	codes := 0
	for _, op := range ops {
		series = append(series, testutil.Series{Name: RequestDurationMetric, Labels: testutil.Labels("route", op.route, "method", op.method)})
		for _, code := range op.codes {
			series = append(series, testutil.Series{Name: RequestsMetric, Labels: requestLabels(op.route, op.method, code)})
			codes++
		}
	}
	if len(ops) < 20 || codes < 3*len(ops) {
		t.Fatalf("%d operations with %d codes read from the specification: the specification was not read", len(ops), codes)
	}
	for _, c := range domain.Channels() {
		for _, p := range domain.Priorities() {
			for _, source := range []string{sourceSingle, sourceBatch} {
				series = append(series, testutil.Series{Name: CreatedMetric, Labels: testutil.Labels("channel", string(c), "priority", p.String(), "source", source)})
			}
			series = append(series, testutil.Series{Name: metrics.FinishedMetric, Labels: testutil.Labels("channel", string(c), "priority", p.String(), "status", "cancelled")})
		}
	}
	for _, c := range []string{"sms", "email", "push", metrics.Unknown} {
		for _, r := range rejectReasons {
			series = append(series, testutil.Series{Name: RejectedMetric, Labels: rejectedLabels(c, r)})
		}
	}
	series = append(series,
		testutil.Series{Name: ReplaysMetric},
		testutil.Series{Name: RequestsMetric, Labels: requestLabels("/v1/notifications/{id}", http.MethodGet, "404")},
		testutil.Series{Name: queue.EventsPublishedMetric},
	)
	testutil.RequireSeries(t, reg, series...)
}

// The route label is the route template: requests for two notification ids
// add to one series, and nothing of the path reaches a label. A path that
// matches no route is counted under one fixed route, and a method that is
// not a standard one under one fixed method, so neither creates a series per
// request.
func TestMetrics_RouteLabelIsTheTemplateNeverThePath(t *testing.T) {
	c, reg := newMeasuredFakeAPI(t, &fakeRepo{})
	first, second := uuid.NewString(), uuid.NewString()
	for _, id := range []string{first, second} {
		if r := c.get("/v1/notifications/" + id); r.status != http.StatusNotFound {
			t.Fatalf("GET: %d", r.status)
		}
	}
	for _, path := range []string{"/nope/" + first, "/nope/" + second} {
		if r := c.get(path); r.status != http.StatusNotFound {
			t.Fatalf("GET %s: %d", path, r.status)
		}
	}
	if r := c.do("PURGE", "/nope/"+first, "", nil); r.status != http.StatusNotFound {
		t.Fatalf("PURGE: %d", r.status)
	}

	if v := testutil.MetricValue(t, reg, RequestsMetric, requestLabels("/v1/notifications/{id}", "GET", "404")); v != 2 {
		t.Errorf("requests of the template = %v, want 2", v)
	}
	if v := testutil.MetricValue(t, reg, RequestsMetric, requestLabels(routeUnmatched, "GET", "404")); v != 2 {
		t.Errorf("unmatched GET requests = %v, want 2", v)
	}
	if v := testutil.MetricValue(t, reg, RequestsMetric, requestLabels(routeUnmatched, methodOther, "404")); v != 1 {
		t.Errorf("unmatched requests of a non-standard method = %v, want 1", v)
	}
	if n, _ := testutil.HistogramSample(t, reg, RequestDurationMetric, testutil.Labels("route", "/v1/notifications/{id}", "method", "GET")); n != 2 {
		t.Errorf("%d durations of the template, want 2", n)
	}
	for _, name := range []string{RequestsMetric, RequestDurationMetric} {
		for route := range testutil.SeriesValues(t, reg, name, "route") {
			if strings.Contains(route, first) || strings.Contains(route, second) || strings.Contains(route, "/nope") {
				t.Errorf("%s has the route %q: a raw path became a label", name, route)
			}
		}
		for method := range testutil.SeriesValues(t, reg, name, "method") {
			if method == "PURGE" {
				t.Errorf("%s has the method PURGE as a label", name)
			}
		}
	}
}

// A stored single create is counted with its channel, priority and source;
// a refused one with the channel its body names ("unknown" when it names no
// valid one) and the reason; a server error refuses nothing.
func TestMetrics_SingleCreateCountsCreatedAndRejected(t *testing.T) {
	repo := &fakeRepo{}
	c, reg := newMeasuredFakeAPI(t, repo)
	created := func(channel, priority string) float64 {
		return testutil.MetricValue(t, reg, CreatedMetric, testutil.Labels("channel", channel, "priority", priority, "source", sourceSingle))
	}
	rejected := func(channel, reason string) float64 {
		return testutil.MetricValue(t, reg, RejectedMetric, rejectedLabels(channel, reason))
	}

	if r := c.post(`{"channel":"sms","recipient":"+905551234567","content":"x","priority":"high"}`); r.status != http.StatusAccepted {
		t.Fatalf("valid create: %d %s", r.status, r.body)
	}
	if v := created("sms", "high"); v != 1 {
		t.Errorf("created sms/high = %v, want 1", v)
	}

	steps := []struct {
		name    string
		body    string
		headers map[string][]string
		status  int
		channel string
		reason  string
	}{
		{"invalid field", `{"channel":"email","recipient":"not-an-address","content":"x"}`, nil, 422, "email", reasonInvalid},
		{"invalid channel", `{"channel":"fax","recipient":"+905551234567","content":"x"}`, nil, 422, metrics.Unknown, reasonInvalid},
		{"not JSON", `{"channel":`, nil, 400, metrics.Unknown, reasonMalformed},
		{"media type", `{"channel":"push"}`, map[string][]string{"Content-Type": {"text/plain"}}, 415, metrics.Unknown, reasonMalformed},
		{"invalid Idempotency-Key", `{"channel":"push","recipient":"tok","content":"x"}`, map[string][]string{"Idempotency-Key": {"a b"}}, 400, "push", reasonIdempotencyKey},
	}
	for _, s := range steps {
		before := rejected(s.channel, s.reason)
		r := c.do(http.MethodPost, "/v1/notifications", s.body, s.headers)
		if r.status != s.status {
			t.Fatalf("%s: status %d, want %d (%s)", s.name, r.status, s.status, r.body)
		}
		if v := rejected(s.channel, s.reason); v != before+1 {
			t.Errorf("%s: rejected %s/%s = %v, want %v", s.name, s.channel, s.reason, v, before+1)
		}
	}

	repo.err = errors.New("unexpected store failure")
	total := sumValues(testutil.SeriesValues(t, reg, RejectedMetric, "reason"))
	if r := c.post(`{"channel":"sms","recipient":"+905551234567","content":"x"}`); r.status < 500 {
		t.Fatalf("create on a failing database: %d", r.status)
	}
	if v := sumValues(testutil.SeriesValues(t, reg, RejectedMetric, "reason")); v != total {
		t.Errorf("rejections went from %v to %v on a server error", total, v)
	}
	if v := created("sms", "normal"); v != 0 {
		t.Errorf("created sms/normal = %v after a server error, want 0", v)
	}
}

func sumValues(m map[string]float64) float64 {
	var s float64
	for _, v := range m {
		s += v
	}
	return s
}
