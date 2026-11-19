//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/api"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/events"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/config"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/tracing"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/queue"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/relay"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/worker"
)

// tracingScheduleLead is how far ahead the scheduled notification of the
// tracing test is due.
const tracingScheduleLead = 300 * time.Millisecond

// Scope names of the third-party instrumentations.
const (
	scopeHTTP = "go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	scopePGX  = "github.com/exaring/otelpgx"
)

// traced is the api, the relay and the worker role, wired as their services
// wire them, each with its own tracer provider (its service.name) and its
// own pool's statement spans, all exporting to one in-memory exporter.
type traced struct {
	e   *env
	exp *tracetest.InMemoryExporter
	api *httptest.Server
}

func (e *env) startTraced(cfg config.Config, roles bool) *traced {
	e.t.Helper()
	tr := &traced{e: e, exp: tracetest.NewInMemoryExporter()}
	obs := config.Observability{TracesSampler: tracing.SamplerParentBasedAlwaysOn}
	provider := func(role string) trace.TracerProvider {
		p, err := tracing.NewWithExporter(tr.exp, obs, role)
		if err != nil {
			e.t.Fatal(err)
		}
		return p.TracerProvider()
	}

	apiTP := provider("api")
	madeAt := e.dbNow() // the API's clock stands still, as in apiServer
	apiPool := e.tracedPool(cfg, apiTP)
	tr.api = httptest.NewServer(api.NewHandler(api.Options{
		Repository:           store.New(apiPool, store.ProfilesFrom(cfg.Retry)),
		DB:                   apiPool,
		Logger:               e.log,
		Retry:                cfg.Retry,
		MaxBodyBytes:         cfg.API.MaxBodyBytes,
		ExpiresAtMinLead:     cfg.API.ExpiresAtMinLead,
		ScheduledAtTolerance: cfg.API.SchedulePastTolerance,
		ScheduledAtMaxAhead:  cfg.API.ScheduleMaxHorizon,
		Now:                  func() time.Time { return madeAt },
		TracerProvider:       apiTP,
	}))
	e.t.Cleanup(func() {
		tr.api.Client().CloseIdleConnections()
		tr.api.Close()
	})
	if !roles {
		return tr
	}

	workerTP := provider("worker")
	role, err := worker.NewRole(cfg, worker.Resources{
		Pool:           e.tracedPool(cfg, workerTP),
		Conn:           e.dial(),
		Redis:          e.rdb,
		Registry:       prometheus.NewRegistry(),
		TracerProvider: workerTP,
	}, e.log)
	if err != nil {
		e.t.Fatalf("worker role: %v", err)
	}
	start(e.t, role.Run)

	relayTP := provider("relay")
	conn := e.dial()
	if err := conn.DeclareTopology(cfg.Worker.ConsumerTimeout); err != nil {
		e.t.Fatalf("declare: %v", err)
	}
	pub, err := queue.NewPublisher(conn, cfg.Relay.BatchSize)
	if err != nil {
		e.t.Fatalf("publisher: %v", err)
	}
	rcfg := relay.ConfigFrom(cfg.Relay)
	rcfg.TracerProvider = relayTP
	r := relay.New(store.New(e.tracedPool(cfg, relayTP), store.ProfilesFrom(cfg.Retry)), pub, conn, events.Nop{}, e.log,
		rcfg, func() float64 { return 0.5 }, nil)
	start(e.t, func(ctx context.Context) error { return r.Run(ctx, conn.GaveUp(), conn.Err) })
	return tr
}

// tracedPool opens a pool as the roles do, with its statement spans on tp.
func (e *env) tracedPool(cfg config.Config, tp trace.TracerProvider) *pgxpool.Pool {
	e.t.Helper()
	pool, err := store.OpenPoolWith(context.Background(), cfg.Connections.DatabaseURL, store.WithTracerProvider(tp))
	if err != nil {
		e.t.Fatalf("pool: %v", err)
	}
	e.t.Cleanup(pool.Close)
	return pool
}

// post creates a notification through the API with the given headers and
// body fields and returns its id.
func (tr *traced) post(header http.Header, extra string) uuid.UUID {
	tr.e.t.Helper()
	ctx, cancel := tr.e.ctx()
	defer cancel()
	body := `{"channel":"sms","recipient":"` + testRecipient + `","content":"` + testContent + `"` + extra + `}`
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tr.api.URL+"/v1/notifications", strings.NewReader(body))
	if err != nil {
		tr.e.t.Fatal(err)
	}
	req.Header = header.Clone()
	req.Header.Set("Content-Type", "application/json")
	res, err := tr.api.Client().Do(req)
	if err != nil {
		tr.e.t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	b, _ := io.ReadAll(res.Body)
	var out struct {
		ID uuid.UUID `json:"id"`
	}
	if res.StatusCode != http.StatusAccepted || json.Unmarshal(b, &out) != nil {
		tr.e.t.Fatalf("create: status %d", res.StatusCode)
	}
	return out.ID
}

// get reads a notification through the API.
func (tr *traced) get(id uuid.UUID) {
	tr.e.t.Helper()
	if status, _ := tr.e.call(tr.api, http.MethodGet, "/v1/notifications/"+id.String(), ""); status != http.StatusOK {
		tr.e.t.Fatalf("get: status %d", status)
	}
}

// delivery is the spans of one delivery of a notification.
type delivery struct {
	publish, process, call tracetest.SpanStub
}

// deliveries waits until the notification's n-th delivery has ended (its
// worker.process span is exported) and returns the spans of each.
func (tr *traced) deliveries(id uuid.UUID, n int) []delivery {
	tr.e.t.Helper()
	testutil.Eventually(tr.e.t, func() bool { return len(tr.byName(id, "worker.process")) >= n }, waitTimeout)
	spans := tr.exp.GetSpans()
	publishes, processes := tr.byName(id, "relay.publish"), tr.byName(id, "worker.process")
	out := make([]delivery, 0, n)
	for _, p := range processes {
		d := delivery{process: p}
		for _, pub := range publishes {
			if pub.SpanContext.SpanID() == p.Parent.SpanID() {
				d.publish = pub
			}
		}
		for _, s := range spans {
			if s.SpanKind == trace.SpanKindClient && s.InstrumentationScope.Name == scopeHTTP && s.Parent.SpanID() == p.SpanContext.SpanID() {
				d.call = s
			}
		}
		if !d.publish.SpanContext.IsValid() || !d.call.SpanContext.IsValid() {
			tr.e.t.Fatalf("delivery %d: publish span %v, provider call span %v", len(out), d.publish.SpanContext.IsValid(), d.call.SpanContext.IsValid())
		}
		out = append(out, d)
	}
	return out
}

// byName returns the exported spans of name whose notification.id is id, in
// the order they ended.
func (tr *traced) byName(id uuid.UUID, name string) []tracetest.SpanStub {
	var out []tracetest.SpanStub
	for _, s := range tr.exp.GetSpans() {
		if s.Name == name && attrOf(s, tracing.AttrNotificationID) == id.String() {
			out = append(out, s)
		}
	}
	return out
}

// server returns the one request span of name.
func (tr *traced) server(name string) tracetest.SpanStub {
	tr.e.t.Helper()
	var found []tracetest.SpanStub
	for _, s := range tr.exp.GetSpans() {
		if s.SpanKind == trace.SpanKindServer && s.Name == name {
			found = append(found, s)
		}
	}
	if len(found) != 1 {
		tr.e.t.Fatalf("%d request spans named %q", len(found), name)
	}
	return found[0]
}

// hasChild reports whether a span of scope has parent as its parent.
func (tr *traced) hasChild(parent tracetest.SpanStub, scope string) bool {
	for _, s := range tr.exp.GetSpans() {
		if s.InstrumentationScope.Name == scope && s.Parent.SpanID() == parent.SpanContext.SpanID() {
			return true
		}
	}
	return false
}

func attrOf(s tracetest.SpanStub, k attribute.Key) string {
	for _, kv := range s.Attributes {
		if kv.Key == k {
			return kv.Value.String()
		}
	}
	return ""
}

func traceParentOf(s tracetest.SpanStub) string { return tracing.FormatTraceParent(s.SpanContext) }

var uuidPattern = regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)

// checkSpans asserts over every exported span: no span name carries an id;
// no attribute, event or status description contains the test's recipient
// or content; and every span of the deliveries carries notification.id.
func (tr *traced) checkSpans(id uuid.UUID, ds []delivery) {
	t := tr.e.t
	t.Helper()
	leaks := func(v string) bool { return strings.Contains(v, testRecipient) || strings.Contains(v, testContent) }
	for _, s := range tr.exp.GetSpans() {
		if uuidPattern.MatchString(s.Name) || leaks(s.Name) {
			t.Errorf("span name %q carries an id or personal data", s.Name)
		}
		if leaks(s.Status.Description) {
			t.Errorf("span %q: status description carries the recipient or the content", s.Name)
		}
		for _, kv := range s.Attributes {
			if leaks(kv.Value.String()) {
				t.Errorf("span %q: attribute %s carries the recipient or the content", s.Name, kv.Key)
			}
		}
		for _, ev := range s.Events {
			for _, kv := range ev.Attributes {
				if leaks(ev.Name) || leaks(kv.Value.String()) {
					t.Errorf("span %q: event %q carries the recipient or the content", s.Name, ev.Name)
				}
			}
		}
	}
	for i, d := range ds {
		for _, s := range []tracetest.SpanStub{d.publish, d.process, d.call} {
			if attrOf(s, tracing.AttrNotificationID) != id.String() {
				t.Errorf("delivery %d: span %q without the notification's id", i, s.Name)
			}
		}
	}
}

// The first attempt of an ordinary notification is one trace from the
// request to the provider: the api's request span (named by its route
// template), relay.publish, worker.process and the provider call share its
// trace id, each the child of the one before, with the statements of the
// insert and of the claim under the request and the delivery. The provider
// receives the call span's traceparent, and the row stores the request
// span's context.
func TestTracing_FirstAttemptIsOneTrace(t *testing.T) {
	e := newEnv(t)
	tr := e.startTraced(e.config(nil), true)

	id := tr.post(http.Header{}, "")
	e.waitStatus(id, domain.StatusSent)
	d := tr.deliveries(id, 1)[0]
	tr.get(id)

	request := tr.server("POST /v1/notifications")
	if request.Parent.IsValid() {
		t.Errorf("request span has parent %v, want a root (no client traceparent)", request.Parent)
	}
	traceID := request.SpanContext.TraceID()
	for name, s := range map[string]tracetest.SpanStub{"relay.publish": d.publish, "worker.process": d.process, "provider call": d.call} {
		if s.SpanContext.TraceID() != traceID {
			t.Errorf("%s in trace %s, want the request's %s", name, s.SpanContext.TraceID(), traceID)
		}
	}
	if d.publish.Parent.SpanID() != request.SpanContext.SpanID() || len(d.publish.Links) != 0 {
		t.Errorf("relay.publish parent %s links %v, want the request span %s", d.publish.Parent.SpanID(), d.publish.Links, request.SpanContext.SpanID())
	}
	// deliveries already matched process → publish and call → process.
	if got := e.get(id).TraceParent; got != traceParentOf(request) {
		t.Errorf("stored trace_parent %q, want the request span's context %q", got, traceParentOf(request))
	}
	reqs := e.prov.requests()
	if len(reqs) != 1 || reqs[0].traceparent != traceParentOf(d.call) {
		t.Errorf("provider traceparent %v, want the call span's %q", reqs, traceParentOf(d.call))
	}
	if !tr.hasChild(request, scopePGX) || !tr.hasChild(d.process, scopePGX) {
		t.Error("no statement span under the request or under worker.process")
	}
	if attrOf(d.process, tracing.AttrOutcome) != string(domain.OutcomeSuccess) || attrOf(d.process, tracing.AttrAttempt) != "1" {
		t.Errorf("worker.process attributes %v", d.process.Attributes)
	}
	// The read is named by its route template too, never by its path.
	tr.server("GET /v1/notifications/{id}")
	tr.checkSpans(id, []delivery{d})
}

// A retry is a new trace linked to the request: the second attempt's
// relay.publish is a root span whose one link is the stored trace_parent,
// and its worker.process and provider call are in that new trace.
func TestTracing_RetryIsNewTraceLinkedToTheRequest(t *testing.T) {
	e := newEnv(t)
	tr := e.startTraced(e.config(faultVars(e.prov.srv.URL)), true)

	// The first request is answered 503 (retryable); every later one 202.
	e.prov.answer(http.StatusServiceUnavailable)
	release := e.prov.block(t)
	id := tr.post(http.Header{}, "")
	testutil.Eventually(t, func() bool { return e.prov.count() == 1 }, waitTimeout)
	e.prov.answer(0)
	release()
	e.waitStatus(id, domain.StatusSent)

	ds := tr.deliveries(id, 2)
	request := tr.server("POST /v1/notifications")
	first, retry := ds[0], ds[1]
	if attrOf(first.process, tracing.AttrAttempt) != "1" || attrOf(retry.process, tracing.AttrAttempt) != "2" {
		first, retry = retry, first
	}
	if first.publish.SpanContext.TraceID() != request.SpanContext.TraceID() ||
		attrOf(first.process, tracing.AttrOutcome) != string(domain.OutcomeRetryable) {
		t.Errorf("first attempt: trace %s outcome %q, want the request's trace and retryable",
			first.publish.SpanContext.TraceID(), attrOf(first.process, tracing.AttrOutcome))
	}
	stored := tracing.ParseTraceParent(e.get(id).TraceParent)
	p := retry.publish
	if p.Parent.IsValid() || p.SpanContext.TraceID() == request.SpanContext.TraceID() {
		t.Errorf("retry's relay.publish: parent %v trace %s, want a root in a new trace", p.Parent, p.SpanContext.TraceID())
	}
	if len(p.Links) != 1 || p.Links[0].SpanContext.TraceID() != stored.TraceID() || p.Links[0].SpanContext.SpanID() != stored.SpanID() {
		t.Errorf("retry's links %v, want one to the stored trace_parent %s", p.Links, e.get(id).TraceParent)
	}
	if retry.process.SpanContext.TraceID() != p.SpanContext.TraceID() || retry.call.SpanContext.TraceID() != p.SpanContext.TraceID() {
		t.Error("the retry's worker.process or provider call is not in the retry's trace")
	}
	reqs := e.prov.requests()
	if len(reqs) != 2 || tracing.ParseTraceParent(reqs[1].traceparent).TraceID() != p.SpanContext.TraceID() {
		t.Errorf("the retried provider request does not carry the retry's trace")
	}
	tr.checkSpans(id, ds)
}

// A scheduled notification starts a new trace linked to the request on its
// first attempt too.
func TestTracing_ScheduledDeliveryIsNewLinkedTrace(t *testing.T) {
	e := newEnv(t)
	tr := e.startTraced(e.config(nil), true)

	when := e.dbNow().Add(tracingScheduleLead).Truncate(time.Microsecond).UTC()
	id := tr.post(http.Header{}, `,"scheduled_at":"`+when.Format(time.RFC3339Nano)+`"`)
	e.waitStatus(id, domain.StatusSent)
	if e.get(id).ScheduledAt == nil {
		t.Fatal("the notification was stored without scheduled_at")
	}
	d := tr.deliveries(id, 1)[0]
	request := tr.server("POST /v1/notifications")
	p := d.publish
	if attrOf(p, tracing.AttrAttempt) != "1" || p.Parent.IsValid() || p.SpanContext.TraceID() == request.SpanContext.TraceID() {
		t.Errorf("relay.publish attempt %s parent %v trace %s, want a first attempt in a new trace",
			attrOf(p, tracing.AttrAttempt), p.Parent, p.SpanContext.TraceID())
	}
	if len(p.Links) != 1 || p.Links[0].SpanContext.SpanID() != request.SpanContext.SpanID() ||
		p.Links[0].SpanContext.TraceID() != request.SpanContext.TraceID() {
		t.Errorf("links %v, want one to the request span", p.Links)
	}
	if d.process.SpanContext.TraceID() != p.SpanContext.TraceID() || d.call.SpanContext.TraceID() != p.SpanContext.TraceID() {
		t.Error("worker.process or the provider call is not in the delivery's trace")
	}
	tr.checkSpans(id, []delivery{d})
}

// A client's own traceparent is continued: the request span is its child,
// and the stored trace_parent is the request span's context, not the
// client's. A malformed one is ignored: the request span is a root.
func TestTracing_ClientTraceParentIsContinued(t *testing.T) {
	e := newEnv(t)
	tr := e.startTraced(e.config(nil), false)

	id := tr.post(http.Header{"Traceparent": {testTraceParent}}, "")
	request := tr.server("POST /v1/notifications")
	client := tracing.ParseTraceParent(testTraceParent)
	if request.Parent.TraceID() != client.TraceID() || request.Parent.SpanID() != client.SpanID() || !request.Parent.IsRemote() {
		t.Errorf("request span parent %v, want the client's %s", request.Parent, testTraceParent)
	}
	if got := e.get(id).TraceParent; got != traceParentOf(request) || got == testTraceParent {
		t.Errorf("stored trace_parent %q, want the request span's context %q", got, traceParentOf(request))
	}

	tr.exp.Reset()
	id = tr.post(http.Header{"Traceparent": {"00-bad"}}, "")
	request = tr.server("POST /v1/notifications")
	if request.Parent.IsValid() || e.get(id).TraceParent != traceParentOf(request) {
		t.Errorf("malformed traceparent: parent %v, stored %q", request.Parent, e.get(id).TraceParent)
	}
	tr.checkSpans(id, nil)
}
