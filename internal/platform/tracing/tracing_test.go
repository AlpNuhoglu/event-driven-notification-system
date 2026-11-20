package tracing

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/config"
)

const sampledParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

func newTest(t *testing.T, sampler string, arg float64) (*Provider, *tracetest.InMemoryExporter) {
	t.Helper()
	exp := tracetest.NewInMemoryExporter()
	p, err := NewWithExporter(exp, config.Observability{TracesSampler: sampler, TracesSamplerArg: arg}, "test")
	if err != nil {
		t.Fatal(err)
	}
	return p, exp
}

// OTEL_TRACES_SAMPLER_ARG = 0 with the parent-based ratio sampler exports no
// root span, and a span whose parent is sampled is still sampled: the
// relay's and the worker's spans follow the request's decision.
func TestSampler_RatioZeroDropsRootsAndKeepsSampledParents(t *testing.T) {
	p, exp := newTest(t, SamplerParentBasedTraceIDRatio, 0)
	tr := Tracer(p.TracerProvider(), "internal/test")

	_, root := tr.Start(context.Background(), "root")
	root.End()
	if n := len(exp.GetSpans()); n != 0 || root.SpanContext().IsSampled() {
		t.Fatalf("%d spans exported for a root at ratio 0", n)
	}

	ctx := trace.ContextWithRemoteSpanContext(context.Background(), ParseTraceParent(sampledParent))
	_, child := tr.Start(ctx, "child")
	child.End()
	if spans := exp.GetSpans(); len(spans) != 1 || spans[0].Name != "child" {
		t.Fatalf("spans %v, want the child of the sampled parent", spans)
	}

	// An unsampled parent stays unsampled at any ratio.
	p, exp = newTest(t, SamplerParentBasedTraceIDRatio, 1)
	unsampled := ParseTraceParent(strings.TrimSuffix(sampledParent, "01") + "00")
	ctx = trace.ContextWithRemoteSpanContext(context.Background(), unsampled)
	_, span := Tracer(p.TracerProvider(), "internal/test").Start(ctx, "child")
	span.End()
	if n := len(exp.GetSpans()); n != 0 {
		t.Errorf("%d spans exported under an unsampled parent", n)
	}
}

// Every name of the specification that needs no extension is accepted; any
// other is a configuration error.
func TestSampler_Names(t *testing.T) {
	for _, name := range []string{
		"", SamplerAlwaysOn, SamplerAlwaysOff, SamplerTraceIDRatio,
		SamplerParentBasedAlwaysOn, SamplerParentBasedAlwaysOff, SamplerParentBasedTraceIDRatio,
	} {
		if _, err := Sampler(name, 0.5); err != nil {
			t.Errorf("%q: %v", name, err)
		}
	}
	if _, err := Sampler("jaeger_remote", 1); err == nil {
		t.Error("an unsupported sampler was accepted")
	}
}

// The span of a request the api received keeps only the attributes of its
// list: everything else the instrumentation records there is text the
// client chose (the path, the user agent, the Host header, X-Forwarded-For,
// a method of its own), or is not known to be safe. The client span of the
// provider call keeps its server.address: that is the configured provider.
func TestExporter_ServerSpanKeepsOnlyItsList(t *testing.T) {
	p, exp := newTest(t, SamplerAlwaysOn, 1)
	chosen := "client-chosen-text"
	tracer := p.TracerProvider().Tracer("go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp")

	_, server := tracer.Start(context.Background(), "GET /v1/notifications/{id}", trace.WithSpanKind(trace.SpanKindServer))
	server.SetAttributes(
		attribute.String("http.request.method", "_OTHER"),
		attribute.String("http.request.method_original", chosen),
		attribute.Int("http.response.status_code", 404),
		attribute.String("http.route", "/v1/notifications/{id}"),
		attribute.String("url.scheme", "http"),
		attribute.String("url.path", "/v1/notifications/"+chosen),
		attribute.String("user_agent.original", chosen),
		attribute.String("server.address", chosen+".example.com"),
		attribute.Int("server.port", 8080),
		attribute.String("client.address", chosen),
		attribute.String("network.peer.address", "10.0.0.7"),
		attribute.String("network.protocol.version", "1.1"),
		attribute.String("an.attribute.of.a.later.version", chosen),
	)
	server.End()
	_, client := tracer.Start(context.Background(), "HTTP POST", trace.WithSpanKind(trace.SpanKindClient))
	client.SetAttributes(attribute.String("server.address", "provider.example.com"), attribute.String("url.full", "https://provider.example.com/send?x=1"))
	client.End()

	spans := exp.GetSpans()
	if len(spans) != 2 {
		t.Fatalf("%d spans", len(spans))
	}
	kept := map[attribute.Key]string{}
	for _, kv := range spans[0].Attributes {
		kept[kv.Key] = kv.Value.String()
		if strings.Contains(kv.Value.String(), chosen) {
			t.Errorf("the server span keeps %s = %q, which holds text the client chose", kv.Key, kv.Value.String())
		}
	}
	want := map[attribute.Key]string{
		"http.request.method": "_OTHER", "http.response.status_code": "404", "http.route": "/v1/notifications/{id}",
		"url.scheme": "http", "server.port": "8080", "network.peer.address": "10.0.0.7", "network.protocol.version": "1.1",
	}
	if len(kept) != len(want) {
		t.Errorf("the server span keeps %v, want exactly %v", kept, want)
	}
	for k, v := range want {
		if kept[k] != v {
			t.Errorf("%s = %q, want %q", k, kept[k], v)
		}
	}
	of := map[attribute.Key]string{}
	for _, kv := range spans[1].Attributes {
		of[kv.Key] = kv.Value.String()
	}
	if of["server.address"] != "provider.example.com" || of["url.full"] != "https://provider.example.com/send" {
		t.Errorf("the client span has %v, want its server.address and a url.full without the query", of)
	}
}

// A span of third-party instrumentation leaves without error texts, URL
// queries or SQL arguments; the application's own spans pass unchanged.
func TestExporter_SanitizesThirdPartySpans(t *testing.T) {
	p, exp := newTest(t, SamplerAlwaysOn, 1)
	secret := "+905551234567"

	_, span := p.TracerProvider().Tracer("go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp").Start(context.Background(), "HTTP POST")
	span.SetAttributes(
		attribute.String("url.full", "https://user:pw@provider.example.com/send?to="+secret+"#frag"),
		attribute.String("url.query", "to="+secret),
		attribute.String("db.query.parameter.0", secret),
		attribute.String("server.address", "provider.example.com"),
	)
	span.RecordError(errors.New("invalid number " + secret))
	span.SetStatus(codes.Error, "Post \"https://provider.example.com/send?to="+secret+"\": refused")
	span.End()

	_, own := Tracer(p.TracerProvider(), "internal/worker").Start(context.Background(), "worker.process")
	SetOutcome(own, "retryable", true)
	own.End()

	spans := exp.GetSpans()
	if len(spans) != 2 {
		t.Fatalf("%d spans", len(spans))
	}
	third, mine := spans[0], spans[1]
	got := map[attribute.Key]string{}
	for _, kv := range third.Attributes {
		got[kv.Key] = kv.Value.String()
	}
	want := map[attribute.Key]string{
		"url.full":       "https://provider.example.com/send",
		"server.address": "provider.example.com",
	}
	if len(got) != len(want) || got["url.full"] != want["url.full"] || got["server.address"] != want["server.address"] {
		t.Errorf("attributes %v, want %v", got, want)
	}
	if third.Status.Code != codes.Error || third.Status.Description != "" {
		t.Errorf("status %v, want an error without description", third.Status)
	}
	if len(third.Events) != 1 || len(third.Events[0].Attributes) != 1 || third.Events[0].Attributes[0].Key != "exception.type" {
		t.Errorf("events %v, want the exception's type only", third.Events)
	}
	if mine.Status.Description != "retryable" {
		t.Errorf("own span's status %v, want its outcome kept", mine.Status)
	}
}

// The traceparent of a span context and back.
func TestTraceParent_RoundTrip(t *testing.T) {
	sc := ParseTraceParent(sampledParent)
	if !sc.IsValid() || !sc.IsRemote() || !sc.IsSampled() || FormatTraceParent(sc) != sampledParent {
		t.Errorf("parsed %v formatted %q", sc, FormatTraceParent(sc))
	}
	for _, bad := range []string{"", "00-bad", "00-00000000000000000000000000000000-00f067aa0ba902b7-01"} {
		if ParseTraceParent(bad).IsValid() || FormatTraceParent(ParseTraceParent(bad)) != "" {
			t.Errorf("%q parsed as valid", bad)
		}
	}
}

// blackhole accepts TCP connections and never answers: an OTLP endpoint
// whose export hangs until its context ends.
func blackhole(t *testing.T) string {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			_ = c.Close()
		}
	})
	return "http://" + ln.Addr().String()
}

// Start does not fail while the endpoint does not answer, and Shutdown
// returns within its bound although the buffered span cannot be exported.
func TestStart_UnreachableEndpointNeverHoldsTheStop(t *testing.T) {
	const bound = 200 * time.Millisecond
	obs := config.Observability{
		OTLPEndpoint: blackhole(t), TracesSampler: SamplerParentBasedAlwaysOn, TracingShutdownTimeout: bound,
	}
	p, err := Start(context.Background(), obs, "relay", slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	_, span := Tracer(nil, "internal/test").Start(context.Background(), "buffered")
	if !span.SpanContext().IsSampled() {
		t.Fatal("the global provider was not installed")
	}
	span.End()

	start := time.Now()
	err = p.Shutdown(context.Background())
	took := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("shutdown error %v, want the bound's deadline", err)
	}
	// The bound plus scheduling slack; the exporter's own timeout is 10 s.
	if took > bound+time.Second {
		t.Errorf("shutdown took %s, bound %s", took, bound)
	}
}

// An empty endpoint turns tracing off; a malformed endpoint or sampler is a
// configuration error.
func TestStart_Configuration(t *testing.T) {
	log := slog.New(slog.DiscardHandler)
	p, err := Start(context.Background(), config.Observability{}, "api", log)
	if err != nil || p.Shutdown(context.Background()) != nil {
		t.Errorf("tracing off: %v", err)
	}
	if _, err := Start(context.Background(), config.Observability{OTLPEndpoint: "jaeger:4317"}, "api", log); err == nil {
		t.Error("an endpoint without scheme was accepted")
	}
	if _, err := Start(context.Background(), config.Observability{OTLPEndpoint: "http://jaeger:4317", TracesSampler: "nope"}, "api", log); err == nil {
		t.Error("an unsupported sampler was accepted")
	}
}
