// Package tracing builds a role's OpenTelemetry tracer provider (ADR-0009
// §6): spans are exported over OTLP gRPC directly to Jaeger through a
// BatchSpanProcessor, sampled by the parent-based sampler that
// OTEL_TRACES_SAMPLER and OTEL_TRACES_SAMPLER_ARG choose, and propagated in
// the W3C traceparent header. Tracing is optional: an unreachable endpoint
// never fails a role's start, and the export of the buffered spans at stop is
// bounded by TRACING_SHUTDOWN_TIMEOUT.
//
// Every span leaves the process through an exporter that removes, from the
// spans of third-party instrumentation, what could carry personal data: error
// texts, URL queries and SQL arguments (see sanitize.go).
package tracing

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"runtime/debug"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/config"
)

// modulePath is the import path of this module. The tracers of its packages
// are named under it (Tracer), which is how the exporter tells the
// application's own spans from those of third-party instrumentation.
const modulePath = "github.com/AlpNuhoglu/event-driven-notification-system"

// The span attributes of the application's own spans (ADR-0009 §6). Never
// the recipient or the content.
const (
	AttrNotificationID = attribute.Key("notification.id")
	AttrChannel        = attribute.Key("channel")
	AttrPriority       = attribute.Key("priority")
	AttrAttempt        = attribute.Key("attempt")
	AttrOutcome        = attribute.Key("outcome")
	AttrProvider       = attribute.Key("provider")
)

// The sampler names of OTEL_TRACES_SAMPLER that are supported: the ones of
// the OpenTelemetry specification that need no extension.
const (
	SamplerAlwaysOn                = "always_on"
	SamplerAlwaysOff               = "always_off"
	SamplerTraceIDRatio            = "traceidratio"
	SamplerParentBasedAlwaysOn     = "parentbased_always_on"
	SamplerParentBasedAlwaysOff    = "parentbased_always_off"
	SamplerParentBasedTraceIDRatio = "parentbased_traceidratio"
)

// Provider is a role's tracer provider.
type Provider struct {
	tp      *sdktrace.TracerProvider // nil when tracing is off
	timeout time.Duration
}

// Start builds the role's provider from the configuration and installs it,
// with the W3C trace-context propagator, as the global provider, which the
// application's tracers and the instrumentations use when they are given no
// provider of their own. Export errors are reported to logger at warn level.
//
// The exporter connects lazily, so an unreachable endpoint does not fail the
// start: its spans are dropped once the processor's buffer is full. An empty
// endpoint turns tracing off (nothing is installed). An invalid endpoint or
// sampler is a configuration error.
func Start(ctx context.Context, obs config.Observability, role string, logger *slog.Logger) (*Provider, error) {
	if obs.OTLPEndpoint == "" {
		return &Provider{}, nil
	}
	sampler, err := Sampler(obs.TracesSampler, obs.TracesSamplerArg)
	if err != nil {
		return nil, err
	}
	if err := checkEndpoint(obs.OTLPEndpoint); err != nil {
		return nil, err
	}
	exp, err := otlptracegrpc.New(ctx, otlptracegrpc.WithEndpointURL(obs.OTLPEndpoint))
	if err != nil {
		return nil, fmt.Errorf("tracing: exporter: %w", err)
	}
	p := &Provider{
		tp: sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(sanitizing{exp}),
			sdktrace.WithSampler(sampler),
			sdktrace.WithResource(roleResource(role)),
		),
		timeout: obs.TracingShutdownTimeout,
	}
	otel.SetTracerProvider(p.tp)
	otel.SetTextMapPropagator(Propagator())
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		logger.Warn("tracing: export failed; spans may be lost", "error", err)
	}))
	return p, nil
}

// NewWithExporter builds a provider that hands every span to exp when it
// ends (a synchronous processor), through the same sanitizing exporter and
// sampler as Start. It installs nothing globally. It is for tests, with the
// SDK's in-memory exporter.
func NewWithExporter(exp sdktrace.SpanExporter, obs config.Observability, role string) (*Provider, error) {
	sampler, err := Sampler(obs.TracesSampler, obs.TracesSamplerArg)
	if err != nil {
		return nil, err
	}
	return &Provider{
		tp: sdktrace.NewTracerProvider(
			sdktrace.WithSyncer(sanitizing{exp}),
			sdktrace.WithSampler(sampler),
			sdktrace.WithResource(roleResource(role)),
		),
		timeout: obs.TracingShutdownTimeout,
	}, nil
}

// TracerProvider returns the provider's tracer provider; a no-op one when
// tracing is off.
func (p *Provider) TracerProvider() trace.TracerProvider {
	if p.tp == nil {
		return noop.NewTracerProvider()
	}
	return p.tp
}

// Shutdown exports the spans still buffered and stops the exporter, for at
// most the configured shutdown timeout: it returns the context's error when
// the export has not finished by then, and the role stops anyway.
func (p *Provider) Shutdown(ctx context.Context) error {
	if p.tp == nil {
		return nil
	}
	if p.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.timeout)
		defer cancel()
	}
	if err := p.tp.Shutdown(ctx); err != nil {
		return fmt.Errorf("tracing: shutdown: %w", err)
	}
	return nil
}

// Sampler returns the sampler that OTEL_TRACES_SAMPLER names, with
// OTEL_TRACES_SAMPLER_ARG as the ratio of the ratio-based ones. An empty
// name is the specification's default, parentbased_always_on.
func Sampler(name string, arg float64) (sdktrace.Sampler, error) {
	switch name {
	case SamplerAlwaysOn:
		return sdktrace.AlwaysSample(), nil
	case SamplerAlwaysOff:
		return sdktrace.NeverSample(), nil
	case SamplerTraceIDRatio:
		return sdktrace.TraceIDRatioBased(arg), nil
	case SamplerParentBasedAlwaysOn, "":
		return sdktrace.ParentBased(sdktrace.AlwaysSample()), nil
	case SamplerParentBasedAlwaysOff:
		return sdktrace.ParentBased(sdktrace.NeverSample()), nil
	case SamplerParentBasedTraceIDRatio:
		return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(arg)), nil
	}
	return nil, errors.New("tracing: OTEL_TRACES_SAMPLER: unsupported sampler")
}

// checkEndpoint accepts an absolute http or https URL; the exporter would
// silently replace anything else by its default endpoint.
func checkEndpoint(endpoint string) error {
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return errors.New("tracing: OTEL_EXPORTER_OTLP_ENDPOINT must be an absolute http or https URL")
	}
	return nil
}

// Propagator is the propagator of every process boundary: the W3C
// traceparent header (HTTP and AMQP). It is passed to the instrumentations
// explicitly, so propagation does not depend on the global propagator.
func Propagator() propagation.TextMapPropagator {
	return propagation.TraceContext{}
}

// OrGlobal returns tp, or the global provider (the one Start installs) when
// tp is nil. A component resolves its provider when it is built, after
// Start has run.
func OrGlobal(tp trace.TracerProvider) trace.TracerProvider {
	if tp == nil {
		return otel.GetTracerProvider()
	}
	return tp
}

// Tracer returns the tracer of one of the application's packages (pkg is
// its path below the module, for example "internal/relay") from tp, or from
// the global provider when tp is nil.
func Tracer(tp trace.TracerProvider, pkg string) trace.Tracer {
	return OrGlobal(tp).Tracer(modulePath + "/" + pkg)
}

// SetOutcome records the outcome of what span covered. A failed outcome
// marks the span as an error whose description is the outcome's name, never
// an error text that could quote a provider's answer.
func SetOutcome(span trace.Span, outcome string, failed bool) {
	span.SetAttributes(AttrOutcome.String(outcome))
	if failed {
		span.SetStatus(codes.Error, outcome)
	}
}

// FormatTraceParent returns the W3C traceparent value (version 00) of sc,
// or "" when sc is not valid.
func FormatTraceParent(sc trace.SpanContext) string {
	if !sc.IsValid() {
		return ""
	}
	flags := "00"
	if sc.IsSampled() {
		flags = "01"
	}
	return "00-" + sc.TraceID().String() + "-" + sc.SpanID().String() + "-" + flags
}

// traceParentLen is the length of a version 00 traceparent value.
const traceParentLen = 55

// ParseTraceParent returns the remote span context of a W3C traceparent
// value; it is not valid when the value is empty or malformed. As the
// specification asks, a value of a later version is read by its first four
// fields, and version ff is invalid.
func ParseTraceParent(s string) trace.SpanContext {
	if len(s) < traceParentLen || s[2] != '-' || s[35] != '-' || s[52] != '-' ||
		(len(s) > traceParentLen && s[traceParentLen] != '-') {
		return trace.SpanContext{}
	}
	version, flags := s[0:2], s[53:55]
	if !lowerHex(version) || version == "ff" || (version == "00" && len(s) != traceParentLen) ||
		!lowerHex(s[3:35]) || !lowerHex(s[36:52]) || !lowerHex(flags) {
		return trace.SpanContext{}
	}
	tid, err := trace.TraceIDFromHex(s[3:35])
	if err != nil {
		return trace.SpanContext{}
	}
	sid, err := trace.SpanIDFromHex(s[36:52])
	if err != nil {
		return trace.SpanContext{}
	}
	f, err := hex.DecodeString(flags)
	if err != nil {
		return trace.SpanContext{}
	}
	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: tid, SpanID: sid, TraceFlags: trace.TraceFlags(f[0]) & trace.FlagsSampled, Remote: true,
	})
}

func lowerHex(s string) bool {
	for i := range len(s) {
		if c := s[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// roleResource describes the process: service.name is the role,
// service.version the build's VCS revision (or module version).
func roleResource(role string) *resource.Resource {
	return resource.NewSchemaless(
		attribute.String("service.name", role),
		attribute.String("service.version", buildVersion()),
	)
}

func buildVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	for _, s := range info.Settings {
		if s.Key == "vcs.revision" && s.Value != "" {
			return s.Value
		}
	}
	if info.Main.Version != "" {
		return info.Main.Version
	}
	return "unknown"
}
