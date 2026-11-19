package provider

import (
	"context"
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/tracing"
)

// tracedTransport is the client's transport with the provider call's span
// (ADR-0009 §6): otelhttp starts a client span, a child of the span in the
// request's context (the worker's worker.process), and sends its context as
// the traceparent header. The span carries the notification's id and
// channel and the provider's name; of the request, otelhttp records the
// method and the URL, whose userinfo, query and fragment the tracing
// package's exporter removes, and never the body.
type tracedTransport struct {
	*otelhttp.Transport
	base *http.Transport
}

func newTracedTransport(base *http.Transport, tp trace.TracerProvider, name string) tracedTransport {
	return tracedTransport{
		Transport: otelhttp.NewTransport(annotating{base},
			otelhttp.WithTracerProvider(tracing.OrGlobal(tp)),
			otelhttp.WithPropagators(tracing.Propagator()),
			otelhttp.WithSpanOptions(trace.WithAttributes(tracing.AttrProvider.String(name))),
		),
		base: base,
	}
}

// CloseIdleConnections closes the idle connections of the base transport;
// otelhttp's transport does not pass the call on.
func (t tracedTransport) CloseIdleConnections() { t.base.CloseIdleConnections() }

// annotating runs under otelhttp, so the context of the request it sees
// holds the client span; it adds the attributes Send put in the context.
type annotating struct{ next http.RoundTripper }

func (a annotating) RoundTrip(r *http.Request) (*http.Response, error) {
	if attrs, ok := r.Context().Value(spanAttrsKey{}).([]attribute.KeyValue); ok {
		trace.SpanFromContext(r.Context()).SetAttributes(attrs...)
	}
	return a.next.RoundTrip(r) //nolint:wrapcheck // a transport's error is passed through unchanged
}

type spanAttrsKey struct{}

// withSpanAttrs puts the attributes of the request's client span in ctx.
func withSpanAttrs(ctx context.Context, r Request) context.Context {
	return context.WithValue(ctx, spanAttrsKey{}, []attribute.KeyValue{
		tracing.AttrNotificationID.String(r.NotificationID.String()),
		tracing.AttrChannel.String(string(r.Channel)),
	})
}
