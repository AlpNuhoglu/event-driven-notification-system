package provider

import (
	"bytes"
	"context"
	"fmt"
	"io"
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
	r, err := inMemoryBody(r)
	if err != nil {
		return nil, err
	}
	return a.next.RoundTrip(r) //nolint:wrapcheck // a transport's error is passed through unchanged
}

// inMemoryBody returns r with its body read into memory, for the base
// transport. otelhttp wraps the body of a request to count its bytes, and
// net/http treats a body it does not know to be in memory differently: it
// sends the headers first and every write of the body at once, so the whole
// request can reach the provider, and the provider can answer or close the
// connection, before the transport calls the WroteRequest hook. A call that
// failed at that moment was reported as not written, and so classified as
// unreachable instead of unknown (seen in P6 as a 202 with WroteRequest
// false). With a body net/http knows to be in memory the transport calls
// the hook when the request is in its buffer and flushes afterwards, so the
// provider cannot have the whole request before the hook has run. The
// bodies are the small JSON documents of Send.
func inMemoryBody(r *http.Request) (*http.Request, error) {
	if r.Body == nil || r.Body == http.NoBody {
		return r, nil
	}
	b, err := io.ReadAll(r.Body)
	if cerr := r.Body.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return nil, fmt.Errorf("provider: read request body: %w", err)
	}
	out := r.Clone(r.Context())
	out.Body = io.NopCloser(bytes.NewReader(b))
	out.ContentLength = int64(len(b))
	// One attempt is exactly one request (Send): the copy cannot be replayed
	// either.
	out.GetBody = nil
	return out, nil
}

type spanAttrsKey struct{}

// withSpanAttrs puts the attributes of the request's client span in ctx.
func withSpanAttrs(ctx context.Context, r Request) context.Context {
	return context.WithValue(ctx, spanAttrsKey{}, []attribute.KeyValue{
		tracing.AttrNotificationID.String(r.NotificationID.String()),
		tracing.AttrChannel.String(string(r.Channel)),
	})
}
