package api

import (
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/tracing"
)

// traced wraps the API's handler in its request span (ADR-0009 §6). The
// span continues the client's traceparent when it sends a valid one, and is
// a root span otherwise. Its context is what a create stores in
// notifications.trace_parent (withRequestHeaders), so that the first
// delivery of the notification hangs under the request.
//
// Before otelhttp reads the header, the traceparent header is normalised by
// the same rule that decides what is stored: a request with more than one
// value, or a malformed one, carries none. The span is named by the method
// until the router has matched a route, then by the route template
// (nameRequestSpan); never by the raw path. Of the request otelhttp records
// the path, never the query.
func traced(next http.Handler, tp trace.TracerProvider) http.Handler {
	h := otelhttp.NewHandler(next, "",
		otelhttp.WithTracerProvider(tracing.OrGlobal(tp)),
		otelhttp.WithPropagators(tracing.Propagator()),
		otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string { return methodLabel(r.Method) }),
	)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if v := traceParent(r.Header.Values(headerTraceParent)); v == "" {
			r.Header.Del(headerTraceParent)
		} else {
			r.Header.Set(headerTraceParent, v)
		}
		h.ServeHTTP(w, r)
	})
}

// nameRequestSpan names the request span after the route the router
// matched ("POST /v1/notifications", "GET /v1/notifications/{id}"), the
// label http_requests_total uses, and records it as http.route. A request
// that matches no route keeps the method as its name.
func nameRequestSpan(r *http.Request, pattern string) {
	if pattern == "" {
		return
	}
	route := routeLabel(pattern)
	span := trace.SpanFromContext(r.Context())
	span.SetName(methodLabel(r.Method) + " " + route)
	span.SetAttributes(attribute.String("http.route", route))
}
