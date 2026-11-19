package tracing

import (
	"context"
	"net/url"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// sanitizing is the exporter every span passes on its way out. The
// application's own spans (tracers named under modulePath) carry only the
// attributes of ADR-0009 §6 and outcome names, and pass unchanged. The spans
// of third-party instrumentation (otelhttp, otelpgx) are reduced, because
// they record texts the application does not choose:
//
//   - a status description and an exception event's message are error
//     texts: a transport error quotes the full request URL, and a database
//     error can quote a value. The status code and the exception type stay.
//   - url.full keeps scheme, host and path; userinfo, query and fragment are
//     removed. url.query is dropped.
//   - url.path and user_agent.original of a request the api received are
//     dropped: both are text the client chose, and a path that is not a
//     route can hold anything. The span keeps http.route, the route
//     template, and the delivery's spans carry notification.id.
//   - any db.query.parameter.* attribute is dropped (otelpgx records none
//     unless asked to; this guards the configuration).
type sanitizing struct {
	sdktrace.SpanExporter
}

func (s sanitizing) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	out := make([]sdktrace.ReadOnlySpan, len(spans))
	for i, sp := range spans {
		out[i] = sanitize(sp)
	}
	return s.SpanExporter.ExportSpans(ctx, out) //nolint:wrapcheck // the exporter's own error, passed through unchanged
}

func sanitize(sp sdktrace.ReadOnlySpan) sdktrace.ReadOnlySpan {
	if own(sp.InstrumentationScope().Name) {
		return sp
	}
	return sanitized{
		ReadOnlySpan: sp,
		attrs:        sanitizeAttrs(sp.Attributes()),
		events:       sanitizeEvents(sp.Events()),
		status:       sdktrace.Status{Code: sp.Status().Code},
	}
}

// own reports whether a tracer name belongs to the application.
func own(scope string) bool {
	return strings.HasPrefix(scope, modulePath+"/")
}

// sanitized is a span with replaced attributes, events and status; the
// embedded span supplies everything else.
type sanitized struct {
	sdktrace.ReadOnlySpan
	attrs  []attribute.KeyValue
	events []sdktrace.Event
	status sdktrace.Status
}

func (s sanitized) Attributes() []attribute.KeyValue { return s.attrs }
func (s sanitized) Events() []sdktrace.Event         { return s.events }
func (s sanitized) Status() sdktrace.Status          { return s.status }

func sanitizeAttrs(in []attribute.KeyValue) []attribute.KeyValue {
	out := make([]attribute.KeyValue, 0, len(in))
	for _, kv := range in {
		switch k := string(kv.Key); {
		case k == "url.query", k == "url.path", k == "user_agent.original", strings.HasPrefix(k, "db.query.parameter"):
			continue
		case k == "url.full":
			out = append(out, attribute.String(k, stripURL(kv.Value.AsString())))
		default:
			out = append(out, kv)
		}
	}
	return out
}

// stripURL keeps scheme, host and path of a URL; a value that does not
// parse is dropped entirely.
func stripURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return (&url.URL{Scheme: u.Scheme, Host: u.Host, Path: u.Path}).String()
}

func sanitizeEvents(in []sdktrace.Event) []sdktrace.Event {
	out := make([]sdktrace.Event, len(in))
	for i, ev := range in {
		out[i] = ev
		if ev.Name != "exception" {
			continue
		}
		attrs := make([]attribute.KeyValue, 0, len(ev.Attributes))
		for _, kv := range ev.Attributes {
			if kv.Key == "exception.type" {
				attrs = append(attrs, kv)
			}
		}
		out[i].Attributes = attrs
	}
	return out
}
