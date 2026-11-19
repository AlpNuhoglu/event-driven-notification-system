// Package logging builds the one log handler every role writes through
// (ADR-0009 §4): JSON lines on the role's stdout, the level from LOG_LEVEL,
// the standard fields on every record, and key-based redaction of personal
// data as the backstop behind the typed redaction of domain.Recipient.
//
// Every record carries service (the role) and version (the build's git
// SHA). A record logged with a context carries, when the context has them,
// correlation_id (WithCorrelationID) and trace_id and span_id (the
// OpenTelemetry span context of the context). These fields are always at the
// top level of the line, also for a logger with an open group.
package logging

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"

	"go.opentelemetry.io/otel/trace"
)

// Version is the build's git SHA. The image build sets it at link time:
//
//	-ldflags "-X github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/logging.Version=<sha>"
//
// A binary built without it reports "dev".
var Version = "dev"

// The standard field names of ADR-0009 §4. A log call that concerns a
// notification uses these names for its attributes; the handler adds
// FieldService, FieldVersion, FieldCorrelationID, FieldTraceID and
// FieldSpanID itself.
const (
	FieldService        = "service"
	FieldVersion        = "version"
	FieldCorrelationID  = "correlation_id"
	FieldTraceID        = "trace_id"
	FieldSpanID         = "span_id"
	FieldNotificationID = "notification_id"
	FieldBatchID        = "batch_id"
	FieldChannel        = "channel"
	FieldPriority       = "priority"
	FieldAttempt        = "attempt"
)

// Options configures New and NewHandler.
type Options struct {
	// Service is the role's name, written as service on every record.
	Service string
	// Version is written as version on every record; Version (the package
	// variable) when empty.
	Version string
	// Level is the minimum level; info when nil.
	Level slog.Leveler
}

// New returns a logger that writes JSON lines to w through NewHandler.
func New(w io.Writer, o Options) *slog.Logger {
	return slog.New(NewHandler(w, o))
}

// NewHandler returns the central handler: slog's JSON handler with Redact as
// its ReplaceAttr, the standard fields service and version on every record,
// and correlation_id, trace_id and span_id taken from the record's context.
func NewHandler(w io.Writer, o Options) slog.Handler {
	version := o.Version
	if version == "" {
		version = Version
	}
	level := o.Level
	if level == nil {
		level = slog.LevelInfo
	}
	inner := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level, ReplaceAttr: Redact}).
		WithAttrs([]slog.Attr{slog.String(FieldService, o.Service), slog.String(FieldVersion, version)})
	return &handler{inner: inner}
}

// ParseLevel maps the value of LOG_LEVEL (debug, info, warn, error, in any
// case) to its level. An empty value is info.
func ParseLevel(s string) (slog.Level, error) {
	if s == "" {
		return slog.LevelInfo, nil
	}
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	}
	return slog.LevelInfo, fmt.Errorf("logging: unknown level %q", s)
}

type correlationKey struct{}

// WithCorrelationID returns ctx carrying the correlation id: every record
// logged with the returned context carries it as correlation_id. An empty
// id returns ctx unchanged.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	return context.WithValue(ctx, correlationKey{}, id)
}

// CorrelationID returns the correlation id ctx carries, or "".
func CorrelationID(ctx context.Context) string {
	id, _ := ctx.Value(correlationKey{}).(string)
	return id
}

// handler wraps the JSON handler. It keeps the attributes and groups of
// WithAttrs and WithGroup itself and applies them when a record is handled,
// so that the fields read from the context stay at the top level of the
// line whatever group the logger has open.
type handler struct {
	inner slog.Handler
	goas  []groupOrAttrs
}

// groupOrAttrs is one WithGroup (group set) or one WithAttrs (attrs set).
type groupOrAttrs struct {
	group string
	attrs []slog.Attr
}

func (h *handler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.inner.Enabled(ctx, l)
}

func (h *handler) WithAttrs(as []slog.Attr) slog.Handler {
	if len(as) == 0 {
		return h
	}
	return &handler{inner: h.inner, goas: append(slices.Clip(h.goas), groupOrAttrs{attrs: slices.Clone(as)})}
}

func (h *handler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	return &handler{inner: h.inner, goas: append(slices.Clip(h.goas), groupOrAttrs{group: name})}
}

// Handle writes the context's fields first, then the attributes of With
// and of the record, nested in the groups of WithGroup as slog nests them.
// A key is written once per level: a record's attribute replaces a With
// attribute of the same key (a worker's logger carries the channel, and a
// transition's line names it again), and a field of the context replaces
// a top-level attribute of the same key.
//
// Every attribute passes Redact here (redactAttrs), and once more in the
// JSON handler as its ReplaceAttr, where it then removes nothing. The first
// pass is needed because the JSON handler mishandles a group that its
// ReplaceAttr empties (Go 1.27): it leaves the group's name on the path it
// passes to ReplaceAttr for the attributes after it and drops the comma
// before the next one, which makes the line invalid JSON.
func (h *handler) Handle(ctx context.Context, r slog.Record) error {
	out := slog.NewRecord(r.Time, r.Level, r.Message, r.PC)
	attrs := make([]slog.Attr, 0, r.NumAttrs())
	r.Attrs(func(a slog.Attr) bool {
		attrs = append(attrs, a)
		return true
	})
	for i := len(h.goas) - 1; i >= 0; i-- {
		g := h.goas[i]
		if g.group == "" {
			attrs = append(without(g.attrs, attrs), attrs...)
			continue
		}
		// A group without attributes is left out, as slog does.
		if len(attrs) > 0 {
			attrs = []slog.Attr{{Key: g.group, Value: slog.GroupValue(attrs...)}}
		}
	}
	fields := contextAttrs(ctx)
	out.AddAttrs(fields...)
	out.AddAttrs(redactAttrs(nil, without(attrs, fields))...)
	return h.inner.Handle(ctx, out)
}

// without returns the attributes of as whose keys are not keys of others.
func without(as, others []slog.Attr) []slog.Attr {
	if len(others) == 0 {
		return slices.Clip(as)
	}
	out := make([]slog.Attr, 0, len(as))
	for _, a := range as {
		if !slices.ContainsFunc(others, func(o slog.Attr) bool { return o.Key == a.Key }) {
			out = append(out, a)
		}
	}
	return out
}

// contextAttrs returns correlation_id, trace_id and span_id of ctx, each
// only when ctx has it.
func contextAttrs(ctx context.Context) []slog.Attr {
	if ctx == nil {
		return nil
	}
	var as []slog.Attr
	if id := CorrelationID(ctx); id != "" {
		as = append(as, slog.String(FieldCorrelationID, id))
	}
	sc := trace.SpanContextFromContext(ctx)
	if sc.HasTraceID() {
		as = append(as, slog.String(FieldTraceID, sc.TraceID().String()))
	}
	if sc.HasSpanID() {
		as = append(as, slog.String(FieldSpanID, sc.SpanID().String()))
	}
	return as
}
