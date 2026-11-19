package relay

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/config"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/tracing"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/queue"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
)

// laneTraceParent is a valid stored trace parent per channel, so that a
// message can be matched to its row.
func laneTraceParent(c domain.Channel) string {
	ids := map[domain.Channel]string{
		domain.ChannelSMS:   "4bf92f3577b34da6a3ce929d0e0e4736",
		domain.ChannelEmail: "5bf92f3577b34da6a3ce929d0e0e4736",
		domain.ChannelPush:  "6bf92f3577b34da6a3ce929d0e0e4736",
	}
	return "00-" + ids[c] + "-00f067aa0ba902b7-01"
}

// requestParent is the stored trace_parent of the rows below: the context
// of the api's request span.
const requestParent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"

// confirming is a publisher that confirms the ids in confirm (all when nil)
// or fails the batch with err.
type confirming struct {
	confirm map[uuid.UUID]bool
	err     error
	msgs    []queue.Message
}

func (p *confirming) PublishBatch(_ context.Context, msgs []queue.Message, _ time.Duration) ([]uuid.UUID, error) {
	p.msgs = append(p.msgs, msgs...)
	if p.err != nil {
		return nil, p.err
	}
	var ids []uuid.UUID
	for _, m := range msgs {
		if p.confirm == nil || p.confirm[m.ID] {
			ids = append(ids, m.ID)
		}
	}
	return ids, nil
}

var smsNormal = store.Lane{Channel: domain.ChannelSMS, Priority: domain.PriorityNormal}

// publishTraced runs the lane's publish function over rows with a relay
// whose spans go to an in-memory exporter, and returns the spans by
// notification id and the messages.
func publishTraced(t *testing.T, pub *confirming, rows ...store.DueRow) (map[uuid.UUID]tracetest.SpanStub, []queue.Message, error) {
	t.Helper()
	exp := tracetest.NewInMemoryExporter()
	p, err := tracing.NewWithExporter(exp, config.Observability{TracesSampler: tracing.SamplerParentBasedAlwaysOn}, "relay")
	if err != nil {
		t.Fatal(err)
	}
	cfg := unitConfig
	cfg.TracerProvider = p.TracerProvider()
	r := New(&fakeStore{}, pub, &fakeConn{}, &recorder{}, slog.New(slog.DiscardHandler), cfg, func() float64 { return 0.5 }, nil)
	_, perr := r.publishFunc(smsNormal)(context.Background(), rows)
	spans := map[uuid.UUID]tracetest.SpanStub{}
	for _, s := range exp.GetSpans() {
		if s.Name != "relay.publish" {
			t.Errorf("span %q, want relay.publish only", s.Name)
		}
		id, err := uuid.Parse(attr(s.Attributes, tracing.AttrNotificationID).AsString())
		if err != nil {
			t.Fatalf("span without notification.id: %v", s.Attributes)
		}
		spans[id] = s
	}
	if len(spans) != len(rows) {
		t.Fatalf("%d publish spans for %d rows", len(spans), len(rows))
	}
	return spans, pub.msgs, perr
}

func attr(attrs []attribute.KeyValue, k attribute.Key) attribute.Value {
	for _, kv := range attrs {
		if kv.Key == k {
			return kv.Value
		}
	}
	return attribute.Value{}
}

// headerOf returns the traceparent the message of id carries.
func headerOf(t *testing.T, msgs []queue.Message, id uuid.UUID) trace.SpanContext {
	t.Helper()
	for _, m := range msgs {
		if m.ID == id {
			return tracing.ParseTraceParent(m.TraceParent)
		}
	}
	t.Fatalf("no message for %s", id)
	return trace.SpanContext{}
}

// The first attempt of a notification without scheduled_at is published
// under the request: its span is a child of the stored trace_parent, in the
// request's trace, without links, and the message carries the publish
// span's own context.
func TestPublishSpan_FirstAttemptIsChildOfStoredTraceParent(t *testing.T) {
	row := store.DueRow{ID: uuid.New(), TraceParent: requestParent}
	spans, msgs, err := publishTraced(t, &confirming{}, row)
	if err != nil {
		t.Fatal(err)
	}
	s, parent := spans[row.ID], tracing.ParseTraceParent(requestParent)
	if s.SpanContext.TraceID() != parent.TraceID() || s.Parent.SpanID() != parent.SpanID() || !s.Parent.IsRemote() {
		t.Errorf("span %s parent %s, want a child of %s", s.SpanContext.TraceID(), s.Parent.SpanID(), requestParent)
	}
	if len(s.Links) != 0 {
		t.Errorf("links %v, want none", s.Links)
	}
	if h := headerOf(t, msgs, row.ID); h.SpanID() != s.SpanContext.SpanID() || h.TraceID() != s.SpanContext.TraceID() {
		t.Errorf("message traceparent %v, want the publish span's context %v", h, s.SpanContext)
	}
	if s.SpanKind != trace.SpanKindProducer || attr(s.Attributes, tracing.AttrAttempt).AsInt64() != 1 ||
		attr(s.Attributes, tracing.AttrChannel).AsString() != "sms" || attr(s.Attributes, tracing.AttrPriority).AsString() != "normal" ||
		attr(s.Attributes, tracing.AttrOutcome).AsString() != publishConfirmed || s.Status.Code == codes.Error {
		t.Errorf("kind %v attributes %v status %v", s.SpanKind, s.Attributes, s.Status)
	}
}

// A retry (attempt_count > 0) and every delivery of a scheduled
// notification, its first included, start a new trace: a root span linked
// to the stored trace_parent, whose context the message carries.
func TestPublishSpan_RetryAndScheduledAreLinkedRoots(t *testing.T) {
	for _, tc := range []struct {
		name string
		row  store.DueRow
	}{
		{"retry", store.DueRow{ID: uuid.New(), TraceParent: requestParent, AttemptCount: 2}},
		{"scheduled first attempt", store.DueRow{ID: uuid.New(), TraceParent: requestParent, Scheduled: true}},
		{"scheduled retry", store.DueRow{ID: uuid.New(), TraceParent: requestParent, AttemptCount: 1, Scheduled: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spans, msgs, err := publishTraced(t, &confirming{}, tc.row)
			if err != nil {
				t.Fatal(err)
			}
			s, parent := spans[tc.row.ID], tracing.ParseTraceParent(requestParent)
			if s.Parent.IsValid() || s.SpanContext.TraceID() == parent.TraceID() {
				t.Errorf("span in trace %s with parent %v, want a root in a new trace", s.SpanContext.TraceID(), s.Parent)
			}
			if len(s.Links) != 1 || s.Links[0].SpanContext.TraceID() != parent.TraceID() || s.Links[0].SpanContext.SpanID() != parent.SpanID() {
				t.Errorf("links %v, want one to %s", s.Links, requestParent)
			}
			if h := headerOf(t, msgs, tc.row.ID); h.SpanID() != s.SpanContext.SpanID() || h.TraceID() != s.SpanContext.TraceID() {
				t.Errorf("message traceparent %v, want the publish span's context %v", h, s.SpanContext)
			}
			if got := attr(s.Attributes, tracing.AttrAttempt).AsInt64(); got != int64(tc.row.AttemptCount+1) {
				t.Errorf("attempt %d, want %d", got, tc.row.AttemptCount+1)
			}
		})
	}
}

// A row without a valid stored trace parent gets a root span without a
// link, on its first attempt too.
func TestPublishSpan_NoStoredTraceParentIsRootWithoutLink(t *testing.T) {
	row := store.DueRow{ID: uuid.New()}
	spans, msgs, err := publishTraced(t, &confirming{}, row)
	if err != nil {
		t.Fatal(err)
	}
	s := spans[row.ID]
	if s.Parent.IsValid() || len(s.Links) != 0 || !s.SpanContext.IsValid() {
		t.Errorf("parent %v links %v, want a root without links", s.Parent, s.Links)
	}
	if h := headerOf(t, msgs, row.ID); h.SpanID() != s.SpanContext.SpanID() {
		t.Errorf("message traceparent %v, want %v", h, s.SpanContext)
	}
}

// Each span ends with what the broker did with its message: confirmed is
// published; nacked or returned is not_confirmed, an error; a failed batch
// fails every span. The status description is the outcome's name.
func TestPublishSpan_EndsWithTheOutcomeOfThePublish(t *testing.T) {
	a, b := store.DueRow{ID: uuid.New(), TraceParent: requestParent}, store.DueRow{ID: uuid.New(), TraceParent: requestParent}
	spans, _, err := publishTraced(t, &confirming{confirm: map[uuid.UUID]bool{a.ID: true}}, a, b)
	if err != nil {
		t.Fatal(err)
	}
	if got := attr(spans[a.ID].Attributes, tracing.AttrOutcome).AsString(); got != publishConfirmed || spans[a.ID].Status.Code != codes.Unset {
		t.Errorf("confirmed: outcome %q status %v", got, spans[a.ID].Status)
	}
	if got := attr(spans[b.ID].Attributes, tracing.AttrOutcome).AsString(); got != publishNotConfirmed ||
		spans[b.ID].Status != (sdktrace.Status{Code: codes.Error, Description: publishNotConfirmed}) {
		t.Errorf("not confirmed: outcome %q status %v", got, spans[b.ID].Status)
	}

	spans, _, err = publishTraced(t, &confirming{err: errors.New("confirm timeout")}, a, b)
	if err == nil {
		t.Fatal("the publisher's error was not returned")
	}
	for id, s := range spans {
		if attr(s.Attributes, tracing.AttrOutcome).AsString() != publishFailed || s.Status.Code != codes.Error ||
			strings.Contains(s.Status.Description, "timeout") {
			t.Errorf("%s: outcome %v status %v, want failed without the error text", id, s.Attributes, s.Status)
		}
	}
}
