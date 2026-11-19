package worker

import (
	"testing"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/config"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/tracing"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
)

// publishContext is the traceparent header of the deliveries below: the
// context of the relay's publish span.
const publishContext = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

// tracedHarness is a harness whose worker records its spans in exp.
func tracedHarness(t *testing.T) (*harness, *tracetest.InMemoryExporter) {
	t.Helper()
	exp := tracetest.NewInMemoryExporter()
	p, err := tracing.NewWithExporter(exp, config.Observability{TracesSampler: tracing.SamplerParentBasedAlwaysOn}, "worker")
	if err != nil {
		t.Fatal(err)
	}
	h := newHarness(t)
	h.tp = p.TracerProvider()
	h.build()
	return h, exp
}

// processSpan waits for the one worker.process span the delivery produces.
func processSpan(h *harness, exp *tracetest.InMemoryExporter) tracetest.SpanStub {
	h.t.Helper()
	h.eventually(func() bool { return len(exp.GetSpans()) == 1 })
	s := exp.GetSpans()[0]
	if s.Name != "worker.process" {
		h.t.Fatalf("span %q, want worker.process", s.Name)
	}
	return s
}

func spanAttr(s tracetest.SpanStub, k attribute.Key) attribute.Value {
	for _, kv := range s.Attributes {
		if kv.Key == k {
			return kv.Value
		}
	}
	return attribute.Value{}
}

// worker.process continues the delivery's traceparent header (a child of
// the relay's publish span), covers the claim to the final write, carries
// the delivery's attributes and ends with the attempt's outcome; the
// provider is called inside it, so the provider call's span is its child.
func TestProcessSpan_ContinuesTheDeliveryHeader(t *testing.T) {
	h, exp := tracedHarness(t)
	m := &fakeMessage{id: uuid.New(), prio: prioNormal, tp: publishContext}
	h.w.Push(m)
	h.start()
	s := processSpan(h, exp)

	parent := tracing.ParseTraceParent(publishContext)
	if s.SpanContext.TraceID() != parent.TraceID() || s.Parent.SpanID() != parent.SpanID() {
		t.Errorf("trace %s parent %s, want a child of %s", s.SpanContext.TraceID(), s.Parent.SpanID(), publishContext)
	}
	if s.SpanKind != oteltrace.SpanKindConsumer {
		t.Errorf("kind %v, want consumer", s.SpanKind)
	}
	for k, want := range map[attribute.Key]string{
		tracing.AttrNotificationID: m.id.String(),
		tracing.AttrChannel:        string(domain.ChannelSMS),
		tracing.AttrProvider:       h.prov.Name(),
		tracing.AttrOutcome:        string(domain.OutcomeSuccess),
	} {
		if got := spanAttr(s, k).AsString(); got != want {
			t.Errorf("%s = %q, want %q", k, got, want)
		}
	}
	// The fake claim opens attempt 3.
	if got := spanAttr(s, tracing.AttrAttempt).AsInt64(); got != 3 || spanAttr(s, tracing.AttrPriority).AsString() == "" {
		t.Errorf("attempt %d priority %q", got, spanAttr(s, tracing.AttrPriority).AsString())
	}
	if s.Status.Code != codes.Unset {
		t.Errorf("status %v, want unset for a success", s.Status)
	}
	sends := h.prov.sent()
	if len(sends) != 1 || sends[0].span.SpanID() != s.SpanContext.SpanID() {
		t.Fatalf("the provider was not called inside worker.process: %+v", sends)
	}
	// The final write ended before the span did.
	if _, sent, _, _ := h.st.snapshot(); len(sent) != 1 || !s.EndTime.After(s.StartTime) {
		t.Errorf("final writes %d", len(sent))
	}
}

// A failed send marks worker.process as an error whose description is the
// outcome's name, never an error text.
func TestProcessSpan_FailedSendIsAnErrorNamedByItsOutcome(t *testing.T) {
	h, exp := tracedHarness(t)
	h.prov.respond = func(Request) Response { return Response{StatusCode: 503, Err: nil} }
	h.w.Push(&fakeMessage{id: uuid.New(), prio: prioNormal, tp: publishContext})
	h.start()
	s := processSpan(h, exp)
	if s.Status.Code != codes.Error || s.Status.Description != string(domain.OutcomeRetryable) ||
		spanAttr(s, tracing.AttrOutcome).AsString() != string(domain.OutcomeRetryable) {
		t.Errorf("status %v outcome %q, want an error named retryable", s.Status, spanAttr(s, tracing.AttrOutcome).AsString())
	}
}

// A delivery without a traceparent header starts its own trace.
func TestProcessSpan_WithoutHeaderIsARoot(t *testing.T) {
	h, exp := tracedHarness(t)
	h.push(prioNormal)
	h.start()
	s := processSpan(h, exp)
	if s.Parent.IsValid() || !s.SpanContext.IsValid() {
		t.Errorf("parent %v, want a root span", s.Parent)
	}
}

// A claim that matches no row ends the span with not_applied, which is not
// an error.
func TestProcessSpan_ClaimNotApplied(t *testing.T) {
	h, exp := tracedHarness(t)
	h.st.claimErr = func(uuid.UUID) error { return store.ErrNotApplied }
	h.w.Push(&fakeMessage{id: uuid.New(), prio: prioNormal, tp: publishContext})
	h.start()
	s := processSpan(h, exp)
	if spanAttr(s, tracing.AttrOutcome).AsString() != processNotApplied || s.Status.Code != codes.Unset {
		t.Errorf("outcome %q status %v", spanAttr(s, tracing.AttrOutcome).AsString(), s.Status)
	}
}
