package worker

import (
	"context"

	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/tracing"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
)

// The outcomes of a worker.process span that made no attempt. An attempt
// ends the span with its own outcome (ADR-0007 §2), aborted included.
const (
	// processRequeued: the claim kept failing until shutdown; the delivery
	// was requeued.
	processRequeued = "requeued"
	// processNotApplied: the claim matched no row (a duplicate, cancelled,
	// expired or not-due row); the delivery was acked.
	processNotApplied = "not_applied"
	// processExpired: the claim found expires_at passed and failed the
	// notification.
	processExpired = "expired"
)

// startProcess starts the worker.process span of a delivery: a child of the
// span context in its traceparent header (the relay's relay.publish span,
// whichever trace that span is in), or a root span when the header is
// missing or invalid. The returned context carries the span to the claim,
// the provider call (whose client span is its child), the final write and
// their log lines.
func (w *Worker) startProcess(ctx context.Context, m Message) (context.Context, oteltrace.Span) {
	if sc := tracing.ParseTraceParent(m.TraceParent()); sc.IsValid() {
		ctx = oteltrace.ContextWithRemoteSpanContext(ctx, sc)
	}
	return w.tracer.Start(ctx, "worker.process",
		oteltrace.WithSpanKind(oteltrace.SpanKindConsumer),
		oteltrace.WithAttributes(
			tracing.AttrNotificationID.String(m.NotificationID().String()),
			tracing.AttrChannel.String(string(w.cfg.Channel)),
			tracing.AttrProvider.String(w.provider.Name()),
		))
}

// processClaimed adds what the claim returned: the priority and the number
// of the attempt the claim opened.
func processClaimed(span oteltrace.Span, n store.Notification) {
	span.SetAttributes(
		tracing.AttrPriority.String(n.Priority.String()),
		tracing.AttrAttempt.Int(n.AttemptCount),
	)
}

// processOutcome ends a delivery that made no attempt; only a requeue after
// a failing claim is an error.
func processOutcome(span oteltrace.Span, outcome string) {
	tracing.SetOutcome(span, outcome, outcome == processRequeued)
}

// attemptOutcome records the outcome of the attempt on the worker.process
// span in ctx. Every outcome other than success and aborted (which sent
// nothing) marks the span as an error with the outcome's name.
func attemptOutcome(ctx context.Context, o domain.Outcome) {
	failed := o != domain.OutcomeSuccess && o != domain.OutcomeAborted
	tracing.SetOutcome(oteltrace.SpanFromContext(ctx), string(o), failed)
}
