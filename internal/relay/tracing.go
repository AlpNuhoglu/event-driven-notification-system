package relay

import (
	"context"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/tracing"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
)

// The outcomes of a relay.publish span.
const (
	// publishConfirmed: the broker confirmed the message and did not return
	// it; the row becomes queued when the lane transaction commits.
	publishConfirmed = "published"
	// publishNotConfirmed: the broker nacked or returned the message; the
	// row keeps its status.
	publishNotConfirmed = "not_confirmed"
	// publishFailed: the batch failed and the lane transaction rolls back.
	publishFailed = "failed"
)

// startPublish starts the relay.publish span of one row, by the hybrid rule
// (D13, ADR-0009 §6):
//
//   - the first attempt (attempt_count = 0) of a notification without
//     scheduled_at: a child of the stored trace_parent, so the request, the
//     relay, the worker and the provider call are one trace;
//   - a retry (attempt_count > 0), or any delivery of a scheduled
//     notification: a new root span, linked to the stored trace_parent.
//
// trace_parent is written once at creation, so every link points to the
// original request. A row without a valid trace_parent gets a root span
// without a link.
func (r *Relay) startPublish(ctx context.Context, lane store.Lane, row store.DueRow) trace.Span {
	opts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			tracing.AttrNotificationID.String(row.ID.String()),
			tracing.AttrChannel.String(string(lane.Channel)),
			tracing.AttrPriority.String(lane.Priority.String()),
			// The attempt this publish leads to: the claim makes it
			// attempt_count + 1.
			tracing.AttrAttempt.Int(row.AttemptCount+1),
		),
	}
	parent := tracing.ParseTraceParent(row.TraceParent)
	switch {
	case childOfRequest(row) && parent.IsValid():
		ctx = trace.ContextWithRemoteSpanContext(ctx, parent)
	case parent.IsValid():
		opts = append(opts, trace.WithNewRoot(), trace.WithLinks(trace.Link{SpanContext: parent}))
	default:
		opts = append(opts, trace.WithNewRoot())
	}
	_, span := r.tracer.Start(ctx, "relay.publish", opts...)
	return span
}

// childOfRequest is the hybrid rule's choice: only the first attempt of a
// notification that is not scheduled belongs to the request's trace.
func childOfRequest(row store.DueRow) bool {
	return row.AttemptCount == 0 && !row.Scheduled
}

// endPublish ends the publish spans of a batch with its outcome: an error
// fails every span; otherwise a row is published when its id is in
// published.
func endPublish(spans []trace.Span, rows []store.DueRow, published []uuid.UUID, err error) {
	ok := make(map[uuid.UUID]bool, len(published))
	for _, id := range published {
		ok[id] = true
	}
	for i, span := range spans {
		switch {
		case err != nil:
			tracing.SetOutcome(span, publishFailed, true)
		case ok[rows[i].ID]:
			tracing.SetOutcome(span, publishConfirmed, false)
		default:
			tracing.SetOutcome(span, publishNotConfirmed, true)
		}
		span.End()
	}
}
