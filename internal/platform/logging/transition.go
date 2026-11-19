package logging

import (
	"context"
	"log/slog"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

// Transition logs the status change e of one notification, after its
// transaction has committed. A final outcome (sent, failed, cancelled) is
// logged at info, every intermediate transition (pending, queued,
// processing, retrying) at debug (ADR-0009 §4). The message is
// "notification <status>"; the line carries notification_id, batch_id (when
// the notification has one), channel, priority, status, attempt (the
// attempts made so far), failure_reason (when failed) and attrs.
//
// Its correlation_id is the one ctx carries; when ctx carries none, the
// event's own (the notification's row), as in the relay.
func Transition(ctx context.Context, l *slog.Logger, e domain.StatusChanged, attrs ...slog.Attr) {
	level := slog.LevelDebug
	if final(e.Status) {
		level = slog.LevelInfo
	}
	if !l.Enabled(ctx, level) {
		return
	}
	if CorrelationID(ctx) == "" {
		ctx = WithCorrelationID(ctx, e.CorrelationID)
	}
	as := make([]slog.Attr, 0, 7+len(attrs))
	as = append(as, slog.String(FieldNotificationID, e.ID.String()))
	if e.BatchID != nil {
		as = append(as, slog.String(FieldBatchID, e.BatchID.String()))
	}
	as = append(as,
		slog.String(FieldChannel, string(e.Channel)),
		slog.String(FieldPriority, e.Priority.String()),
		slog.String("status", string(e.Status)),
		slog.Int(FieldAttempt, e.AttemptCount),
	)
	if e.FailureReason != nil {
		as = append(as, slog.String("failure_reason", string(*e.FailureReason)))
	}
	l.LogAttrs(ctx, level, "notification "+string(e.Status), append(as, attrs...)...)
}

// final reports whether s is a final outcome: the statuses that
// notifications_finished_total counts.
func final(s domain.Status) bool {
	switch s {
	case domain.StatusSent, domain.StatusFailed, domain.StatusCancelled:
		return true
	}
	return false
}
