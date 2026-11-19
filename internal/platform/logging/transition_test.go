package logging

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

// A final outcome is one info line, an intermediate transition a debug
// line; both carry the notification's standard fields.
func TestTransition_LevelByStatus(t *testing.T) {
	batch := uuid.New()
	reason := domain.ReasonExpired
	for _, tt := range []struct {
		status domain.Status
		level  string
	}{
		{domain.StatusPending, "DEBUG"},
		{domain.StatusQueued, "DEBUG"},
		{domain.StatusProcessing, "DEBUG"},
		{domain.StatusRetrying, "DEBUG"},
		{domain.StatusSent, "INFO"},
		{domain.StatusFailed, "INFO"},
		{domain.StatusCancelled, "INFO"},
	} {
		var buf bytes.Buffer
		e := domain.StatusChanged{
			ID: uuid.New(), BatchID: &batch, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh,
			Status: tt.status, AttemptCount: 2, CorrelationID: "corr-row",
		}
		if tt.status == domain.StatusFailed {
			e.FailureReason = &reason
		}
		Transition(context.Background(), New(&buf, Options{Service: "worker", Level: slog.LevelDebug}), e,
			slog.String("outcome", "success"))
		m := only(t, &buf)
		want := map[string]any{
			"level": tt.level, "msg": "notification " + string(tt.status), "notification_id": e.ID.String(),
			"batch_id": batch.String(), "channel": "sms", "priority": "high", "status": string(tt.status),
			"attempt": float64(2), "correlation_id": "corr-row", "outcome": "success",
		}
		for k, v := range want {
			if m[k] != v {
				t.Errorf("%s: %s = %v, want %v", tt.status, k, m[k], v)
			}
		}
		if got := m["failure_reason"]; (tt.status == domain.StatusFailed) != (got == "expired") {
			t.Errorf("%s: failure_reason %v", tt.status, got)
		}

		// At info level only the final outcomes are written.
		buf.Reset()
		Transition(context.Background(), New(&buf, Options{Service: "worker"}), e)
		if written := buf.Len() > 0; written != (tt.level == "INFO") {
			t.Errorf("%s at info level: written %v", tt.status, written)
		}
	}
}

// The context's correlation id (the api's request) wins over the row's.
func TestTransition_CorrelationIDOfTheContextFirst(t *testing.T) {
	var buf bytes.Buffer
	e := domain.StatusChanged{ID: uuid.New(), Channel: domain.ChannelEmail, Status: domain.StatusCancelled, CorrelationID: "corr-row"}
	Transition(WithCorrelationID(context.Background(), "corr-request"), New(&buf, Options{Service: "api"}), e)
	if m := only(t, &buf); m["correlation_id"] != "corr-request" {
		t.Errorf("correlation_id %v, want the request's", m["correlation_id"])
	}
	if m := only(t, &buf); m["batch_id"] != nil || m["failure_reason"] != nil {
		t.Errorf("absent fields written: %v", m)
	}
}
