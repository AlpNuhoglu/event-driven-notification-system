package domain

import (
	"time"

	"github.com/google/uuid"
)

// StatusChanged is the event every state transition returns.
type StatusChanged struct {
	ID            uuid.UUID
	BatchID       *uuid.UUID
	Channel       Channel
	Priority      Priority
	Status        Status // the new status
	Version       int64
	FailureReason *FailureReason // nil unless Status is failed
	AttemptCount  int
	UpdatedAt     time.Time
	// CorrelationID is the notification's correlation id, for the log lines
	// of the transition. It is not part of the published event (the event
	// body has a fixed set of members), and it is empty for the transitions
	// whose statement returns only the event's columns (reconciliation and
	// requeue-stale, transition 8).
	CorrelationID string
}

// RoutingKey returns the broker routing key "status.{channel}.{status}".
func (e StatusChanged) RoutingKey() string {
	return "status." + string(e.Channel) + "." + string(e.Status)
}
