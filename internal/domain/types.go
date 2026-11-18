package domain

import "fmt"

// Channel is a delivery channel.
type Channel string

// The supported channels.
const (
	// ChannelSMS delivers by SMS.
	ChannelSMS Channel = "sms"
	// ChannelEmail delivers by e-mail.
	ChannelEmail Channel = "email"
	// ChannelPush delivers by push notification.
	ChannelPush Channel = "push"
)

// Channels returns the three channels in a fixed order.
func Channels() []Channel {
	return []Channel{ChannelSMS, ChannelEmail, ChannelPush}
}

// ParseChannel converts a string to a Channel or returns an error naming the allowed values.
func ParseChannel(s string) (Channel, error) {
	for _, c := range Channels() {
		if string(c) == s {
			return c, nil
		}
	}
	return "", fmt.Errorf("invalid channel %q: allowed values are sms, email, push", s)
}

// Priority is a notification priority; the value is the database value.
type Priority int16

// The supported priorities. Lower numbers are more urgent.
const (
	// PriorityHigh is the most urgent priority.
	PriorityHigh Priority = 0
	// PriorityNormal is the default priority.
	PriorityNormal Priority = 1
	// PriorityLow is the least urgent priority.
	PriorityLow Priority = 2
)

// Priorities returns the priorities in the order the relay visits lanes: high, normal, low.
func Priorities() []Priority {
	return []Priority{PriorityHigh, PriorityNormal, PriorityLow}
}

// ParsePriority converts "high", "normal" or "low" to a Priority.
func ParsePriority(s string) (Priority, error) {
	switch s {
	case "high":
		return PriorityHigh, nil
	case "normal":
		return PriorityNormal, nil
	case "low":
		return PriorityLow, nil
	}
	return 0, fmt.Errorf("invalid priority %q: allowed values are high, normal, low", s)
}

// String returns the API name of the priority.
func (p Priority) String() string {
	switch p {
	case PriorityHigh:
		return "high"
	case PriorityNormal:
		return "normal"
	case PriorityLow:
		return "low"
	}
	return fmt.Sprintf("priority(%d)", int16(p))
}

// Valid reports whether p is one of the three priorities.
func (p Priority) Valid() bool {
	return p >= PriorityHigh && p <= PriorityLow
}

// AMQP returns the AMQP message priority: 6 - p, so high is 6, normal 5 and
// low 4. It never returns less than 4 for a valid priority, because RabbitMQ
// treats a missing priority as 4 and the client cannot send 0 (decision D7);
// a value below 4 would let a message overtake nothing and rank below
// messages that carry no priority at all.
func (p Priority) AMQP() uint8 {
	return uint8(6 - p) //nolint:gosec // callers pass valid priorities; result is 4..6
}

// Status is the delivery state of a notification.
type Status string

// The notification states.
const (
	// StatusPending is accepted and waiting until due_at.
	StatusPending Status = "pending"
	// StatusQueued is published to the broker.
	StatusQueued Status = "queued"
	// StatusProcessing is claimed by a worker holding a lease.
	StatusProcessing Status = "processing"
	// StatusRetrying failed with a retryable outcome and waits for its back-off.
	StatusRetrying Status = "retrying"
	// StatusSent means the provider accepted the message. Terminal.
	StatusSent Status = "sent"
	// StatusFailed is a permanent error, exhausted budget or expiry.
	StatusFailed Status = "failed"
	// StatusCancelled was cancelled by the client.
	StatusCancelled Status = "cancelled"
)

// Statuses returns the seven statuses.
func Statuses() []Status {
	return []Status{
		StatusPending, StatusQueued, StatusProcessing, StatusRetrying,
		StatusSent, StatusFailed, StatusCancelled,
	}
}

// FailureReason explains why a notification is failed.
type FailureReason string

// The failure reasons.
const (
	// ReasonProviderRejected is a permanent rejection by the provider.
	ReasonProviderRejected FailureReason = "provider_rejected"
	// ReasonMaxFailures means the retry budget is exhausted.
	ReasonMaxFailures FailureReason = "max_failures"
	// ReasonMaxFailuresUnknownOutcome means the budget is exhausted and the last outcome was unknown.
	ReasonMaxFailuresUnknownOutcome FailureReason = "max_failures_unknown_outcome"
	// ReasonExpired means expires_at passed.
	ReasonExpired FailureReason = "expired"
	// ReasonInternalError is an internal error.
	ReasonInternalError FailureReason = "internal_error"
)

// Outcome is the result of one delivery attempt.
type Outcome string

// The attempt outcomes.
const (
	// OutcomeInFlight is an attempt that has started and not finished.
	OutcomeInFlight Outcome = "in_flight"
	// OutcomeSuccess is acceptance by the provider.
	OutcomeSuccess Outcome = "success"
	// OutcomeRetryable is a retryable provider error.
	OutcomeRetryable Outcome = "retryable"
	// OutcomePermanent is a permanent provider error.
	OutcomePermanent Outcome = "permanent"
	// OutcomeProviderFatal is an error that makes the whole provider unusable.
	OutcomeProviderFatal Outcome = "provider_fatal"
	// OutcomeThrottled is a rate-limit answer from the provider.
	OutcomeThrottled Outcome = "throttled"
	// OutcomeUnknown is an attempt whose result is not known.
	OutcomeUnknown Outcome = "unknown"
	// OutcomeUnreachable means the provider could not be reached.
	OutcomeUnreachable Outcome = "unreachable"
	// OutcomeAborted is an attempt abandoned before an answer.
	OutcomeAborted Outcome = "aborted"
	// OutcomeInternalError is an internal error during the attempt.
	OutcomeInternalError Outcome = "internal_error"
)
