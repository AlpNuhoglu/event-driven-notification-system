package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
)

// Message is one delivery from the broker (implemented by internal/queue).
type Message interface {
	// NotificationID is the notification the delivery carries.
	NotificationID() uuid.UUID
	// Priority is the AMQP priority: 6 high, 5 normal, 4 low.
	Priority() uint8
	// Epoch is the connection epoch the delivery arrived on (D76).
	Epoch() uint64
	// Ack acknowledges the delivery.
	Ack() error
	// Nack rejects the delivery and asks the broker to requeue it.
	Nack() error
}

// Limiter reserves rate-limit slots (implemented by internal/ratelimit).
// ok == false means the next free slot is beyond the reservation horizon and
// nothing was reserved; the caller asks again after the retry interval.
// ok == true means a slot is reserved wait from now, where now is read after
// the call returned.
type Limiter interface {
	Reserve(ctx context.Context, channel domain.Channel) (wait time.Duration, ok bool, err error)
}

// Breaker is the circuit breaker of the channel's provider. allowed == false
// means: do not send, and do not ask again before until.
type Breaker interface {
	Allow() (allowed bool, until time.Time)
}

// AlwaysAllow is the Breaker of this phase: it never opens (the real breaker
// arrives in P3).
type AlwaysAllow struct{}

// Allow always allows.
func (AlwaysAllow) Allow() (bool, time.Time) { return true, time.Time{} }

// Provider sends one notification to the external provider (implemented by
// internal/provider; the wiring adapts its types to Request and Response).
type Provider interface {
	// Name is the provider name recorded on claims and attempts.
	Name() string
	// Send makes one request. It reports every failure in the Response.
	Send(ctx context.Context, req Request) Response
}

// Request is one provider request. To is the real recipient: it must reach
// the provider and nothing else, so every formatting path of Request leaves
// To and Content out.
type Request struct {
	NotificationID uuid.UUID
	Channel        domain.Channel
	To             string
	Content        string
	TraceParent    string
}

// String returns the notification id and channel only.
func (r Request) String() string {
	return fmt.Sprintf("Request{NotificationID: %s, Channel: %s}", r.NotificationID, r.Channel)
}

// GoString returns the same as String, for the %#v verb.
func (r Request) GoString() string { return r.String() }

// LogValue implements slog.LogValuer without To and Content.
func (r Request) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("notification_id", r.NotificationID.String()),
		slog.String("channel", string(r.Channel)),
	)
}

// Response is the result of one provider request.
type Response struct {
	// StatusCode is the HTTP status, 0 when no response arrived.
	StatusCode int
	// Err is the transport error, if any.
	Err error
	// MessageID is the provider's message id, empty when none was returned.
	MessageID string
	// Latency is the duration of the request.
	Latency time.Duration
	// WroteRequest reports that the request was written completely and
	// without error. Without a response it decides between unreachable
	// (nothing was sent) and unknown (ADR-0007 section 2).
	WroteRequest bool
	// RetryAfter is the response's Retry-After header, verbatim; empty when
	// there is none.
	RetryAfter string
}

// Store holds the transitions the worker performs (implemented by
// *store.Store).
type Store interface {
	Claim(ctx context.Context, id uuid.UUID, lease time.Duration, provider string) (store.Claim, domain.StatusChanged, error)
	CompleteSent(ctx context.Context, id uuid.UUID, attemptNo int, provider string, r store.AttemptResult) (domain.StatusChanged, error)
	CompleteFailure(ctx context.Context, id uuid.UUID, attemptNo int, r store.AttemptResult, jitter float64) (domain.StatusChanged, error)
	AbortAttempt(ctx context.Context, id uuid.UUID, attemptNo int, delay time.Duration) (domain.StatusChanged, error)
}

// Clock is the worker's time source. Now must carry a monotonic reading in
// production, so that slots and the send deadline are measured on the
// monotonic clock.
type Clock interface {
	Now() time.Time
	// WaitUntil returns nil once t has been reached, or ctx's error if ctx
	// ends first (also when it has already ended).
	WaitUntil(ctx context.Context, t time.Time) error
}

// SystemClock is the Clock of production: time.Now and a timer.
type SystemClock struct{}

// Now returns time.Now().
func (SystemClock) Now() time.Time { return time.Now() }

// WaitUntil waits for a timer until t, or until ctx ends.
func (SystemClock) WaitUntil(ctx context.Context, t time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	d := time.Until(t)
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
