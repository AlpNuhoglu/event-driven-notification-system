package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
)

// ErrStale is returned by Message.Ack and Message.Nack for a delivery from
// an older connection epoch (D76): nothing was sent to the broker, which has
// requeued the delivery. The worker forgets such a delivery.
var ErrStale = errors.New("delivery from an older connection epoch")

// Message is one delivery from the broker (implemented by internal/queue).
type Message interface {
	// NotificationID is the notification the delivery carries.
	NotificationID() uuid.UUID
	// Priority is the AMQP priority: 6 high, 5 normal, 4 low.
	Priority() uint8
	// Epoch is the connection epoch the delivery arrived on (D76).
	Epoch() uint64
	// Stale reports whether the delivery is from an older connection epoch
	// now. Once true it stays true.
	Stale() bool
	// Ack acknowledges the delivery. For a delivery from an older epoch it
	// sends nothing and returns ErrStale; the epoch check and the ack are
	// one step.
	Ack() error
	// Nack rejects the delivery and asks the broker to requeue it. For a
	// delivery from an older epoch it sends nothing and returns ErrStale,
	// like Ack.
	Nack() error
	// TraceParent is the delivery's traceparent header: the context of the
	// relay's publish span, which worker.process continues. Empty when the
	// message has none.
	TraceParent() string
}

// Limiter reserves rate-limit slots (implemented by internal/ratelimit).
// ok == false means the next free slot is beyond the reservation horizon and
// nothing was reserved; the caller asks again after the retry interval.
// ok == true means a slot is reserved wait from now, where now is read after
// the call returned.
type Limiter interface {
	Reserve(ctx context.Context, channel domain.Channel) (wait time.Duration, ok bool, err error)
}

// Breaker is the circuit breaker of the channel's provider (implemented by
// internal/breaker; ADR-0007 sections 7 and 8).
type Breaker interface {
	// Ready is asked by the pacer loop before it reserves a rate-limit slot.
	// It consumes nothing. ok == false means: prepare nothing, and do not
	// ask again before until.
	Ready() (ok bool, until time.Time)
	// Admit is asked at the slot time, immediately before the send. ok ==
	// true means: send, and record the outcome on the permit exactly once.
	// In a half-open circuit the one admitted send is the probe, so a permit
	// that is never recorded would keep the circuit half-open for good.
	// ok == false means: send nothing; until is when to ask again.
	Admit() (p Permit, ok bool, until time.Time)
}

// Permit is the right to make one send.
type Permit interface {
	// Record reports the outcome of the send. retryAfter is the usable
	// Retry-After hint of the response, zero when there is none. It returns
	// the end of the open period when the circuit is open after this record,
	// and the zero time otherwise.
	Record(outcome domain.Outcome, retryAfter time.Duration) (openUntil time.Time)
}

// AlwaysAllow is a Breaker that never opens, for tests and for callers
// without a provider to protect.
type AlwaysAllow struct{}

// Ready is always ready.
func (AlwaysAllow) Ready() (bool, time.Time) { return true, time.Time{} }

// Admit always admits.
func (AlwaysAllow) Admit() (Permit, bool, time.Time) { return noPermit{}, true, time.Time{} }

type noPermit struct{}

func (noPermit) Record(domain.Outcome, time.Duration) time.Time { return time.Time{} }

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
	CompleteSent(ctx context.Context, id uuid.UUID, attemptNo int, provider string, r store.AttemptResult) (store.Sent, domain.StatusChanged, error)
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
