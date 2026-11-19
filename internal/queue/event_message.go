package queue

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

// The status-event side of the topology (ADR-0001 "Status events").
const (
	// EventsExchange is the durable topic exchange every status event is
	// published to, with the routing key status.{channel}.{status}.
	EventsExchange = "notifications.events"
	// EventType is the event's name, carried in the message's type property.
	EventType = "notification.status_changed"
	// EventsBindingKey is the binding key that matches every status event.
	EventsBindingKey = "status.#"

	eventContentType = "application/json"
)

// eventBody is the JSON body of a status event. Every member is always
// present; batch_id and failure_reason are null when the notification has
// none. Nothing in it is personal data: no recipient, no content. The ids
// are strings here, written in the canonical form and read back with
// domain.ParseID, so that decoding a body never reaches the UUID library's
// lenient parser (it would accept other spellings of an id).
type eventBody struct {
	ID            string    `json:"id"`
	BatchID       *string   `json:"batch_id"`
	Channel       string    `json:"channel"`
	Priority      string    `json:"priority"`
	Status        string    `json:"status"`
	Version       int64     `json:"version"`
	FailureReason *string   `json:"failure_reason"`
	AttemptCount  int       `json:"attempt_count"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// EncodeEvent returns the message body of a status event.
func EncodeEvent(e domain.StatusChanged) ([]byte, error) {
	b := eventBody{
		ID:           e.ID.String(),
		Channel:      string(e.Channel),
		Priority:     e.Priority.String(),
		Status:       string(e.Status),
		Version:      e.Version,
		AttemptCount: e.AttemptCount,
		UpdatedAt:    e.UpdatedAt.UTC(),
	}
	if e.BatchID != nil {
		batch := e.BatchID.String()
		b.BatchID = &batch
	}
	if e.FailureReason != nil {
		r := string(*e.FailureReason)
		b.FailureReason = &r
	}
	return json.Marshal(b)
}

// DecodeEvent reads a message body written by EncodeEvent. A member this
// version does not know is ignored, so that a publisher of a later version
// does not silence a consumer of this one; a body without an id, with a
// version below 1 or with a channel, priority or status this version does
// not know is an error, and its consumer drops the message. An id or a
// batch_id in any spelling but the canonical one is an error as well.
func DecodeEvent(body []byte) (domain.StatusChanged, error) {
	var b eventBody
	if err := json.Unmarshal(body, &b); err != nil {
		return domain.StatusChanged{}, fmt.Errorf("status event: %w", err)
	}
	if b.ID == "" {
		return domain.StatusChanged{}, errors.New("status event: no id")
	}
	id, err := domain.ParseID(b.ID)
	if err != nil {
		return domain.StatusChanged{}, fmt.Errorf("status event: id: %w", err)
	}
	if id == uuid.Nil {
		return domain.StatusChanged{}, errors.New("status event: no id")
	}
	var batchID *uuid.UUID
	if b.BatchID != nil {
		batch, err := domain.ParseID(*b.BatchID)
		if err != nil {
			return domain.StatusChanged{}, fmt.Errorf("status event: batch_id: %w", err)
		}
		batchID = &batch
	}
	if b.Version < 1 {
		return domain.StatusChanged{}, fmt.Errorf("status event: version %d", b.Version)
	}
	ch, err := domain.ParseChannel(b.Channel)
	if err != nil {
		return domain.StatusChanged{}, fmt.Errorf("status event: %w", err)
	}
	prio, err := domain.ParsePriority(b.Priority)
	if err != nil {
		return domain.StatusChanged{}, fmt.Errorf("status event: %w", err)
	}
	status := domain.Status(b.Status)
	if !slices.Contains(domain.Statuses(), status) {
		return domain.StatusChanged{}, fmt.Errorf("status event: unknown status %q", b.Status)
	}
	e := domain.StatusChanged{
		ID:           id,
		BatchID:      batchID,
		Channel:      ch,
		Priority:     prio,
		Status:       status,
		Version:      b.Version,
		AttemptCount: b.AttemptCount,
		UpdatedAt:    b.UpdatedAt,
	}
	if b.FailureReason != nil {
		r := domain.FailureReason(*b.FailureReason)
		e.FailureReason = &r
	}
	return e, nil
}

// eventPublishing returns the message of a status event: the body of
// EncodeEvent, the event's name as the type and transient delivery, because
// events are best-effort and their consumers' queues are not durable.
func eventPublishing(e domain.StatusChanged) (amqp.Publishing, error) {
	body, err := EncodeEvent(e)
	if err != nil {
		return amqp.Publishing{}, err
	}
	return amqp.Publishing{
		ContentType:  eventContentType,
		Type:         EventType,
		DeliveryMode: amqp.Transient,
		Body:         body,
	}, nil
}

// declareEventsExchange declares EventsExchange idempotently. Publishers and
// consumers both declare it, at start and on every (re)connection (D2).
func declareEventsExchange(ch *amqp.Channel) error {
	if err := ch.ExchangeDeclare(EventsExchange, amqp.ExchangeTopic, true, false, false, false, nil); err != nil {
		return fmt.Errorf("exchange %s: %w", EventsExchange, err)
	}
	return nil
}
