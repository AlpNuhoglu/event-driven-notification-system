package queue

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

// Delivery is one message taken from a dispatch queue.
type Delivery struct {
	// ID is the notification id carried in the body.
	ID uuid.UUID
	// Priority is the AMQP priority of the message (6, 5 or 4; 0 if absent).
	Priority uint8
	// Epoch is the connection epoch the delivery arrived on (D76).
	Epoch uint64

	d amqp.Delivery
}

// Ack acknowledges the delivery; the broker removes the message.
func (d Delivery) Ack() error {
	if err := d.d.Ack(false); err != nil {
		return fmt.Errorf("ack: %w", err)
	}
	return nil
}

// Nack returns the delivery to its queue (requeue).
func (d Delivery) Nack() error {
	if err := d.d.Nack(false, true); err != nil {
		return fmt.Errorf("nack: %w", err)
	}
	return nil
}

// Handler receives each delivery. It owns the delivery from then on and must
// eventually Ack or Nack it.
type Handler func(Delivery)

// Consumer consumes one channel's dispatch queue with manual acknowledgement
// on its own AMQP channel.
type Consumer struct {
	conn   *Connection
	ch     *amqp.Channel
	queue  string
	logger *slog.Logger
}

// NewConsumer opens a channel on conn with a prefetch of prefetch messages
// for the dispatch queue of channel c.
func NewConsumer(conn *Connection, c domain.Channel, prefetch int, logger *slog.Logger) (*Consumer, error) {
	if prefetch <= 0 {
		return nil, fmt.Errorf("new consumer: prefetch must be positive, got %d", prefetch)
	}
	ch, err := conn.channel()
	if err != nil {
		return nil, fmt.Errorf("new consumer: %w", err)
	}
	if err := ch.Qos(prefetch, 0, false); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("new consumer: qos: %w", err)
	}
	return &Consumer{conn: conn, ch: ch, queue: DispatchQueue(c), logger: logger}, nil
}

// Run consumes until ctx is cancelled and hands every delivery to handle,
// stamped with the connection epoch at its arrival. A body that is not a
// UUID is rejected without requeue, so it goes to the poison queue; it is
// logged without its content.
//
// When ctx is cancelled, Run cancels the consumer, returns the deliveries
// that arrived but were not handed over to the queue (nack, requeue) and
// returns nil. An unexpected end of the delivery stream (a server-side
// cancellation, a closed channel) is an error; resubscribing is P3.
func (c *Consumer) Run(ctx context.Context, handle Handler) error {
	tag, err := consumerTag(c.queue)
	if err != nil {
		return err
	}
	deliveries, err := c.ch.Consume(c.queue, tag, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume %s: %w", c.queue, err)
	}
	for {
		select {
		case <-ctx.Done():
			return c.stop(tag, deliveries)
		case d, ok := <-deliveries:
			if !ok {
				if ctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("consume %s: delivery stream ended", c.queue)
			}
			c.dispatch(d, handle)
		}
	}
}

func (c *Consumer) dispatch(d amqp.Delivery, handle Handler) {
	epoch := c.conn.Epoch()
	id, err := uuid.ParseBytes(d.Body)
	if err != nil {
		c.logger.Warn("rejecting a delivery whose body is not a notification id",
			"queue", c.queue, "body_bytes", len(d.Body))
		if err := d.Reject(false); err != nil {
			c.logger.Warn("reject failed", "queue", c.queue, "error", err)
		}
		return
	}
	handle(Delivery{ID: id, Priority: d.Priority, Epoch: epoch, d: d})
}

// stop cancels the consumer and requeues the deliveries that were already
// on their way to it.
func (c *Consumer) stop(tag string, deliveries <-chan amqp.Delivery) error {
	if err := c.ch.Cancel(tag, false); err != nil {
		if errors.Is(err, amqp.ErrClosed) {
			return nil // the channel is gone; the broker requeues what it held
		}
		return fmt.Errorf("cancel consumer %s: %w", c.queue, err)
	}
	for d := range deliveries {
		if err := d.Nack(false, true); err != nil {
			c.logger.Warn("nack after cancel failed", "queue", c.queue, "error", err)
		}
	}
	return nil
}

// Close closes the consumer's channel. The broker requeues every delivery
// that was not acknowledged.
func (c *Consumer) Close() error {
	if err := c.ch.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
		return fmt.Errorf("close consumer channel: %w", err)
	}
	return nil
}

func consumerTag(queue string) (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("consumer tag: %w", err)
	}
	return queue + "-" + hex.EncodeToString(b), nil
}
