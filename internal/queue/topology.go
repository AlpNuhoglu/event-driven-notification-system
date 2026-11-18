package queue

import (
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

// The exchanges of the dispatch topology (ADR-0001).
const (
	// DispatchExchange is the direct exchange the relay publishes to, with the
	// channel name as the routing key.
	DispatchExchange = "notifications.dispatch"
	// DeadLetterExchange receives the messages a dispatch queue dead-letters.
	DeadLetterExchange = "notifications.dlx"
)

// DispatchQueue returns the name of a channel's dispatch queue, dispatch.{channel}.
func DispatchQueue(c domain.Channel) string {
	return "dispatch." + string(c)
}

// PoisonQueue returns the name of a channel's poison queue, dispatch.{channel}.poison.
func PoisonQueue(c domain.Channel) string {
	return DispatchQueue(c) + ".poison"
}

// dispatchQueueArgs returns the arguments of every dispatch queue: a quorum
// queue (strict priority needs no argument; x-max-priority would fail the
// declaration, experiment E1), the consumer timeout in milliseconds as an
// int64 (a Go int is encoded as 32 bits and wraps, experiment E6) and the
// dead-letter exchange.
func dispatchQueueArgs(consumerTimeout time.Duration) amqp.Table {
	return amqp.Table{
		amqp.QueueTypeArg:        amqp.QueueTypeQuorum,
		amqp.ConsumerTimeoutArg:  consumerTimeout.Milliseconds(), // int64
		"x-dead-letter-exchange": DeadLetterExchange,
	}
}

// DeclareTopology declares the dispatch topology idempotently: the two
// exchanges, and for every channel its quorum dispatch queue bound to
// DispatchExchange and its classic poison queue bound to DeadLetterExchange,
// both with the channel name as the routing key. consumerTimeout is the
// x-consumer-timeout of the dispatch queues (CONSUMER_TIMEOUT).
//
// The channel the declarations are made on stays open: the library records
// declarations per channel and redeclares them on a recovered connection, but
// forgets them when that channel is closed (experiment E4).
func (c *Connection) DeclareTopology(consumerTimeout time.Duration) error {
	if consumerTimeout.Milliseconds() <= 0 {
		return fmt.Errorf("declare topology: consumer timeout must be at least 1ms, got %s", consumerTimeout)
	}
	ch, err := c.channel()
	if err != nil {
		return fmt.Errorf("declare topology: %w", err)
	}
	if err := declare(ch, consumerTimeout); err != nil {
		_ = ch.Close()
		return fmt.Errorf("declare topology: %w", err)
	}
	c.mu.Lock()
	old := c.topo
	c.topo = ch
	c.mu.Unlock()
	if old != nil {
		_ = old.Close()
	}
	return nil
}

func declare(ch *amqp.Channel, consumerTimeout time.Duration) error {
	for _, ex := range []string{DispatchExchange, DeadLetterExchange} {
		if err := ch.ExchangeDeclare(ex, amqp.ExchangeDirect, true, false, false, false, nil); err != nil {
			return fmt.Errorf("exchange %s: %w", ex, err)
		}
	}
	for _, c := range domain.Channels() {
		q := DispatchQueue(c)
		if _, err := ch.QueueDeclare(q, true, false, false, false, dispatchQueueArgs(consumerTimeout)); err != nil {
			return fmt.Errorf("queue %s: %w", q, err)
		}
		if err := ch.QueueBind(q, string(c), DispatchExchange, false, nil); err != nil {
			return fmt.Errorf("bind %s: %w", q, err)
		}
		p := PoisonQueue(c)
		if _, err := ch.QueueDeclare(p, true, false, false, false, amqp.Table{amqp.QueueTypeArg: amqp.QueueTypeClassic}); err != nil {
			return fmt.Errorf("queue %s: %w", p, err)
		}
		if err := ch.QueueBind(p, string(c), DeadLetterExchange, false, nil); err != nil {
			return fmt.Errorf("bind %s: %w", p, err)
		}
	}
	return nil
}
