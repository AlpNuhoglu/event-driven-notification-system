package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

// Message is one notification to publish to the dispatch exchange.
type Message struct {
	ID          uuid.UUID
	Channel     domain.Channel
	Priority    domain.Priority
	TraceParent string
}

// ErrPublisherChannelClosed reports that the publisher's AMQP channel was
// closed: its return listener is gone, so no later batch can be checked for
// unroutable messages. Every batch fails with it from then on. The relay
// treats it like a recovery that gave up and exits, so that the restart
// policy brings up a process with a new publisher (D5).
var ErrPublisherChannelClosed = errors.New("publisher channel closed")

// Publisher is the relay's publisher: one AMQP channel in confirm mode with a
// buffered return listener (decisions D2, D7, D10).
//
// It publishes one batch at a time: PublishBatch holds a lock for the whole
// batch, so concurrent calls run one after the other.
type Publisher struct {
	conn     *Connection
	ch       *amqp.Channel
	returns  chan amqp.Return
	maxBatch int

	mu sync.Mutex // one batch at a time
}

// NewPublisher opens the publisher's channel on conn and puts it in confirm
// mode. maxBatch is the largest batch PublishBatch accepts (RELAY_BATCH_SIZE).
//
// The return listener holds two batches. The library waits at most 5 s for a
// full listener and then drops the return (the U8 reading), so the listener
// must never be full while a batch waits for its confirms; PublishBatch also
// reads it while it waits.
func NewPublisher(conn *Connection, maxBatch int) (*Publisher, error) {
	if maxBatch <= 0 {
		return nil, fmt.Errorf("new publisher: max batch must be positive, got %d", maxBatch)
	}
	ch, err := conn.channel()
	if err != nil {
		return nil, fmt.Errorf("new publisher: %w", err)
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("new publisher: confirm mode: %w", err)
	}
	return &Publisher{
		conn:     conn,
		ch:       ch,
		returns:  ch.NotifyReturn(make(chan amqp.Return, 2*maxBatch)),
		maxBatch: maxBatch,
	}, nil
}

// PublishBatch publishes msgs and returns the ids that were confirmed (ack)
// AND not returned. An error means the whole batch is to be treated as failed
// (the relay rolls back); messages already published stay in the queue, and
// the duplicates this causes are harmless by design (ADR-0002).
//
// Every message is published to DispatchExchange with the channel name as
// the routing key, mandatory, persistent, MessageId and body = the
// notification id, Priority = msg.Priority.AMQP() and the traceparent header
// when TraceParent is set.
//
// The rules:
//   - It waits at most confirmTimeout for all confirms. A timeout, a publish
//     error, a closed channel or a connection that stops being open during
//     the batch is an error.
//   - A nack excludes that id; it is not an error.
//   - A returned message (basic.return: unroutable) excludes its id. The
//     library hands a message's return over before that message's confirm,
//     on the same goroutine (assumption U8). The ids are therefore decided
//     only after every confirm has arrived and the return listener has then
//     been drained without blocking: every return that preceded a confirm is
//     in the listener or has already been read.
//   - Returns left over from an earlier batch are discarded before
//     publishing. One that arrives during this batch can only exclude an id,
//     never include one.
func (p *Publisher) PublishBatch(ctx context.Context, msgs []Message, confirmTimeout time.Duration) ([]uuid.UUID, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(msgs) > p.maxBatch {
		return nil, fmt.Errorf("publish batch: %d messages, at most %d allowed", len(msgs), p.maxBatch)
	}
	for _, m := range msgs {
		if !m.Priority.Valid() {
			return nil, fmt.Errorf("publish batch: message %s has an invalid priority %d", m.ID, m.Priority)
		}
	}
	if len(msgs) == 0 {
		return nil, nil
	}

	ctx, cancel := context.WithTimeout(ctx, confirmTimeout)
	defer cancel()
	lost := p.conn.lostSignal()
	if isClosed(lost) {
		return nil, errors.New("publish batch: connection not open")
	}
	if err := p.drainReturns(nil); err != nil {
		return nil, fmt.Errorf("publish batch: %w", err)
	}

	confirms := make([]*amqp.DeferredConfirmation, len(msgs))
	for i, m := range msgs {
		dc, err := p.ch.PublishWithDeferredConfirmWithContext(ctx, DispatchExchange, string(m.Channel),
			true, false, publishing(m))
		if err != nil {
			return nil, fmt.Errorf("publish batch: publish: %w", err)
		}
		if dc == nil {
			return nil, errors.New("publish batch: channel is not in confirm mode")
		}
		confirms[i] = dc
	}

	returned := make(map[string]bool)
	for _, dc := range confirms {
		if err := p.waitConfirm(ctx, lost, dc, returned); err != nil {
			return nil, fmt.Errorf("publish batch: %w", err)
		}
	}
	// Every confirm has arrived. Read what the listener holds now.
	if err := p.drainReturns(returned); err != nil {
		return nil, fmt.Errorf("publish batch: %w", err)
	}
	if isClosed(lost) {
		return nil, errors.New("publish batch: connection lost during the batch")
	}

	published := make([]uuid.UUID, 0, len(msgs))
	for i, m := range msgs {
		if confirms[i].Acked() && !returned[m.ID.String()] {
			published = append(published, m.ID)
		}
	}
	return published, nil
}

// waitConfirm waits until dc is confirmed (ack or nack). Returns that arrive
// meanwhile are recorded in returned, so the listener never fills up.
func (p *Publisher) waitConfirm(ctx context.Context, lost <-chan struct{}, dc *amqp.DeferredConfirmation, returned map[string]bool) error {
	for {
		select {
		case <-dc.Done():
			return nil
		case r, ok := <-p.returns:
			if !ok {
				return ErrPublisherChannelClosed
			}
			returned[r.MessageId] = true
		case <-lost:
			return errors.New("connection lost while waiting for confirms")
		case <-ctx.Done():
			return fmt.Errorf("wait for confirms: %w", ctx.Err())
		}
	}
}

// drainReturns reads the return listener without blocking and records each
// message id in returned; with a nil map the returns are discarded.
func (p *Publisher) drainReturns(returned map[string]bool) error {
	for {
		select {
		case r, ok := <-p.returns:
			if !ok {
				return ErrPublisherChannelClosed
			}
			if returned != nil {
				returned[r.MessageId] = true
			}
		default:
			return nil
		}
	}
}

func publishing(m Message) amqp.Publishing {
	var headers amqp.Table
	if m.TraceParent != "" {
		headers = amqp.Table{"traceparent": m.TraceParent}
	}
	id := m.ID.String()
	return amqp.Publishing{
		Headers:      headers,
		ContentType:  "text/plain",
		DeliveryMode: amqp.Persistent,
		Priority:     m.Priority.AMQP(),
		MessageId:    id,
		Body:         []byte(id),
	}
}
