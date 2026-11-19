package queue

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

// ErrStale is returned by Ack, Nack and the rejection of an invalid body
// for a delivery from an older epoch (D76). Nothing was sent to the broker:
// the broker has requeued the delivery or will redeliver it, and its
// delivery tag may name another message now.
var ErrStale = errors.New("delivery from an older connection epoch")

// cancelListener is the capacity of the listener for server-side consumer
// cancellations. The library waits a bounded time for a full listener and
// then drops the notification; Run drains it continuously.
const cancelListener = 64

// Delivery is one message taken from a dispatch queue.
type Delivery struct {
	// ID is the notification id carried in the body.
	ID uuid.UUID
	// Priority is the AMQP priority of the message (6, 5 or 4; 0 if absent).
	Priority uint8
	// Epoch is the epoch of the consumer's channel the delivery arrived on
	// (D76): the epoch of the subscription that received it.
	Epoch uint64

	d amqp.Delivery
	k *Consumer
}

// Ack acknowledges the delivery; the broker removes the message. For a
// delivery from an older epoch it sends nothing and returns ErrStale; the
// epoch check and the ack are one step, which no epoch advance can split.
func (d Delivery) Ack() error {
	if err := d.k.answer(d.Epoch, func() error { return d.d.Ack(false) }); err != nil {
		return fmt.Errorf("ack: %w", err)
	}
	return nil
}

// Nack returns the delivery to its queue (requeue). For a delivery from an
// older epoch it sends nothing and returns ErrStale, like Ack.
func (d Delivery) Nack() error {
	if err := d.k.answer(d.Epoch, func() error { return d.d.Nack(false, true) }); err != nil {
		return fmt.Errorf("nack: %w", err)
	}
	return nil
}

// Stale reports whether the delivery is from an older epoch now. Once true it
// stays true. It is for dropping deliveries early; the decision that matters
// is taken again inside Ack and Nack.
func (d Delivery) Stale() bool {
	return d.k.stale(d.Epoch)
}

// Handler receives each delivery. It owns the delivery from then on and must
// eventually Ack or Nack it, or forget it once it is stale.
type Handler func(Delivery)

// Consumer consumes one channel's dispatch queue with manual acknowledgement
// on its own AMQP channel.
//
// The epoch (D76). Delivery tags are numbered per channel session: they
// restart at 1 when the library reopens the channel, after a lost connection
// or after a channel error, on the same *amqp.Channel (experiment E4). An ack
// for a delivery from an earlier session would therefore name another
// message. The consumer keeps an epoch per channel and the rule:
//
//   - Each subscription (basic.consume) has a consumer tag of its own and the
//     epoch at which it was made; every delivery read from its stream carries
//     that epoch. The library never resubscribes a tag (Connection replaces
//     its topology recovery), so all deliveries of one stream come from the
//     session the subscription was made on. The stamp therefore does not
//     depend on when the delivery is read.
//   - The epoch advances, under the write lock, before the session the
//     current subscription lives on can be replaced: in the Dial hook of every
//     reconnection attempt, in the channel-close hook (while the channel is
//     closed), and when the broker has cancelled the subscription. The
//     library calls both hooks synchronously on the goroutine that then
//     resets the channel.
//   - Ack, Nack and Reject compare the delivery's epoch with the current one
//     and send the frame under the read lock. An ack is therefore sent either
//     before the advance (on the old session, or into a closed channel, which
//     fails) or not at all.
//   - A subscription is made under the read lock, from reading the epoch to
//     the broker's consume-ok, so its epoch belongs to the session it was
//     made on.
type Consumer struct {
	conn    *Connection
	ch      *amqp.Channel
	queue   string
	logger  *slog.Logger
	cancels chan string // server-side cancellations; closed with the channel

	// answering is held for reading while a delivery is answered or a
	// subscription is made, and for writing while the epoch advances.
	answering sync.RWMutex
	epoch     uint64

	mu      sync.Mutex // serializes subscriptions; guards the fields below
	running bool       // between the start of Run and the start of its stop
	sub     *subscription
	changed chan struct{} // closed when sub is replaced
}

// subscription is one basic.consume on the consumer's channel.
type subscription struct {
	tag        string
	deliveries <-chan amqp.Delivery
	epoch      uint64
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
	k := &Consumer{
		conn:    conn,
		ch:      ch,
		queue:   DispatchQueue(c),
		logger:  logger,
		cancels: ch.NotifyCancel(make(chan string, cancelListener)),
		changed: make(chan struct{}),
	}
	conn.addConsumer(k)
	return k, nil
}

// Epoch returns the current epoch of the consumer's channel.
func (k *Consumer) Epoch() uint64 {
	k.answering.RLock()
	defer k.answering.RUnlock()
	return k.epoch
}

// Run consumes until ctx is cancelled and hands every delivery to handle,
// stamped with the epoch of its subscription. A body that is not a UUID is
// rejected without requeue, so it goes to the poison queue; it is logged
// without its content.
//
// After every new subscription that replaced an earlier one (a reconnection,
// a reopened channel, a server-side cancellation) Run calls resubscribed, if
// it is not nil: the deliveries handed over before are stale now.
//
// A server-side cancellation of the subscription (basic.cancel, for example
// after the queue's x-consumer-timeout, D8) is followed by a new
// subscription on the same channel; if that fails because the channel or
// the connection is closed, the library's recovery subscribes again when it
// has reopened them.
//
// When ctx is cancelled, Run cancels the subscription, returns the
// deliveries that arrived but were not handed over (nack, requeue; not for a
// stale one) and returns nil. A delivery stream that ends because the channel
// is closed for good (recovery gave up, or the channel was closed) is an
// error.
func (k *Consumer) Run(ctx context.Context, handle Handler, resubscribed func()) error {
	k.mu.Lock()
	k.running = true
	k.mu.Unlock()
	if _, err := k.subscribe(); err != nil {
		k.mu.Lock()
		k.running = false
		k.mu.Unlock()
		return err
	}
	cancelled := map[string]bool{}
	var seen *subscription
	for {
		s, changed := k.current()
		var deliveries <-chan amqp.Delivery
		if s != nil {
			deliveries = s.deliveries
			if s != seen {
				if seen != nil && resubscribed != nil {
					resubscribed()
				}
				seen = s
			}
		}
		select {
		case <-ctx.Done():
			return k.stop()
		case <-changed:
		case tag, ok := <-k.cancels:
			if !ok {
				return k.end(ctx)
			}
			cancelled[tag] = true
		case d, ok := <-deliveries:
			if ok {
				k.dispatch(d, s.epoch, handle)
				continue
			}
			if cur, _ := k.current(); cur != s {
				continue // replaced meanwhile; the old stream was retired
			}
			// The library sends the tag to the cancel listener before it
			// closes the stream of a cancelled consumer.
			if open := k.readCancels(cancelled); !open || !cancelled[s.tag] {
				return k.end(ctx)
			}
			k.logger.Warn("consumer cancelled by the broker; subscribing again", "queue", k.queue)
			k.dropCancelled(s)
			if _, err := k.subscribe(); err != nil {
				k.logger.Warn("subscribing again failed; the channel's recovery subscribes when it has reopened the channel",
					"queue", k.queue, "error", err)
			}
		}
	}
}

// end is the end of the delivery stream for good: an error unless ctx is
// cancelled.
func (k *Consumer) end(ctx context.Context) error {
	k.mu.Lock()
	k.running = false
	k.mu.Unlock()
	if ctx.Err() != nil {
		return nil
	}
	return fmt.Errorf("consume %s: delivery stream ended", k.queue)
}

// readCancels moves the cancellations waiting in the listener to seen,
// without blocking. It returns false if the listener is closed (the channel
// is closed for good).
func (k *Consumer) readCancels(seen map[string]bool) bool {
	for {
		select {
		case tag, ok := <-k.cancels:
			if !ok {
				return false
			}
			seen[tag] = true
		default:
			return true
		}
	}
}

func (k *Consumer) current() (*subscription, <-chan struct{}) {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.sub, k.changed
}

// subscribe makes a new subscription at the current epoch, unless the
// consumer is not running or its subscription is already of the current
// epoch. It reports whether it subscribed. Run calls it at start and after a
// server-side cancellation; the connection's topology recovery calls it after
// the channel has been reopened. Whichever comes first subscribes.
func (k *Consumer) subscribe() (bool, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if !k.running {
		return false, nil
	}
	tag, err := consumerTag(k.queue)
	if err != nil {
		return false, err
	}
	k.answering.RLock()
	epoch := k.epoch
	if k.sub != nil && k.sub.epoch == epoch {
		k.answering.RUnlock()
		return false, nil
	}
	deliveries, err := k.ch.Consume(k.queue, tag, false, false, false, false, nil)
	k.answering.RUnlock()
	if err != nil {
		return false, fmt.Errorf("consume %s: %w", k.queue, err)
	}
	old := k.sub
	k.sub = &subscription{tag: tag, deliveries: deliveries, epoch: epoch}
	k.signal()
	if old != nil {
		go k.retire(old)
	}
	return true, nil
}

// signal tells Run that the subscription changed. The caller holds k.mu.
func (k *Consumer) signal() {
	close(k.changed)
	k.changed = make(chan struct{})
}

// retire releases a replaced subscription. Its deliveries are stale and are
// dropped unanswered. The broker does not know its tag any more (a new
// session, or a cancelled consumer) and ignores the cancel, as AMQP requires
// for an unknown tag; the cancel lets the library close the stream. If it
// fails, the stream ends when the channel is closed.
func (k *Consumer) retire(s *subscription) {
	_ = k.ch.Cancel(s.tag, true)
	for range s.deliveries { //nolint:revive // drained: every delivery of a replaced subscription is stale
	}
}

// dropCancelled advances the epoch after the broker cancelled s, if s is
// still the current subscription, and leaves the consumer without one.
func (k *Consumer) dropCancelled(s *subscription) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.sub != s {
		return
	}
	k.answering.Lock()
	if k.epoch == s.epoch {
		k.epoch++
	}
	k.answering.Unlock()
	k.sub = nil
	k.signal()
}

// advance advances the epoch: the channel's session is about to be
// replaced. With onlyIfClosed it advances only while the channel is closed
// (the channel-close hook: a late call for a channel that has been reopened
// meanwhile must not orphan the new subscription).
func (k *Consumer) advance(onlyIfClosed bool) {
	k.answering.Lock()
	defer k.answering.Unlock()
	if onlyIfClosed && !k.ch.IsClosed() {
		return
	}
	k.epoch++
}

// answer sends a delivery's answer with send, unless epoch is not the
// current epoch: then it sends nothing and returns ErrStale.
func (k *Consumer) answer(epoch uint64, send func() error) error {
	if k == nil {
		return ErrStale
	}
	k.answering.RLock()
	defer k.answering.RUnlock()
	if epoch != k.epoch {
		return ErrStale
	}
	return send()
}

func (k *Consumer) stale(epoch uint64) bool {
	if k == nil {
		return true
	}
	return k.Epoch() != epoch
}

func (k *Consumer) dispatch(d amqp.Delivery, epoch uint64, handle Handler) {
	// The body is the id as text, as the publisher writes it; any other
	// spelling of an id is a malformed message like any other.
	id, err := domain.ParseID(string(d.Body))
	if err != nil {
		k.logger.Warn("rejecting a delivery whose body is not a notification id",
			"queue", k.queue, "body_bytes", len(d.Body))
		if err := k.answer(epoch, func() error { return d.Reject(false) }); err != nil && !errors.Is(err, ErrStale) {
			k.logger.Warn("reject failed", "queue", k.queue, "error", err)
		}
		return
	}
	handle(Delivery{ID: id, Priority: d.Priority, Epoch: epoch, d: d, k: k})
}

// stop ends the subscription and requeues the deliveries that were already
// on their way to it. Recovery makes no new subscription afterwards.
func (k *Consumer) stop() error {
	k.mu.Lock()
	k.running = false
	s := k.sub
	k.mu.Unlock()
	if s == nil {
		return nil
	}
	if err := k.ch.Cancel(s.tag, false); err != nil {
		if errors.Is(err, amqp.ErrClosed) {
			return nil // the channel is gone; the broker requeues what it held
		}
		return fmt.Errorf("cancel consumer %s: %w", k.queue, err)
	}
	for d := range s.deliveries {
		err := k.answer(s.epoch, func() error { return d.Nack(false, true) })
		if err != nil && !errors.Is(err, ErrStale) {
			k.logger.Warn("nack after cancel failed", "queue", k.queue, "error", err)
		}
	}
	return nil
}

// Close closes the consumer's channel. The broker requeues every delivery
// that was not acknowledged.
func (k *Consumer) Close() error {
	k.conn.removeConsumer(k)
	if err := k.ch.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
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
