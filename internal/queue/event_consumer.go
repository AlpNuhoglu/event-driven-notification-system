package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

// EventQueueConfig is the configuration of a gateway instance's event queue
// (ADR-0001 "Status events").
type EventQueueConfig struct {
	// MaxLength is the queue's x-max-length (EVENT_QUEUE_MAX_LENGTH): when
	// the gateway falls behind, the broker drops the oldest events.
	MaxLength int
	// MessageTTL is the queue's x-message-ttl (EVENT_QUEUE_MESSAGE_TTL),
	// sent in whole milliseconds.
	MessageTTL time.Duration
}

// EventConsumer is the status-event consumer of one gateway instance, on a
// connection of its own with the library's automatic recovery (D5).
//
// It consumes through its own exclusive, auto-delete classic queue, which
// the broker names, bound to EventsExchange with EventsBindingKey and
// acknowledged automatically: events are best-effort (ADR-0001). An
// exclusive queue disappears with the connection (and, being auto-delete,
// with its consumer), so the exchange, a new queue, its binding and the
// subscription are declared at start and again in the library's topology
// recovery hook after every reconnection and after a channel was reopened,
// before the connection reports open (D2). The broker naming the queue keeps
// a new declaration from colliding with an old queue that the broker has not
// removed yet.
//
// It is separate from Connection, which carries the dispatch topology and the
// worker's epochs: none of that applies here, because nothing is
// acknowledged.
type EventConsumer struct {
	conn   *amqp.Connection
	logger *slog.Logger
	args   amqp.Table
	// active is true while Run consumes from a subscription on an open
	// connection; it is set by Run's loop only.
	active atomic.Bool

	// subscribing serializes subscriptions: the first one, and the recovery
	// hook's after a reconnection or a reopened channel.
	subscribing sync.Mutex

	mu sync.Mutex
	ch *amqp.Channel
	// sub is the current subscription; nil before the first one and after
	// its stream ended.
	sub *eventSubscription
	// recovering is true from a lost connection or channel until the
	// recovery hook has subscribed again.
	recovering bool
	// changed is closed when sub or recovering changes, and replaced.
	changed chan struct{}
	gaveUp  chan struct{} // closed once, when recovery gives up
	err     error         // the error recovery gave up with
}

// eventSubscription is one basic.consume on the consumer's channel, on the
// queue declared for it.
type eventSubscription struct {
	queue      string
	tag        string
	deliveries <-chan amqp.Delivery
}

// DialEvents connects to url with automatic recovery (maxRetries attempts,
// each after retryInterval: AMQP_RECOVERY_MAX_RETRIES,
// AMQP_RECOVERY_RETRY_INTERVAL), declares the event queue and subscribes to
// it. The deliveries are read by Run.
func DialEvents(url string, retryInterval time.Duration, maxRetries int, q EventQueueConfig, logger *slog.Logger) (*EventConsumer, error) {
	if retryInterval <= 0 || maxRetries <= 0 {
		return nil, fmt.Errorf("dial rabbitmq: recovery needs a positive interval and retry count, got %s and %d", retryInterval, maxRetries)
	}
	if q.MaxLength <= 0 || q.MessageTTL.Milliseconds() <= 0 {
		return nil, fmt.Errorf("event queue: max length and message ttl must be positive, got %d and %s", q.MaxLength, q.MessageTTL)
	}
	uri, err := amqp.ParseURI(url)
	if err != nil {
		return nil, fmt.Errorf("dial rabbitmq: %w", err)
	}
	timeout := defaultConnectionTimeout
	if uri.ConnectionTimeout > 0 {
		timeout = time.Duration(uri.ConnectionTimeout) * time.Millisecond
	}
	k := &EventConsumer{
		logger: logger,
		args: amqp.Table{
			amqp.QueueTypeArg:       amqp.QueueTypeClassic,
			amqp.QueueMaxLenArg:     int64(q.MaxLength),
			amqp.QueueMessageTTLArg: q.MessageTTL.Milliseconds(), // int64
		},
		changed: make(chan struct{}),
		gaveUp:  make(chan struct{}),
	}
	hooks := &eventRecovery{k: k, inner: &amqp.DefaultConnectionRecovery{}}
	dial := amqp.DefaultDial(timeout)
	conn, err := amqp.DialConfig(url, amqp.Config{
		Dial: func(network, addr string) (net.Conn, error) {
			// Every dial after the first is a reconnection attempt: the
			// subscription of the old connection is dead. This runs before
			// the attempt resets the connection's closed flag.
			k.setRecovering(true)
			return dial(network, addr)
		},
		Recovery: &amqp.Recovery{
			ReconnectionConfig: &amqp.ReconnectionConfig{MaxRetryCount: maxRetries, RetryInterval: retryInterval},
			ConnectionRecovery: hooks,
			TopologyRecovery:   hooks,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("dial rabbitmq: %w", err)
	}
	k.conn = conn
	states := make(chan *amqp.StateChanged, 16)
	conn.NotifyStateChange(states)
	go k.watch(states)

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("event consumer: open channel: %w", err)
	}
	k.mu.Lock()
	k.ch = ch
	k.mu.Unlock()
	if err := k.subscribe(); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return k, nil
}

// eventRecovery holds the hooks the library calls during its recovery.
type eventRecovery struct {
	k     *EventConsumer
	inner amqp.ConnectionRecovery
}

// OnConnectionClose starts the library's connection recovery.
func (r *eventRecovery) OnConnectionClose(conn *amqp.Connection, err *amqp.Error) {
	r.inner.OnConnectionClose(conn, err)
}

// OnChannelClose marks the consumer as recovering while its channel is
// closed, then lets the library reopen the channel; the topology hook
// subscribes again.
func (r *eventRecovery) OnChannelClose(ch *amqp.Channel, err *amqp.Error) {
	r.k.mu.Lock()
	ours := r.k.ch == ch
	r.k.mu.Unlock()
	if ours && ch.IsClosed() {
		r.k.setRecovering(true)
		r.k.logger.Warn("status events: rabbitmq channel closed; the library reopens it", "error", errString(err))
	}
	r.inner.OnChannelClose(ch, err)
}

// RecoverTopology declares the event queue again and subscribes on the
// reopened channel. An error fails the reconnection attempt (the library
// tries again) or the channel's recovery.
func (r *eventRecovery) RecoverTopology(_ *amqp.Connection, channels []*amqp.Channel) ([]amqp.TopologyRecoveryEntity, error) {
	r.k.mu.Lock()
	ch := r.k.ch
	r.k.mu.Unlock()
	if ch == nil || !slices.Contains(channels, ch) {
		return nil, nil
	}
	if err := r.k.subscribe(); err != nil {
		r.k.logger.Warn("status events: subscribing again after a reconnection failed", "error", err)
		return nil, err
	}
	return nil, nil
}

// subscribe declares EventsExchange, a new exclusive, auto-delete queue and
// its binding, and subscribes to the queue on the consumer's channel. The
// new subscription replaces the current one, which is retired.
func (k *EventConsumer) subscribe() error {
	k.subscribing.Lock()
	defer k.subscribing.Unlock()
	k.mu.Lock()
	ch := k.ch
	k.mu.Unlock()

	if err := declareEventsExchange(ch); err != nil {
		return fmt.Errorf("event consumer: %w", err)
	}
	q, err := ch.QueueDeclare("", false, true, true, false, k.args)
	if err != nil {
		return fmt.Errorf("event consumer: declare queue: %w", err)
	}
	if err := ch.QueueBind(q.Name, EventsBindingKey, EventsExchange, false, nil); err != nil {
		return fmt.Errorf("event consumer: bind %s: %w", q.Name, err)
	}
	tag, err := consumerTag("gateway.events")
	if err != nil {
		return err
	}
	deliveries, err := ch.Consume(q.Name, tag, true, true, false, false, nil)
	if err != nil {
		return fmt.Errorf("event consumer: consume %s: %w", q.Name, err)
	}

	k.mu.Lock()
	old := k.sub
	k.sub = &eventSubscription{queue: q.Name, tag: tag, deliveries: deliveries}
	k.recovering = false
	k.signal()
	k.mu.Unlock()
	if old != nil {
		go k.retire(old)
	}
	return nil
}

// retire releases a replaced subscription: its session is gone, and the
// broker ignores the cancel of a tag it does not know. The cancel lets the
// library close the stream, which is drained.
func (k *EventConsumer) retire(s *eventSubscription) {
	_ = k.ch.Cancel(s.tag, true)
	for range s.deliveries { //nolint:revive // drained: a replaced subscription delivers nothing more of use
	}
}

// setRecovering records whether the consumer is waiting for its
// subscription to be renewed.
func (k *EventConsumer) setRecovering(v bool) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.recovering != v {
		k.recovering = v
		k.signal()
	}
}

// signal tells Run that the state changed. The caller holds k.mu.
func (k *EventConsumer) signal() {
	close(k.changed)
	k.changed = make(chan struct{})
}

// watch applies the library's state changes until it closes the listener.
func (k *EventConsumer) watch(states <-chan *amqp.StateChanged) {
	for sc := range states {
		log := k.logger.Info
		if sc.To == amqp.StateReconnecting || sc.Err != nil {
			log = k.logger.Warn
		}
		log("status events: rabbitmq connection state changed", "from", sc.From.String(), "to", sc.To.String(), "error", errString(sc.Err))

		k.mu.Lock()
		switch {
		case sc.To == amqp.StateReconnecting:
			// A notification read late, after the recovery has subscribed
			// again, must not mark the new subscription as lost. The
			// library sets the closed flag before it reports reconnecting
			// and clears it only in a reconnection attempt, whose Dial hook
			// marks the loss itself; the flag is read under k.mu, which the
			// recovery hook needs to clear the mark.
			if k.conn.IsClosed() && !k.recovering {
				k.recovering = true
				k.signal()
			}
		case sc.To == amqp.StateClosed && sc.Err != nil && k.err == nil:
			k.err = sc.Err
			close(k.gaveUp)
		}
		k.mu.Unlock()
	}
}

// Active reports whether the consumer is consuming: Run is running, it has
// a subscription and the connection is not being recovered. It is the
// gateway's readiness signal for status events.
func (k *EventConsumer) Active() bool {
	return k.active.Load()
}

// Queue returns the name of the current event queue (the broker names it;
// it changes with every new subscription), or "" when there is none.
func (k *EventConsumer) Queue() string {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.sub == nil {
		return ""
	}
	return k.sub.queue
}

// GaveUp returns a channel that is closed when automatic recovery has given
// up; Err then returns its error.
func (k *EventConsumer) GaveUp() <-chan struct{} {
	return k.gaveUp
}

// Err returns the error recovery gave up with, or nil.
func (k *EventConsumer) Err() error {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.err
}

// Run hands every status event to handle until ctx is cancelled (it then
// returns nil) or the consumer cannot go on, which is an error: recovery
// gave up, or the stream of the current subscription ended. The library
// ends that stream when the channel is closed for good and when the broker
// cancels the consumer, which it does not do to an exclusive queue's own
// consumer in normal operation (not even an operator can delete the queue);
// the restarted process subscribes anew. A reconnection does not end it:
// the recovery hook replaces the subscription first.
//
// handle must not block: it is called on Run's goroutine, and the
// deliveries wait in the library's buffer meanwhile. A body that does not
// decode is logged, without its content, and dropped.
func (k *EventConsumer) Run(ctx context.Context, handle func(domain.StatusChanged)) error {
	defer k.active.Store(false)
	for {
		k.mu.Lock()
		s, changed, recovering := k.sub, k.changed, k.recovering
		k.mu.Unlock()
		k.active.Store(s != nil && !recovering)
		var deliveries <-chan amqp.Delivery
		if s != nil {
			deliveries = s.deliveries
		}
		select {
		case <-ctx.Done():
			return nil
		case <-k.gaveUp:
			return errors.Join(errors.New("status events: rabbitmq connection recovery gave up"), k.Err())
		case <-changed:
		case d, ok := <-deliveries:
			if ok {
				k.dispatch(d, handle)
				continue
			}
			// A replaced subscription's stream ends when it is retired.
			k.mu.Lock()
			current := k.sub == s
			k.mu.Unlock()
			if current {
				if ctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("status events: the subscription to %s ended", s.queue)
			}
		}
	}
}

func (k *EventConsumer) dispatch(d amqp.Delivery, handle func(domain.StatusChanged)) {
	e, err := DecodeEvent(d.Body)
	if err != nil {
		// Neither the body nor the decoder's message is logged: the message
		// can quote a part of the body.
		k.logger.Warn("status events: dropping a message that does not decode",
			"routing_key", d.RoutingKey, "body_bytes", len(d.Body))
		return
	}
	handle(e)
}

// Close closes the connection. It does not close GaveUp.
func (k *EventConsumer) Close() error {
	err := k.conn.Close()
	if err == nil || errors.Is(err, amqp.ErrClosed) {
		return nil
	}
	return fmt.Errorf("close rabbitmq connection: %w", err)
}
