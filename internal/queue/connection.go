// Package queue is the RabbitMQ side of the dispatch path (ADR-0001): the
// connection with the client library's automatic recovery, the topology, the
// relay's confirming publisher and the worker's consumer.
package queue

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"slices"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// defaultConnectionTimeout is the dial and handshake timeout when the URL
// sets no connection_timeout. It is the client library's own default, which
// a Dial function of our own has to restate (amqp091-go connection.go,
// defaultConnectionTimeout).
const defaultConnectionTimeout = 30 * time.Second

// Connection is one AMQP connection with the library's automatic recovery
// enabled explicitly (decision D5). It follows the library's state
// notifications (NotifyStateChange; NotifyClose is not a reliable signal,
// experiment E4) and exposes what the roles need: whether the connection can
// be published on now, every (re)connection, and a signal when recovery has
// given up.
//
// It takes part in the library's recovery through three hooks that the
// library calls synchronously on its recovery goroutines (amqp091-go
// v1.17.0):
//
//   - Config.Dial, called by Connection.Reconnect at the start of every
//     reconnection attempt, before the attempt resets the connection and
//     reopens its channels (connection.go, Reconnect: "conn, err =
//     dialer(...)" precedes c.resetState() and, in phase 1,
//     ch.reconnectChannel()). Here every consumer's epoch advances and the
//     connection stops counting as usable.
//   - ConnectionRecovery.OnChannelClose, called by the channel's close
//     watcher before the default recovery reopens a channel after a channel
//     error (channel.go, watchChannel; recovery.go,
//     DefaultConnectionRecovery.OnChannelClose -> ch.Reconnect ->
//     reconnectChannel -> openChannelSession -> resetState). Here the epoch of
//     a consumer on that channel advances if the channel is closed.
//   - TopologyRecovery.RecoverTopology, called after a reconnection's
//     channels have been reopened and before the connection reports open
//     (connection.go, Reconnect, phase 2), and after a single channel has
//     been reopened (channel.go, Channel.Reconnect). Here the topology is
//     declared again (on a reconnection) and every running consumer on a
//     reopened channel subscribes again. It replaces the library's own
//     topology recovery, which would replay the recorded declarations and
//     resubscribe consumers under their old tags.
type Connection struct {
	conn   *amqp.Connection
	logger *slog.Logger
	// closed reads the library's closed flag (conn.IsClosed). It is a field
	// so that a test can place a reconnection attempt at an exact point of
	// IsOpen.
	closed func() bool

	mu sync.Mutex
	// lost is closed when the connection stops being open and replaced by a
	// new, open channel when it is open again (in the topology recovery,
	// before the connection reports open). A caller that keeps the value it
	// read therefore learns about every loss after that read, even one that
	// has already been recovered.
	lost   chan struct{}
	gaveUp chan struct{} // closed once, when recovery gives up
	err    error         // the error recovery gave up with

	// consumerTimeout is the x-consumer-timeout of the topology; zero until
	// DeclareTopology is called, and then the topology is declared on every
	// reconnection.
	consumerTimeout time.Duration
	// dials counts the dial attempts, the first connection included; it is
	// the connection's generation.
	dials uint64
	// live is the generation known to be open (lost is open while live ==
	// dials, until the library reports a loss).
	live uint64
	// declared is the generation on which the topology was last declared
	// (and the running consumers subscribed); 0 for never. The connection is
	// usable while declared == dials.
	declared uint64
	// opens counts the generations on which the connection became usable;
	// opened is closed when it grows and then replaced.
	opens  uint64
	opened chan struct{}
	// consumers are the consumers on this connection; their epochs advance
	// when the connection is reset.
	consumers []*Consumer
}

// Dial connects to url with automatic recovery: maxRetries reconnection
// attempts, each after a wait of retryInterval (AMQP_RECOVERY_MAX_RETRIES,
// AMQP_RECOVERY_RETRY_INTERVAL). State changes are logged on logger.
func Dial(url string, retryInterval time.Duration, maxRetries int, logger *slog.Logger) (*Connection, error) {
	if retryInterval <= 0 || maxRetries <= 0 {
		return nil, fmt.Errorf("dial rabbitmq: recovery needs a positive interval and retry count, got %s and %d", retryInterval, maxRetries)
	}
	uri, err := amqp.ParseURI(url)
	if err != nil {
		return nil, fmt.Errorf("dial rabbitmq: %w", err)
	}
	timeout := defaultConnectionTimeout
	if uri.ConnectionTimeout > 0 {
		timeout = time.Duration(uri.ConnectionTimeout) * time.Millisecond
	}
	c := &Connection{
		logger: logger,
		lost:   make(chan struct{}),
		gaveUp: make(chan struct{}),
		opened: make(chan struct{}),
	}
	hooks := &recovery{c: c, inner: &amqp.DefaultConnectionRecovery{}}
	dial := amqp.DefaultDial(timeout)
	conn, err := amqp.DialConfig(url, amqp.Config{
		Dial: func(network, addr string) (net.Conn, error) {
			c.newGeneration()
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
	c.conn = conn
	c.closed = conn.IsClosed
	c.recovered(1) // the first connection is open
	states := make(chan *amqp.StateChanged, 16)
	conn.NotifyStateChange(states) // closed at once if the connection is already closed
	go c.watch(states)
	return c, nil
}

// recovery holds the hooks the library calls during its recovery.
type recovery struct {
	c     *Connection
	inner amqp.ConnectionRecovery
}

// OnConnectionClose starts the library's connection recovery. The epochs
// advance in the Dial hook, at the start of each attempt: this callback can
// also run for an extra error after the connection has already recovered
// (NotifyClose may deliver more than one error, experiment E4), and an
// advance then would orphan a valid subscription.
func (r *recovery) OnConnectionClose(conn *amqp.Connection, err *amqp.Error) {
	r.inner.OnConnectionClose(conn, err)
}

// OnChannelClose advances the epoch of a consumer on ch while ch is closed,
// and then lets the library reopen the channel. The advance happens before
// the reopen resets the channel's delivery tags, on this goroutine.
func (r *recovery) OnChannelClose(ch *amqp.Channel, err *amqp.Error) {
	if k := r.c.consumerOn(ch); k != nil {
		k.advance(true)
	}
	if err != nil { // the library calls this hook only with an error
		r.c.logger.Warn("rabbitmq channel closed with an error; the library reopens it", "error", err.Error())
	}
	r.inner.OnChannelClose(ch, err)
}

// RecoverTopology declares the topology again on a reconnection and
// subscribes every running consumer on the reopened channels again. An error
// fails the reconnection attempt (the library tries again) or the channel's
// recovery (the channel is closed for good).
func (r *recovery) RecoverTopology(conn *amqp.Connection, channels []*amqp.Channel) ([]amqp.TopologyRecoveryEntity, error) {
	return nil, r.c.recoverTopology(conn, channels)
}

// newGeneration is the Dial hook: a reconnection attempt begins. Every
// session on the old connection is dead (the library only dials while the
// connection is closed), so every consumer's epoch advances; the connection
// is not usable until the topology is declared on the new one. The initial
// dial is generation 1 and has no consumers yet.
func (c *Connection) newGeneration() {
	c.mu.Lock()
	c.dials++
	if c.dials > 1 {
		closeIfOpen(c.lost)
	}
	consumers := slices.Clone(c.consumers)
	c.mu.Unlock()
	for _, k := range consumers {
		k.advance(false)
	}
}

func (c *Connection) recoverTopology(conn *amqp.Connection, channels []*amqp.Channel) error {
	c.mu.Lock()
	gen := c.dials
	timeout := c.consumerTimeout
	// A reconnection is a new generation the topology was not declared on
	// yet. A channel that was reopened after a channel error is not a
	// reconnection: the topology on the broker is as it was (D2 asks for the
	// declaration on every (re)connection).
	reconnection := c.declared != gen
	var consumers []*Consumer
	for _, k := range c.consumers {
		if slices.Contains(channels, k.ch) {
			consumers = append(consumers, k)
		}
	}
	c.mu.Unlock()

	if timeout > 0 && reconnection {
		if err := declareOn(conn, timeout); err != nil {
			c.logger.Warn("rabbitmq: declaring the topology after a reconnection failed", "error", err)
			return fmt.Errorf("declare topology: %w", err)
		}
	}
	for _, k := range consumers {
		if _, err := k.subscribe(); err != nil {
			c.logger.Warn("rabbitmq: subscribing again after a reconnection failed", "queue", k.queue, "error", err)
			return err
		}
	}
	c.recovered(gen)
	if timeout > 0 && reconnection {
		c.markUsable(gen)
	}
	return nil
}

// recovered records that generation gen is open: the library has reopened
// the connection and its channels, and reports open when this hook returns.
// If gen is still the current generation, lost is open again. (Setting it
// here, and not when the state notification arrives, keeps lost and IsOpen
// in step: a notification can be read late.)
func (c *Connection) recovered(gen uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if gen != c.dials || c.live == gen {
		return
	}
	c.live = gen
	if isClosed(c.lost) {
		c.lost = make(chan struct{})
	}
}

// markUsable records that the topology is declared on generation gen. If gen
// is still the current generation and was not recorded before, the
// connection counts as usable and one more (re)connection is signalled.
func (c *Connection) markUsable(gen uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.consumerTimeout == 0 || gen != c.dials || c.declared == gen {
		return
	}
	c.declared = gen
	c.opens++
	close(c.opened)
	c.opened = make(chan struct{})
}

// watch applies every state change until the library closes the listener
// after the final transition to closed.
func (c *Connection) watch(states <-chan *amqp.StateChanged) {
	for sc := range states {
		log := c.logger.Info // opening, and a clean close
		if sc.To == amqp.StateReconnecting || sc.Err != nil {
			log = c.logger.Warn
		}
		log("rabbitmq connection state changed", "from", sc.From.String(), "to", sc.To.String(), "error", errString(sc.Err))

		c.mu.Lock()
		switch sc.To {
		case amqp.StateOpen:
			// lost was opened again by the topology recovery (recovered).
		case amqp.StateReconnecting:
			// A notification read late, after the connection was open
			// again, must not close lost: only a loss that is still current
			// does. The library sets the closed flag before it reports
			// reconnecting, and clears it only during a reconnection
			// attempt, whose Dial hook closes lost itself.
			if c.closed() {
				closeIfOpen(c.lost)
			}
		default: // closing or closed: final
			closeIfOpen(c.lost)
		}
		if sc.To == amqp.StateClosed && sc.Err != nil && c.err == nil {
			c.err = sc.Err
			close(c.gaveUp)
		}
		c.mu.Unlock()
	}
	c.mu.Lock()
	closeIfOpen(c.lost)
	c.mu.Unlock()
}

// IsOpen reports whether publishing to the dispatch topology is safe now:
// the connection is open and the topology has been declared on it. It is
// false from the moment the library notices a lost connection until the
// topology has been declared on the recovered one (D2), and on a connection
// whose topology was never declared. The relay starts no transaction while
// it is false.
//
// It combines two facts that a reconnection attempt changes one after the
// other: the Dial hook starts a new generation (dials), then the library
// clears its closed flag (resetState), and only later, in the topology
// recovery, is the topology declared (declared). The order of the two reads
// here is what makes the answer safe: **the library's flag first, our
// generation second.**
//
//   - If the flag is read as set, the answer is false.
//   - If it is read as cleared, either the connection has been open since
//     the topology was declared on it, or an attempt cleared it; that
//     attempt's Dial hook has then already run, so the generation read
//     afterwards is the attempt's (or a later one), and declared equals it
//     only once the topology is declared on that connection.
//
// Read the other way round, an attempt that began between the two reads made
// IsOpen answer true for the old generation with the cleared flag of the new
// connection, before the topology was declared on it
// (TestIsOpen_ReconnectionAttemptBetweenItsTwoReads).
//
// Like every such check the answer can be stale at once: a connection lost
// right after it is still reported open, and the publish then fails.
func (c *Connection) IsOpen() bool {
	// The library sets the closed flag first thing when it shuts the
	// connection down, before any recovery attempt dials again.
	if c.closed() {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.declared != 0 && c.declared == c.dials
}

// Opens reports the (re)connections: the number of times the connection has
// become usable (open with the topology declared on it, as IsOpen; the
// first connection counts once DeclareTopology has succeeded) and a channel
// that is closed when that number next grows.
//
// A caller that keeps the number it has handled and compares it with the
// number returned misses no (re)connection, also not one that happened
// between two calls; several that happened between two calls are seen as
// one step of more than one. The number never grows before the topology is
// declared on the new connection. A channel that the library reopens after a
// channel error is not a reconnection and does not count.
//
//	handled := uint64(0)
//	for {
//		n, next := conn.Opens()
//		if n > handled {
//			handled = n
//			// a (re)connection: reconcile
//		}
//		select {
//		case <-next:
//		case <-ctx.Done():
//			return
//		}
//	}
func (c *Connection) Opens() (uint64, <-chan struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.opens, c.opened
}

// lostSignal returns the channel that is closed when the connection stops
// being open (already closed if it is not open now).
func (c *Connection) lostSignal() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lost
}

// GaveUp returns a channel that is closed when automatic recovery has given
// up: the connection reached the state closed with an error. The role then
// exits with a non-zero code (ADR-0005). It is not closed by Close.
func (c *Connection) GaveUp() <-chan struct{} {
	return c.gaveUp
}

// Err returns the error recovery gave up with, or nil.
func (c *Connection) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// channel opens a new AMQP channel on the connection.
func (c *Connection) channel() (*amqp.Channel, error) {
	ch, err := c.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("open channel: %w", err)
	}
	return ch, nil
}

func (c *Connection) addConsumer(k *Consumer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.consumers = append(c.consumers, k)
}

func (c *Connection) removeConsumer(k *Consumer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.consumers = slices.DeleteFunc(c.consumers, func(x *Consumer) bool { return x == k })
}

// consumerOn returns the consumer on ch, or nil.
func (c *Connection) consumerOn(ch *amqp.Channel) *Consumer {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, k := range c.consumers {
		if k.ch == ch {
			return k
		}
	}
	return nil
}

// Close closes the connection. It does not close GaveUp.
func (c *Connection) Close() error {
	err := c.conn.Close()
	if errors.Is(err, amqp.ErrClosed) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("close rabbitmq connection: %w", err)
	}
	return nil
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func closeIfOpen(ch chan struct{}) {
	if !isClosed(ch) {
		close(ch)
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
