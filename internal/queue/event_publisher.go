package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/config"
)

// EventPublisherConfig configures an EventPublisher.
type EventPublisherConfig struct {
	// URL is the broker (RABBITMQ_URL). The publisher opens a connection of
	// its own to it. A URL that cannot be parsed is reported when the
	// publisher first dials, like an unreachable broker, so that it never
	// stops a role from starting.
	URL string
	// BufferSize is the number of events that can wait to be sent
	// (EVENT_BUFFER_SIZE); an event that finds the buffer full is dropped.
	BufferSize int
	// RetryInterval is the shortest time between two dial attempts
	// (AMQP_RECOVERY_RETRY_INTERVAL).
	RetryInterval time.Duration
	// FlushTimeout bounds Close: how long the buffered events get to reach
	// the broker before the connection is cut (EVENT_FLUSH_TIMEOUT). It is
	// added to a role's stop, so for the worker it has to fit between the
	// drain (15 s) and the stop grace period (20 s).
	FlushTimeout time.Duration
}

// EventPublisherConfigFrom returns the event publisher's settings of the
// loaded configuration. Every role that publishes status events uses it.
func EventPublisherConfigFrom(c config.Config) EventPublisherConfig {
	return EventPublisherConfig{
		URL:           c.Connections.RabbitMQURL,
		BufferSize:    c.Events.BufferSize,
		RetryInterval: c.Connections.AMQPRecoveryRetryInterval,
		FlushTimeout:  c.Events.FlushTimeout,
	}
}

// EventStats are the counters of an EventPublisher since it was made.
// Every event handed to Publish ends in exactly one of them, except the
// events still in the buffer or being sent.
type EventStats struct {
	// Published counts the events written to the connection. They are
	// published without confirms, so this is not proof that the broker
	// routed them.
	Published uint64
	// DroppedBufferFull counts the events that found the buffer full.
	DroppedBufferFull uint64
	// DroppedNotConnected counts the events dropped because there was no
	// connection: the broker could not be reached, a dial was not due yet,
	// or the publisher was closed.
	DroppedNotConnected uint64
	// DroppedPublishError counts the events whose publish failed on an open
	// connection (the connection is then dropped and dialled again later).
	DroppedPublishError uint64
}

// EventPublisher publishes status events to EventsExchange (ADR-0001
// "Status events", ADR-0005, D11). It implements events.Publisher and is
// optional by construction:
//
//   - Publish is a non-blocking hand-off to a bounded buffer; when the buffer
//     is full the event is dropped and counted. A transition never waits for
//     it and never fails because of it.
//   - It is lazy: making one does no I/O and starts nothing. The first
//     Publish starts its goroutine, which dials when it has an event to send.
//     A role therefore starts, serves and stays ready whether the broker is
//     reachable or not.
//   - It uses a connection of its own, never a role's dispatch connection, so
//     that a fault on the events path cannot touch dispatch. The connection
//     does not use the library's automatic recovery: after a loss the
//     publisher dials again when the next event arrives, at most once per
//     RetryInterval, and never gives up. Events that arrive while it has no
//     connection are dropped, not held.
//   - It declares the exchange on every connection (a broker that came back
//     empty has it again) and publishes without confirms and without
//     mandatory: no consumer bound is normal.
//
// A lost connection and a connection (re)established are logged once each,
// at warn and info; a broker that stays down adds only debug lines.
type EventPublisher struct {
	cfg    EventPublisherConfig
	logger *slog.Logger
	buf    chan domain.StatusChanged

	// closeMu orders Publish against Close: Publish holds it shared while it
	// hands an event over, Close exclusively while it sets closed, so that
	// no event enters the buffer after Close has begun.
	closeMu   sync.RWMutex
	closed    bool
	start     sync.Once // starts run, or (from Close) marks it never started
	closeOnce sync.Once
	stopping  chan struct{} // closed by Close: send what is buffered and stop
	done      chan struct{} // closed when run has returned

	// abortCtx is cancelled when the flush time is up: a dial or a write in
	// progress is cut and no further dial starts.
	abortCtx context.Context
	abort    context.CancelFunc
	sockMu   sync.Mutex
	sock     net.Conn // the socket of the current or dialling connection

	// Owned by run.
	conn     *amqp.Connection
	ch       *amqp.Channel
	chClosed chan *amqp.Error // the channel's close notification
	lastDial time.Time
	down     bool // an outage has been logged and has not ended

	published, droppedFull, droppedNotConnected, droppedPublishError atomic.Uint64
	// dials counts the dial attempts.
	dials atomic.Uint64
}

// NewEventPublisher returns a publisher of status events. It does no I/O.
func NewEventPublisher(cfg EventPublisherConfig, logger *slog.Logger) (*EventPublisher, error) {
	switch {
	case cfg.BufferSize <= 0:
		return nil, fmt.Errorf("event publisher: buffer size must be positive, got %d", cfg.BufferSize)
	case cfg.RetryInterval <= 0, cfg.FlushTimeout <= 0:
		return nil, fmt.Errorf("event publisher: retry interval and flush timeout must be positive, got %s and %s",
			cfg.RetryInterval, cfg.FlushTimeout)
	case logger == nil:
		return nil, errors.New("event publisher: a logger is required")
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &EventPublisher{
		cfg:      cfg,
		logger:   logger,
		buf:      make(chan domain.StatusChanged, cfg.BufferSize),
		stopping: make(chan struct{}),
		done:     make(chan struct{}),
		abortCtx: ctx,
		abort:    cancel,
	}, nil
}

// Publish hands e to the publisher and returns at once. The event is
// dropped, and counted, when the buffer is full or the publisher is closed.
func (p *EventPublisher) Publish(e domain.StatusChanged) {
	p.closeMu.RLock()
	defer p.closeMu.RUnlock()
	if p.closed {
		p.droppedNotConnected.Add(1)
		return
	}
	p.start.Do(func() { go p.run() })
	select {
	case p.buf <- e:
	default:
		p.droppedFull.Add(1)
	}
}

// Stats returns the counters.
func (p *EventPublisher) Stats() EventStats {
	return EventStats{
		Published:           p.published.Load(),
		DroppedBufferFull:   p.droppedFull.Load(),
		DroppedNotConnected: p.droppedNotConnected.Load(),
		DroppedPublishError: p.droppedPublishError.Load(),
	}
}

// Close gives the buffered events up to FlushTimeout to be sent (dialling
// if needed), then closes the connection. When the time is up, a dial or a
// write still in progress is cut and what is left is dropped, so Close
// returns within about FlushTimeout even when the broker does not answer.
// Events published after Close are dropped. Close is safe to call more than
// once; later calls return at once.
func (p *EventPublisher) Close() {
	p.closeOnce.Do(func() {
		p.closeMu.Lock()
		p.closed = true
		p.closeMu.Unlock()

		started := true
		p.start.Do(func() { started = false })
		if started {
			close(p.stopping)
			flush := time.NewTimer(p.cfg.FlushTimeout)
			select {
			case <-p.done:
			case <-flush.C:
				p.cut()
				<-p.done
			}
			flush.Stop()
		}
		p.cut()
		// run has returned: whatever is still buffered is never sent.
		for {
			select {
			case <-p.buf:
				p.droppedNotConnected.Add(1)
			default:
				return
			}
		}
	})
}

// cut aborts the dial or write in progress and every later dial.
func (p *EventPublisher) cut() {
	p.abort()
	p.sockMu.Lock()
	defer p.sockMu.Unlock()
	if p.sock != nil {
		_ = p.sock.Close()
	}
}

// run sends the buffered events until Close, then flushes and disconnects.
func (p *EventPublisher) run() {
	defer close(p.done)
	defer p.disconnect()
	for {
		select {
		case e := <-p.buf:
			p.send(e)
		case err := <-p.chClosed: // nil channel while disconnected
			p.lost(err)
		case <-p.stopping:
			for {
				select {
				case e := <-p.buf:
					p.send(e)
				default:
					return
				}
			}
		}
	}
}

// send publishes e, dialling first if there is no connection.
func (p *EventPublisher) send(e domain.StatusChanged) {
	if p.ch == nil && !p.connect() {
		p.droppedNotConnected.Add(1)
		return
	}
	msg, err := eventPublishing(e)
	if err != nil { // cannot happen for an event of a known status
		p.droppedPublishError.Add(1)
		p.logger.Debug("events: encoding a status event failed", "error", err)
		return
	}
	// Without confirms the publish only writes the frames; the context is
	// checked before the write. A channel error caused by the publish (an
	// exchange deleted under the connection) arrives on chClosed.
	if err := p.ch.PublishWithContext(p.abortCtx, EventsExchange, e.RoutingKey(), false, false, msg); err != nil {
		p.droppedPublishError.Add(1)
		p.lost(err)
		return
	}
	p.published.Add(1)
}

// connect dials the broker, opens a channel and declares the exchange. It
// does not dial within RetryInterval of the previous attempt, nor after the
// flush time of Close is up.
func (p *EventPublisher) connect() bool {
	if p.abortCtx.Err() != nil {
		return false
	}
	now := time.Now()
	if !p.lastDial.IsZero() && now.Sub(p.lastDial) < p.cfg.RetryInterval {
		return false
	}
	p.lastDial = now
	p.dials.Add(1)

	conn, err := p.dial()
	if err != nil {
		p.unreachable(err)
		return false
	}
	ch, err := conn.Channel()
	if err == nil {
		err = declareEventsExchange(ch)
	}
	if err != nil {
		p.closeConn(conn)
		p.unreachable(err)
		return false
	}
	p.conn, p.ch = conn, ch
	p.chClosed = ch.NotifyClose(make(chan *amqp.Error, 1))
	p.down = false
	p.logger.Info("events: connected to rabbitmq; status events are published", "exchange", EventsExchange)
	return true
}

// dial opens the AMQP connection. The socket is recorded so that Close can
// cut it, and the dial and the handshake are bounded by the URL's
// connection_timeout, or by the library's default, as for the dispatch
// connection.
func (p *EventPublisher) dial() (*amqp.Connection, error) {
	uri, err := amqp.ParseURI(p.cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("parse rabbitmq url: %w", err)
	}
	timeout := defaultConnectionTimeout
	if uri.ConnectionTimeout > 0 {
		timeout = time.Duration(uri.ConnectionTimeout) * time.Millisecond
	}
	conn, err := amqp.DialConfig(p.cfg.URL, amqp.Config{
		Dial: func(network, addr string) (net.Conn, error) {
			d := net.Dialer{Timeout: timeout}
			c, err := d.DialContext(p.abortCtx, network, addr)
			if err != nil {
				return nil, err
			}
			// The library clears this deadline once the handshake is done.
			if err := c.SetDeadline(time.Now().Add(timeout)); err != nil {
				_ = c.Close()
				return nil, err
			}
			p.sockMu.Lock()
			p.sock = c
			p.sockMu.Unlock()
			// cut cancels the context before it reads sock: one of the two
			// sees the other.
			if err := p.abortCtx.Err(); err != nil {
				_ = c.Close()
				return nil, err
			}
			return c, nil
		},
	})
	if err != nil {
		return nil, fmt.Errorf("dial rabbitmq: %w", err)
	}
	return conn, nil
}

// unreachable records a failed dial. The first failure of an outage is
// logged at warn; later ones are debug lines.
func (p *EventPublisher) unreachable(err error) {
	if p.down {
		p.logger.Debug("events: rabbitmq still unreachable", "error", err)
		return
	}
	p.down = true
	p.logger.Warn("events: rabbitmq unreachable; status events are dropped until it is back", "error", err)
}

// lost drops the current connection after its channel closed or a publish
// failed. The next event dials again.
func (p *EventPublisher) lost(err error) {
	p.disconnect()
	if p.down {
		return
	}
	p.down = true
	p.logger.Warn("events: rabbitmq connection lost; status events are dropped until it is back", "error", errText(err))
}

// disconnect closes the current connection, if any.
func (p *EventPublisher) disconnect() {
	if p.conn == nil {
		return
	}
	p.closeConn(p.conn)
	p.conn, p.ch, p.chClosed = nil, nil, nil
}

// closeConn closes conn and its socket. The close handshake is bounded by
// RetryInterval, and by Close's cut, so that a broker that stopped answering
// cannot hold it.
func (p *EventPublisher) closeConn(conn *amqp.Connection) {
	_ = conn.CloseDeadline(time.Now().Add(p.cfg.RetryInterval))
	p.sockMu.Lock()
	defer p.sockMu.Unlock()
	if p.sock != nil {
		_ = p.sock.Close()
		p.sock = nil
	}
}

// errText returns the message of err, also of a nil *amqp.Error inside a
// non-nil interface (a channel closed without an error).
func errText(err error) string {
	var ae *amqp.Error
	if err == nil || (errors.As(err, &ae) && ae == nil) {
		return "closed"
	}
	return err.Error()
}
