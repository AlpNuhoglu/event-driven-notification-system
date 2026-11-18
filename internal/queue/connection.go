// Package queue is the RabbitMQ side of the dispatch path (ADR-0001): the
// connection with the client library's automatic recovery, the topology, the
// relay's confirming publisher and the worker's consumer.
package queue

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Connection is one AMQP connection with the library's automatic recovery
// enabled explicitly (decision D5). It follows the library's state
// notifications (NotifyStateChange; NotifyClose is not a reliable signal,
// experiment E4) and exposes what the roles need: whether the connection is
// open now, a signal when recovery has given up, and the connection epoch.
type Connection struct {
	conn   *amqp.Connection
	logger *slog.Logger

	// epoch is the connection epoch stamped on every delivery (D76). P3
	// advances it each time the connection is lost (see watch); until then
	// it stays 0.
	epoch atomic.Uint64

	mu sync.Mutex
	// lost is closed when the connection stops being open and replaced by a
	// new, open channel when it is open again. A caller that keeps the value
	// it read therefore learns about every loss after that read, even one
	// that has already been recovered.
	lost   chan struct{}
	gaveUp chan struct{} // closed once, when recovery gives up
	err    error         // the error recovery gave up with
	topo   *amqp.Channel // the channel the topology was declared on
}

// Dial connects to url with automatic recovery: maxRetries reconnection
// attempts, each after a wait of retryInterval (AMQP_RECOVERY_MAX_RETRIES,
// AMQP_RECOVERY_RETRY_INTERVAL). State changes are logged on logger.
func Dial(url string, retryInterval time.Duration, maxRetries int, logger *slog.Logger) (*Connection, error) {
	if retryInterval <= 0 || maxRetries <= 0 {
		return nil, fmt.Errorf("dial rabbitmq: recovery needs a positive interval and retry count, got %s and %d", retryInterval, maxRetries)
	}
	conn, err := amqp.DialConfig(url, amqp.Config{
		Recovery: &amqp.Recovery{
			ReconnectionConfig: &amqp.ReconnectionConfig{MaxRetryCount: maxRetries, RetryInterval: retryInterval},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("dial rabbitmq: %w", err)
	}
	c := &Connection{
		conn:   conn,
		logger: logger,
		lost:   make(chan struct{}),
		gaveUp: make(chan struct{}),
	}
	states := make(chan *amqp.StateChanged, 16)
	conn.NotifyStateChange(states) // closed at once if the connection is already closed
	go c.watch(states)
	return c, nil
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
		if sc.To == amqp.StateOpen {
			if isClosed(c.lost) {
				c.lost = make(chan struct{})
			}
			// P3: reconciliation is triggered here (reconnecting -> open).
		} else {
			closeIfOpen(c.lost)
			// P3: the epoch is advanced here when the connection is lost.
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

// IsOpen reports whether the connection is open now. While it is not, the
// relay starts no transaction.
func (c *Connection) IsOpen() bool {
	return !isClosed(c.lostSignal())
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

// Epoch returns the current connection epoch (D76). It is stamped on every
// delivery; in this phase it does not advance.
func (c *Connection) Epoch() uint64 {
	return c.epoch.Load()
}

// channel opens a new AMQP channel on the connection.
func (c *Connection) channel() (*amqp.Channel, error) {
	ch, err := c.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("open channel: %w", err)
	}
	return ch, nil
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
