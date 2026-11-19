package gateway

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
)

// Reasons of the close frames the gateway sends.
const (
	goingAwayReason     = "server shutting down"
	sendQueueFullReason = "send queue full"
)

// errWriterGone is returned when a message cannot be handed to the writer
// because the writer has ended.
var errWriterGone = errors.New("connection writer ended")

// client is one WebSocket connection.
type client struct {
	g  *Gateway
	ws *websocket.Conn

	// events is the bounded queue of live events, filled by Dispatch
	// without blocking and read by the writer. It is never closed.
	events chan []byte
	// direct carries snapshot and error messages from the reader to the
	// writer. It is unbuffered: the reader waits until the writer takes each.
	direct chan []byte
	// writerDone is closed when the writer has ended.
	writerDone chan struct{}

	// closing is closed, once, when the connection is to be closed with
	// closeCode and closeReason (a full send queue, shutdown); the writer
	// then sends the close frame.
	closeOnce   sync.Once
	closing     chan struct{}
	closeCode   websocket.StatusCode
	closeReason string

	// ids and batches are the connection's subscriptions. They are used by
	// the reader only, and by Gateway.release after the reader has ended.
	ids     map[uuid.UUID]struct{}
	batches map[uuid.UUID]struct{}
}

func newClient(g *Gateway, ws *websocket.Conn) *client {
	return &client{
		g:          g,
		ws:         ws,
		events:     make(chan []byte, g.opts.SendQueueSize),
		direct:     make(chan []byte),
		writerDone: make(chan struct{}),
		closing:    make(chan struct{}),
		ids:        map[uuid.UUID]struct{}{},
		batches:    map[uuid.UUID]struct{}{},
	}
}

// serve runs the connection's reader on this goroutine and its writer and
// pinger next to it, until the connection ends. base must not be cancelled
// before then.
func (c *client) serve(base context.Context) {
	// The connection's context ends when any of its goroutines ends: the
	// connection is over then.
	ctx, cancel := context.WithCancel(base)
	defer cancel()
	var wg sync.WaitGroup
	wg.Go(func() {
		defer cancel()
		c.write(ctx)
	})
	wg.Go(func() {
		defer cancel()
		c.ping(ctx)
	})
	c.read(ctx)
	cancel()
	wg.Wait()
	_ = c.ws.CloseNow()
}

// read reads and serves the client's requests until the connection ends.
func (c *client) read(ctx context.Context) {
	for {
		typ, data, err := c.ws.Read(ctx)
		if err != nil {
			return
		}
		if typ != websocket.MessageText {
			if c.hand(ctx, encodeError(invalidMessage("binary messages are not supported"))) != nil {
				return
			}
			continue
		}
		req, perr := parseRequest(data)
		if perr != nil {
			if c.hand(ctx, encodeError(perr)) != nil {
				return
			}
			continue
		}
		switch req.action {
		case actionSubscribe:
			err = c.subscribe(ctx, req)
		case actionUnsubscribe:
			c.unsubscribe(req)
		}
		if err != nil {
			return
		}
	}
}

// subscribe serves a subscribe request: all or nothing against the limit,
// then the registration, then the snapshot. An error means the connection
// is over.
//
// The order is what makes the snapshot safe (ADR-0005 "Snapshot on
// subscribe"): the subscription is registered before Postgres is read, so a
// transition that commits after the read has its event dispatched to this
// connection, and the client's version rule discards whichever of snapshot
// and event is older. With the read first, a transition committing between
// the read and the registration would reach the client in neither
// (TestGateway_SubscriptionIsRegisteredBeforeTheSnapshotIsRead).
func (c *client) subscribe(ctx context.Context, req request) error {
	added := 0
	for _, id := range req.ids {
		if _, ok := c.ids[id]; !ok {
			added++
		}
	}
	if req.batch != nil {
		if _, ok := c.batches[*req.batch]; !ok {
			added++
		}
	}
	if limit := c.g.opts.MaxSubscriptions; len(c.ids)+len(c.batches)+added > limit {
		return c.hand(ctx, encodeError(&protocolError{
			code:    codeTooManySubscriptions,
			message: fmt.Sprintf("the request would take this connection over %d subscriptions; nothing was subscribed", limit),
		}))
	}

	c.register(req)
	snap, err := c.g.snapshot(ctx, req.ids, req.batch)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		c.g.opts.Logger.Warn("gateway: reading the snapshot failed", "error", err)
		return c.hand(ctx, encodeError(&protocolError{
			code:    codeSnapshotUnavailable,
			message: "the current state could not be read; the subscription is registered and live events will arrive",
		}))
	}
	for _, e := range snap {
		msg, err := encodeStatus(e)
		if err != nil {
			c.g.opts.Logger.Warn("gateway: dropping a snapshot message that cannot be encoded", "id", e.ID.String(), "error", err)
			continue
		}
		if err := c.hand(ctx, msg); err != nil {
			return err
		}
	}
	return nil
}

// register records the request's subscriptions on the connection and in
// the gateway's index.
func (c *client) register(req request) {
	for _, id := range req.ids {
		c.ids[id] = struct{}{}
	}
	if req.batch != nil {
		c.batches[*req.batch] = struct{}{}
	}
	c.g.subscribe(c, req.ids, req.batch)
}

// unsubscribe removes the request's subscriptions; one that does not exist
// is ignored. No message answers it.
func (c *client) unsubscribe(req request) {
	var ids []uuid.UUID
	for _, id := range req.ids {
		if _, ok := c.ids[id]; ok {
			delete(c.ids, id)
			ids = append(ids, id)
		}
	}
	var batch *uuid.UUID
	if req.batch != nil {
		if _, ok := c.batches[*req.batch]; ok {
			delete(c.batches, *req.batch)
			batch = req.batch
		}
	}
	c.g.unsubscribe(c, ids, batch)
}

// hand gives msg to the writer and waits until it has taken it. It fails
// when the connection ends first.
func (c *client) hand(ctx context.Context, msg []byte) error {
	select {
	case c.direct <- msg:
		return nil
	case <-c.writerDone:
		return errWriterGone
	case <-ctx.Done():
		return ctx.Err()
	}
}

// enqueue puts a live event on the send queue without blocking. When the
// queue is full the connection is closed with status 1013 (try again
// later): its client reconnects and receives a fresh snapshot.
func (c *client) enqueue(msg []byte) {
	select {
	case <-c.closing:
		return
	default:
	}
	select {
	case c.events <- msg:
	default:
		if c.closeWith(websocket.StatusTryAgainLater, sendQueueFullReason) {
			c.g.slowClosed.Add(1)
		}
	}
}

// closeWith asks the writer to close the connection with code and reason.
// Only the first call has an effect; it reports whether this call was it.
// It never blocks.
func (c *client) closeWith(code websocket.StatusCode, reason string) bool {
	first := false
	c.closeOnce.Do(func() {
		c.closeCode, c.closeReason = code, reason
		close(c.closing)
		first = true
	})
	return first
}

// write writes the messages handed over and the queued live events until
// the connection ends or is to be closed; then it sends the close frame. A
// close request takes precedence over messages that are waiting.
func (c *client) write(ctx context.Context) {
	defer close(c.writerDone)
	for {
		select {
		case <-c.closing:
			c.close()
			return
		default:
		}
		var msg []byte
		select {
		case <-ctx.Done():
			return
		case <-c.closing:
			c.close()
			return
		case msg = <-c.direct:
		case msg = <-c.events:
		}
		wctx, cancel := context.WithTimeout(ctx, c.g.opts.WriteTimeout)
		err := c.ws.Write(wctx, websocket.MessageText, msg)
		cancel()
		if err != nil {
			// The library has closed the connection.
			return
		}
	}
}

// close sends the close frame of closeWith and waits for the client's
// answer; the library bounds both.
func (c *client) close() {
	_ = c.ws.Close(c.closeCode, c.closeReason)
}

// ping pings the client every PingInterval. A client that has not answered
// when the next ping is due is disconnected without a close handshake.
func (c *client) ping(ctx context.Context) {
	interval := c.g.opts.PingInterval
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		pctx, cancel := context.WithTimeout(ctx, interval)
		err := c.ws.Ping(pctx)
		cancel()
		if err != nil {
			if ctx.Err() == nil {
				c.g.opts.Logger.Debug("gateway: a client did not answer a ping; closing the connection")
				_ = c.ws.CloseNow()
			}
			return
		}
	}
}
