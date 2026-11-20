// Package gateway is the WebSocket gateway role (ADR-0005 "Gateway",
// ADR-0006 §10): clients subscribe to notification ids and batch ids on
// GET /v1/ws, receive the current state of each at once (the snapshot) and
// then every status event the role consumes from RabbitMQ.
//
// The goroutines of one connection:
//
//   - the reader (the HTTP handler's goroutine) reads the client's requests,
//     registers and removes subscriptions, reads the snapshot from Postgres
//     and hands snapshot and error messages to the writer, one at a time and
//     with back-pressure: it reads no further request until they are handed
//     over;
//   - the writer writes those messages and the live events of the
//     connection's bounded send queue, and closes the connection with a close
//     frame when asked (a full send queue, shutdown);
//   - the pinger pings the client every WS_PING_INTERVAL.
//
// Live events reach a connection through Dispatch, which never blocks: a
// connection whose send queue is full is closed with status 1013 (try again
// later), and the client reconnects and receives a fresh snapshot.
package gateway

import (
	"context"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

// Path is the WebSocket endpoint (ADR-0006 endpoint table).
const Path = "/v1/ws"

// Snapshots reads the current state of notifications for the snapshot on
// subscribe. *store.Store implements it on the gateway's read-only pool.
type Snapshots interface {
	// SnapshotByIDs returns the current state of each notification in ids
	// that exists, in any order.
	SnapshotByIDs(ctx context.Context, ids []uuid.UUID) ([]domain.StatusChanged, error)
	// SnapshotByBatch returns the current state of every notification of
	// the batch.
	SnapshotByBatch(ctx context.Context, batchID uuid.UUID) ([]domain.StatusChanged, error)
}

// Options configures a Gateway. Every field is required.
type Options struct {
	Snapshots Snapshots
	Logger    *slog.Logger
	// OriginPatterns are the allowed cross-origin hosts (WS_ORIGIN_PATTERNS),
	// as websocket.AcceptOptions.OriginPatterns: empty allows the same
	// origin only; a request without an Origin header is always accepted.
	OriginPatterns []string
	// MaxSubscriptions bounds the distinct notification ids plus distinct
	// batch ids of one connection (WS_MAX_SUBSCRIPTIONS).
	MaxSubscriptions int
	// SendQueueSize is the capacity of a connection's queue of live events
	// (WS_SEND_QUEUE_SIZE).
	SendQueueSize int
	// PingInterval is the time between pings, and the time a client has to
	// answer one (WS_PING_INTERVAL).
	PingInterval time.Duration
	// WriteTimeout bounds every write to a client, and the handing over of
	// the whole snapshot of one subscribe request (WS_WRITE_TIMEOUT).
	WriteTimeout time.Duration
}

// Gateway serves the WebSocket endpoint and fans status events out to the
// subscribed connections.
type Gateway struct {
	opts      Options
	readLimit int64

	// mu guards the subscription index and the connection registry.
	mu sync.RWMutex
	// byID and byBatch are the subscription index: the connections
	// subscribed to a notification id, and to a batch id.
	byID    map[uuid.UUID]map[*client]struct{}
	byBatch map[uuid.UUID]map[*client]struct{}
	// clients are the open connections; closing is set by Shutdown, after
	// which no connection is admitted.
	clients map[*client]struct{}
	closing bool
	// handlers counts the connections being served, for Shutdown.
	handlers sync.WaitGroup

	// slowClosed counts the connections closed because their send queue
	// was full.
	slowClosed atomic.Int64
	// snapshotTimeouts counts the connections closed because their client
	// did not take a snapshot within WriteTimeout.
	snapshotTimeouts atomic.Int64
}

// New returns a Gateway.
func New(opts Options) *Gateway {
	return &Gateway{
		opts: opts,
		// A request of MaxSubscriptions ids: 38 bytes per quoted id and its
		// comma, with room for whitespace and the other members.
		readLimit: 4096 + 64*int64(opts.MaxSubscriptions),
		byID:      map[uuid.UUID]map[*client]struct{}{},
		byBatch:   map[uuid.UUID]map[*client]struct{}{},
		clients:   map[*client]struct{}{},
	}
}

// Handler returns the HTTP handler of the gateway's port: GET /v1/ws.
func (g *Gateway) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+Path, g.serveWS)
	return mux
}

// serveWS upgrades the request and serves the connection until it ends. A
// disallowed origin is refused before the upgrade with 403.
func (g *Gateway) serveWS(w http.ResponseWriter, r *http.Request) {
	ws, err := websocket.Accept(&handshakeWriter{ResponseWriter: w}, r, &websocket.AcceptOptions{OriginPatterns: g.opts.OriginPatterns})
	if err != nil {
		// Accept has answered (403 for a disallowed origin). Its error is
		// not logged: it quotes the request's Origin header.
		g.opts.Logger.Debug("websocket upgrade refused")
		return
	}
	ws.SetReadLimit(g.readLimit)
	c := newClient(g, ws)
	if !g.admit(c) {
		_ = ws.Close(websocket.StatusGoingAway, goingAwayReason)
		return
	}
	defer g.release(c)
	// The request's cancellation is not used after the upgrade (the
	// connection is hijacked); the connection has its own (client.serve).
	c.serve(context.WithoutCancel(r.Context()))
}

// The bodies of a refused handshake.
const (
	originRefused    = "the request's origin is not allowed\n"
	handshakeRefused = "the WebSocket handshake was refused\n"
)

// handshakeWriter is the response writer the WebSocket library gets for the
// handshake. When the library refuses a request, it answers with a text
// that quotes the request: the Origin header of a refused origin, the
// header that broke the protocol. A gateway message never repeats what the
// client sent (ADR-0006 §10), so the status stays and the body is replaced
// by a fixed one. A handshake that succeeds passes through: the library
// takes the connection over through Unwrap.
type handshakeWriter struct {
	http.ResponseWriter
	refused bool
}

// WriteHeader writes a refusal's status with the gateway's own body.
func (h *handshakeWriter) WriteHeader(status int) {
	if status < http.StatusBadRequest {
		h.ResponseWriter.WriteHeader(status)
		return
	}
	h.refused = true
	body := handshakeRefused
	if status == http.StatusForbidden {
		body = originRefused
	}
	h.ResponseWriter.Header().Set("Content-Type", "text/plain; charset=utf-8")
	h.ResponseWriter.WriteHeader(status)
	_, _ = h.ResponseWriter.Write([]byte(body))
}

// Write drops the library's text of a refusal and reports it written.
func (h *handshakeWriter) Write(p []byte) (int, error) {
	if h.refused {
		return len(p), nil
	}
	return h.ResponseWriter.Write(p)
}

// Unwrap lets the library reach the connection to take it over.
func (h *handshakeWriter) Unwrap() http.ResponseWriter { return h.ResponseWriter }

// admit registers a new connection, unless the gateway is shutting down.
func (g *Gateway) admit(c *client) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closing {
		return false
	}
	g.clients[c] = struct{}{}
	g.handlers.Add(1)
	return true
}

// release removes an ended connection and its subscriptions.
func (g *Gateway) release(c *client) {
	g.mu.Lock()
	delete(g.clients, c)
	for id := range c.ids {
		removeFrom(g.byID, id, c)
	}
	for id := range c.batches {
		removeFrom(g.byBatch, id, c)
	}
	g.mu.Unlock()
	g.handlers.Done()
}

// subscribe adds c to the index for ids and batch.
func (g *Gateway) subscribe(c *client, ids []uuid.UUID, batch *uuid.UUID) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, id := range ids {
		addTo(g.byID, id, c)
	}
	if batch != nil {
		addTo(g.byBatch, *batch, c)
	}
}

// unsubscribe removes c from the index for ids and batch.
func (g *Gateway) unsubscribe(c *client, ids []uuid.UUID, batch *uuid.UUID) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, id := range ids {
		removeFrom(g.byID, id, c)
	}
	if batch != nil {
		removeFrom(g.byBatch, *batch, c)
	}
}

func addTo(index map[uuid.UUID]map[*client]struct{}, id uuid.UUID, c *client) {
	set := index[id]
	if set == nil {
		set = map[*client]struct{}{}
		index[id] = set
	}
	set[c] = struct{}{}
}

func removeFrom(index map[uuid.UUID]map[*client]struct{}, id uuid.UUID, c *client) {
	set := index[id]
	delete(set, c)
	if len(set) == 0 {
		delete(index, id)
	}
}

// Dispatch sends a live event to every connection subscribed to its id or
// to its batch id, once per connection. It never blocks on a connection: a
// connection whose send queue is full is closed (status 1013). The gateway
// keeps no per-notification state and does not filter by version.
func (g *Gateway) Dispatch(e domain.StatusChanged) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	byID := g.byID[e.ID]
	var byBatch map[*client]struct{}
	if e.BatchID != nil {
		byBatch = g.byBatch[*e.BatchID]
	}
	if len(byID) == 0 && len(byBatch) == 0 {
		return
	}
	msg, err := encodeStatus(e)
	if err != nil {
		g.opts.Logger.Warn("gateway: dropping an event that cannot be encoded", "id", e.ID.String(), "error", err)
		return
	}
	for c := range byID {
		c.enqueue(msg)
	}
	for c := range byBatch {
		if _, sent := byID[c]; !sent {
			c.enqueue(msg)
		}
	}
}

// Shutdown closes every connection with a close frame (1001, going away)
// and admits no new one. It waits for the connections to end until ctx is
// done; the close of a connection that has not ended by then goes on in the
// background, bounded by the WebSocket library's own close timeouts.
func (g *Gateway) Shutdown(ctx context.Context) error {
	g.mu.Lock()
	g.closing = true
	clients := slices.Collect(maps.Keys(g.clients))
	g.mu.Unlock()
	for _, c := range clients {
		c.closeWith(websocket.StatusGoingAway, goingAwayReason)
	}
	done := make(chan struct{})
	go func() {
		g.handlers.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Connections returns the number of open connections.
func (g *Gateway) Connections() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.clients)
}

// SlowClientsClosed returns the number of connections closed because their
// send queue was full.
func (g *Gateway) SlowClientsClosed() int64 {
	return g.slowClosed.Load()
}

// SnapshotTimeouts returns the number of connections closed because their
// client did not take a snapshot in time.
func (g *Gateway) SnapshotTimeouts() int64 {
	return g.snapshotTimeouts.Load()
}

// snapshot reads the current state of the notifications in ids and of the
// batch, each notification once.
func (g *Gateway) snapshot(ctx context.Context, ids []uuid.UUID, batch *uuid.UUID) ([]domain.StatusChanged, error) {
	var out []domain.StatusChanged
	if len(ids) > 0 {
		got, err := g.opts.Snapshots.SnapshotByIDs(ctx, ids)
		if err != nil {
			return nil, err
		}
		out = got
	}
	if batch != nil {
		got, err := g.opts.Snapshots.SnapshotByBatch(ctx, *batch)
		if err != nil {
			return nil, err
		}
		seen := make(map[uuid.UUID]bool, len(out))
		for _, e := range out {
			seen[e.ID] = true
		}
		for _, e := range got {
			if !seen[e.ID] {
				out = append(out, e)
			}
		}
	}
	return out, nil
}
