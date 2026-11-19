package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

func TestMain(m *testing.M) { testutil.Main(m) }

// Durations are parameters, in milliseconds.
const (
	waitTimeout  = 10_000 * time.Millisecond
	longInterval = 600_000 * time.Millisecond // a ping interval no test reaches
	writeTimeout = 10_000 * time.Millisecond
	// readinessTimeout bounds one readiness check.
	readinessTimeout = 5_000 * time.Millisecond
)

func discard() *slog.Logger { return slog.New(slog.DiscardHandler) }

// fakeSnapshots is an in-memory Snapshots.
type fakeSnapshots struct {
	mu   sync.Mutex
	rows map[uuid.UUID]domain.StatusChanged
	err  error
}

func newFakeSnapshots(rows ...domain.StatusChanged) *fakeSnapshots {
	f := &fakeSnapshots{rows: map[uuid.UUID]domain.StatusChanged{}}
	for _, r := range rows {
		f.rows[r.ID] = r
	}
	return f
}

func (f *fakeSnapshots) setErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func (f *fakeSnapshots) SnapshotByIDs(_ context.Context, ids []uuid.UUID) ([]domain.StatusChanged, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	var out []domain.StatusChanged
	for _, id := range ids {
		if r, ok := f.rows[id]; ok {
			out = append(out, r)
		}
	}
	return out, nil
}

func (f *fakeSnapshots) SnapshotByBatch(_ context.Context, batchID uuid.UUID) ([]domain.StatusChanged, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	var out []domain.StatusChanged
	for _, r := range f.rows {
		if r.BatchID != nil && *r.BatchID == batchID {
			out = append(out, r)
		}
	}
	return out, nil
}

// event returns a status event of id at version, in batch when it is not
// nil.
func event(id uuid.UUID, batch *uuid.UUID, status domain.Status, version int64) domain.StatusChanged {
	return domain.StatusChanged{
		ID: id, BatchID: batch, Channel: domain.ChannelSMS, Priority: domain.PriorityNormal,
		Status: status, Version: version, UpdatedAt: time.Date(2026, 11, 19, 12, 0, 0, 123456000, time.UTC),
	}
}

func testOptions(snapshots Snapshots) Options {
	return Options{
		Snapshots:        snapshots,
		Logger:           discard(),
		MaxSubscriptions: 1000,
		SendQueueSize:    256,
		PingInterval:     longInterval,
		WriteTimeout:     writeTimeout,
	}
}

// server is a Gateway behind an httptest server.
type server struct {
	g   *Gateway
	srv *httptest.Server
	url string // ws://host:port/v1/ws
}

// startServer serves a Gateway with opts. The connections are closed and
// the server stopped when the test ends.
func startServer(t *testing.T, opts Options, configure ...func(*httptest.Server)) *server {
	t.Helper()
	g := New(opts)
	srv := httptest.NewUnstartedServer(g.Handler())
	for _, c := range configure {
		c(srv)
	}
	srv.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
		defer cancel()
		_ = g.Shutdown(ctx)
		srv.Close()
	})
	return &server{g: g, srv: srv, url: "ws" + strings.TrimPrefix(srv.URL, "http") + Path}
}

// subscribers returns the number of connections subscribed to the id.
func (s *server) subscribers(id uuid.UUID) int {
	s.g.mu.RLock()
	defer s.g.mu.RUnlock()
	return len(s.g.byID[id])
}

// batchSubscribers returns the number of connections subscribed to the batch.
func (s *server) batchSubscribers(id uuid.UUID) int {
	s.g.mu.RLock()
	defer s.g.mu.RUnlock()
	return len(s.g.byBatch[id])
}

// wsClient is a test client.
type wsClient struct {
	t  *testing.T
	ws *websocket.Conn
}

// dial connects to the server; the connection is closed when the test
// ends.
func dial(t *testing.T, url string, opts *websocket.DialOptions) *wsClient {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	ws, resp, err := websocket.Dial(ctx, url, opts)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = ws.CloseNow() })
	return &wsClient{t: t, ws: ws}
}

// send writes a JSON text message.
func (c *wsClient) send(v any) {
	c.t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		c.t.Fatal(err)
	}
	c.sendRaw(websocket.MessageText, b)
}

func (c *wsClient) sendRaw(typ websocket.MessageType, b []byte) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	if err := c.ws.Write(ctx, typ, b); err != nil {
		c.t.Fatalf("write: %v", err)
	}
}

// subscribe sends a subscribe request for ids and the batch (nil: none).
func (c *wsClient) subscribe(ids []uuid.UUID, batch *uuid.UUID) {
	c.t.Helper()
	c.send(subscription("subscribe", ids, batch))
}

func (c *wsClient) unsubscribe(ids []uuid.UUID, batch *uuid.UUID) {
	c.t.Helper()
	c.send(subscription("unsubscribe", ids, batch))
}

func subscription(action string, ids []uuid.UUID, batch *uuid.UUID) map[string]any {
	m := map[string]any{"action": action}
	if ids != nil {
		m["notification_ids"] = ids
	}
	if batch != nil {
		m["batch_id"] = batch
	}
	return m
}

// message is a server message as the test reads it.
type message struct {
	raw     map[string]json.RawMessage
	Type    string     `json:"type"`
	ID      uuid.UUID  `json:"id"`
	BatchID *uuid.UUID `json:"batch_id"`
	Status  string     `json:"status"`
	Version int64      `json:"version"`
	Reason  *string    `json:"failure_reason"`
	Updated time.Time  `json:"updated_at"`
	Code    string     `json:"code"`
	Message string     `json:"message"`
}

// next reads the next server message.
func (c *wsClient) next() message {
	c.t.Helper()
	m, err := c.read(waitTimeout)
	if err != nil {
		c.t.Fatalf("read: %v", err)
	}
	return m
}

func (c *wsClient) read(timeout time.Duration) (message, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	typ, b, err := c.ws.Read(ctx)
	if err != nil {
		return message{}, err
	}
	if typ != websocket.MessageText {
		return message{}, errors.New("a binary message")
	}
	var m message
	if err := json.Unmarshal(b, &m); err != nil {
		return message{}, err
	}
	if err := json.Unmarshal(b, &m.raw); err != nil {
		return message{}, err
	}
	return m, nil
}

// nextStatus reads the next message and fails unless it is a
// status_changed message.
func (c *wsClient) nextStatus() message {
	c.t.Helper()
	m := c.next()
	if m.Type != "status_changed" {
		c.t.Fatalf("message of type %q (code %q, message %q), want status_changed", m.Type, m.Code, m.Message)
	}
	return m
}

// nextError reads the next message and fails unless it is an error
// message.
func (c *wsClient) nextError() message {
	c.t.Helper()
	m := c.next()
	if m.Type != "error" {
		c.t.Fatalf("message of type %q for %s, want error", m.Type, m.ID)
	}
	return m
}

// closeStatus reads until the connection ends and returns its close status
// (-1 without a close frame) and the number of messages read before.
func (c *wsClient) closeStatus() (websocket.StatusCode, int) {
	c.t.Helper()
	n := 0
	for {
		_, err := c.read(waitTimeout)
		if err == nil {
			n++
			continue
		}
		if errors.Is(err, context.DeadlineExceeded) {
			c.t.Fatalf("the connection did not end within %s (%d messages read)", waitTimeout, n)
		}
		return websocket.CloseStatus(err), n
	}
}

// within waits for a value on ch for at most timeout.
func within[T any](t *testing.T, ch <-chan T, timeout time.Duration, what string) T {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case v := <-ch:
		return v
	case <-timer.C:
		t.Fatalf("%s: nothing within %s", what, timeout)
		var zero T
		return zero
	}
}

// smallReadBuffer is a DialOptions whose connection has a small socket
// receive buffer, set before the connection is made so that the TCP window
// stays small: a client that does not read fills it quickly.
func smallReadBuffer() *websocket.DialOptions {
	d := &net.Dialer{Control: func(_, _ string, rc syscall.RawConn) error {
		var serr error
		if err := rc.Control(func(fd uintptr) {
			serr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, 4096)
		}); err != nil {
			return err
		}
		return serr
	}}
	return &websocket.DialOptions{HTTPClient: &http.Client{Transport: &http.Transport{DialContext: d.DialContext}}}
}

// smallWriteBuffer gives every connection the server accepts a small socket
// send buffer.
func smallWriteBuffer(s *httptest.Server) {
	s.Listener = smallBufferListener{s.Listener}
}

type smallBufferListener struct{ net.Listener }

func (l smallBufferListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.SetWriteBuffer(4096)
	}
	return c, nil
}
