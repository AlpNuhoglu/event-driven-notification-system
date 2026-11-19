//go:build integration

package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/config"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/queue"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

// The api role's status-event publisher is optional (ADR-0005, D11, D39):
// the role starts, serves and stays ready without a broker, and its
// shutdown does not wait on one.

const roleWait = 10_000 * time.Millisecond

// roleRun is the api role run by run, as Service runs it, with a publisher
// made from the same configuration.
type roleRun struct {
	t    *testing.T
	addr string
	ev   *queue.EventPublisher
	logs *syncBuffer
	stop context.CancelFunc
	done chan error
}

// startRole runs the api role on a migrated database of its own with the
// given variables.
func startRole(t *testing.T, vars map[string]string) *roleRun {
	t.Helper()
	all := map[string]string{"DATABASE_URL": testutil.NewMigratedDatabase(t), "API_ADDR": "127.0.0.1:0"}
	for k, v := range vars {
		all[k] = v
	}
	cfg, err := config.LoadFrom(func(k string) (string, bool) { v, ok := all[k]; return v, ok })
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	logs := &syncBuffer{}
	logger := slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	ev, err := queue.NewEventPublisher(queue.EventPublisherConfigFrom(cfg), logger)
	if err != nil {
		t.Fatalf("event publisher: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &roleRun{t: t, ev: ev, logs: logs, stop: cancel, done: make(chan error, 1)}
	go func() { r.done <- run(ctx, cfg, logger, ev) }()
	t.Cleanup(func() {
		cancel()
		http.DefaultClient.CloseIdleConnections()
		if t.Failed() {
			t.Logf("api log:\n%s", logs.String())
		}
	})
	testutil.Eventually(t, func() bool {
		r.addr = logValue(logs.String(), "api listening", "api_addr")
		return r.addr != ""
	}, roleWait)
	return r
}

// logValue returns the string value of key in the first log line with
// message msg.
func logValue(out, msg, key string) string {
	for _, line := range strings.Split(out, "\n") {
		var m map[string]any
		if json.Unmarshal([]byte(line), &m) != nil || m["msg"] != msg {
			continue
		}
		s, _ := m[key].(string)
		return s
	}
	return ""
}

// do sends a request to the role and returns the status and the body.
func (r *roleRun) do(method, path, body string) (int, []byte) {
	r.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), roleWait)
	defer cancel()
	var rd io.Reader = http.NoBody
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://"+r.addr+path, rd)
	if err != nil {
		r.t.Fatal(err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		r.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer func() { _ = res.Body.Close() }()
	b, err := io.ReadAll(res.Body)
	if err != nil {
		r.t.Fatal(err)
	}
	return res.StatusCode, b
}

// createAndCancel creates a notification due in an hour and cancels it,
// both answered as without a broker problem: 202, then 200.
func (r *roleRun) createAndCancel() {
	r.t.Helper()
	when := time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)
	status, body := r.do(http.MethodPost, "/v1/notifications",
		`{"channel":"sms","recipient":"+905551234567","content":"hello","scheduled_at":"`+when+`"}`)
	if status != http.StatusAccepted {
		r.t.Fatalf("create: status %d: %s", status, body)
	}
	var id string
	if err := json.Unmarshal([]byte(rawField(r.t, body, "id")), &id); err != nil {
		r.t.Fatalf("create: id: %v", err)
	}
	if status, body := r.do(http.MethodPost, "/v1/notifications/"+id+"/cancel", ""); status != http.StatusOK {
		r.t.Fatalf("cancel: status %d: %s", status, body)
	}
}

// stopped cancels the role's context and waits until run has returned; it
// returns how long that took.
func (r *roleRun) stopped() time.Duration {
	r.t.Helper()
	start := time.Now()
	r.stop()
	var err error
	if !testutil.Within(func() bool {
		select {
		case err = <-r.done:
			return true
		default:
			return false
		}
	}, roleWait) {
		r.t.Fatalf("the api role did not stop within %s", roleWait)
	}
	if err != nil {
		r.t.Errorf("the api role returned %v", err)
	}
	return time.Since(start)
}

// The plan's P5 test: with RabbitMQ stopped (RABBITMQ_URL at an address
// nothing listens on) the api starts, is ready, accepts a create and a
// cancel, and the cancel's event is dropped and counted. The outage is one
// warn line.
func TestService_ServesWithRabbitMQStopped(t *testing.T) {
	r := startRole(t, map[string]string{"RABBITMQ_URL": "amqp://guest:guest@" + testutil.UnusedAddr(t) + "/"})
	if status, body := r.do(http.MethodGet, "/readyz", ""); status != http.StatusOK {
		t.Fatalf("readyz: status %d: %s", status, body)
	}
	r.createAndCancel()
	testutil.Eventually(t, func() bool { return r.ev.Stats().DroppedNotConnected == 1 }, roleWait)
	if s := r.ev.Stats(); s != (queue.EventStats{DroppedNotConnected: 1}) {
		t.Errorf("stats %+v, want the cancel's event dropped as not connected", s)
	}
	if status, _ := r.do(http.MethodGet, "/readyz", ""); status != http.StatusOK {
		t.Errorf("readyz after the dropped event: status %d", status)
	}
	if n := strings.Count(r.logs.String(), `"level":"WARN"`); n != 1 {
		t.Errorf("%d warn lines, want 1 (the broker is unreachable)", n)
	}
	r.stopped()
}

// The api's shutdown gives a held event its flush time and no more: with
// the events connection stalled, the role stops after the flush time
// (EVENT_FLUSH_TIMEOUT) and the event is counted as dropped.
func TestService_ShutdownDoesNotHangOnStalledBroker(t *testing.T) {
	const flush = 200 * time.Millisecond
	proxy := testutil.NewProxy(t, testutil.NewRabbitMQVhost(t))
	proxy.Pause()
	r := startRole(t, map[string]string{
		"RABBITMQ_URL":        proxy.URL() + "?connection_timeout=600000",
		"EVENT_FLUSH_TIMEOUT": "200ms",
	})
	r.createAndCancel()
	testutil.Eventually(t, func() bool { return proxy.BytesToBroker() > 0 }, roleWait)
	if took := r.stopped(); took < flush {
		t.Errorf("stopped after %s, before the flush time of %s", took, flush)
	}
	if s := r.ev.Stats(); s != (queue.EventStats{DroppedNotConnected: 1}) {
		t.Errorf("stats %+v, want the held event dropped as not connected", s)
	}
}
