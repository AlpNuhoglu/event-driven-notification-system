//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

const (
	defaultGatewayURL = "ws://localhost:8082/v1/ws"
	// gatewayAhead is how far ahead of the stack's clock the followed
	// notifications are scheduled: long enough for the client to subscribe
	// and receive the snapshot while they are still pending, so that every
	// later status arrives as a live event.
	gatewayAhead = 3 * time.Second
)

// wsStatus is a status_changed message of the gateway.
type wsStatus struct {
	Type    string  `json:"type"`
	ID      string  `json:"id"`
	BatchID *string `json:"batch_id"`
	Status  string  `json:"status"`
	Version int64   `json:"version"`
	Code    string  `json:"code"`
	Message string  `json:"message"`
}

// follower is a WebSocket client of the gateway that keeps, per
// notification, the newest version it was told of (the client rule of
// ADR-0006 §10: a message whose version is not greater is ignored).
type follower struct {
	t       *testing.T
	ws      *websocket.Conn
	version map[string]int64
	status  map[string]string
	// statuses are the statuses of the accepted messages, in arrival order.
	statuses map[string][]string
}

func dialGateway(t *testing.T) *follower {
	t.Helper()
	url := envOr("E2E_GATEWAY_URL", defaultGatewayURL)
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	ws, resp, err := websocket.Dial(ctx, url, nil)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		t.Fatalf("dial the gateway at %s: %v", url, err)
	}
	t.Cleanup(func() { _ = ws.Close(websocket.StatusNormalClosure, "") })
	return &follower{t: t, ws: ws, version: map[string]int64{}, status: map[string]string{}, statuses: map[string][]string{}}
}

func (f *follower) subscribe(req map[string]any) {
	f.t.Helper()
	req["action"] = "subscribe"
	b, err := json.Marshal(req)
	if err != nil {
		f.t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	if err := f.ws.Write(ctx, websocket.MessageText, b); err != nil {
		f.t.Fatalf("subscribe: %v", err)
	}
}

// next reads the next message until deadline and applies the version rule.
func (f *follower) next(deadline time.Time) (wsStatus, error) {
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	_, b, err := f.ws.Read(ctx)
	if err != nil {
		return wsStatus{}, err
	}
	var m wsStatus
	if err := json.Unmarshal(b, &m); err != nil {
		return wsStatus{}, fmt.Errorf("decode %s: %w", b, err)
	}
	if m.Type != "status_changed" {
		return m, fmt.Errorf("message of type %q (%s: %s), want status_changed", m.Type, m.Code, m.Message)
	}
	if m.Version > f.version[m.ID] {
		f.version[m.ID] = m.Version
		f.status[m.ID] = m.Status
		f.statuses[m.ID] = append(f.statuses[m.ID], m.Status)
	}
	return m, nil
}

// snapshot reads one message for each of ids (the snapshot) and checks that
// each is pending, carries a version and, when batch is not empty, the batch.
func (f *follower) snapshot(ids []string, batch string) {
	f.t.Helper()
	deadline := time.Now().Add(requestTimeout)
	for range ids {
		m, err := f.next(deadline)
		if err != nil {
			f.t.Fatalf("snapshot: %v (have %v)", err, f.version)
		}
		if m.Status != "pending" || m.Version < 1 {
			f.t.Fatalf("snapshot of %s: status %q, version %d; want pending with a version (scheduled %s ahead)",
				m.ID, m.Status, m.Version, gatewayAhead)
		}
		if batch != "" && (m.BatchID == nil || *m.BatchID != batch) {
			f.t.Errorf("snapshot of %s: batch_id %v, want %s", m.ID, m.BatchID, batch)
		}
	}
	for _, id := range ids {
		if f.version[id] == 0 {
			f.t.Fatalf("no snapshot message for %s (have %v)", id, f.version)
		}
	}
}

// untilSent reads live events until every one of ids is sent, and checks
// that each went through live events after its snapshot.
func (f *follower) untilSent(ids []string, timeout time.Duration) {
	f.t.Helper()
	snapshot := map[string]int64{}
	for _, id := range ids {
		snapshot[id] = f.version[id]
	}
	deadline := time.Now().Add(timeout)
	for {
		pending := 0
		for _, id := range ids {
			if f.status[id] != "sent" {
				pending++
			}
		}
		if pending == 0 {
			break
		}
		if _, err := f.next(deadline); err != nil {
			f.t.Fatalf("%d of %d notifications not sent through live events within %s: %v; statuses received %v",
				pending, len(ids), timeout, err, f.statuses)
		}
	}
	for _, id := range ids {
		if f.version[id] <= snapshot[id] || len(f.statuses[id]) < 2 {
			f.t.Errorf("%s: statuses %v, want the snapshot and live events up to sent", id, f.statuses[id])
		}
	}
}

// dueSoon returns a scheduled_at gatewayAhead after the stack's clock: the
// created_at of a notification scheduled a day ahead (see
// TestComposeE2E_Scheduling), which is cancelled when the test ends.
func (s *stack) dueSoon(t *testing.T) string {
	t.Helper()
	var n scheduled
	s.postJSON(t, "/v1/notifications", map[string]any{
		"channel": "email", "recipient": fakeEmail, "content": "clock",
		"scheduled_at": time.Now().Add(24 * time.Hour).Format(time.RFC3339),
	}, http.StatusAccepted, &n)
	t.Cleanup(func() {
		_, _, _, _ = s.do(http.MethodPost, s.apiURL+"/v1/notifications/"+n.ID+"/cancel", nil)
	})
	return n.CreatedAt.Add(gatewayAhead).Format(time.RFC3339Nano)
}

// A client follows a notification and a batch through the gateway: it
// creates them (scheduled a few seconds ahead, so that they are pending
// when it subscribes), subscribes, receives the snapshot (pending, with a
// version) and then every later status as a live event, up to sent.
func TestComposeE2E_GatewayFollowsNotificationsToSent(t *testing.T) {
	s := newStack(t)
	s.resetMock(t)

	t.Run("notification", func(t *testing.T) {
		due := s.dueSoon(t)
		var n scheduled
		s.postJSON(t, "/v1/notifications", map[string]any{
			"channel": "sms", "recipient": fakePhone, "content": "followed", "priority": "high", "scheduled_at": due,
		}, http.StatusAccepted, &n)
		f := dialGateway(t)
		f.subscribe(map[string]any{"notification_ids": []string{n.ID}})
		f.snapshot([]string{n.ID}, "")
		f.untilSent([]string{n.ID}, gatewayAhead+sendTimeout)
		t.Logf("%s: %s", n.ID, strings.Join(f.statuses[n.ID], " -> "))
		got := s.waitSent(t, n.ID, sendTimeout)
		if got.Status != "sent" {
			t.Errorf("GET: status %q, want sent", got.Status)
		}
	})

	t.Run("batch", func(t *testing.T) {
		due := s.dueSoon(t)
		item := func(channel, recipient string) map[string]any {
			return map[string]any{
				"channel": channel, "recipient": recipient, "content": "followed in a batch",
				"priority": "high", "scheduled_at": due,
			}
		}
		var b struct {
			BatchID  string `json:"batch_id"`
			Accepted []struct {
				ID string `json:"id"`
			} `json:"accepted"`
		}
		s.postJSON(t, "/v1/notifications/batch", map[string]any{"notifications": []any{
			item("sms", fakePhone), item("email", fakeEmail), item("push", fakeDevice),
		}}, http.StatusAccepted, &b)
		if b.BatchID == "" || len(b.Accepted) != 3 {
			t.Fatalf("batch %q with %d accepted items, want 3", b.BatchID, len(b.Accepted))
		}
		ids := make([]string, len(b.Accepted))
		for i, a := range b.Accepted {
			ids[i] = a.ID
		}
		f := dialGateway(t)
		f.subscribe(map[string]any{"batch_id": b.BatchID})
		f.snapshot(ids, b.BatchID)
		f.untilSent(ids, gatewayAhead+sendTimeout)
		for _, id := range ids {
			t.Logf("%s: %s", id, strings.Join(f.statuses[id], " -> "))
		}
	})
}
