package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/config"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/provider"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/queue"
)

// A delivery without a priority property is sorted with the low ones, as
// the broker sorts it.
func TestDelivery_Priority(t *testing.T) {
	id := uuid.New()
	for _, tc := range []struct{ in, want uint8 }{{0, 4}, {4, 4}, {5, 5}, {6, 6}} {
		m := delivery{queue.Delivery{ID: id, Priority: tc.in, Epoch: 3}}
		if got := m.Priority(); got != tc.want {
			t.Errorf("priority %d: got %d, want %d", tc.in, got, tc.want)
		}
		if m.NotificationID() != id || m.Epoch() != 3 {
			t.Errorf("id %s epoch %d", m.NotificationID(), m.Epoch())
		}
	}
}

// The provider adapter passes every field of the request on and maps the
// response back.
func TestProviderClient_Send(t *testing.T) {
	id := uuid.New()
	var body map[string]string
	var key, trace string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key, trace = r.Header.Get("Idempotency-Key"), r.Header.Get("traceparent")
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode: %v", err)
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"messageId":"m-1","status":"accepted","timestamp":"2026-01-01T00:00:00Z"}`))
	}))
	defer srv.Close()
	c, err := provider.New(provider.Config{URL: srv.URL, Name: "mock", Timeout: waitTimeout}, prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	p := providerClient{c}
	res := p.Send(context.Background(), Request{
		NotificationID: id, Channel: domain.ChannelSMS, To: testRecipient, Content: testContent,
		TraceParent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	})
	if p.Name() != "mock" || res.StatusCode != http.StatusAccepted || res.Err != nil || res.MessageID != "m-1" {
		t.Errorf("name %q response %+v", p.Name(), res)
	}
	if body["to"] != testRecipient || body["channel"] != "sms" || body["content"] != testContent || len(body) != 3 {
		t.Errorf("body has %d fields", len(body))
	}
	if key != id.String() || trace == "" {
		t.Errorf("idempotency key %q traceparent %q", key, trace)
	}
}

func TestNewRole_RequiresResources(t *testing.T) {
	cfg, err := config.LoadFrom(func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewRole(cfg, Resources{}, slog.New(slog.DiscardHandler)); err == nil {
		t.Error("NewRole without resources: want an error")
	}
}
