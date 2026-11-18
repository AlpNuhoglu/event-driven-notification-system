package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/puddle/v2"

	spec "github.com/AlpNuhoglu/event-driven-notification-system/api"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/config"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
)

func readErrorsDoc(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("../../docs/errors.md")
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// syncBuffer is a log sink that handlers may write to concurrently.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// fakeRepo is an in-memory Repository for the tests that need no database.
type fakeRepo struct {
	err   error
	panic bool
	last  store.NewNotification
}

func (f *fakeRepo) CreateNotification(_ context.Context, n store.NewNotification) (store.Notification, error) {
	if f.panic {
		panic("boom")
	}
	if f.err != nil {
		return store.Notification{}, f.err
	}
	f.last = n
	now := time.Now()
	return store.Notification{
		ID: uuid.Must(uuid.NewV7()), Channel: n.Channel, Priority: n.Priority, Recipient: n.Recipient,
		Status: domain.StatusPending, Version: 1, ExpiresAt: now.Add(n.TTL), MaxFailures: n.MaxFailures,
		CorrelationID: n.CorrelationID, TraceParent: n.TraceParent, CreatedAt: now, UpdatedAt: now,
	}, nil
}

func (f *fakeRepo) GetNotification(context.Context, uuid.UUID) (store.Notification, error) {
	if f.err != nil {
		return store.Notification{}, f.err
	}
	return store.Notification{}, store.ErrNotFound
}

func (f *fakeRepo) Ping(context.Context) error { return f.err }

func testRetry() config.RetryConfig {
	return config.RetryConfig{
		High:   config.Retry{TTL: 600_000 * time.Millisecond, MaxFailures: 5},
		Normal: config.Retry{TTL: 21_600_000 * time.Millisecond, MaxFailures: 8},
		Low:    config.Retry{TTL: 86_400_000 * time.Millisecond, MaxFailures: 10},
	}
}

func newFakeAPI(t *testing.T, repo *fakeRepo) (*apiClient, *syncBuffer) {
	t.Helper()
	logs := &syncBuffer{}
	h := NewHandler(Options{
		Repository:   repo,
		DB:           repo,
		Logger:       slog.New(slog.NewJSONHandler(logs, nil)),
		Retry:        testRetry(),
		MaxBodyBytes: 1024,
		DBTimeout:    1000 * time.Millisecond,
	})
	return newAPIClient(t, h), logs
}

func TestRouter_UnknownPathAndMethod(t *testing.T) {
	c, _ := newFakeAPI(t, &fakeRepo{})
	id := uuid.NewString()
	tests := []struct {
		method, path string
		status       int
		allow        string
	}{
		{http.MethodGet, "/nope", 404, ""},
		{http.MethodGet, "/v1/notifications/", 404, ""},
		{http.MethodGet, "//healthz", 404, ""},
		{http.MethodGet, "/v1/../healthz", 404, ""},
		{http.MethodDelete, "/v1/notifications/" + id, 405, "GET, HEAD"},
		{http.MethodPut, "/healthz", 405, "GET, HEAD"},
		{http.MethodGet, "/v1/notifications", 405, "POST"},
	}
	for _, tt := range tests {
		res := c.do(tt.method, tt.path, "", nil)
		if res.status != tt.status {
			t.Errorf("%s %s: status %d, want %d", tt.method, tt.path, res.status, tt.status)
		}
		if got := res.header.Get("Allow"); got != tt.allow {
			t.Errorf("%s %s: Allow %q, want %q", tt.method, tt.path, got, tt.allow)
		}
	}
}

func TestCreate_MediaTypeAndSize(t *testing.T) {
	repo := &fakeRepo{}
	c, _ := newFakeAPI(t, repo)
	for _, ct := range []string{"", "text/plain", "application/x-www-form-urlencoded", "application/jsonx", "application/problem+json"} {
		res := c.do(http.MethodPost, "/v1/notifications", validBody, map[string][]string{"Content-Type": {ct}})
		if res.status != http.StatusUnsupportedMediaType {
			t.Errorf("Content-Type %q: status %d, want 415", ct, res.status)
		}
	}
	res := c.do(http.MethodPost, "/v1/notifications", validBody, map[string][]string{"Content-Type": {"application/json; charset=utf-8"}})
	if res.status != http.StatusAccepted {
		t.Errorf("JSON with charset: status %d", res.status)
	}
	big := `{"channel":"sms","recipient":"+905551234567","content":"` + strings.Repeat("a", 2048) + `"}`
	if res := c.post(big); res.status != http.StatusRequestEntityTooLarge {
		t.Errorf("large body: status %d, want 413", res.status)
	}
}

func TestCreate_UsesPriorityProfile(t *testing.T) {
	repo := &fakeRepo{}
	c, _ := newFakeAPI(t, repo)
	if res := c.post(`{"channel":"sms","recipient":"+905551234567","content":"x","priority":"low"}`); res.status != http.StatusAccepted {
		t.Fatalf("status %d: %s", res.status, res.body)
	}
	if repo.last.TTL != testRetry().Low.TTL || repo.last.MaxFailures != 10 || repo.last.Priority != domain.PriorityLow {
		t.Errorf("stored %+v", repo.last)
	}
}

func TestStoreErrors(t *testing.T) {
	id := uuid.NewString()
	for _, tt := range []struct {
		err    error
		status int
	}{
		{puddle.ErrClosedPool, http.StatusServiceUnavailable},
		{errors.New("unexpected"), http.StatusInternalServerError},
	} {
		c, logs := newFakeAPI(t, &fakeRepo{err: tt.err})
		if res := c.post(validBody); res.status != tt.status {
			t.Errorf("%v: create status %d, want %d", tt.err, res.status, tt.status)
		}
		if res := c.get("/v1/notifications/" + id); res.status != tt.status {
			t.Errorf("%v: get status %d, want %d", tt.err, res.status, tt.status)
		}
		if !strings.Contains(logs.String(), `"error":"`+tt.err.Error()+`"`) {
			t.Errorf("%v: cause not in the log line: %s", tt.err, logs)
		}
	}
}

func TestPanicIsAProblem(t *testing.T) {
	c, logs := newFakeAPI(t, &fakeRepo{panic: true})
	res := c.post(validBody)
	if res.status != http.StatusInternalServerError {
		t.Errorf("status %d", res.status)
	}
	if strings.Contains(string(res.body), "boom") || strings.Contains(logs.String(), "boom") {
		t.Errorf("panic value leaked: %s / %s", res.body, logs)
	}
}

func TestOpenAPISpecServed(t *testing.T) {
	c, _ := newFakeAPI(t, &fakeRepo{})
	res := c.get("/openapi.yaml")
	if res.status != http.StatusOK || !bytes.Equal(res.body, spec.OpenAPI()) {
		t.Errorf("status %d, %d bytes", res.status, len(res.body))
	}
	if ct := res.header.Get("Content-Type"); ct != "application/yaml" {
		t.Errorf("Content-Type %q", ct)
	}
}

// One line per request: method, route template, status, duration and
// correlation id; never the raw path.
func TestAccessLog(t *testing.T) {
	c, logs := newFakeAPI(t, &fakeRepo{})
	id := uuid.NewString()
	res := c.do(http.MethodGet, "/v1/notifications/"+id, "", map[string][]string{"X-Request-ID": {"req-42"}})
	if res.status != http.StatusNotFound {
		t.Fatalf("status %d", res.status)
	}
	c.get("/nope")
	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("want 2 lines, got %d: %s", len(lines), logs)
	}
	var first map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"msg": "http request", "method": "GET", "route": "GET /v1/notifications/{id}",
		"status": float64(404), "correlation_id": "req-42",
	}
	for k, v := range want {
		if first[k] != v {
			t.Errorf("%s = %v, want %v", k, first[k], v)
		}
	}
	if _, ok := first["duration_ms"]; !ok {
		t.Error("no duration_ms")
	}
	if strings.Contains(logs.String(), id) || strings.Contains(logs.String(), "/nope") {
		t.Errorf("raw path logged: %s", logs)
	}
	if !strings.Contains(lines[1], `"route":"unmatched"`) {
		t.Errorf("second line: %s", lines[1])
	}
}
