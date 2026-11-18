//go:build integration

package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

func TestMain(m *testing.M) { testutil.Main(m) }

// Durations are parameters, in milliseconds.
const (
	itDBTimeout = 5_000 * time.Millisecond
	itLease     = 60_000 * time.Millisecond
	itMaxBody   = 4_096
	traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	email       = "user@example.com"
)

// testAPI is the real handler on a real database.
type testAPI struct {
	*apiClient
	pool  *pgxpool.Pool
	store *store.Store
	logs  *syncBuffer
}

func newTestAPI(t *testing.T) *testAPI {
	t.Helper()
	pool := testutil.NewPool(t)
	return newTestAPIOn(t, pool, &syncBuffer{})
}

func newTestAPIOn(t *testing.T, pool *pgxpool.Pool, logs *syncBuffer) *testAPI {
	t.Helper()
	st := store.New(pool, store.ProfilesFrom(testRetry()))
	h := NewHandler(Options{
		Repository:   st,
		DB:           pool,
		Logger:       slog.New(slog.NewJSONHandler(logs, nil)),
		Retry:        testRetry(),
		MaxBodyBytes: itMaxBody,
		DBTimeout:    itDBTimeout,
	})
	return &testAPI{apiClient: newAPIClient(t, h), pool: pool, store: st, logs: logs}
}

func ctxT(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), itDBTimeout)
	t.Cleanup(cancel)
	return ctx
}

// row is what the tests read back from the notifications table.
type row struct {
	status        string
	maxFailures   int
	ttl           time.Duration
	correlationID string
	traceParent   string
	recipient     string
	content       string
}

func (a *testAPI) row(t *testing.T, id string) row {
	t.Helper()
	var r row
	var createdAt, expiresAt time.Time
	err := a.pool.QueryRow(ctxT(t), `
		SELECT status, max_failures, created_at, expires_at, correlation_id, trace_parent, recipient, content
		FROM notifications WHERE id = $1`, id).
		Scan(&r.status, &r.maxFailures, &createdAt, &expiresAt, &r.correlationID, &r.traceParent, &r.recipient, &r.content)
	if err != nil {
		t.Fatalf("read row: %v", err)
	}
	r.ttl = expiresAt.Sub(createdAt)
	return r
}

func (a *testAPI) count(t *testing.T) int {
	t.Helper()
	var n int
	if err := a.pool.QueryRow(ctxT(t), `SELECT count(*) FROM notifications`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// rawField returns the raw JSON of one member of a response body.
func rawField(t *testing.T, body []byte, name string) string {
	t.Helper()
	var m map[string]json.RawMessage
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("decode %s: %v", body, err)
	}
	v, ok := m[name]
	if !ok {
		t.Fatalf("no %q in %s", name, body)
	}
	return string(v)
}

func TestCreate_EachPriority(t *testing.T) {
	a := newTestAPI(t)
	for _, tt := range []struct {
		priority    string
		ttl         time.Duration
		maxFailures int
	}{
		{"high", testRetry().High.TTL, testRetry().High.MaxFailures},
		{"normal", testRetry().Normal.TTL, testRetry().Normal.MaxFailures},
		{"low", testRetry().Low.TTL, testRetry().Low.MaxFailures},
	} {
		reqID := "req-" + tt.priority
		res := a.do(http.MethodPost, "/v1/notifications",
			`{"channel":"sms","recipient":"+905551234567","content":"hello","priority":"`+tt.priority+`"}`,
			map[string][]string{"X-Request-ID": {reqID}, "traceparent": {traceparent}})
		if res.status != http.StatusAccepted {
			t.Fatalf("%s: status %d: %s", tt.priority, res.status, res.body)
		}
		body := res.object(t)
		id, _ := body["id"].(string)
		if got := res.header.Get("Location"); got != "/v1/notifications/"+id {
			t.Errorf("%s: Location %q, id %q", tt.priority, got, id)
		}
		want := map[string]any{
			"channel": "sms", "recipient": phone, "priority": tt.priority, "status": "pending",
			"version": float64(1), "attempt_count": float64(0), "batch_id": nil, "scheduled_at": nil,
			"failure_reason": nil, "provider_message_id": nil, "sent_at": nil,
		}
		for k, v := range want {
			if body[k] != v {
				t.Errorf("%s: %s = %v, want %v", tt.priority, k, body[k], v)
			}
		}
		if _, ok := body["content"]; ok {
			t.Errorf("%s: content echoed", tt.priority)
		}
		for _, k := range []string{"created_at", "updated_at", "expires_at"} {
			s, _ := body[k].(string)
			if ts, err := time.Parse(time.RFC3339Nano, s); err != nil || !strings.HasSuffix(s, "Z") || ts.IsZero() {
				t.Errorf("%s: %s = %q, want RFC 3339 in UTC", tt.priority, k, s)
			}
		}
		r := a.row(t, id)
		if r.status != "pending" || r.maxFailures != tt.maxFailures || r.ttl != tt.ttl {
			t.Errorf("%s: row status %s, max_failures %d, expires_at - created_at %s; want pending, %d, %s",
				tt.priority, r.status, r.maxFailures, r.ttl, tt.maxFailures, tt.ttl)
		}
		if r.correlationID != reqID || r.traceParent != traceparent {
			t.Errorf("%s: correlation_id %q, trace_parent %q", tt.priority, r.correlationID, r.traceParent)
		}
		if r.recipient != phone || r.content != "hello" {
			t.Errorf("%s: stored values differ", tt.priority)
		}
	}
}

func TestCreate_DefaultPriority(t *testing.T) {
	a := newTestAPI(t)
	res := a.post(`{"channel":"email","recipient":"user@example.com","content":"hello"}`)
	if res.status != http.StatusAccepted {
		t.Fatalf("status %d: %s", res.status, res.body)
	}
	body := res.object(t)
	if body["priority"] != "normal" {
		t.Errorf("priority = %v", body["priority"])
	}
	r := a.row(t, body["id"].(string))
	if r.maxFailures != testRetry().Normal.MaxFailures || r.ttl != testRetry().Normal.TTL {
		t.Errorf("row %+v", r)
	}
}

// The create and the get response carry the real recipient. Marshalling a
// domain.Recipient directly would send the masked form.
func TestRecipientIsRevealed(t *testing.T) {
	a := newTestAPI(t)
	for _, tt := range []struct{ channel, recipient string }{
		{"sms", phone},
		{"email", email},
	} {
		masked := domain.Recipient(tt.recipient).Masked()
		if masked == tt.recipient {
			t.Fatalf("mask of %s is the value itself", tt.channel)
		}
		want, err := json.Marshal(tt.recipient)
		if err != nil {
			t.Fatal(err)
		}
		res := a.post(`{"channel":"` + tt.channel + `","recipient":"` + tt.recipient + `","content":"hello"}`)
		if res.status != http.StatusAccepted {
			t.Fatalf("%s: status %d", tt.channel, res.status)
		}
		got := rawField(t, res.body, "recipient")
		if got != string(want) {
			t.Errorf("create %s: recipient %s, want %s", tt.channel, got, want)
		}
		if strings.Contains(got, masked) || strings.Contains(got, "***") {
			t.Errorf("create %s: recipient is masked: %s", tt.channel, got)
		}
		id := strings.Trim(rawField(t, res.body, "id"), `"`)
		g := a.get("/v1/notifications/" + id)
		if g.status != http.StatusOK {
			t.Fatalf("get: %d", g.status)
		}
		if got := rawField(t, g.body, "recipient"); got != string(want) || strings.Contains(got, "***") {
			t.Errorf("get %s: recipient %s, want %s", tt.channel, got, want)
		}
	}
}

func TestGet_SentNotification(t *testing.T) {
	a := newTestAPI(t)
	res := a.post(validBody)
	if res.status != http.StatusAccepted {
		t.Fatalf("status %d", res.status)
	}
	id := uuid.MustParse(res.object(t)["id"].(string))
	claim, _, err := a.store.Claim(ctxT(t), id, itLease, "mock")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	pmid := "pm-123"
	ev, err := a.store.CompleteSent(ctxT(t), id, claim.Notification.AttemptCount, "mock",
		store.AttemptResult{Outcome: domain.OutcomeSuccess, ProviderMessageID: &pmid})
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	g := a.get("/v1/notifications/" + id.String())
	if g.status != http.StatusOK {
		t.Fatalf("get: %d %s", g.status, g.body)
	}
	body := g.object(t)
	if body["status"] != "sent" || body["version"] != float64(ev.Version) || body["provider_message_id"] != pmid ||
		body["attempt_count"] != float64(1) || body["failure_reason"] != nil {
		t.Errorf("body %v (event version %d)", body, ev.Version)
	}
	if s, ok := body["sent_at"].(string); !ok || !strings.HasSuffix(s, "Z") {
		t.Errorf("sent_at = %v", body["sent_at"])
	}
}

func TestGet_UnknownAndInvalidID(t *testing.T) {
	a := newTestAPI(t)
	if res := a.get("/v1/notifications/" + uuid.NewString()); res.status != http.StatusNotFound {
		t.Errorf("unknown id: status %d", res.status)
	}
	for _, bad := range []string{"abc", "123", url.PathEscape("0193f0d2-7c1e-7b3a-9d41-5be0a1c2e3f4x")} {
		res := a.get("/v1/notifications/" + bad)
		if res.status != http.StatusBadRequest {
			t.Errorf("%s: status %d", bad, res.status)
			continue
		}
		if got := res.problemCodes(t); len(got) != 1 || got[0] != "id:invalid_uuid" {
			t.Errorf("%s: codes %v", bad, got)
		}
		if strings.Contains(string(res.body), bad) {
			t.Errorf("%s: value echoed: %s", bad, res.body)
		}
	}
}

// Validation through the real handler: status and codes; nothing is stored.
func TestCreate_Validation(t *testing.T) {
	a := newTestAPI(t)
	tests := []struct {
		name   string
		body   string
		status int
		codes  []string
	}{
		{"missing channel", `{"recipient":"+905551234567","content":"x"}`, 422, []string{"channel:required"}},
		{"missing recipient", `{"channel":"sms","content":"x"}`, 422, []string{"recipient:required"}},
		{"missing content", `{"channel":"sms","recipient":"+905551234567"}`, 422, []string{"content:required"}},
		{"invalid channel", `{"channel":"fax","recipient":"+905551234567","content":"x"}`, 422, []string{"channel:invalid_value"}},
		{"invalid priority", `{"channel":"sms","recipient":"+905551234567","content":"x","priority":"urgent"}`, 422, []string{"priority:invalid_value"}},
		{"empty recipient", `{"channel":"sms","recipient":"","content":"x"}`, 422, []string{"recipient:empty"}},
		{"empty content", `{"channel":"sms","recipient":"+905551234567","content":""}`, 422, []string{"content:empty"}},
		{"recipient too long", `{"channel":"push","recipient":"` + strings.Repeat("t", 513) + `","content":"x"}`, 422, []string{"recipient:too_long"}},
		{"NUL in content", `{"channel":"sms","recipient":"+905551234567","content":"a\u0000"}`, 422, []string{"content:invalid_character"}},
		{"unknown field", `{"channel":"sms","recipient":"+905551234567","content":"x","scheduled_at":"2030-01-01T00:00:00Z"}`, 422, []string{"scheduled_at:unknown_field"}},
		{"dedupe_key is not known yet", `{"channel":"sms","recipient":"+905551234567","content":"x","dedupe_key":"k"}`, 422, []string{"dedupe_key:unknown_field"}},
		{
			"wrong types", `{"channel":1,"recipient":[],"content":false,"priority":2}`, 422,
			[]string{"channel:invalid_type", "recipient:invalid_type", "content:invalid_type", "priority:invalid_type"},
		},
		{
			"several at once", `{"channel":"fax","content":"","extra":1}`, 422,
			[]string{"channel:invalid_value", "recipient:required", "content:empty", "extra:unknown_field"},
		},
		{"duplicate field", `{"channel":"sms","recipient":"+905551234567","recipient":"+905551234567","content":"x"}`, 422, []string{"recipient:duplicate_field"}},
		{"empty body", ``, 400, []string{":empty_body"}},
		{"non-object body", `["sms"]`, 400, []string{":not_an_object"}},
		{"null body", `null`, 400, []string{":not_an_object"}},
		{"malformed JSON", `{"channel":"sms",`, 400, []string{":malformed_json"}},
	}
	for _, tt := range tests {
		var res response
		if tt.body == "" {
			res = a.do(http.MethodPost, "/v1/notifications", "", map[string][]string{"Content-Type": {"application/json"}})
		} else {
			res = a.post(tt.body)
		}
		if res.status != tt.status {
			t.Errorf("%s: status %d, want %d: %s", tt.name, res.status, tt.status, res.body)
			continue
		}
		if got := res.problemCodes(t); strings.Join(got, ",") != strings.Join(tt.codes, ",") {
			t.Errorf("%s: codes %v, want %v", tt.name, got, tt.codes)
		}
	}
	if n := a.count(t); n != 0 {
		t.Errorf("%d rows stored by invalid requests", n)
	}
	// The boundary is accepted.
	if res := a.post(`{"channel":"push","recipient":"` + strings.Repeat("t", 512) + `","content":"x"}`); res.status != http.StatusAccepted {
		t.Errorf("512-character recipient: status %d", res.status)
	}
}

func TestHTTPErrors(t *testing.T) {
	a := newTestAPI(t)
	big := `{"channel":"sms","recipient":"+905551234567","content":"` + strings.Repeat("a", itMaxBody) + `"}`
	if res := a.post(big); res.status != http.StatusRequestEntityTooLarge {
		t.Errorf("large body: %d", res.status)
	}
	if res := a.do(http.MethodPost, "/v1/notifications", validBody, map[string][]string{"Content-Type": {"text/plain"}}); res.status != http.StatusUnsupportedMediaType {
		t.Errorf("text/plain: %d", res.status)
	}
	if res := a.do(http.MethodPost, "/v1/notifications", validBody, map[string][]string{"Content-Type": {""}}); res.status != http.StatusUnsupportedMediaType {
		t.Errorf("no Content-Type: %d", res.status)
	}
	res := a.do(http.MethodDelete, "/v1/notifications/"+uuid.NewString(), "", nil)
	if res.status != http.StatusMethodNotAllowed || res.header.Get("Allow") != "GET, HEAD" {
		t.Errorf("DELETE: %d, Allow %q", res.status, res.header.Get("Allow"))
	}
	if res := a.do(http.MethodPost, "/healthz", validBody, nil); res.status != http.StatusMethodNotAllowed {
		t.Errorf("POST /healthz: %d", res.status)
	}
	if res := a.get("/v1/unknown"); res.status != http.StatusNotFound {
		t.Errorf("unknown path: %d", res.status)
	}
	if n := a.count(t); n != 0 {
		t.Errorf("%d rows stored", n)
	}
}

func TestRequestID(t *testing.T) {
	a := newTestAPI(t)
	// Accepted, echoed, stored and equal to correlation_id.
	res := a.do(http.MethodPost, "/v1/notifications", validBody, map[string][]string{"X-Request-ID": {"client-req_1.2"}})
	if res.header.Get("X-Request-ID") != "client-req_1.2" {
		t.Errorf("valid id not echoed: %q", res.header.Get("X-Request-ID"))
	}
	if r := a.row(t, res.object(t)["id"].(string)); r.correlationID != "client-req_1.2" {
		t.Errorf("stored correlation id %q", r.correlationID)
	}
	res = a.do(http.MethodPost, "/v1/notifications", `{}`, map[string][]string{"X-Request-ID": {"client-req-2"}})
	if res.status != http.StatusUnprocessableEntity || res.object(t)["correlation_id"] != "client-req-2" {
		t.Errorf("problem correlation id: %s", res.body)
	}

	// Replaced when invalid or absent, and always echoed (the contract check
	// asserts the echo equals correlation_id on every problem).
	for _, hdr := range [][]string{{strings.Repeat("a", 129)}, {"bad id"}, {"a", "b"}, nil} {
		res := a.do(http.MethodPost, "/v1/notifications", validBody,
			map[string][]string{"X-Request-ID": hdr, "traceparent": {"00-bad"}})
		got := res.header.Get("X-Request-ID")
		if id, err := uuid.Parse(got); err != nil || id.Version() != 7 {
			t.Errorf("X-Request-ID %q: echoed %q, want a new UUIDv7", hdr, got)
			continue
		}
		r := a.row(t, res.object(t)["id"].(string))
		if r.correlationID != got {
			t.Errorf("stored %q, echoed %q", r.correlationID, got)
		}
		if r.traceParent != "" {
			t.Errorf("invalid traceparent stored: %q", r.traceParent)
		}
	}
	for _, path := range []string{"/healthz", "/readyz", "/openapi.yaml", "/nope", "/v1/notifications/x"} {
		res := a.do(http.MethodGet, path, "", map[string][]string{"X-Request-ID": {"echo-me"}})
		if res.header.Get("X-Request-ID") != "echo-me" {
			t.Errorf("%s: X-Request-ID %q", path, res.header.Get("X-Request-ID"))
		}
	}
}

func TestHealth(t *testing.T) {
	a := newTestAPI(t)
	if res := a.get("/healthz"); res.status != http.StatusOK {
		t.Errorf("healthz: %d", res.status)
	}
	res := a.get("/readyz")
	if res.status != http.StatusOK {
		t.Fatalf("readyz: %d %s", res.status, res.body)
	}
	checks, _ := res.object(t)["checks"].([]any)
	if len(checks) != 1 || checks[0].(map[string]any)["name"] != "postgres" {
		t.Errorf("checks %v", checks)
	}
}

// deadPool returns a pool on a database that does not exist.
func deadPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	u, err := url.Parse(testutil.NewMigratedDatabase(t))
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/db_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	pool, err := pgxpool.New(context.Background(), u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func TestUnavailable(t *testing.T) {
	closed := testutil.NewPool(t)
	closed.Close()
	for name, pool := range map[string]*pgxpool.Pool{"missing database": deadPool(t), "closed pool": closed} {
		a := newTestAPIOn(t, pool, &syncBuffer{})
		for _, res := range []response{
			a.post(validBody),
			a.get("/v1/notifications/" + uuid.NewString()),
			a.get("/readyz"),
		} {
			if res.status != http.StatusServiceUnavailable {
				t.Errorf("%s: status %d, want 503: %s", name, res.status, res.body)
			}
		}
		if res := a.get("/healthz"); res.status != http.StatusOK {
			t.Errorf("%s: healthz %d", name, res.status)
		}
		// The failed readiness answer names the check, its status and its
		// duration (ADR-0009 §7), inside the problem document.
		var ready struct {
			Checks []struct {
				Name       string   `json:"name"`
				Status     string   `json:"status"`
				DurationMS *float64 `json:"duration_ms"`
			} `json:"checks"`
		}
		if err := json.Unmarshal([]byte(a.get("/readyz").body), &ready); err != nil {
			t.Fatalf("%s: readyz body: %v", name, err)
		}
		if len(ready.Checks) != 1 || ready.Checks[0].Name != "postgres" || ready.Checks[0].Status != "fail" || ready.Checks[0].DurationMS == nil {
			t.Errorf("%s: readyz checks = %+v, want postgres fail with a duration", name, ready.Checks)
		}
		if !strings.Contains(a.logs.String(), `"status":503`) {
			t.Errorf("%s: no 503 log line", name)
		}
	}
}

// No log line, and no response other than the successful notification,
// contains the recipient or the content.
func TestNoPersonalDataInLogsOrErrors(t *testing.T) {
	logs := &syncBuffer{}
	a := newTestAPIOn(t, testutil.NewPool(t), logs)
	down := newTestAPIOn(t, deadPool(t), logs)
	const content = "canary-content-7d1f2c"
	body := func(channel, recipient string) string {
		return `{"channel":"` + channel + `","recipient":"` + recipient + `","content":"` + content + `"}`
	}

	var errorBodies []string
	for _, recipient := range []string{phone, email} {
		ok := a.post(body("sms", recipient))
		if ok.status != http.StatusAccepted {
			t.Fatalf("valid create: %d", ok.status)
		}
		for _, res := range []response{
			a.post(body("fax", recipient)), // 422
			a.post(`{"channel":"` + recipient + `","recipient":"` + recipient + `","content":"` + content + `","priority":"` + content + `"}`),
			a.do(http.MethodPost, "/v1/notifications", body("sms", recipient), map[string][]string{"Content-Type": {"text/plain"}}), // 415
			down.post(body("sms", recipient)), // 503
		} {
			if res.status < 400 {
				t.Fatalf("expected an error, got %d", res.status)
			}
			errorBodies = append(errorBodies, string(res.body))
		}
	}
	for _, secret := range []string{"905551234567", email, content} {
		if strings.Contains(logs.String(), secret) {
			t.Errorf("log contains %q:\n%s", secret, logs)
		}
		for _, b := range errorBodies {
			if strings.Contains(b, secret) {
				t.Errorf("error body contains %q: %s", secret, b)
			}
		}
	}
	if strings.Count(logs.String(), "\n") != 10 {
		t.Errorf("want one log line per request (10), got:\n%s", logs)
	}
}
