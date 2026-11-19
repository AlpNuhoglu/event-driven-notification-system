package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// requestLine is one line of the api's logger.
type requestLine struct {
	Level         string `json:"level"`
	Msg           string `json:"msg"`
	Route         string `json:"route"`
	Method        string `json:"method"`
	Status        int    `json:"status"`
	CorrelationID string `json:"correlation_id"`
	Error         string `json:"error"`
}

func requestLines(t *testing.T, logs *syncBuffer) []requestLine {
	t.Helper()
	var out []requestLine
	for raw := range strings.Lines(logs.String()) {
		var l requestLine
		if err := json.Unmarshal([]byte(raw), &l); err != nil {
			t.Fatalf("log line %q: %v", raw, err)
		}
		out = append(out, l)
	}
	return out
}

// A 2xx and a 4xx are one debug line each and nothing at info or above; a
// 5xx is one error line (ADR-0009 §4). No line has the query string, an id
// outside the route template, a header value or the body.
func TestRequestLog_LevelsAndContent(t *testing.T) {
	id := uuid.NewString()
	const (
		canaryQuery   = "canary-query-value-55713"
		canaryHeader  = "canary-header-value-88123"
		canaryIdemKey = "canary-idempotency-61"
	)
	headers := map[string][]string{
		"X-Request-ID":    {"req-log-1"},
		"User-Agent":      {canaryHeader},
		"Idempotency-Key": {canaryIdemKey},
	}
	for _, tt := range []struct {
		name   string
		repo   *fakeRepo
		method string
		path   string
		body   string
		status int
		level  string
		route  string
	}{
		{"2xx create", &fakeRepo{}, http.MethodPost, "/v1/notifications", validBody, http.StatusAccepted, "DEBUG", "POST /v1/notifications"},
		{"4xx unknown id", &fakeRepo{}, http.MethodGet, "/v1/notifications/" + id, "", http.StatusNotFound, "DEBUG", "GET /v1/notifications/{id}"},
		{"4xx bad query", &fakeRepo{}, http.MethodGet, "/v1/notifications?status=" + canaryQuery + "&x=" + canaryQuery, "", http.StatusBadRequest, "DEBUG", "GET /v1/notifications"},
		{"4xx invalid body", &fakeRepo{}, http.MethodPost, "/v1/notifications", `{"channel":"sms","recipient":"+905551234567","content":"` + canaryQuery + `","priority":"urgent"}`, http.StatusUnprocessableEntity, "DEBUG", "POST /v1/notifications"},
		{"5xx", &fakeRepo{err: errors.New("database broke: " + canaryQuery)}, http.MethodGet, "/v1/notifications/" + id, "", http.StatusInternalServerError, "ERROR", "GET /v1/notifications/{id}"},
	} {
		c, logs := newFakeAPI(t, tt.repo)
		h := headers
		if tt.method == http.MethodGet {
			h = map[string][]string{"X-Request-ID": {"req-log-1"}, "User-Agent": {canaryHeader}}
		}
		res := c.do(tt.method, tt.path, tt.body, h)
		if res.status != tt.status {
			t.Fatalf("%s: status %d, want %d: %s", tt.name, res.status, tt.status, res.body)
		}
		var reqLines []requestLine
		for _, l := range requestLines(t, logs) {
			if l.Msg == "http request" {
				reqLines = append(reqLines, l)
				continue
			}
			// Other lines of the request (a created notification) are
			// debug lines.
			if l.Level != "DEBUG" {
				t.Errorf("%s: line %q at %s", tt.name, l.Msg, l.Level)
			}
		}
		if len(reqLines) != 1 {
			t.Fatalf("%s: %d request lines, want 1:\n%s", tt.name, len(reqLines), logs)
		}
		l := reqLines[0]
		if l.Level != tt.level || l.Route != tt.route || l.Method != tt.method || l.Status != tt.status || l.CorrelationID != "req-log-1" {
			t.Errorf("%s: line %+v, want %s %s %s %d with the correlation id", tt.name, l, tt.level, tt.method, tt.route, tt.status)
		}
		if tt.level == "ERROR" && l.Error == "" {
			t.Errorf("%s: the error line has no error class", tt.name)
		}
		out := logs.String()
		for _, secret := range []string{id, canaryQuery, canaryHeader, canaryIdemKey, "+905551234567", "hello", "?"} {
			if strings.Contains(out, secret) {
				t.Errorf("%s: the log contains %q:\n%s", tt.name, secret, out)
			}
		}
	}
}
