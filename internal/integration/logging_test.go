//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/api"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/config"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/logging"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/queue"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/relay"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/worker"
)

// roleLogs is the output of one role's central log handler.
type roleLogs struct {
	role string
	buf  *lockedBuffer
	log  *slog.Logger
}

// wiredRoles runs the api (an httptest server), the relay and the worker,
// each with the central log handler at level, as bootstrap builds it, on
// cfg. The roles' logs are printed when the test fails.
type wiredRoles struct {
	e                *env
	api, relay, work roleLogs
	srv              *httptest.Server
}

func newRoleLogs(t *testing.T, role string, level slog.Level) roleLogs {
	t.Helper()
	buf := &lockedBuffer{}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("%s logs:\n%s", role, buf.String())
		}
	})
	return roleLogs{role: role, buf: buf, log: logging.New(buf, logging.Options{Service: role, Level: level})}
}

func (e *env) startWired(cfg config.Config, level slog.Level) *wiredRoles {
	e.t.Helper()
	w := &wiredRoles{
		e:     e,
		api:   newRoleLogs(e.t, "api", level),
		relay: newRoleLogs(e.t, "relay", level),
		work:  newRoleLogs(e.t, "worker", level),
	}
	conn, err := queue.Dial(e.amqpURL, 20*time.Millisecond, 1000, w.work.log)
	if err != nil {
		e.t.Fatalf("dial: %v", err)
	}
	e.t.Cleanup(func() { _ = conn.Close() })
	role, err := worker.NewRole(cfg, worker.Resources{
		Pool: e.rolePool(cfg), Conn: conn, Redis: e.rdb, Registry: prometheus.NewRegistry(),
	}, w.work.log)
	if err != nil {
		e.t.Fatalf("worker role: %v", err)
	}
	start(e.t, role.Run)
	start(e.t, func(ctx context.Context) error { return relay.Service(ctx, cfg, w.relay.log) })
	w.srv = httptest.NewServer(api.NewHandler(api.Options{
		Repository:   store.New(e.pool, store.ProfilesFrom(cfg.Retry)),
		DB:           e.pool,
		Logger:       w.api.log,
		Retry:        cfg.Retry,
		MaxBodyBytes: cfg.API.MaxBodyBytes,
	}))
	e.t.Cleanup(w.srv.Close)
	return w
}

// call sends a request to the api with X-Request-ID requestID and returns
// the status and the body.
func (w *wiredRoles) call(method, path, body, requestID string) (int, map[string]json.RawMessage) {
	t := w.e.t
	t.Helper()
	ctx, cancel := w.e.ctx()
	defer cancel()
	var rd io.Reader = http.NoBody
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, w.srv.URL+path, rd)
	if err != nil {
		t.Fatal(err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("X-Request-ID", requestID)
	res, err := w.srv.Client().Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer func() { _ = res.Body.Close() }()
	b, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]json.RawMessage
	_ = json.Unmarshal(b, &m)
	return res.StatusCode, m
}

// create posts a notification and returns its id.
func (w *wiredRoles) create(body, requestID string) uuid.UUID {
	w.e.t.Helper()
	status, m := w.call(http.MethodPost, "/v1/notifications", body, requestID)
	if status != http.StatusAccepted {
		w.e.t.Fatalf("create (%s): status %d", requestID, status)
	}
	var id uuid.UUID
	if err := json.Unmarshal(m["id"], &id); err != nil {
		w.e.t.Fatal(err)
	}
	return id
}

// wiredLine is the part of a line of the central handler these tests read.
type wiredLine struct {
	Level          string `json:"level"`
	Msg            string `json:"msg"`
	Service        string `json:"service"`
	NotificationID string `json:"notification_id"`
	CorrelationID  string `json:"correlation_id"`
}

func (r roleLogs) lines(t *testing.T) []wiredLine {
	t.Helper()
	var out []wiredLine
	for raw := range strings.Lines(r.buf.String()) {
		var l wiredLine
		if err := json.Unmarshal([]byte(raw), &l); err != nil {
			t.Fatalf("%s: not a JSON line %q: %v", r.role, raw, err)
		}
		if l.Service != r.role {
			t.Errorf("%s: line with service %q: %s", r.role, l.Service, raw)
		}
		out = append(out, l)
	}
	return out
}

// echoProvider answers 202, except for the recipient fail, which it answers
// 400 with a body that quotes the request: the recipient and the content.
func echoProvider(t *testing.T, fail string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p struct{ To, Content string }
		_ = json.NewDecoder(r.Body).Decode(&p)
		w.Header().Set("Content-Type", "application/json")
		if p.To == fail {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = fmt.Fprintf(w, `{"error":"invalid recipient %s","echo":%q}`, p.To, p.Content)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = fmt.Fprintf(w, `{"messageId":%q,"status":"accepted","timestamp":"2026-01-01T00:00:00Z"}`, uuid.NewString())
	}))
	t.Cleanup(srv.Close)
	return srv
}

// Every role's real code paths, logging at debug level through the central
// handler: a create, rejected requests whose validation errors name fields,
// a send, a failure whose provider body echoes the recipient and the
// content, a cancel, and a template with a variable. No line of any role
// carries a recipient, a content or a variable value, at any level, and
// every line about a notification carries the correlation id of the request
// that created it (ADR-0009 §4, §5).
func TestLogs_NoPersonalDataAndCorrelationIDsInEveryRole(t *testing.T) {
	e := newEnv(t)
	tag := uuid.NewString()[:8]
	sent := domain.Recipient("canary-sent-" + tag + "@example.com")
	failing := domain.Recipient("canary-fail-" + tag + "@example.com")
	cancelled := domain.Recipient("canary-cancel-" + tag + "@example.com")
	content := "canary content " + tag + " <b>\"quoted\"</b>"
	variable := "canary-variable-" + tag
	prov := echoProvider(t, failing.Reveal())
	cfg := e.config(map[string]string{"PROVIDER_URL": prov.URL})
	w := e.startWired(cfg, slog.LevelDebug)

	emailBody := func(r domain.Recipient, extra string) string {
		c, _ := json.Marshal(content)
		return `{"channel":"email","recipient":"` + r.Reveal() + `","content":` + string(c) + extra + `}`
	}
	want := map[uuid.UUID]string{} // notification id -> correlation id
	sentID := w.create(emailBody(sent, ""), "corr-sent-"+tag)
	want[sentID] = "corr-sent-" + tag
	failID := w.create(emailBody(failing, ""), "corr-fail-"+tag)
	want[failID] = "corr-fail-" + tag
	later := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	cancelID := w.create(emailBody(cancelled, `,"scheduled_at":"`+later+`"`), "corr-cancel-"+tag)
	want[cancelID] = "corr-cancel-" + tag

	if status, _ := w.call(http.MethodPost, "/v1/templates",
		`{"name":"canary-`+tag+`","channel":"sms","body":"Code {{ .code }}","variables":["code"]}`, "corr-tpl-"+tag); status != http.StatusCreated {
		t.Fatalf("create template: %d", status)
	}
	tplID := w.create(`{"channel":"sms","recipient":"`+testRecipient+`","template":{"name":"canary-`+tag+`","variables":{"code":"`+variable+`"}}}`,
		"corr-tplsend-"+tag)
	want[tplID] = "corr-tplsend-" + tag

	// Rejected: an invalid priority (422, a field error), a recipient that
	// is not an e-mail address, and a body that is not JSON (400).
	for i, body := range []string{
		emailBody(sent, `,"priority":"`+variable+`"`),
		`{"channel":"email","recipient":"` + variable + `","content":"` + tag + `"}`,
		`{"channel":"email","recipient":"` + sent.Reveal() + `","content":"` + content,
	} {
		if status, _ := w.call(http.MethodPost, "/v1/notifications", body, fmt.Sprintf("corr-reject-%d-%s", i, tag)); status/100 != 4 {
			t.Errorf("rejected request %d: status %d", i, status)
		}
	}
	if status, _ := w.call(http.MethodPost, "/v1/notifications/"+cancelID.String()+"/cancel", "", "corr-cancel-"+tag); status != http.StatusOK {
		t.Fatalf("cancel: %d", status)
	}

	e.waitStatus(sentID, domain.StatusSent)
	e.waitStatus(tplID, domain.StatusSent)
	e.waitStatus(failID, domain.StatusFailed)
	for _, a := range e.attempts(failID) {
		if a.Error != nil && (strings.Contains(*a.Error, failing.Reveal()) || strings.Contains(*a.Error, tag)) {
			t.Errorf("the stored provider error carries the recipient or the content: %q", *a.Error)
		}
	}

	// The tag is in the correlation ids; the values that carry it are not.
	secrets := []string{
		sent.Reveal(), failing.Reveal(), cancelled.Reveal(), content, variable, testRecipient,
		"canary content", "canary-sent", "canary-fail", "canary-cancel",
	}
	if b, err := json.Marshal(content); err == nil {
		secrets = append(secrets, strings.Trim(string(b), `"`))
	}
	seen := map[string]map[uuid.UUID]bool{}
	for _, r := range []roleLogs{w.api, w.relay, w.work} {
		out := r.buf.String()
		for _, s := range secrets {
			if strings.Contains(out, s) {
				t.Errorf("%s: the log contains %q", r.role, s)
			}
		}
		seen[r.role] = map[uuid.UUID]bool{}
		for _, l := range r.lines(t) {
			id, err := uuid.Parse(l.NotificationID)
			if err != nil {
				continue
			}
			if l.CorrelationID != want[id] {
				t.Errorf("%s: %q of %s has correlation_id %q, want %q", r.role, l.Msg, id, l.CorrelationID, want[id])
			}
			seen[r.role][id] = true
		}
	}
	// Each role logged the notifications it handled.
	for role, ids := range map[string][]uuid.UUID{
		"api":    {sentID, failID, cancelID, tplID},
		"relay":  {sentID, failID, tplID},
		"worker": {sentID, failID, tplID},
	} {
		for _, id := range ids {
			if !seen[role][id] {
				t.Errorf("%s: no line for notification %s", role, id)
			}
		}
	}
}

// At info level, a notification sent through the wired roles is one line,
// the final outcome: no line for its intermediate transitions (created,
// queued, processing) and no request line (ADR-0009 §4).
func TestLogs_InfoLevelShowsOnlyTheFinalOutcome(t *testing.T) {
	e := newEnv(t)
	cfg := e.config(nil)
	w := e.startWired(cfg, slog.LevelInfo)
	id := w.create(`{"channel":"sms","recipient":"`+testRecipient+`","content":"`+testContent+`"}`, "corr-info")
	e.waitStatus(id, domain.StatusSent)
	// The worker logs the outcome after the final write commits.
	var lines []wiredLine
	testutil.Eventually(t, func() bool {
		lines = nil
		for _, r := range []roleLogs{w.api, w.relay, w.work} {
			for _, l := range r.lines(t) {
				if l.NotificationID == id.String() || l.CorrelationID == "corr-info" {
					lines = append(lines, l)
				}
			}
		}
		return len(lines) > 0
	}, waitTimeout)
	if len(lines) != 1 || lines[0].Msg != "notification sent" || lines[0].Level != "INFO" ||
		lines[0].Service != "worker" || lines[0].CorrelationID != "corr-info" {
		t.Errorf("lines of the notification at info level: %+v, want one: notification sent", lines)
	}
}
