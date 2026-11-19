//go:build integration

package integration

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/api"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
)

// A notification created from a template through the API reaches the
// provider with the rendered text as its content: the worker sends what was
// rendered at creation and stored.
func TestE2E_TemplateRenderedContentIsSent(t *testing.T) {
	e := newEnv(t)
	cfg := e.config(nil)
	e.startWorker(cfg, 0)
	e.startRelay(cfg)
	srv := httptest.NewServer(api.NewHandler(api.Options{
		Repository:   store.New(e.pool, store.ProfilesFrom(cfg.Retry)),
		DB:           e.pool,
		Logger:       e.log,
		Retry:        cfg.Retry,
		MaxBodyBytes: cfg.API.MaxBodyBytes,
	}))
	t.Cleanup(srv.Close)
	post := func(path, body string) (int, map[string]json.RawMessage) {
		t.Helper()
		ctx, cancel := e.ctx()
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		res, err := srv.Client().Do(req)
		if err != nil {
			t.Fatalf("POST %s: %v", path, err)
		}
		defer func() { _ = res.Body.Close() }()
		b, err := io.ReadAll(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]json.RawMessage
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatalf("POST %s: %s", path, b)
		}
		return res.StatusCode, m
	}

	if status, _ := post("/v1/templates", `{"name":"otp","channel":"sms","body":"Your code is {{ .code }}","variables":["code"]}`); status != http.StatusCreated {
		t.Fatalf("create template: %d", status)
	}
	status, body := post("/v1/notifications",
		`{"channel":"sms","recipient":"`+testRecipient+`","template":{"name":"otp","variables":{"code":482913}}}`)
	if status != http.StatusAccepted {
		t.Fatalf("create notification: %d", status)
	}
	var id uuid.UUID
	if err := json.Unmarshal(body["id"], &id); err != nil {
		t.Fatal(err)
	}
	e.waitStatus(id, domain.StatusSent)

	reqs := e.prov.requests()
	if len(reqs) != 1 {
		t.Fatalf("provider received %d requests, want 1", len(reqs))
	}
	var content string
	if err := json.Unmarshal(reqs[0].body["content"], &content); err != nil {
		t.Fatal(err)
	}
	// Compared without printing: the test fails without echoing the value.
	if content != "Your code is 482913" {
		t.Errorf("the content sent is not the rendered text (length %d)", len(content))
	}
	n := e.get(id)
	if n.TemplateName == nil || *n.TemplateName != "otp" || n.TemplateVersion == nil || *n.TemplateVersion != 1 {
		t.Errorf("provenance %v %v", n.TemplateName, n.TemplateVersion)
	}
	srv.Client().CloseIdleConnections()
}
