package demo

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

const probeTimeout = 2 * time.Second

const quotaURLID = "43b89abc-51c2-438c-9847-e1e8ccc8e078"

// providerServer is a provider that counts every HTTP request it receives.
func providerServer(t *testing.T) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hits.Add(1) }))
	t.Cleanup(srv.Close)
	return srv, &hits
}

func TestProbeReachableHostPassesWithoutSendingARequest(t *testing.T) {
	srv, hits := providerServer(t)
	p := Prober{Timeout: probeTimeout}
	notes, err := p.Check(context.Background(), srv.URL+"/", 5)
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if len(notes) != 1 || !strings.Contains(notes[0], "reachable") {
		t.Fatalf("notes = %q", notes)
	}
	if n := hits.Load(); n != 0 {
		t.Fatalf("the provider received %d requests", n)
	}
}

func TestProbeUnreachableHostIsRefused(t *testing.T) {
	l, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close() // a closed port
	p := Prober{Timeout: probeTimeout}
	_, err = p.Check(context.Background(), "http://"+addr+"/", 5)
	if err == nil || !strings.Contains(err.Error(), "cannot be reached") || !strings.Contains(err.Error(), "nothing was sent") {
		t.Fatalf("err = %v", err)
	}
}

func TestProbeRejectsAURLThatIsNotHTTP(t *testing.T) {
	p := Prober{Timeout: probeTimeout}
	for _, u := range []string{"", "ftp://x/", "http:///path", "::bad"} {
		if _, err := p.Check(context.Background(), u, 1); err == nil {
			t.Errorf("Check(%q) = nil error", u)
		}
	}
}

// fakeWebhookAPI answers the two calls of the probe like webhook.site does
// and counts them.
type fakeWebhookAPI struct {
	tokenStatus int
	requests    string // body of the requests listing
	calls       atomic.Int64
}

func (f *fakeWebhookAPI) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /token/{id}", func(w http.ResponseWriter, _ *http.Request) {
		f.calls.Add(1)
		w.WriteHeader(f.tokenStatus)
		_, _ = w.Write([]byte(`{"uuid":"x"}`))
	})
	mux.HandleFunc("GET /token/{id}/requests", func(w http.ResponseWriter, r *http.Request) {
		f.calls.Add(1)
		if r.URL.Query().Get("per_page") != "1" {
			http.Error(w, "per_page", http.StatusBadRequest)
			return
		}
		_, _ = w.Write([]byte(f.requests))
	})
	return mux
}

// webhookProbe returns a prober whose connection check succeeds without
// touching the network and whose webhook API is a local fake.
func webhookProbe(t *testing.T, f *fakeWebhookAPI) Prober {
	t.Helper()
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)
	return Prober{
		Timeout: probeTimeout,
		Dial:    func(context.Context, string, bool) error { return nil },
		Webhook: HTTPWebhookAPI{BaseURL: srv.URL, Client: srv.Client()},
	}
}

func TestProbeWebhookWithRoomLeftGoesOn(t *testing.T) {
	f := &fakeWebhookAPI{tokenStatus: 200, requests: `{"total": 12, "data": []}`}
	notes, err := webhookProbe(t, f).Check(context.Background(), "https://webhook.site/"+quotaURLID, 5)
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if got := strings.Join(notes, "\n"); !strings.Contains(got, "38 requests left") {
		t.Fatalf("notes = %q", got)
	}
}

func TestProbeWebhookWithTooFewRequestsLeftIsRefused(t *testing.T) {
	f := &fakeWebhookAPI{tokenStatus: 200, requests: `{"total": 47}`}
	_, err := webhookProbe(t, f).Check(context.Background(), "https://webhook.site/"+quotaURLID, 5)
	if err == nil || !strings.Contains(err.Error(), "3 requests left") {
		t.Fatalf("err = %v", err)
	}
}

func TestProbeWebhookThatExpiredIsRefusedWithAdvice(t *testing.T) {
	f := &fakeWebhookAPI{tokenStatus: 404}
	_, err := webhookProbe(t, f).Check(context.Background(), "https://webhook.site/"+quotaURLID, 5)
	if err == nil || !strings.Contains(err.Error(), "expired") || !strings.Contains(err.Error(), "PROVIDER_URL") {
		t.Fatalf("err = %v", err)
	}
}

func TestProbeWebhookWithUnusableTotalIsUnknownAndGoesOn(t *testing.T) {
	for name, body := range map[string]string{
		"missing":    `{"data": []}`,
		"string":     `{"total": "12"}`,
		"negative":   `{"total": -3}`,
		"fractional": `{"total": 1.5}`,
		"not json":   `<html>`,
		"null":       `{"total": null}`,
	} {
		t.Run(name, func(t *testing.T) {
			f := &fakeWebhookAPI{tokenStatus: 200, requests: body}
			notes, err := webhookProbe(t, f).Check(context.Background(), "https://webhook.site/"+quotaURLID, 5)
			if err != nil {
				t.Fatalf("Check: %v", err)
			}
			if got := strings.Join(notes, "\n"); !strings.Contains(got, "unknown") {
				t.Fatalf("notes = %q", got)
			}
		})
	}
}

func TestProbeWebhookAPIErrorIsUnknownAndGoesOn(t *testing.T) {
	f := &fakeWebhookAPI{tokenStatus: 500}
	notes, err := webhookProbe(t, f).Check(context.Background(), "https://webhook.site/"+quotaURLID, 5)
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if got := strings.Join(notes, "\n"); !strings.Contains(got, "could not be checked") {
		t.Fatalf("notes = %q", got)
	}
}

func TestProbeWebhookTotalOfWholeFloatIsRead(t *testing.T) {
	f := &fakeWebhookAPI{tokenStatus: 200, requests: `{"total": 47.0}`}
	_, err := webhookProbe(t, f).Check(context.Background(), "https://webhook.site/"+quotaURLID, 5)
	if err == nil || !strings.Contains(err.Error(), "3 requests left") {
		t.Fatalf("err = %v", err)
	}
}

func TestProbeWebhookURLWithoutTokenSkipsTheQuotaCheck(t *testing.T) {
	f := &fakeWebhookAPI{tokenStatus: 200, requests: `{"total": 50}`}
	notes, err := webhookProbe(t, f).Check(context.Background(), "https://webhook.site/<your-uuid>", 5)
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if f.calls.Load() != 0 || !strings.Contains(strings.Join(notes, "\n"), "unknown") {
		t.Fatalf("calls = %d, notes = %q", f.calls.Load(), notes)
	}
}

func TestProbeOtherHostIsNeverAskedAboutAQuota(t *testing.T) {
	srv, hits := providerServer(t)
	f := &fakeWebhookAPI{tokenStatus: 404}
	p := webhookProbe(t, f)
	p.Dial = nil
	if _, err := p.Check(context.Background(), srv.URL+"/"+uuid.NewString(), 5); err != nil {
		t.Fatalf("Check: %v", err)
	}
	if f.calls.Load() != 0 || hits.Load() != 0 {
		t.Fatalf("webhook API calls = %d, provider requests = %d", f.calls.Load(), hits.Load())
	}
}
