package health

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandler(t *testing.T) {
	srv := httptest.NewServer(Handler())
	defer srv.Close()

	tests := []struct {
		method, path string
		status       int
	}{
		{http.MethodGet, "/healthz", 200},
		{http.MethodGet, "/readyz", 200},
		{http.MethodGet, "/other", 404},
		{http.MethodPost, "/healthz", 405},
	}
	for _, tt := range tests {
		req, err := http.NewRequestWithContext(context.Background(), tt.method, srv.URL+tt.path, http.NoBody)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != tt.status {
			t.Errorf("%s %s = %d, want %d", tt.method, tt.path, resp.StatusCode, tt.status)
		}
		if tt.status == 200 {
			if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
				t.Errorf("%s content type = %q", tt.path, ct)
			}
		}
	}
}

func port(t *testing.T, srv *httptest.Server) string {
	t.Helper()
	_, p, err := net.SplitHostPort(strings.TrimPrefix(srv.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestProbeAddressForms(t *testing.T) {
	srv := httptest.NewServer(Handler())
	defer srv.Close()
	p := port(t, srv)

	for _, addr := range []string{":" + p, "0.0.0.0:" + p, "127.0.0.1:" + p} {
		if err := Probe(context.Background(), addr); err != nil {
			t.Errorf("Probe(%q) = %v", addr, err)
		}
	}
}

func TestProbeUnhealthy(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	if err := Probe(context.Background(), "127.0.0.1:"+port(t, srv)); err == nil {
		t.Error("expected error for 503")
	}
}

func TestProbeRedirectNotFollowed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/healthz", http.StatusFound)
	}))
	defer srv.Close()
	if err := Probe(context.Background(), "127.0.0.1:"+port(t, srv)); err == nil {
		t.Error("expected error for redirect")
	}
}

func TestProbeNothingListening(t *testing.T) {
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	if err := Probe(context.Background(), addr); err == nil {
		t.Error("expected error when nothing listens")
	}
}

func TestProbeInvalidAddress(t *testing.T) {
	if err := Probe(context.Background(), "nonsense"); err == nil {
		t.Error("expected error for invalid address")
	}
}
