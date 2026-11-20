// Package health provides the admin endpoints (health, metrics, profiles) and a
// probe for the health endpoints.
package health

import (
	"context"
	"fmt"
	"net"
	"net/http"
	// The import also registers the profiles on http.DefaultServeMux; no
	// server of this module serves that mux.
	"net/http/pprof" //nolint:gosec // G108: served on the admin port only, by AdminHandler (D33)
	"time"
)

// Handler serves GET /healthz and GET /readyz without any dependency check:
// both report ok while the process serves. Any other path is answered with
// 404, any other method with 405.
func Handler() http.Handler {
	return healthMux(NewReadiness(0))
}

// AdminHandler is the handler of the admin port (ADR-0009 §8): GET /healthz
// (liveness: the process serves, no dependency is checked), GET /readyz
// (the checks of readiness, 503 when one fails) and GET /metrics, served by
// metrics, and the profiles of the Go runtime under GET /debug/pprof/. The
// profiles are registered on this handler only, never on a public port: they
// expose the process's internals and can be used to load it (D33).
func AdminHandler(metrics http.Handler, readiness *Readiness) http.Handler {
	mux := healthMux(readiness)
	mux.Handle("GET /metrics", metrics)
	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
	return mux
}

func healthMux(readiness *Readiness) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.Handle("GET /readyz", readiness)
	return mux
}

// Probe requests /readyz on the admin address (a listen address such as ":8081"
// or "0.0.0.0:8081") and returns nil only when the answer is 200.
func Probe(ctx context.Context, adminAddr string) error {
	host, port, err := net.SplitHostPort(adminAddr)
	if err != nil {
		return fmt.Errorf("invalid admin address: %w", err)
	}
	if ip := net.ParseIP(host); host == "" || (ip != nil && ip.IsUnspecified()) {
		host = "127.0.0.1"
	}
	url := "http://" + net.JoinHostPort(host, port) + "/readyz"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return err
	}
	client := &http.Client{
		Timeout: 2 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("readyz returned status %d", resp.StatusCode)
	}
	return nil
}
