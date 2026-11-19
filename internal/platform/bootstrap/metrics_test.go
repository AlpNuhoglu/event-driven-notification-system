package bootstrap

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/config"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/ratelimit"
)

// noRedis is a primary limiter that never answers, as an unreachable Redis.
type noRedis struct{}

func (noRedis) Reserve(context.Context, domain.Channel) (time.Duration, bool, error) {
	return 0, false, io.ErrUnexpectedEOF
}

func get(t *testing.T, method, url string) (int, string, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, url, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, resp.Header.Get("Content-Type"), string(b)
}

// The admin port serves /metrics over the default gatherer, with the metrics
// a role registers there (here the worker's limiter_fallback_active), next
// to the unchanged health endpoints (ADR-0009 §8).
func TestAdminPortServesMetrics(t *testing.T) {
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	registered := make(chan *ratelimit.Fallback, 1)
	svc := func(ctx context.Context, _ config.Config, logger *slog.Logger) error {
		f, err := ratelimit.NewFallback(noRedis{}, ratelimit.FallbackConfig{
			PerSecond: 98, Replicas: 1, Horizon: 250 * time.Millisecond,
			Timeout: 250 * time.Millisecond, ProbeInterval: time.Second,
			Registry: prometheus.DefaultRegisterer, Logger: logger,
		})
		if err != nil {
			return err
		}
		registered <- f
		<-ctx.Done()
		return nil
	}
	var stdout, stderr bytes.Buffer
	done := make(chan int, 1)
	go func() { done <- serveService(ctx, ln, "worker", config.Config{}, svc, &stdout, &stderr) }()
	f := <-registered
	base := "http://" + ln.Addr().String()

	code, ctype, body := get(t, http.MethodGet, base+"/metrics")
	if code != http.StatusOK || !strings.HasPrefix(ctype, "text/plain") {
		t.Fatalf("GET /metrics: status %d, content type %q", code, ctype)
	}
	if !strings.Contains(body, "\nlimiter_fallback_active 0\n") || !strings.Contains(body, "go_goroutines") {
		t.Errorf("GET /metrics lacks limiter_fallback_active 0 or the Go metrics:\n%s", body)
	}

	if _, _, err := f.Reserve(t.Context(), domain.ChannelSMS); err != nil {
		t.Fatal(err)
	}
	if _, _, body := get(t, http.MethodGet, base+"/metrics"); !strings.Contains(body, "\nlimiter_fallback_active 1\n") {
		t.Errorf("GET /metrics after the switch lacks limiter_fallback_active 1:\n%s", body)
	}

	// Without dependency checks both answer ok; /readyz lists its checks.
	for path, want := range map[string]string{"/healthz": `{"status":"ok"}`, "/readyz": `{"status":"ok","checks":[]}`} {
		code, ctype, body := get(t, http.MethodGet, base+path)
		if code != http.StatusOK || ctype != "application/json" || strings.TrimSpace(body) != want {
			t.Errorf("GET %s: status %d, content type %q, body %q", path, code, ctype, body)
		}
	}
	if code, _, _ := get(t, http.MethodPost, base+"/metrics"); code != http.StatusMethodNotAllowed {
		t.Errorf("POST /metrics: status %d, want 405", code)
	}
	if code, _, _ := get(t, http.MethodGet, base+"/debug"); code != http.StatusNotFound {
		t.Errorf("GET /debug: status %d, want 404", code)
	}

	cancel()
	if code := <-done; code != 0 {
		t.Errorf("exit code %d, stderr %q", code, stderr.String())
	}
}
