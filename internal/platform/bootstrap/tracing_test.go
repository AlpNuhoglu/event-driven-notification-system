package bootstrap

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/config"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/tracing"
)

// silentEndpoint accepts TCP connections and never answers, like a Jaeger
// that hangs: an export to it blocks until its context ends.
func silentEndpoint(t *testing.T) string {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			_ = c.Close()
		}
	})
	return "http://" + ln.Addr().String()
}

// A role starts while its OTLP endpoint does not answer, records spans, and
// stops within TRACING_SHUTDOWN_TIMEOUT although its buffered spans cannot
// be exported; the exit code stays 0.
func TestService_StartsAndStopsWithUnreachableTracing(t *testing.T) {
	const bound = 200 * time.Millisecond
	cfg, err := config.LoadFrom(lookupWith(map[string]string{
		"OTEL_EXPORTER_OTLP_ENDPOINT": silentEndpoint(t),
		"OTEL_TRACES_SAMPLER":         tracing.SamplerParentBasedAlwaysOn,
		"TRACING_SHUTDOWN_TIMEOUT":    bound.String(),
	}))
	if err != nil {
		t.Fatal(err)
	}
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// The exporter can still report its failure after the role has returned.
	stdout := &lockedWriter{}
	var stderr bytes.Buffer
	started := make(chan bool, 1)
	done := make(chan int, 1)
	go func() {
		done <- serveService(ctx, ln, "worker", cfg, func(ctx context.Context, _ config.Config, _ *slog.Logger) error {
			_, span := tracing.Tracer(nil, "internal/test").Start(ctx, "work")
			span.End()
			started <- span.SpanContext().IsSampled()
			<-ctx.Done()
			return nil
		}, stdout, &stderr)
	}()
	if sampled := <-started; !sampled {
		t.Error("the role's spans are not recorded")
	}

	stop := time.Now()
	cancel()
	code := <-done
	took := time.Since(stop)
	if code != 0 {
		t.Errorf("exit code %d, stderr %q", code, stderr.String())
	}
	// The bound plus the admin server's shutdown and scheduling slack.
	if took > bound+time.Second {
		t.Errorf("stop took %s, tracing bound %s", took, bound)
	}
	if !strings.Contains(stdout.String(), "tracing: buffered spans not exported at stop") {
		t.Errorf("the stop did not report the lost spans: %q", stdout.String())
	}
}

// lockedWriter is a buffer safe for concurrent writers and readers.
type lockedWriter struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p) //nolint:wrapcheck // bytes.Buffer never fails
}

func (w *lockedWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

// An invalid tracing configuration stops the role at start.
func TestService_InvalidTracingConfiguration(t *testing.T) {
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{Observability: config.Observability{OTLPEndpoint: "http://jaeger:4317", TracesSampler: "nope"}}
	var stdout, stderr bytes.Buffer
	ran := false
	code := serveService(context.Background(), ln, "api", cfg, func(context.Context, config.Config, *slog.Logger) error {
		ran = true
		return nil
	}, &stdout, &stderr)
	if code != 1 || ran || !strings.Contains(stderr.String(), "OTEL_TRACES_SAMPLER") {
		t.Errorf("code %d, service ran %v, stderr %q", code, ran, stderr.String())
	}
}
