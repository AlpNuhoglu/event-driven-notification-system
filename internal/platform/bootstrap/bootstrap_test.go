package bootstrap

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
	"testing"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/config"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/health"
)

func lookupWith(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}

func TestServeAndShutdown(t *testing.T) {
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var stdout, stderr bytes.Buffer
	done := make(chan int, 1)
	go func() { done <- serve(ctx, ln, "api", &stdout, &stderr) }()

	if err := health.Probe(ctx, ln.Addr().String()); err != nil {
		t.Fatalf("probe: %v", err)
	}
	cancel()
	if code := <-done; code != 0 {
		t.Fatalf("exit code = %d, stderr=%q", code, stderr.String())
	}
	out := stdout.String()
	if !strings.Contains(out, `"service started"`) || !strings.Contains(out, `"service stopped"`) {
		t.Errorf("unexpected log output: %q", out)
	}
	if !strings.Contains(out, `"role":"api"`) {
		t.Errorf("missing role attribute: %q", out)
	}
}

func TestHealthcheck(t *testing.T) {
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var out, errb bytes.Buffer
	done := make(chan int, 1)
	go func() { done <- serve(ctx, ln, "api", &out, &errb) }()

	env := lookupWith(map[string]string{"ADMIN_ADDR": ln.Addr().String()})
	var so, se bytes.Buffer
	if code := Run(ctx, "api", []string{"healthcheck"}, env, &so, &se); code != 0 {
		t.Errorf("healthcheck code = %d, stderr=%q", code, se.String())
	}
	cancel()
	<-done

	// The listener is now closed: nothing listens.
	se.Reset()
	if code := Run(context.Background(), "api", []string{"healthcheck"}, env, &so, &se); code != 1 {
		t.Errorf("healthcheck code = %d, want 1", code)
	}
	if !strings.HasPrefix(se.String(), "healthcheck: ") {
		t.Errorf("stderr = %q", se.String())
	}
}

func TestUnknownArgument(t *testing.T) {
	var so, se bytes.Buffer
	if code := Run(context.Background(), "api", []string{"bogus"}, lookupWith(nil), &so, &se); code != 2 {
		t.Errorf("code = %d, want 2", code)
	}
	if got := se.String(); got != "usage: api [healthcheck]\n" {
		t.Errorf("stderr = %q", got)
	}
}

func TestInvalidConfig(t *testing.T) {
	var so, se bytes.Buffer
	env := lookupWith(map[string]string{"RELAY_TICK_INTERVAL": "banana"})
	if code := Run(context.Background(), "api", nil, env, &so, &se); code != 1 {
		t.Errorf("code = %d, want 1", code)
	}
	if !strings.Contains(se.String(), "RELAY_TICK_INTERVAL") {
		t.Errorf("stderr = %q", se.String())
	}
	if strings.Contains(se.String(), "banana") {
		t.Errorf("stderr leaks value: %q", se.String())
	}
}

func runService(t *testing.T, svc Service) (cancel context.CancelFunc, done <-chan int, stderr *bytes.Buffer) {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	var stdout bytes.Buffer
	stderr = &bytes.Buffer{}
	ch := make(chan int, 1)
	go func() { ch <- serveService(ctx, ln, "relay", config.Config{}, svc, &stdout, stderr) }()
	return cancel, ch, stderr
}

// A service runs until the context is cancelled; its in-flight work is
// awaited before the process ends, and the exit code is 0.
func TestService_CleanStop(t *testing.T) {
	started, finish := make(chan struct{}), make(chan struct{})
	cancel, done, stderr := runService(t, func(ctx context.Context, _ config.Config, _ *slog.Logger) error {
		close(started)
		<-ctx.Done()
		<-finish // in-flight work
		return ctx.Err()
	})
	<-started
	cancel()
	select {
	case code := <-done:
		t.Fatalf("returned %d before the service finished", code)
	default:
	}
	close(finish)
	if code := <-done; code != 0 {
		t.Errorf("exit code = %d, stderr=%q", code, stderr.String())
	}
}

// A service that fails on its own (a dependency down at start, recovery that
// gave up) ends the process with a non-zero code.
func TestService_ErrorExitsNonZero(t *testing.T) {
	_, done, stderr := runService(t, func(context.Context, config.Config, *slog.Logger) error {
		return errors.New("amqp: recovery gave up")
	})
	if code := <-done; code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "recovery gave up") {
		t.Errorf("stderr = %q", stderr.String())
	}
}

// A service that fails while stopping also yields a non-zero code.
func TestService_ErrorDuringStop(t *testing.T) {
	started := make(chan struct{})
	cancel, done, _ := runService(t, func(ctx context.Context, _ config.Config, _ *slog.Logger) error {
		close(started)
		<-ctx.Done()
		return errors.New("drain timed out")
	})
	<-started
	cancel()
	if code := <-done; code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
}
