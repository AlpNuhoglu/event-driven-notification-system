package bootstrap

import (
	"bytes"
	"context"
	"net"
	"strings"
	"testing"

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

func TestRunOneShot(t *testing.T) {
	var so, se bytes.Buffer
	if code := RunOneShot("migrate", nil, &so, &se); code != 0 || so.String() != "migrate: no migrations yet\n" {
		t.Errorf("code=%d stdout=%q", code, so.String())
	}
	so.Reset()
	if code := RunOneShot("migrate", []string{"healthcheck"}, &so, &se); code != 1 {
		t.Errorf("code = %d, want 1", code)
	}
	if want := "healthcheck: migrate is a one-shot command and has no health endpoint\n"; se.String() != want {
		t.Errorf("stderr = %q", se.String())
	}
	se.Reset()
	if code := RunOneShot("migrate", []string{"x"}, &so, &se); code != 2 {
		t.Errorf("code = %d, want 2", code)
	}
}
