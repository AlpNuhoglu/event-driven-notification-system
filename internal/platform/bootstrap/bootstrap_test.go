package bootstrap

import (
	"bytes"
	"context"
	"errors"
	"io"
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
	if !strings.Contains(out, `"service":"api","version":"dev"`) {
		t.Errorf("missing service and version fields: %q", out)
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

// A role's operator subcommand runs instead of the role: its arguments are
// parsed first (a usage error exits 2 before the configuration is loaded),
// then it runs with the loaded configuration and its exit code is returned.
// The usage text lists it next to healthcheck.
func TestRunCommands(t *testing.T) {
	var gotArgs []string
	var gotTick string
	cmd := Command{
		Name:  "fix",
		Usage: "fix [--all]",
		Parse: func(args []string) (Action, error) {
			gotArgs = args
			if len(args) > 0 && args[0] == "--bad" {
				return nil, errors.New("flag provided but not defined: -bad")
			}
			return func(_ context.Context, cfg config.Config, stdout, _ io.Writer) int {
				gotTick = cfg.Relay.TickInterval.String()
				_, _ = io.WriteString(stdout, "fixed\n")
				return 3
			}, nil
		},
	}
	env := lookupWith(map[string]string{"RELAY_TICK_INTERVAL": "7ms"})

	var so, se bytes.Buffer
	if code := RunCommands(context.Background(), "relay", []string{"fix", "--all"}, env, &so, &se, nil, cmd); code != 3 {
		t.Errorf("code = %d, want the command's 3; stderr %q", code, se.String())
	}
	if len(gotArgs) != 1 || gotArgs[0] != "--all" || gotTick != "7ms" || so.String() != "fixed\n" {
		t.Errorf("args %v, tick %q, stdout %q", gotArgs, gotTick, so.String())
	}

	// A usage error comes before the configuration, even an invalid one.
	so.Reset()
	se.Reset()
	bad := lookupWith(map[string]string{"RELAY_TICK_INTERVAL": "banana"})
	if code := RunCommands(context.Background(), "relay", []string{"fix", "--bad"}, bad, &so, &se, nil, cmd); code != 2 {
		t.Errorf("code = %d, want 2", code)
	}
	want := "fix: flag provided but not defined: -bad\nusage: relay [healthcheck | fix [--all]]\n"
	if se.String() != want {
		t.Errorf("stderr = %q, want %q", se.String(), want)
	}
	// A valid command with an invalid configuration fails with 1.
	se.Reset()
	if code := RunCommands(context.Background(), "relay", []string{"fix"}, bad, &so, &se, nil, cmd); code != 1 {
		t.Errorf("code = %d, want 1", code)
	}

	se.Reset()
	if code := RunCommands(context.Background(), "relay", []string{"bogus"}, env, &so, &se, nil, cmd); code != 2 {
		t.Errorf("code = %d, want 2", code)
	}
	if se.String() != "usage: relay [healthcheck | fix [--all]]\n" {
		t.Errorf("stderr = %q", se.String())
	}
}

// The role's logger is the central handler at the level of LOG_LEVEL: a
// debug line is written only with LOG_LEVEL=debug, and every line carries
// service and version.
func TestRunService_LoggerFollowsLogLevel(t *testing.T) {
	for _, tt := range []struct {
		level     string
		wantDebug bool
	}{{"debug", true}, {"info", false}, {"", false}} {
		var so, se bytes.Buffer
		env := lookupWith(map[string]string{"ADMIN_ADDR": "127.0.0.1:0", "LOG_LEVEL": tt.level})
		svc := func(_ context.Context, _ config.Config, logger *slog.Logger) error {
			logger.Debug("debug line")
			logger.Info("info line")
			return nil
		}
		if code := RunService(context.Background(), "worker", nil, env, &so, &se, svc); code != 0 {
			t.Fatalf("LOG_LEVEL=%q: exit code %d, stderr %q", tt.level, code, se.String())
		}
		out := so.String()
		if got := strings.Contains(out, `"msg":"debug line"`); got != tt.wantDebug {
			t.Errorf("LOG_LEVEL=%q: debug line written %v, want %v: %s", tt.level, got, tt.wantDebug, out)
		}
		for line := range strings.Lines(strings.TrimSpace(out)) {
			if !strings.Contains(line, `"service":"worker","version":"dev"`) {
				t.Errorf("LOG_LEVEL=%q: line without service and version: %s", tt.level, line)
			}
		}
	}
}
