// Package bootstrap is the shared runner of the long-running roles. It loads
// the configuration, serves the admin endpoints (health and metrics),
// supports the healthcheck subcommand and a role's own operator
// subcommands, and runs the role's own work next to them.
package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/config"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/health"
)

// Service is the work a role runs next to its admin health server. It runs
// until ctx is cancelled, finishes its in-flight work and returns nil. A
// non-nil error (a dependency that cannot be reached at start, a broker
// connection whose recovery gave up) ends the process with a non-zero code,
// so that the container runtime restarts it.
type Service func(ctx context.Context, cfg config.Config, logger *slog.Logger) error

// Run executes a long-running role that has no work of its own yet and
// returns the process exit code. With no arguments it serves the health
// endpoints on ADMIN_ADDR until ctx is done; the argument "healthcheck" probes
// a running instance instead.
func Run(ctx context.Context, role string, args []string, lookup func(string) (string, bool), stdout, stderr io.Writer) int {
	return RunService(ctx, role, args, lookup, stdout, stderr, nil)
}

// RunService is Run for a role with work of its own: svc runs next to the
// health endpoints. The exit code is non-zero when svc returns an error.
func RunService(ctx context.Context, role string, args []string, lookup func(string) (string, bool), stdout, stderr io.Writer, svc Service) int {
	return RunCommands(ctx, role, args, lookup, stdout, stderr, svc)
}

// Command is an operator subcommand of a role's binary (the relay's
// requeue-stale, D64). It runs instead of the role: no admin port is served
// and the role's work does not start, so it can run next to a live instance.
type Command struct {
	// Name is the first argument, which selects the command.
	Name string
	// Usage is the command's synopsis after the binary's name, for example
	// "requeue-stale [--older-than=5m] [--all]".
	Usage string
	// Parse parses the arguments after the name. An error is a usage error:
	// it is printed with the usage and the exit code is 2, before the
	// configuration is loaded.
	Parse func(args []string) (Action, error)
}

// Action runs a parsed command with the loaded configuration and returns
// the process exit code.
type Action func(ctx context.Context, cfg config.Config, stdout, stderr io.Writer) int

// RunCommands is RunService for a role whose binary also carries operator
// subcommands: a first argument naming one of cmds runs that command instead
// of the role.
func RunCommands(ctx context.Context, role string, args []string, lookup func(string) (string, bool), stdout, stderr io.Writer, svc Service, cmds ...Command) int {
	var action Action
	if len(args) > 0 && args[0] != "healthcheck" {
		i := slices.IndexFunc(cmds, func(c Command) bool { return c.Name == args[0] })
		if i < 0 {
			return usage(role, stderr, cmds)
		}
		a, err := cmds[i].Parse(args[1:])
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "%s: %v\n", cmds[i].Name, err)
			return usage(role, stderr, cmds)
		}
		action = a
	}
	cfg, err := config.LoadFrom(lookup)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 1
	}
	if action != nil {
		return action(ctx, cfg, stdout, stderr)
	}
	if len(args) > 0 {
		if err := health.Probe(ctx, cfg.Ports.AdminAddr); err != nil {
			_, _ = fmt.Fprintf(stderr, "healthcheck: %v\n", err)
			return 1
		}
		return 0
	}
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.Ports.AdminAddr)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "listen: %v\n", err)
		return 1
	}
	return serveService(ctx, ln, role, cfg, svc, stdout, stderr)
}

func usage(role string, stderr io.Writer, cmds []Command) int {
	forms := []string{"healthcheck"}
	for _, c := range cmds {
		forms = append(forms, c.Usage)
	}
	_, _ = fmt.Fprintf(stderr, "usage: %s [%s]\n", role, strings.Join(forms, " | "))
	return 2
}

// serve serves the health handler on ln until ctx is done, then shuts down.
func serve(ctx context.Context, ln net.Listener, role string, stdout, stderr io.Writer) int {
	return serveService(ctx, ln, role, config.Config{}, nil, stdout, stderr)
}

// serveService serves the health handler on ln and runs svc, if any, until
// ctx is done or one of them ends on its own; then it stops both.
func serveService(ctx context.Context, ln net.Listener, role string, cfg config.Config, svc Service, stdout, stderr io.Writer) int {
	logger := slog.New(slog.NewJSONHandler(stdout, nil)).With("role", role)
	srv := &http.Server{
		Handler:           AdminHandler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()

	svcCtx, stopSvc := context.WithCancel(ctx)
	defer stopSvc()
	var svcDone chan error // nil without a service: never ready in the select
	if svc != nil {
		svcDone = make(chan error, 1)
		go func() { svcDone <- svc(svcCtx, cfg, logger) }()
	}
	logger.Info("service started", "admin_addr", ln.Addr().String())

	code := 0
	svcEnded := svc == nil
	select {
	case err := <-errc:
		_, _ = fmt.Fprintf(stderr, "serve: %v\n", err)
		code = 1
	case err := <-svcDone:
		svcEnded = true
		code = serviceExit(err, stderr)
	case <-ctx.Done():
	}

	// Stop the service first and wait for its in-flight work.
	stopSvc()
	if !svcEnded {
		if c := serviceExit(<-svcDone, stderr); c != 0 {
			code = c
		}
	}

	shutCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		_, _ = fmt.Fprintf(stderr, "shutdown: %v\n", err)
		code = 1
	}
	logger.Info("service stopped")
	return code
}

// AdminHandler is the handler of every role's admin port: /healthz, /readyz
// and /metrics, the Prometheus handler over the default gatherer, on which the
// roles register their metrics (ADR-0009 §8).
func AdminHandler() http.Handler {
	return health.AdminHandler(promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{}))
}

// serviceExit maps the error a Service returned to an exit code. Ending
// because the context was cancelled is a clean stop.
func serviceExit(err error, stderr io.Writer) int {
	if err == nil || errors.Is(err, context.Canceled) {
		return 0
	}
	_, _ = fmt.Fprintf(stderr, "service: %v\n", err)
	return 1
}
