// Package bootstrap is the temporary shared runner used by every role stub in
// P0. It serves the admin health endpoints and supports a healthcheck command.
package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/config"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/health"
)

// Run executes a long-running role and returns the process exit code. With no
// arguments it serves the health endpoints on ADMIN_ADDR until ctx is done; the
// argument "healthcheck" probes a running instance instead.
func Run(ctx context.Context, role string, args []string, lookup func(string) (string, bool), stdout, stderr io.Writer) int {
	if len(args) > 0 && args[0] != "healthcheck" {
		return usage(role, stderr)
	}
	cfg, err := config.LoadFrom(lookup)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 1
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
	return serve(ctx, ln, role, stdout, stderr)
}

func usage(role string, stderr io.Writer) int {
	_, _ = fmt.Fprintf(stderr, "usage: %s [healthcheck]\n", role)
	return 2
}

// serve serves the health handler on ln until ctx is done, then shuts down.
func serve(ctx context.Context, ln net.Listener, role string, stdout, stderr io.Writer) int {
	logger := slog.New(slog.NewJSONHandler(stdout, nil))
	srv := &http.Server{
		Handler:           health.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	logger.Info("service started", "role", role, "admin_addr", ln.Addr().String())

	select {
	case err := <-errc:
		_, _ = fmt.Fprintf(stderr, "serve: %v\n", err)
		return 1
	case <-ctx.Done():
	}

	shutCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	code := 0
	if err := srv.Shutdown(shutCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		_, _ = fmt.Fprintf(stderr, "shutdown: %v\n", err)
		code = 1
	}
	logger.Info("service stopped")
	return code
}
