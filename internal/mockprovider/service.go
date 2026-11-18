package mockprovider

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/config"
)

// Run serves the mock provider on cfg.Ports.MockProviderAddr until ctx is
// cancelled. It matches bootstrap.Service.
func Run(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.Ports.MockProviderAddr)
	if err != nil {
		return err
	}
	srv := &http.Server{
		Handler:           New(time.Now).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return context.WithoutCancel(ctx) },
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	logger.Info("mock provider listening", "addr", ln.Addr().String())
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	shutCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		return err
	}
	if err := <-errc; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
