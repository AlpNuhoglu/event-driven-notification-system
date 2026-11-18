// Command worker is the delivery worker role: it consumes the dispatch queues
// of WORKER_CHANNELS, paces each channel with the shared rate limiter and
// calls the provider (internal/worker), next to the admin health endpoints.
// "worker healthcheck" probes a running instance.
package main

import (
	"context"
	"os"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/bootstrap"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/shutdown"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/worker"
)

func main() {
	ctx, stop := shutdown.Context(context.Background())
	code := bootstrap.RunService(ctx, "worker", os.Args[1:], os.LookupEnv, os.Stdout, os.Stderr, worker.Service)
	stop()
	os.Exit(code)
}
