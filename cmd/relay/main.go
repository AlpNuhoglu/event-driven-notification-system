// Command relay is the outbox relay role: it publishes due notifications from
// Postgres to RabbitMQ and runs the lease sweeper (internal/relay), next to the
// admin health endpoints. "relay healthcheck" probes a running instance.
package main

import (
	"context"
	"os"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/bootstrap"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/shutdown"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/relay"
)

func main() {
	ctx, stop := shutdown.Context(context.Background())
	code := bootstrap.RunService(ctx, "relay", os.Args[1:], os.LookupEnv, os.Stdout, os.Stderr, relay.Service)
	stop()
	os.Exit(code)
}
