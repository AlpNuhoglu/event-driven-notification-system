// Command relay is the outbox relay role: it publishes due notifications from
// Postgres to RabbitMQ, reconciles queued notifications on every
// (re)connection and runs the lease sweeper and the expiry pass
// (internal/relay), next to the admin endpoints (health and metrics).
// "relay healthcheck" probes a running instance; "relay requeue-stale
// [--older-than=5m] [--all]" returns stale queued notifications to pending.
package main

import (
	"context"
	"os"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/shutdown"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/relay"
)

func main() {
	ctx, stop := shutdown.Context(context.Background())
	code := relay.Main(ctx, os.Args[1:], os.LookupEnv, os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}
