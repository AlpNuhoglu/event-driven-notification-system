// Command gateway is the WebSocket gateway role: it serves GET /v1/ws on
// GATEWAY_ADDR, where clients subscribe to notifications and batches and
// receive their current state and every status change (internal/gateway),
// next to the admin endpoints on ADMIN_ADDR. "gateway healthcheck" probes a
// running instance on its admin port.
package main

import (
	"context"
	"io"
	"os"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/gateway"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/bootstrap"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/shutdown"
)

func main() {
	ctx, stop := shutdown.Context(context.Background())
	code := run(ctx, os.Args[1:], os.LookupEnv, os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

func run(ctx context.Context, args []string, lookup func(string) (string, bool), stdout, stderr io.Writer) int {
	return bootstrap.RunService(ctx, "gateway", args, lookup, stdout, stderr, gateway.Service)
}
