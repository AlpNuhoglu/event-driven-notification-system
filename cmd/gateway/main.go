// Command gateway is the WebSocket gateway role. This is the P0 stub: it serves the admin
// health endpoints only.
package main

import (
	"context"
	"os"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/bootstrap"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/shutdown"
)

func main() {
	ctx, stop := shutdown.Context(context.Background())
	code := bootstrap.Run(ctx, "gateway", os.Args[1:], os.LookupEnv, os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}
