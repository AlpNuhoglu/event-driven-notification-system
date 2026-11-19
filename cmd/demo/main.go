// Command demo shows the system working on a running stack: it checks that
// the provider URL can take a few notifications, creates them through the
// API, follows them over the WebSocket and prints what happened.
// "make demo" runs it inside the worker container.
package main

import (
	"context"
	"os"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/demo"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/shutdown"
)

func main() {
	ctx, stop := shutdown.Context(context.Background())
	code := demo.Main(ctx, os.Args[1:], os.LookupEnv, os.Stdout)
	stop()
	os.Exit(code)
}
