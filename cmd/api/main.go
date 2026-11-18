// Command api is the REST API role: it serves the HTTP API of
// api/openapi.yaml on API_ADDR (internal/api), next to the admin health
// endpoints on ADMIN_ADDR. "api healthcheck" probes a running instance on its
// admin port.
package main

import (
	"context"
	"io"
	"os"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/api"
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
	return bootstrap.RunService(ctx, "api", args, lookup, stdout, stderr, api.Service)
}
