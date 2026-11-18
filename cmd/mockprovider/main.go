// Command mockprovider is the mock notification provider of the mock profile.
package main

import (
	"context"
	"os"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/mockprovider"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/bootstrap"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/shutdown"
)

func main() {
	ctx, stop := shutdown.Context(context.Background())
	code := bootstrap.RunService(ctx, "mockprovider", os.Args[1:], os.LookupEnv, os.Stdout, os.Stderr, mockprovider.Run)
	stop()
	os.Exit(code)
}
