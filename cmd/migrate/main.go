// Command migrate is the database migration role. It is a one-shot command:
// it applies or reverts the schema and exits.
package main

import (
	"fmt"
	"io"
	"os"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/config"
	"github.com/AlpNuhoglu/event-driven-notification-system/migrations"
)

func main() {
	os.Exit(run(os.Args[1:], os.LookupEnv, os.Stdout, os.Stderr))
}

func run(args []string, lookup func(string) (string, bool), stdout, stderr io.Writer) int {
	cmd := "up"
	if len(args) > 0 {
		cmd = args[0]
	}
	if len(args) > 1 {
		return usage(stderr)
	}
	var apply func(string) error
	var done string
	switch cmd {
	case "up":
		apply, done = migrations.Up, "migrate: up to date"
	case "down":
		apply, done = migrations.Down, "migrate: reverted"
	case "healthcheck":
		_, _ = fmt.Fprintln(stderr, "healthcheck: migrate is a one-shot command and has no health endpoint")
		return 1
	default:
		return usage(stderr)
	}
	cfg, err := config.LoadFrom(lookup)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 1
	}
	if err := apply(cfg.Connections.DatabaseURL); err != nil {
		_, _ = fmt.Fprintf(stderr, "migrate: %v\n", err)
		return 1
	}
	_, _ = fmt.Fprintln(stdout, done)
	return 0
}

func usage(stderr io.Writer) int {
	_, _ = fmt.Fprintln(stderr, "usage: migrate [up|down|healthcheck]")
	return 2
}
