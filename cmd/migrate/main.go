// Command migrate is the database migration role. This is the P0 stub: it is a
// one-shot command that does nothing yet.
package main

import (
	"os"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/bootstrap"
)

func main() {
	os.Exit(bootstrap.RunOneShot("migrate", os.Args[1:], os.Stdout, os.Stderr))
}
