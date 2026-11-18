// Package shutdown provides a context that is cancelled on SIGINT or SIGTERM.
package shutdown

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// Context returns a child of parent that is cancelled when the process receives
// SIGINT or SIGTERM. The returned function releases the signal registration.
func Context(parent context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
}
