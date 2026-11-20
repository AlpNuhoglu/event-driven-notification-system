package gateway

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

// failingSource is an event source whose consumer cannot go on, as when
// the broker connection's recovery has given up.
type failingSource struct{ err error }

func (f failingSource) Run(context.Context, func(domain.StatusChanged)) error { return f.err }

// D5: when the event consumer ends with an error (its connection's recovery
// gave up, TestEventConsumer_RunFailsWhenRecoveryGivesUp), Run ends with
// that error, so that the gateway's process exits with a non-zero code and
// is restarted; it does not keep serving connections that receive nothing.
func TestRun_EndsWithTheEventSourcesError(t *testing.T) {
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gaveUp := errors.New("rabbitmq connection recovery gave up")
	done := make(chan error, 1)
	go func() {
		done <- Run(context.Background(), ln, New(testOptions(newFakeSnapshots())), failingSource{gaveUp}, waitTimeout, discard())
	}()
	var runErr error
	ended := false
	testutil.Eventually(t, func() bool {
		select {
		case runErr = <-done:
			ended = true
		default:
		}
		return ended
	}, waitTimeout)
	if !errors.Is(runErr, gaveUp) {
		t.Errorf("Run: %v, want the event source's error", runErr)
	}
}
