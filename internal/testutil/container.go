package testutil

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

// Starting a container can fail for reasons that have nothing to do with the
// code under test. Two were seen in this suite, in ten shuffled runs of
// make test and then in a reproduction with 14 test binaries that start
// containers at the same moment (docs/progress.md, P6):
//
//   - testcontainers-go v0.45.0 shares one reaper container between the test
//     binaries of one go test invocation. A binary that looks the reaper up
//     between another binary's create and start of it finds the status
//     "created", and the library reports that as a permanent error.
//   - Docker Engine 29.9.2 (Docker Desktop) now and then starts a container
//     without the host port its configuration asks for: the container runs,
//     its port map is empty and stays empty.
//
// A new container starts normally after the first; the second comes in
// bursts: the containers one test binary starts lack their port for a while
// (60 seconds and more in one run, three containers in a row over 35 seconds
// in another), and then a new one is normal again. So a helper starts its
// container up to startAttempts times, with a pause that doubles. Only the
// start is repeated: nothing a test asserts runs inside an attempt. Every
// failed attempt is written to standard error, so the output of a run shows
// how often it happened. A burst longer than the attempts still fails the
// package; why Docker leaves the port out is not known.
const startAttempts = 6

// startPause is the wait before the second attempt; it doubles from there.
var startPause = 250 * time.Millisecond

// startRetrying calls attempt until it succeeds, at most startAttempts times.
// An attempt that fails removes its own container with discard. The error
// after the last attempt names every attempt's error.
func startRetrying[T any](name string, attempt func() (T, error)) (T, error) {
	var errs []error
	pause := startPause
	for n := 1; ; n++ {
		v, err := attempt()
		if err == nil {
			return v, nil
		}
		errs = append(errs, fmt.Errorf("attempt %d: %w", n, err))
		if n == startAttempts {
			var zero T
			return zero, errors.Join(errs...)
		}
		fmt.Fprintf(os.Stderr, "testutil: start %s: attempt %d of %d failed, starting another container: %v\n",
			name, n, startAttempts, err)
		<-time.After(pause)
		pause *= 2
	}
}

// discard removes the container of a failed start without waiting for it:
// stopping a container that Docker started without its port took minutes in
// the reproduction. If the removal does not finish, the reaper removes the
// container when the test binary has exited.
func discard(name string, c testcontainers.Container) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := testcontainers.TerminateContainer(c, testcontainers.StopContext(ctx)); err != nil {
			fmt.Fprintf(os.Stderr, "testutil: remove the %s container of a failed start: %v\n", name, err)
		}
	}()
}
