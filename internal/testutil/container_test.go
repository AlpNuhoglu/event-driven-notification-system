package testutil

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestStartRetrying(t *testing.T) {
	pause := startPause
	startPause = time.Millisecond
	t.Cleanup(func() { startPause = pause })

	t.Run("a later attempt succeeds", func(t *testing.T) {
		calls := 0
		got, err := startRetrying("x", func() (string, error) {
			calls++
			if calls < 3 {
				return "", errors.New("not yet")
			}
			return "started", nil
		})
		if err != nil || got != "started" || calls != 3 {
			t.Errorf("got %q, error %v after %d calls; want \"started\", no error, 3 calls", got, err, calls)
		}
	})

	t.Run("the first attempt succeeds", func(t *testing.T) {
		calls := 0
		if _, err := startRetrying("x", func() (int, error) { calls++; return 1, nil }); err != nil || calls != 1 {
			t.Errorf("error %v after %d calls; want no error, 1 call", err, calls)
		}
	})

	t.Run("it gives up and names every attempt", func(t *testing.T) {
		calls := 0
		_, err := startRetrying("x", func() (int, error) {
			calls++
			return 0, errors.New("docker says no")
		})
		if calls != startAttempts {
			t.Errorf("%d calls, want %d", calls, startAttempts)
		}
		if err == nil {
			t.Fatal("no error after the last attempt failed")
		}
		for _, want := range []string{"attempt 1: docker says no", "attempt 4: docker says no"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q does not contain %q", err, want)
			}
		}
	})
}
