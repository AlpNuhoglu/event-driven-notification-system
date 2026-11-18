// Package testutil holds test helpers: Eventually, the only waiting primitive
// the tests use, and a Postgres helper that gives each test its own migrated
// database in a container started lazily once per test binary.
package testutil

import (
	"testing"
	"time"
)

// pollInterval is how often Eventually re-evaluates its condition.
const pollInterval = 5 * time.Millisecond

// Eventually polls condition until it returns true and fails the test if that
// does not happen within timeout. It is the only waiting primitive of the
// tests; they never call time.Sleep.
func Eventually(t testing.TB, condition func() bool, timeout time.Duration) {
	t.Helper()
	if !poll(condition, timeout) {
		t.Fatalf("condition not met within %s", timeout)
	}
}

func poll(condition func() bool, timeout time.Duration) bool {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	tick := time.NewTicker(pollInterval)
	defer tick.Stop()
	for {
		if condition() {
			return true
		}
		select {
		case <-deadline.C:
			return condition()
		case <-tick.C:
		}
	}
}
