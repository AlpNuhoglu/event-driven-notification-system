package worker

import (
	"context"
	"testing"
	"time"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/health"
)

// A check of a dependency the service has not connected to yet fails, so the
// role is not ready before it has started: all three fail, each under its
// name, in the order of ADR-0009 §7.
func TestRegisterReadiness_NothingConnectedYetFailsEveryCheck(t *testing.T) {
	reg := health.NewReadiness(time.Second)
	registerReadiness(reg)
	rep := reg.Run(context.Background())
	if rep.Status != health.StatusFail {
		t.Errorf("status %q, want %q", rep.Status, health.StatusFail)
	}
	want := []string{"postgres", "rabbitmq", "consumer"}
	if len(rep.Checks) != len(want) {
		t.Fatalf("%d checks, want %d: %+v", len(rep.Checks), len(want), rep.Checks)
	}
	for i, c := range rep.Checks {
		if c.Name != want[i] || c.Status != health.StatusFail {
			t.Errorf("check %d: %+v, want %s failing", i, c, want[i])
		}
	}
}

// A role with no consumer has nothing inactive.
func TestRole_ConsumersActiveWithoutUnits(t *testing.T) {
	if !(&Role{}).ConsumersActive() {
		t.Error("ConsumersActive of a role without units is false")
	}
}
