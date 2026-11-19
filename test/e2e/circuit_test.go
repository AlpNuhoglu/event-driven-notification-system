//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/mockprovider"
)

const (
	// circuitNotifications is the number of sms notifications that meet
	// the provider answering 401.
	circuitNotifications = 4
	// circuitProbes is how many probes, one per open period, the scenario
	// waits for while the provider still answers 401.
	circuitProbes = 2
	// circuitMaxFatalOpen bounds the open period the scenario accepts:
	// docker-compose.e2e.yml sets 2000 ms; the default of one minute
	// would make the scenario take minutes.
	circuitMaxFatalOpen = 5 * time.Second
	// fatalOpenVar is the worker's setting of that open period.
	fatalOpenVar = "BREAKER_FATAL_OPEN"
)

// workerSetting reads an environment variable of the running worker
// container, as the worker sees it; false when it is not set.
func workerSetting(name string) (string, bool, error) {
	out, err := compose("ps", "-q", "worker")
	if err != nil {
		return "", false, err
	}
	id := strings.TrimSpace(out)
	if id == "" || strings.Contains(id, "\n") {
		return "", false, fmt.Errorf("want one running worker container, got %q", id)
	}
	ctx, cancel := context.WithTimeout(context.Background(), composeTimeout)
	defer cancel()
	var buf bytes.Buffer
	cmd := exec.CommandContext(ctx, "docker", "inspect", "--format", "{{json .Config.Env}}", id) //nolint:gosec // fixed command, the container id from docker-compose
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	if err := cmd.Run(); err != nil {
		return "", false, fmt.Errorf("docker inspect: %w: %s", err, buf.String())
	}
	var env []string
	if err := json.Unmarshal(buf.Bytes(), &env); err != nil {
		return "", false, fmt.Errorf("decode the worker's environment: %w", err)
	}
	for _, kv := range env {
		if v, ok := strings.CutPrefix(kv, name+"="); ok {
			return v, true, nil
		}
	}
	return "", false, nil
}

// fatalOpen returns the open period of the running worker's circuit after a
// provider_fatal answer, and fails the test when the stack does not run with
// a short one.
func fatalOpen(t *testing.T) time.Duration {
	t.Helper()
	v, ok, err := workerSetting(fatalOpenVar)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("the worker runs without %s (default one minute): start the stack with test/e2e/docker-compose.e2e.yml, as make test-e2e does", fatalOpenVar)
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		t.Fatalf("%s=%q: %v", fatalOpenVar, v, err)
	}
	if d <= 0 || d > circuitMaxFatalOpen {
		t.Fatalf("%s=%s, want a positive duration of at most %s (docker-compose.e2e.yml)", fatalOpenVar, d, circuitMaxFatalOpen)
	}
	return d
}

// smsArrivals returns the arrival times of the sms requests in a report.
func smsArrivals(t *testing.T, rep mockprovider.Report) []time.Time {
	t.Helper()
	var out []time.Time
	for _, s := range rep.Channels["sms"].Arrivals {
		at, err := time.Parse(arrivalLayout, s)
		if err != nil {
			t.Fatalf("arrival %q: %v", s, err)
		}
		out = append(out, at)
	}
	return out
}

// workerLogsSince returns the worker's log lines since the given time.
func workerLogsSince(since time.Time) ([]string, error) {
	out, err := compose("logs", "--no-color", "--no-log-prefix", "--since", since.UTC().Format(time.RFC3339Nano), "worker")
	if err != nil {
		return nil, err
	}
	return strings.Split(out, "\n"), nil
}

// The exit criterion of the breaker pull request: 401 opens the circuit and
// failures stay 0. The mock answers 401 on sms. A few high notifications are
// created: the first answer opens the circuit for BREAKER_FATAL_OPEN, and
// from then on the mock receives one probe per open period, not one request
// per notification and retry. The API shows every notification unfailed;
// it does not expose failures, so the attempts stand for it: only
// provider_fatal and aborted (and the in_flight of an attempt not yet
// finished), none retryable, none budget-consuming. Email keeps sending
// meanwhile. When the mock answers 202 again, the probe closes the circuit
// and every notification ends sent. The stack is left with a closed circuit,
// an empty backlog and a reset mock, as load-smoke needs it.
func TestComposeE2E_CircuitOpensOn401WithoutFailures(t *testing.T) {
	s := newStack(t)
	open := fatalOpen(t)
	s.resetMock(t)
	started := time.Now()

	var ids []string
	t.Cleanup(func() {
		// Leave the stack clean even when the test failed: the mock accepts,
		// every notification ends (the probe closes the circuit), and the
		// mock's records are cleared.
		s.resetMock(t)
		for _, id := range ids {
			if !poll(sendTimeout, statusPollInterval, func() bool {
				n, err := s.get(id)
				return err == nil && (n.Status == "sent" || n.Status == "failed" || n.Status == "cancelled")
			}) {
				t.Errorf("cleanup: notification %s did not end within %s", id, sendTimeout)
			}
		}
		s.resetMock(t)
	})
	s.configureMock(t, map[string]any{"channels": map[string]any{"sms": map[string]any{"status": 401}}})

	for i := range circuitNotifications {
		n, _, err := s.create(createRequest{Channel: "sms", Recipient: fakePhone, Content: fmt.Sprintf("circuit %d", i), Priority: "high"})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, n.ID)
	}

	// The circuit is open once the first 401 has arrived; email is sent
	// meanwhile.
	if !poll(sendTimeout, statusPollInterval, func() bool {
		rep, err := s.report()
		return err == nil && rep.Channels["sms"].Count > 0
	}) {
		t.Fatalf("no sms request reached the mock within %s", sendTimeout)
	}
	email, _, err := s.create(createRequest{Channel: "email", Recipient: fakeEmail, Content: "circuit", Priority: "high"})
	if err != nil {
		t.Fatal(err)
	}
	if got := s.waitSent(t, email.ID, sendTimeout); got.AttemptCount != 1 {
		t.Errorf("email attempt_count %d, want 1", got.AttemptCount)
	}

	// Wait for probes: requests that arrive at least half an open period
	// after the first (the first requests, one per notification at most,
	// arrive together, before the first 401 is recorded).
	var rep mockprovider.Report
	if !poll(circuitProbes*open+sendTimeout, statusPollInterval, func() bool {
		rep, err = s.report()
		if err != nil {
			return false
		}
		at := smsArrivals(t, rep)
		probes := 0
		for _, a := range at {
			if a.Sub(at[0]) >= open/2 {
				probes++
			}
		}
		return probes >= circuitProbes
	}) {
		t.Fatalf("fewer than %d probes within %s (report error %v)", circuitProbes, circuitProbes*open+sendTimeout, err)
	}
	at := smsArrivals(t, rep)
	elapsed := at[len(at)-1].Sub(at[0])
	bound := circuitNotifications + int(elapsed/open) + 1
	t.Logf("circuit: %d sms requests in %s with an open period of %s (bound %d)", len(at), elapsed.Round(time.Millisecond), open, bound)
	if len(at) > bound {
		t.Errorf("the mock received %d sms requests in %s, want at most %d", len(at), elapsed, bound)
	}
	if st := rep.Channels["sms"].Statuses; len(st) != 1 || st["401"] != len(at) {
		t.Errorf("sms answers %v, want only 401", st)
	}
	first := 0
	for first < len(at) && at[first].Sub(at[0]) < open/2 {
		first++
	}
	if first > circuitNotifications {
		t.Errorf("%d sms requests before the first open period ended, want at most one per notification (%d)", first, circuitNotifications)
	}
	for i := first; i < len(at); i++ {
		if g := at[i].Sub(at[i-1]); g < open {
			t.Errorf("sms requests %d and %d arrived %s apart, want at least the open period %s", i, i+1, g, open)
		}
	}

	// The API: nothing failed, nothing consumed budget.
	for _, id := range ids {
		n, err := s.get(id)
		if err != nil {
			t.Fatal(err)
		}
		if n.Status == "failed" || n.Status == "sent" {
			t.Errorf("notification %s is %s while the provider answers 401", id, n.Status)
		}
		list, err := s.attempts(id)
		if err != nil {
			t.Fatal(err)
		}
		for _, a := range list {
			switch a.Outcome {
			case "provider_fatal", "aborted", "in_flight":
			default:
				t.Errorf("notification %s: attempt %d is %s, want provider_fatal or aborted", id, a.AttemptNo, a.Outcome)
			}
		}
	}

	// The provider accepts again: the next probe closes the circuit.
	s.configureMock(t, map[string]any{})
	fatal := 0
	for _, id := range ids {
		n := s.waitSent(t, id, open+sendTimeout)
		list, err := s.attempts(id)
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, a := range list {
			got = append(got, a.Outcome)
		}
		if n.AttemptCount != len(list) || len(got) == 0 || got[len(got)-1] != "success" {
			t.Errorf("notification %s: attempt_count %d, attempts %v; want success last", id, n.AttemptCount, got)
		}
		for _, o := range got[:max(len(got)-1, 0)] {
			if o == "provider_fatal" {
				fatal++
			} else if o != "aborted" {
				t.Errorf("notification %s: attempts %v, want only provider_fatal and aborted before the success", id, got)
			}
		}
	}
	rep, err = s.report()
	if err != nil {
		t.Fatal(err)
	}
	if n401 := rep.Channels["sms"].Statuses["401"]; fatal != n401 {
		t.Errorf("%d provider_fatal attempts for %d requests answered 401, want one each", fatal, n401)
	}
	if got := rep.Channels["sms"].Statuses["202"]; got != circuitNotifications {
		t.Errorf("%d sms requests answered 202, want %d, one per notification", got, circuitNotifications)
	}
	for _, id := range ids {
		if rep.IdempotencyKeys[id] < 1 {
			t.Errorf("notification %s never reached the mock", id)
		}
	}

	// The worker logged each opening at error level, and the close.
	lines, err := workerLogsSince(started)
	if err != nil {
		t.Fatal(err)
	}
	var opened, closed int
	for _, line := range lines {
		var l struct {
			Level, Msg, Channel, To, Outcome string
		}
		if json.Unmarshal([]byte(line), &l) != nil || l.Channel != "sms" || !strings.HasPrefix(l.Msg, "worker: circuit") {
			continue
		}
		switch l.To {
		case "open":
			opened++
			if l.Level != "ERROR" || l.Outcome != "provider_fatal" {
				t.Errorf("opening logged at %s for %s, want ERROR for provider_fatal", l.Level, l.Outcome)
			}
		case "closed":
			closed++
		}
	}
	n401 := rep.Channels["sms"].Statuses["401"]
	if opened < 1+circuitProbes || opened > n401 || closed != 1 {
		t.Errorf("%d openings and %d closes logged for %d answers 401; want between %d and %d openings and one close",
			opened, closed, n401, 1+circuitProbes, n401)
	}
}
