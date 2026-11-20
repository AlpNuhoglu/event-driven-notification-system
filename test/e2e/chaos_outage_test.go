//go:build e2e

package e2e

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	// outageNotifications is the low traffic of the outage scenario (D50).
	outageNotifications = 3
	// outageDuration is how long the provider is down: real seconds, the
	// point of the scenario (D50).
	outageDuration = 60 * time.Second
	// outageWatchInterval is how often the statuses are read during the
	// outage.
	outageWatchInterval = time.Second
	// outageDrainTimeout bounds the wait until every notification is sent
	// after the provider is back: the next probe of the circuit is at most
	// one open period away (BREAKER_OPEN_MAX, 2 minutes), plus a send.
	outageDrainTimeout = 2*time.Minute + sendTimeout
)

// outageOutcomes are the outcomes an attempt may have while the provider is
// down: nothing reached it (unreachable), the circuit opened between the
// claim and the send (aborted), or the attempt is still running.
var outageOutcomes = []string{"unreachable", "aborted", "in_flight"}

// The chaos scenario "Low-traffic provider outage" (D50, ADR-0010 §1): three
// high notifications are created while the provider is down, and it stays
// down for 60 seconds. Down means the mockprovider container is stopped, so
// that a request cannot be written at all (its name does not resolve):
// unreachable in the classification of ADR-0007 §2, a provider-scoped
// outcome that consumes no retry budget and counts toward the circuit, which
// opens after five consecutive failures and then probes once per open
// period. During the outage no notification ends, every attempt is
// unreachable or aborted, and failures stays 0. After the provider is back
// the next probe closes the circuit, and all three end sent with failures 0.
func TestComposeE2E_ProviderOutageLowTraffic(t *testing.T) {
	const label = "provider outage 60s"
	s := newStack(t)
	s.resetMock(t)
	t.Cleanup(func() { s.leaveClean(t) })
	var ids []string
	s.reportChaosAtEnd(t, label, &ids)
	down := false
	t.Cleanup(func() {
		if down {
			if _, err := compose("up", "-d", "--no-deps", "--wait", "mockprovider"); err != nil {
				t.Errorf("cleanup: start mockprovider: %v", err)
			}
		}
	})

	down = true
	if _, err := compose("stop", "mockprovider"); err != nil {
		t.Fatal(err)
	}
	stopped := time.Now()
	for i := range outageNotifications {
		n, _, err := s.create(createRequest{Channel: "sms", Recipient: fakePhone, Content: fmt.Sprintf("outage %d", i), Priority: "high"})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, n.ID)
	}

	// The provider stays down for outageDuration; no notification may end
	// meanwhile (none can be sent, and none may fail).
	var last map[string]int
	var lastErr error
	ended := false
	poll(outageDuration+sendTimeout, outageWatchInterval, func() bool {
		c, err := statusCounts(ids)
		if err != nil {
			lastErr = err
			return false
		}
		last = c
		if c["sent"] > 0 || c["failed"] > 0 || c["cancelled"] > 0 {
			ended = true
			return true
		}
		return time.Since(stopped) >= outageDuration
	})
	if ended {
		t.Fatalf("%s: %.0f s into the outage: %s; want every notification still waiting", label, time.Since(stopped).Seconds(), formatCounts(last))
	}
	if time.Since(stopped) < outageDuration {
		t.Fatalf("%s: the statuses were not readable during the outage: %v", label, lastErr)
	}
	attempts, outcomes := outageAttempts(t, ids)
	circuit, err := metricSum("worker", "circuit_state", map[string]string{"channel": "sms"})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%s: after %.0f s down: %s; %d attempts %v; sms circuit_state %v (0 closed, 1 half-open, 2 open)",
		label, time.Since(stopped).Seconds(), formatCounts(last), attempts, outcomes, circuit)
	for o, n := range outcomes {
		if !slices.Contains(outageOutcomes, o) {
			t.Errorf("%s: %d attempts ended %s while the provider was down, want only %v", label, n, o, outageOutcomes)
		}
	}
	if attempts < outageNotifications {
		t.Errorf("%s: %d attempts during the outage, want at least one per notification (%d): the worker did not try the provider",
			label, attempts, outageNotifications)
	}
	checkNoFailures(t, label+", during the outage", ids)

	if _, err := compose("up", "-d", "--no-deps", "--wait", "mockprovider"); err != nil {
		t.Fatal(err)
	}
	down = false
	back := time.Now()
	t.Logf("%s: the provider was down for %.1f s", label, back.Sub(stopped).Seconds())

	waitAllSent(t, ids, outageDrainTimeout)
	t.Logf("%s: all %d notifications sent %.1f s after the provider was back", label, len(ids), time.Since(back).Seconds())
	checkNoFailures(t, label+", after the outage", ids)
	for _, id := range ids {
		list, err := s.attempts(id)
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, a := range list {
			got = append(got, a.Outcome)
		}
		if len(got) == 0 || got[len(got)-1] != "success" {
			t.Errorf("%s: notification %s: attempts %v, want success last", label, id, got)
			continue
		}
		for _, o := range got[:len(got)-1] {
			if o != "unreachable" && o != "aborted" {
				t.Errorf("%s: notification %s: attempts %v, want only unreachable and aborted before the success", label, id, got)
				break
			}
		}
	}

	rep, err := s.report()
	if err != nil {
		t.Fatal(err)
	}
	repeated, missing := keyCounts(rep, ids)
	t.Logf("%s: provider received %d requests for %d notifications after it was back; %d reached it more than once (reported), %d never",
		label, rep.Total, len(ids), repeated, missing)
	if missing > 0 {
		t.Errorf("%s: %d sent notifications never reached the provider", label, missing)
	}
}

// outageAttempts returns the number of attempts of the notifications ids and
// their count by outcome.
func outageAttempts(t *testing.T, ids []string) (int, map[string]int) {
	t.Helper()
	arr, err := uuidArray(ids)
	if err != nil {
		t.Fatal(err)
	}
	out, err := psql("SELECT outcome, count(*) FROM notification_attempts WHERE notification_id = ANY(" + arr + ") GROUP BY outcome")
	if err != nil {
		t.Fatal(err)
	}
	total, by := 0, map[string]int{}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		o, n, ok := strings.Cut(line, "|")
		if !ok {
			continue
		}
		c, err := strconv.Atoi(n)
		if err != nil {
			t.Fatalf("attempt count %q: %v", line, err)
		}
		by[o] = c
		total += c
	}
	return total, by
}

// checkNoFailures fails the test unless failures, the retry budget used, is
// 0 on every notification of ids. The API does not expose it, so it is read
// in Postgres.
func checkNoFailures(t *testing.T, when string, ids []string) {
	t.Helper()
	arr, err := uuidArray(ids)
	if err != nil {
		t.Fatal(err)
	}
	out, err := psql("SELECT id, failures, attempt_count FROM notifications WHERE id = ANY(" + arr + ") ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	rows := strings.Split(strings.TrimSpace(out), "\n")
	if len(rows) != len(ids) {
		t.Fatalf("%s: %d of %d notifications found", when, len(rows), len(ids))
	}
	for _, row := range rows {
		f := strings.Split(row, "|")
		if len(f) != 3 {
			t.Fatalf("%s: row %q", when, row)
		}
		if f[1] != "0" {
			t.Errorf("%s: notification %s has failures = %s after %s attempts, want 0: a provider outage must not consume the retry budget",
				when, f[0], f[1], f[2])
		}
	}
}
