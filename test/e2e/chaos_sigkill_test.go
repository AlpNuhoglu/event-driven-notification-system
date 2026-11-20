//go:build e2e

package e2e

import (
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	// killLatencyMS is the mock's answer time on sms in the SIGKILL
	// scenario: a send stays in flight at the mock for this long, well below
	// the provider timeout of 10 s, so the kill meets sends in flight.
	killLatencyMS = 3000
	// killNotifications is the number of sms of the SIGKILL scenario. With
	// 25 sends in flight that each take 3 s, most of them are still queued
	// or buffered when the worker is killed.
	killNotifications = 100
	// killAfterRequests is how many requests the mock must have received,
	// with none answered yet, when the worker is killed.
	killAfterRequests = 10
	// sigkillExitCode is the exit code of a process killed by SIGKILL.
	sigkillExitCode = 137
	// sweepMargin is added to the lease when the lease is configured longer
	// than drainTimeout allows for.
	sweepMargin = 30 * time.Second
	// sweeperRecoveredMetric is the relay's counter of the notifications the
	// lease sweeper recovered, by the status it moved them to (kind).
	sweeperRecoveredMetric = "sweeper_recovered_total"
)

// The chaos scenario "Worker killed with SIGKILL mid-send" (ADR-0010 §1,
// ADR-0007 §10: "A worker dies"). With sends in flight at the mock (it
// answers sms after 3 s), the worker container is killed with SIGKILL: no
// drain, no nack, no final write. Docker does not restart a container killed
// through its API (docker-compose kill), so the scenario checks that the
// container exited with 137 and starts it again, as the restart policy would
// after a crash. The rows the kill left processing are exactly the rows the
// lease sweeper must recover once their lease expires: sweeper_recovered_total
// {kind="retrying"} grows by at least their number, and each of them has an
// attempt closed as unknown. Every notification ends sent, none failed, the
// dispatch and poison queues are empty, and the duplicates at the provider
// are reported, not failed: a notification reaches the provider more than
// once only when the kill left it processing.
func TestComposeE2E_WorkerSIGKILLMidSend(t *testing.T) {
	const label = "worker sigkill"
	s := newStack(t)
	s.resetMock(t)
	t.Cleanup(func() { s.leaveClean(t) })
	var ids []string
	s.reportChaosAtEnd(t, label, &ids)
	killed := false
	t.Cleanup(func() {
		if killed {
			if _, err := compose("start", "worker"); err != nil {
				t.Errorf("cleanup: start worker: %v", err)
			}
		}
	})

	before, err := containerStates("worker")
	if err != nil {
		t.Fatal(err)
	}
	lease, sweep := durationSetting(t, "worker", "LEASE_DURATION", 30*time.Second), durationSetting(t, "relay", "LEASE_SWEEP_INTERVAL", 5*time.Second)
	recoveredBefore := sweeperRecovered(t, "retrying")
	failedBefore := sweeperRecovered(t, "failed")

	s.configureMock(t, map[string]any{"channels": map[string]any{"sms": map[string]any{"latency_ms": killLatencyMS}}})
	ids = createN(t, s, createRequest{Channel: "sms", Recipient: fakePhone, Content: label, Priority: "high"}, killNotifications, label)

	// Sends in flight: the mock has received requests and none of them is
	// answered yet (nothing is sent).
	var atKill int
	if !poll(sendTimeout, statusPollInterval, func() bool {
		rep, err := s.report()
		if err != nil || rep.Total < killAfterRequests {
			return false
		}
		c, err := statusCounts(ids)
		atKill = rep.Total
		return err == nil && c["sent"] == 0
	}) {
		t.Fatalf("%s: the mock did not receive %d requests within %s with none answered yet", label, killAfterRequests, sendTimeout)
	}

	killed = true
	if _, err := compose("kill", "worker"); err != nil { // SIGKILL by default
		t.Fatal(err)
	}
	killedAt := time.Now()
	// The worker is dead, and the relay touches a processing row only after
	// its lease has expired: the statuses read now are those the kill left.
	st, err := statuses(ids)
	if err != nil {
		t.Fatal(err)
	}
	var stranded []string
	counts := map[string]int{}
	for _, id := range ids {
		counts[st[id]]++
		if st[id] == "processing" {
			stranded = append(stranded, id)
		}
	}
	t.Logf("%s: killed once the mock had received %d requests and none was answered; the kill left %s", label, atKill, formatCounts(counts))
	if len(stranded) == 0 {
		t.Fatalf("%s: the kill left no notification processing: no send was in flight, so the scenario checks nothing", label)
	}
	if out, err := docker("inspect", "--format", "{{.State.Status}} {{.State.ExitCode}} {{.RestartCount}}", before["worker"].ID); err != nil {
		t.Fatal(err)
	} else if f := strings.Fields(out); len(f) != 3 || f[0] != "exited" || f[1] != strconv.Itoa(sigkillExitCode) {
		t.Fatalf("%s: after docker-compose kill the worker container is %q, want exited with %d", label, strings.TrimSpace(out), sigkillExitCode)
	}

	// The provider answers at once again, and the worker is brought back.
	s.configureMock(t, map[string]any{})
	if _, err := compose("start", "worker"); err != nil {
		t.Fatal(err)
	}
	killed = false
	if err := waitHealthy(healthyTimeout); err != nil {
		t.Fatal(err)
	}

	// The lease sweeper recovers the stranded rows once their lease expires.
	// The bound is that of the stack's defaults (drainTimeout covers a lease
	// and a sweep interval), not the configured sweep interval, so that a
	// sweeper switched off by a very long interval fails here in minutes.
	sweepTimeout := max(drainTimeout, lease+sweepMargin)
	var recovered float64
	if !poll(sweepTimeout, drainPollInterval, func() bool {
		recovered = sweeperRecovered(t, "retrying") - recoveredBefore
		return recovered >= float64(len(stranded))
	}) {
		t.Fatalf("%s: %s{kind=\"retrying\"} grew by %v within %s of the kill (lease %s, sweep interval %s), want at least %d: the rows the kill left processing were not recovered",
			label, sweeperRecoveredMetric, recovered, sweepTimeout, lease, sweep, len(stranded))
	}
	t.Logf("%s: the lease sweeper recovered %v notifications %.1f s after the kill (lease %s, sweep interval %s)",
		label, recovered, time.Since(killedAt).Seconds(), lease, sweep)

	waitAllSent(t, ids, drainTimeout)
	t.Logf("%s: all %d notifications sent %.1f s after the kill", label, len(ids), time.Since(killedAt).Seconds())

	if got := sweeperRecovered(t, "failed") - failedBefore; got != 0 {
		t.Errorf("%s: the lease sweeper failed %v notifications, want none", label, got)
	}
	arr, err := uuidArray(stranded)
	if err != nil {
		t.Fatal(err)
	}
	out, err := psql("SELECT count(DISTINCT notification_id) FROM notification_attempts WHERE outcome = 'unknown' AND notification_id = ANY(" + arr + ")")
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(out); got != strconv.Itoa(len(stranded)) {
		t.Errorf("%s: %s of the %d notifications the kill left processing have an attempt closed as unknown, want all", label, got, len(stranded))
	}
	if err := waitDispatchEmpty(healthyTimeout); err != nil {
		t.Error(err)
	}
	depths := checkPoisonEmpty(t)
	t.Logf("%s: queue depths after the drain: %v", label, depths)

	rep, err := s.report()
	if err != nil {
		t.Fatal(err)
	}
	repeated, missing := keyCounts(rep, ids)
	t.Logf("%s: provider received %d requests for %d notifications; %d reached it more than once (reported), %d never",
		label, rep.Total, len(ids), repeated, missing)
	if missing > 0 {
		t.Errorf("%s: %d sent notifications never reached the provider", label, missing)
	}
	for _, id := range ids {
		if rep.IdempotencyKeys[id] > 1 && !slices.Contains(stranded, id) {
			t.Errorf("%s: notification %s reached the provider %d times, but the kill did not leave it processing", label, id, rep.IdempotencyKeys[id])
		}
	}
}

// sweeperRecovered reads sweeper_recovered_total of one kind on the relay.
func sweeperRecovered(t *testing.T, kind string) float64 {
	t.Helper()
	v, err := metricSum("relay", sweeperRecoveredMetric, map[string]string{"kind": kind})
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// durationSetting returns a duration variable of a running service, or def
// when the service does not set it.
func durationSetting(t *testing.T, service, name string, def time.Duration) time.Duration {
	t.Helper()
	v, ok, err := serviceSetting(service, name)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		t.Fatalf("%s of %s: %q: %v", name, service, v, err)
	}
	return d
}
