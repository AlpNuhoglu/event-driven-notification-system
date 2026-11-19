//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// The chaos scenarios of P3 on the Compose stack (ADR-0010 §1, "Chaos
// scenarios"): the real broker restarted and wiped, requeue-stale run as the
// operator runs it, and the real Postgres restarted during sends. This file
// sorts before the others, so its tests run first and every other test of
// the package (load-smoke last) runs on the stack they leave: each one ends
// with every service up and healthy, no backlog in the broker and a reset
// mock (leaveClean).

const (
	// recoveryTimeout bounds the wait for reconciliation after the broker is
	// back: the AMQP recovery budget of the stack (24 tries 5 s apart) plus
	// a margin.
	recoveryTimeout = 2*time.Minute + 30*time.Second
	// drainTimeout bounds the wait until every notification of a scenario
	// is sent once the fault is over. It covers a lease (30 s) and a sweep
	// interval (5 s) plus a retry: a claim whose commit was lost to a
	// Postgres restart is recovered by the lease sweeper.
	drainTimeout = 3 * time.Minute
	// slowLatencyMS is the mock's answer time on sms while a backlog is
	// built: with 25 concurrent sends the worker delivers about 25 sms per
	// second, so a few hundred notifications stay queued in the broker.
	slowLatencyMS = 1000
	// brokerBacklog is the number of sms of the broker scenarios.
	brokerBacklog = 150
	// createdDuringFault is the number of notifications created while the
	// broker or Postgres is down, after the fault command has returned.
	createdDuringFault = 10
	// faultCreateInterval paces the creates made while a fault command runs.
	faultCreateInterval = 50 * time.Millisecond
	// reconcileRunsMetric and reconciledRowsMetric are the relay's counters
	// of reconciliation runs and of the rows they returned to pending.
	reconcileRunsMetric  = "reconciliation_runs_total"
	reconciledRowsMetric = "reconciled_rows_total"
	// rabbitmqVolume is the Compose name of the broker's data volume.
	rabbitmqVolume = "rabbitmq-data"
)

// slowSMS is the mock configuration that slows sms delivery down.
var slowSMS = map[string]any{"channels": map[string]any{"sms": map[string]any{"latency_ms": slowLatencyMS}}}

// observed are the services whose containers must survive a broker fault.
var observed = []string{"api", "relay", "worker", "mockprovider", "postgres", "redis"}

// relayRuns reads reconciliation_runs_total on the relay's /metrics.
func relayRuns(t *testing.T) float64 {
	t.Helper()
	v, err := metric("relay", reconcileRunsMetric)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// waitRunsAbove waits until the relay's reconciliation_runs_total is above
// before and returns it.
func waitRunsAbove(t *testing.T, before float64) float64 {
	t.Helper()
	var got float64
	var err error
	if !poll(recoveryTimeout, drainPollInterval, func() bool {
		got, err = metric("relay", reconcileRunsMetric)
		return err == nil && got > before
	}) {
		t.Fatalf("%s stayed at %v within %s (last value %v, error %v)", reconcileRunsMetric, before, recoveryTimeout, got, err)
	}
	return got
}

// relayReconciledRows reads reconciled_rows_total on the relay's /metrics.
func relayReconciledRows(t *testing.T) float64 {
	t.Helper()
	v, err := metric("relay", reconciledRowsMetric)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// buildBacklog creates brokerBacklog sms with the mock slowed down and waits
// until some of them are queued in the broker and some in flight.
func buildBacklog(t *testing.T, s *stack, label string) []string {
	t.Helper()
	s.configureMock(t, slowSMS)
	ids := createN(t, s, createRequest{Channel: "sms", Recipient: fakePhone, Content: label, Priority: "high"}, brokerBacklog, label)
	c := waitCounts(t, ids, sendTimeout, "a backlog queued in the broker and sends in flight", func(c map[string]int) bool {
		return c["queued"] > 0 && c["processing"] > 0
	})
	t.Logf("%s: before the fault: %s", label, formatCounts(c))
	return ids
}

// createDuring creates notifications while fn runs, and createdDuringFault
// more after it has returned, and returns their ids and fn's error. The API
// does not depend on the broker, so every create must succeed.
func createDuring(t *testing.T, s *stack, label string, fn func() error) ([]string, error) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- fn() }()
	var ids []string
	add := func() {
		n, _, err := s.create(createRequest{Channel: "sms", Recipient: fakePhone, Content: label, Priority: "high"})
		if err != nil {
			t.Error(err)
			return
		}
		ids = append(ids, n.ID)
	}
	var fnErr error
	tick := time.NewTicker(faultCreateInterval)
	defer tick.Stop()
loop:
	for {
		add()
		select {
		case fnErr = <-done:
			break loop
		case <-tick.C:
		}
	}
	for range createdDuringFault {
		add()
	}
	return ids, fnErr
}

// finishBrokerScenario is the common end of the broker scenarios: the mock
// answers at once again, every notification ends sent, no observed
// container exited, the poison queues are empty, the relay and the worker
// logged no channel error, and the duplicates at the provider are reported.
func finishBrokerScenario(t *testing.T, s *stack, label string, ids []string, before map[string]containerState, since time.Time) {
	t.Helper()
	s.configureMock(t, map[string]any{})
	drained := time.Now()
	waitAllSent(t, ids, drainTimeout)
	t.Logf("%s: all %d notifications sent %.1f s after the reconciliation", label, len(ids), time.Since(drained).Seconds())

	checkNoServiceExited(t, before)
	if err := waitDispatchEmpty(healthyTimeout); err != nil {
		t.Error(err)
	}
	depths := checkPoisonEmpty(t)
	t.Logf("%s: queue depths after the drain: %v", label, depths)

	lines, err := serviceLogsSince(since, "relay", "worker")
	if err != nil {
		t.Fatal(err)
	}
	if bad := channelErrors(lines); len(bad) > 0 {
		t.Errorf("%s: channel errors in the relay's or the worker's log:\n%s", label, strings.Join(bad, "\n"))
	}
	msgs := countMessages(lines)
	t.Logf("%s: log: relay lane failures %d, topology declaration failures %d, reconciliation runs %d, buffered deliveries of an older epoch dropped %d",
		label, msgs["relay: lane failed"], msgs["rabbitmq: declaring the topology after a reconnection failed"],
		msgs["relay: reconciliation ran"], msgs["worker: deliveries from an older connection epoch dropped from the buffer"])
	// A lane whose transaction meets the connection loss rolls back and is
	// published again later; its error shows where the loss met it.
	for _, line := range lines {
		var l logLine
		if json.Unmarshal([]byte(line), &l) == nil && l.Msg == "relay: lane failed" {
			t.Logf("%s: relay lane failure: %s", label, l.Error)
		}
	}

	rep, err := s.report()
	if err != nil {
		t.Fatal(err)
	}
	repeated, missing := keyCounts(rep, ids)
	// A send is repeated only when its attempt's lease expires before the
	// result is written (ADR-0004 §4); with Postgres up the final write
	// lands at once, so a repeat is bounded by the sends in flight at the
	// fault: the worker's concurrency on the one channel.
	bound := workerConcurrency(t)
	t.Logf("%s: provider received %d requests for %d notifications; %d reached it more than once (bound %d), %d never",
		label, rep.Total, len(ids), repeated, bound, missing)
	if missing > 0 {
		t.Errorf("%s: %d sent notifications never reached the provider", label, missing)
	}
	if repeated > bound {
		t.Errorf("%s: %d notifications reached the provider more than once, more than the %d sends that can be in flight", label, repeated, bound)
	}
}

// The exit criterion "docker-compose restart rabbitmq" (ADR-0010 §1): while
// notifications are queued in the broker (the mock answers slowly, so a
// backlog exists), in flight at the mock, and created during the restart,
// the broker is restarted. The relay's reconciliation_runs_total increases,
// no service exits (the AMQP recovery reconnects within its budget), every
// notification ends sent, and the duplicates at the provider are reported.
func TestComposeE2E_RabbitMQRestartEveryNotificationSent(t *testing.T) {
	const label = "rabbitmq restart"
	s := newStack(t)
	s.resetMock(t)
	t.Cleanup(func() { s.leaveClean(t) })

	before, err := containerStates(observed...)
	if err != nil {
		t.Fatal(err)
	}
	runs, rows := relayRuns(t), relayReconciledRows(t)
	ids := buildBacklog(t, s, label)

	since := time.Now()
	during, err := createDuring(t, s, label, func() error {
		_, err := compose("restart", "rabbitmq")
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	ids = append(ids, during...)
	if c, err := statusCounts(ids); err == nil {
		t.Logf("%s: when the restart command returned (%d created during it): %s", label, len(during), formatCounts(c))
	}

	after := waitRunsAbove(t, runs)
	t.Logf("%s: %s %v -> %v, %.1f s after the restart began; %s grew by %v",
		label, reconcileRunsMetric, runs, after, time.Since(since).Seconds(), reconciledRowsMetric, relayReconciledRows(t)-rows)
	finishBrokerScenario(t, s, label, ids, before, since)
}

// The broker started with an empty data volume (ADR-0010 §1, "RabbitMQ wiped
// with its data and restarted"): with notifications queued in the broker,
// rabbitmq is stopped, its container and data volume removed, and it is
// started again. The relay and the worker declare the topology on the new
// broker before they publish and subscribe (no channel error in their logs),
// reconciliation returns the queued rows, whose messages are lost, to
// pending, every notification ends sent and the poison queues are empty.
func TestComposeE2E_RabbitMQWipedEveryNotificationSent(t *testing.T) {
	const label = "rabbitmq wiped"
	s := newStack(t)
	s.resetMock(t)
	t.Cleanup(func() { s.leaveClean(t) })

	volume := rabbitmqVolumeName(t)
	before, err := containerStates(observed...)
	if err != nil {
		t.Fatal(err)
	}
	runs, rows := relayRuns(t), relayReconciledRows(t)
	created := volumeCreated(t, volume)
	ids := buildBacklog(t, s, label)

	since := time.Now()
	broker := false
	t.Cleanup(func() {
		if !broker {
			if _, err := compose("up", "-d", "--no-deps", "--wait", "rabbitmq"); err != nil {
				t.Errorf("cleanup: start rabbitmq: %v", err)
			}
		}
	})
	during, err := createDuring(t, s, label, func() error {
		if _, err := compose("stop", "rabbitmq"); err != nil {
			return err
		}
		if _, err := compose("rm", "--force", "rabbitmq"); err != nil {
			return err
		}
		_, err := docker("volume", "rm", volume)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	ids = append(ids, during...)
	if c, err := statusCounts(ids); err == nil {
		t.Logf("%s: with the broker removed (%d created meanwhile): %s", label, len(during), formatCounts(c))
	}
	if _, err := compose("up", "-d", "--no-deps", "--wait", "rabbitmq"); err != nil {
		t.Fatal(err)
	}
	broker = true

	if now := volumeCreated(t, volume); now == created {
		t.Fatalf("%s: volume %s still has its creation time %s: it was not removed", label, volume, created)
	} else {
		t.Logf("%s: volume %s created %s, before the wipe %s", label, volume, now, created)
	}

	after := waitRunsAbove(t, runs)
	t.Logf("%s: %s %v -> %v, %.1f s after the stop; %s grew by %v",
		label, reconcileRunsMetric, runs, after, time.Since(since).Seconds(), reconciledRowsMetric, relayReconciledRows(t)-rows)
	finishBrokerScenario(t, s, label, ids, before, since)
}

// volumeCreated returns the creation time of a Docker volume.
func volumeCreated(t *testing.T, volume string) string {
	t.Helper()
	out, err := docker("volume", "inspect", "--format", "{{.CreatedAt}}", volume)
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(out)
}

// rabbitmqVolumeName returns the Docker name of the broker's data volume
// under the stack's project, from the resolved Compose configuration.
func rabbitmqVolumeName(t *testing.T) string {
	t.Helper()
	out, err := compose("config", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var cfg struct {
		Volumes map[string]struct {
			Name string `json:"name"`
		} `json:"volumes"`
	}
	if err := json.Unmarshal([]byte(out), &cfg); err != nil {
		t.Fatalf("decode the Compose configuration: %v", err)
	}
	v := cfg.Volumes[rabbitmqVolume].Name
	if v == "" {
		t.Fatalf("no volume %s in the Compose configuration", rabbitmqVolume)
	}
	return v
}

// The exit criterion "docker-compose run --rm relay requeue-stale" (D64):
// with the worker stopped, notifications stay queued; the queued_at of some
// of them is moved more than five minutes back and the relay is stopped.
// The command, run exactly as written, exits 0 and names the number of
// rows; exactly the back-dated rows are pending again and the others still
// queued. Then --all takes the rest. With the relay and the worker started
// again, every notification ends sent, each once at the provider.
func TestComposeE2E_RequeueStale(t *testing.T) {
	const (
		label    = "requeue-stale"
		total    = 10
		stale    = 4
		backdate = "10 minutes"
	)
	s := newStack(t)
	s.resetMock(t)
	t.Cleanup(func() { s.leaveClean(t) })
	stopped := false
	t.Cleanup(func() {
		if stopped {
			if _, err := compose("start", "relay", "worker"); err != nil {
				t.Errorf("cleanup: start relay and worker: %v", err)
			}
		}
	})

	stopped = true
	if _, err := compose("stop", "worker"); err != nil {
		t.Fatal(err)
	}
	ids := createN(t, s, createRequest{Channel: "sms", Recipient: fakePhone, Content: label, Priority: "high"}, total, label)
	waitCounts(t, ids, sendTimeout, "every notification queued", func(c map[string]int) bool { return c["queued"] == total })
	if _, err := compose("stop", "relay"); err != nil {
		t.Fatal(err)
	}

	old, fresh := ids[:stale], ids[stale:]
	arr, err := uuidArray(old)
	if err != nil {
		t.Fatal(err)
	}
	// Test data only: queued_at is not a status, and no transition happens.
	out, err := psql("WITH u AS (UPDATE notifications SET queued_at = queued_at - interval '" + backdate +
		"' WHERE status = 'queued' AND id = ANY(" + arr + ") RETURNING 1) SELECT count(*) FROM u")
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(out); got != fmt.Sprint(stale) {
		t.Fatalf("back-dating updated %q rows, want %d", got, stale)
	}

	// The command exactly as written; compose adds the stack's files and
	// project. compose returns an error for a non-zero exit code.
	out, err = compose("run", "--rm", "relay", "requeue-stale")
	if err != nil {
		t.Fatal(err)
	}
	if want := fmt.Sprintf("requeue-stale: %d notifications returned to pending", stale); !strings.Contains(out, want) {
		t.Fatalf("requeue-stale printed %q, want a line %q", lastLines(out), want)
	}
	t.Logf("docker-compose run --rm relay requeue-stale: %s", lastLines(out))
	st, err := statuses(ids)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range old {
		if st[id] != "pending" {
			t.Errorf("back-dated notification %s is %q after requeue-stale, want pending", id, st[id])
		}
	}
	for _, id := range fresh {
		if st[id] != "queued" {
			t.Errorf("recent notification %s is %q after requeue-stale, want queued", id, st[id])
		}
	}

	out, err = compose("run", "--rm", "relay", "requeue-stale", "--all")
	if err != nil {
		t.Fatal(err)
	}
	if want := fmt.Sprintf("requeue-stale: %d notifications returned to pending", total-stale); !strings.Contains(out, want) {
		t.Fatalf("requeue-stale --all printed %q, want a line %q", lastLines(out), want)
	}
	t.Logf("docker-compose run --rm relay requeue-stale --all: %s", lastLines(out))
	if c, err := statusCounts(ids); err != nil || c["pending"] != total {
		t.Fatalf("after requeue-stale --all: %s (error %v), want every notification pending", formatCounts(c), err)
	}

	if _, err := compose("start", "relay", "worker"); err != nil {
		t.Fatal(err)
	}
	stopped = false
	waitAllSent(t, ids, drainTimeout)
	rep, err := s.report()
	if err != nil {
		t.Fatal(err)
	}
	// The copies left in the broker and the copies published again meet the
	// claim guard: each notification is sent once.
	repeated, missing := keyCounts(rep, ids)
	if repeated != 0 || missing != 0 || rep.Total != total {
		t.Errorf("provider received %d requests for %d notifications: %d more than once, %d never; want each once",
			rep.Total, total, repeated, missing)
	}
}

// lastLines returns the output of a command without Compose's progress
// lines, for messages.
func lastLines(out string) string {
	var keep []string
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if l := strings.TrimSpace(line); l != "" && !strings.HasPrefix(l, "Container ") {
			keep = append(keep, l)
		}
	}
	return strings.Join(keep, "; ")
}

// The chaos scenario "Postgres restarted during sends" (ADR-0010 §1): a few
// hundred notifications are being delivered at the normal rate when the
// real postgres container is restarted. The test's own creates during the
// restart may get 503 (tolerated, counted); afterwards the API serves
// again, every accepted notification ends sent and none failed, the poison
// queues stay empty (a claim that fails during the outage holds its message
// and is retried in place, it is not rejected toward the delivery limit),
// and every service is up and healthy. Duplicates at the provider are
// reported, not failed, below a bound: the sends in flight at the restart.
func TestComposeE2E_PostgresRestartDuringSends(t *testing.T) {
	const (
		label = "postgres restart"
		// sms is about five seconds of sending at 98 per second; email and
		// push run beside it on their own pacers.
		sms, email, push = 500, 100, 100
		// restartAfter is the number of sms the mock must have received when
		// the restart begins: sending is under way.
		restartAfter = 100
	)
	s := newStack(t)
	s.resetMock(t)
	t.Cleanup(func() { s.leaveClean(t) })

	before, err := containerStates("api", "relay", "worker", "mockprovider", "rabbitmq", "redis")
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	ids = append(ids, createN(t, s, createRequest{Channel: "sms", Recipient: fakePhone, Content: label, Priority: "normal"}, sms, label)...)
	ids = append(ids, createN(t, s, createRequest{Channel: "email", Recipient: fakeEmail, Content: label, Priority: "normal"}, email, label)...)
	ids = append(ids, createN(t, s, createRequest{Channel: "push", Recipient: fakeDevice, Content: label, Priority: "normal"}, push, label)...)

	if !poll(sendTimeout, statusPollInterval, func() bool {
		rep, err := s.report()
		return err == nil && rep.Channels["sms"].Count >= restartAfter
	}) {
		t.Fatalf("the mock did not receive %d sms within %s", restartAfter, sendTimeout)
	}
	rep, err := s.report()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%s: the restart begins with %d of %d requests at the mock", label, rep.Total, len(ids))

	since := time.Now()
	var (
		mu       sync.Mutex
		accepted []string
		outcomes = map[string]int{}
	)
	try := func() {
		code, _, body, err := s.do(http.MethodPost, s.apiURL+"/v1/notifications",
			createRequest{Channel: "sms", Recipient: fakePhone, Content: label, Priority: "normal"})
		mu.Lock()
		defer mu.Unlock()
		switch {
		case err != nil:
			outcomes["transport error"]++ // not tracked: it may or may not exist
		case code == http.StatusAccepted:
			var n notification
			if json.Unmarshal(body, &n) == nil && n.ID != "" {
				accepted = append(accepted, n.ID)
			}
			outcomes["202"]++
		default:
			outcomes[fmt.Sprint(code)]++
		}
	}
	done := make(chan error, 1)
	go func() {
		_, err := compose("restart", "postgres")
		done <- err
	}()
	var restartErr error
	tick := time.NewTicker(faultCreateInterval)
	defer tick.Stop()
loop:
	for {
		try()
		select {
		case restartErr = <-done:
			break loop
		case <-tick.C:
		}
	}
	if restartErr != nil {
		t.Fatal(restartErr)
	}
	restarted := time.Since(since)

	// The API serves again: it is ready, and a create made after the restart
	// command returned is accepted.
	mu.Lock()
	acceptedBefore := outcomes["202"]
	mu.Unlock()
	if !poll(time.Minute, statusPollInterval, func() bool {
		try()
		mu.Lock()
		defer mu.Unlock()
		return outcomes["202"] > acceptedBefore
	}) {
		t.Fatalf("the API did not accept a notification within a minute after the restart: %v", outcomes)
	}
	if code, _, body, err := s.do(http.MethodGet, s.apiURL+"/readyz", nil); err != nil || code != http.StatusOK {
		t.Errorf("GET /readyz after the restart: status %d, error %v: %s", code, err, body)
	}
	for range createdDuringFault {
		try()
	}
	mu.Lock()
	t.Logf("%s: restart command took %.1f s; the test's creates during and after it: %v", label, restarted.Seconds(), outcomes)
	for k := range outcomes {
		if k != "202" && k != "503" && k != "transport error" {
			t.Errorf("%s: the API answered %s %d times during the restart; want only 202 or 503", label, k, outcomes[k])
		}
	}
	ids = append(ids, accepted...)
	mu.Unlock()

	waitAllSent(t, ids, drainTimeout)
	t.Logf("%s: all %d accepted notifications sent %.1f s after the restart began", label, len(ids), time.Since(since).Seconds())

	checkNoServiceExited(t, before)
	if err := waitHealthy(healthyTimeout); err != nil {
		t.Error(err)
	}
	if err := waitDispatchEmpty(healthyTimeout); err != nil {
		t.Error(err)
	}
	depths := checkPoisonEmpty(t)
	t.Logf("%s: queue depths after the drain: %v", label, depths)

	lines, err := serviceLogsSince(since, "worker")
	if err != nil {
		t.Fatal(err)
	}
	msgs := countMessages(lines)
	t.Logf("%s: worker log: claim retried %d, claim succeeded after retries %d, claim requeued at shutdown %d; "+
		"final write retried %d, final write succeeded after retries %d, final write lost %d; "+
		"abort attempt retried %d; late success applied %d",
		label,
		msgs["worker: claim failed; retrying with back-off"], msgs["worker: claim succeeded after retries"],
		msgs["worker: claim still failing at shutdown; message requeued"],
		msgs["worker: final write failed; retrying with back-off"], msgs["worker: final write succeeded after retries"],
		msgs["worker: final write lost; the lease sweeper recovers the notification"],
		msgs["worker: abort attempt failed; retrying with back-off"],
		msgs["worker: late success applied: the lease had expired, no newer attempt had started"])

	rep, err = s.report()
	if err != nil {
		t.Fatal(err)
	}
	repeated, missing := keyCounts(rep, ids)
	// A send is repeated only when its result is not written before the
	// attempt's lease expires (ADR-0004 §4). The final write is retried for
	// 60 s, much longer than the restart, so only the sends in flight at the
	// restart can be affected: at most the worker's concurrency per channel.
	bound := workerConcurrency(t) * len(channels)
	t.Logf("%s: provider received %d requests for %d accepted notifications; %d reached it more than once (reported, bound %d), %d never",
		label, rep.Total, len(ids), repeated, bound, missing)
	if missing > 0 {
		t.Errorf("%s: %d sent notifications never reached the provider", label, missing)
	}
	if repeated > bound {
		t.Errorf("%s: %d notifications reached the provider more than once, more than the %d sends that can be in flight", label, repeated, bound)
	}
}
