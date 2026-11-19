//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/queue"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/relay"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

// The recovery paths through the wired roles (ADR-0003 §3 and §4, D5, D14,
// D18): reconciliation on every (re)connection of the relay, the lease
// sweeper and the late success of transition 12, and the worker's final
// write during a database outage.

const (
	logReconciled  = "relay: reconciliation ran"
	logLateSuccess = "worker: late success applied: the lease had expired, no newer attempt had started"
	logWriteFailed = "worker: final write failed; retrying with back-off"
	logWriteRetry  = "worker: final write succeeded after retries"
)

// counterValue returns the value of the counter name on reg.
func counterValue(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() == name && len(mf.GetMetric()) == 1 {
			return mf.GetMetric()[0].GetCounter().GetValue()
		}
	}
	t.Fatalf("counter %s not found", name)
	return 0
}

func (e *env) runs(reg *prometheus.Registry) float64 {
	return counterValue(e.t, reg, relay.ReconciliationRunsMetric)
}

// wipeBroker deletes the test's vhost and creates it again, empty, while the
// relay cannot reach the broker: every message in it is lost.
func (e *env) wipeBroker(proxy *testutil.Proxy) {
	e.t.Helper()
	proxy.Break()
	testutil.WipeRabbitMQVhost(e.t, e.amqpURL)
	if testutil.RabbitMQQueueExists(e.t, e.amqpURL, queue.DispatchQueue(domain.ChannelSMS)) {
		e.t.Fatal("the wiped vhost still has the dispatch queue")
	}
}

// awaitTopology waits until the relay, the only role connected, has
// declared the topology on the recreated vhost, and opens a new admin
// channel (the old one died with the vhost).
func (e *env) awaitTopology() {
	e.t.Helper()
	testutil.Eventually(e.t, func() bool {
		return testutil.RabbitMQQueueExists(e.t, e.amqpURL, queue.DispatchQueue(domain.ChannelSMS))
	}, waitTimeout)
	e.admin = adminChannel(e.t, e.amqpURL)
}

// queuedWithMessages waits until n rows are queued and n messages are ready
// in the SMS dispatch queue (no worker runs).
func (e *env) queuedWithMessages(n int) {
	e.t.Helper()
	testutil.Eventually(e.t, func() bool { return e.countStatus(domain.StatusQueued) == n && e.ready() == n }, waitTimeout)
}

// requireCleanRelayLog fails the test if a lane failed or a channel was
// closed by the broker: the topology existed before every publish.
func (e *env) requireCleanRelayLog() {
	e.t.Helper()
	for _, msg := range []string{"relay: lane failed", "rabbitmq channel closed with an error"} {
		if n := len(logLines(e.logs, msg)); n != 0 {
			e.t.Errorf("%d %q lines: a publish went out before the topology existed", n, msg)
		}
	}
}

// reconcileVars are the relay's settings for the reconnection scenarios: its
// broker connection runs through proxyURL with a recovery budget that
// outlasts every outage of the test.
func reconcileVars(proxyURL string, window time.Duration) map[string]string {
	return map[string]string{
		"RABBITMQ_URL":              proxyURL,
		"AMQP_RECOVERY_MAX_RETRIES": "1000",
		"RECONCILE_MIN_INTERVAL":    millis(window),
	}
}

// Reconnection (transition 8): notifications are queued and their messages
// are lost with the broker's data while the relay is disconnected. After the
// reconnection the relay declares the topology again, reconciles (the
// counter grows, the rows return to pending), publishes them again, and
// every notification is sent, each idempotency key once.
func TestE2E_ReconciliationAfterReconnection(t *testing.T) {
	const n = 5
	e := newEnv(t)
	proxy := testutil.NewProxy(t, e.amqpURL)
	cfg := e.config(reconcileVars(proxy.URL(), 100*time.Millisecond))
	_, reg := e.startRelayWith(cfg, nil)
	testutil.Eventually(t, func() bool { return e.runs(reg) == 1 }, waitTimeout) // the first connection

	ids := e.create(n)
	e.queuedWithMessages(n)
	e.wipeBroker(proxy)
	proxy.Restore(t)

	testutil.Eventually(t, func() bool { return e.runs(reg) == 2 }, waitTimeout)
	if got := counterValue(t, reg, relay.ReconciledRowsMetric); got != n {
		t.Errorf("%s = %v, want %d", relay.ReconciledRowsMetric, got, n)
	}
	lines := logLines(e.logs, logReconciled)
	if len(lines) != 2 || lines[0]["reason"] != "first connection" || lines[0]["rows"] != float64(0) ||
		lines[1]["reason"] != "reconnection" || lines[1]["rows"] != float64(n) {
		t.Errorf("reconciliation lines %v, want the first connection with 0 rows and a reconnection with %d", lines, n)
	}
	// Published again, into the recreated queues.
	e.awaitTopology()
	e.queuedWithMessages(n)
	for _, id := range ids {
		// created 1, queued 2, pending 3 (transition 8), queued 4
		if v := e.get(id).Version; v != 4 {
			t.Errorf("notification %s has version %d, want 4: created, queued, reconciled, queued", id, v)
		}
	}

	_, w := e.startWorker(cfg, 0) // on its own connection to the vhost, not through the proxy
	testutil.Eventually(t, func() bool { return e.countStatus(domain.StatusSent) == n }, waitTimeout)
	e.requireSentOnce(ids)
	e.requireCleanRelayLog()
	if !e.queueEmpty(w) {
		t.Error("the dispatch queue is not empty")
	}
}

// Deferred trigger: the first reconnection after the window runs at once;
// a second reconnection inside the window of that run is deferred and runs
// once when the window ends. The rows that became queued between the two
// reconnections, and whose messages the second one lost, are recovered by
// the deferred run.
func TestE2E_ReconciliationDeferredInsideWindow(t *testing.T) {
	const window = 2_000 * time.Millisecond
	e := newEnv(t)
	proxy := testutil.NewProxy(t, e.amqpURL)
	cfg := e.config(reconcileVars(proxy.URL(), window))
	_, reg := e.startRelayWith(cfg, nil)
	testutil.Eventually(t, func() bool { return e.runs(reg) == 1 }, waitTimeout)
	firstSeen := time.Now() // after the first run started
	testutil.Eventually(t, func() bool { return time.Since(firstSeen) >= window }, window+waitTimeout)

	// Reconnection 1, outside the window of the first run: runs at once.
	a := e.create(2)
	e.queuedWithMessages(2)
	e.wipeBroker(proxy)
	restored1 := time.Now() // before the run of reconnection 1 starts
	proxy.Restore(t)
	testutil.Eventually(t, func() bool { return e.runs(reg) == 2 }, window/2)
	e.awaitTopology()
	e.queuedWithMessages(2) // a published again

	// Rows queued between the two reconnections, then lost.
	b := e.create(2)
	e.queuedWithMessages(4)
	e.wipeBroker(proxy)
	proxy.Restore(t)
	e.awaitTopology() // the relay has reconnected
	if since := time.Since(restored1); since >= window {
		t.Fatalf("the second reconnection came %s after the first, not inside the window of %s; the test is too slow to show the deferral", since, window)
	}
	if got := e.runs(reg); got != 2 {
		t.Fatalf("%v runs right after the second reconnection, want 2: the trigger inside the window must be deferred", got)
	}
	if got := e.countStatus(domain.StatusQueued); got != 4 {
		t.Errorf("%d rows queued while the trigger is deferred, want 4", got)
	}

	testutil.Eventually(t, func() bool { return e.runs(reg) == 3 }, window+waitTimeout)
	if since := time.Since(restored1); since < window {
		t.Errorf("the deferred run came %s after the first reconnection, before the window of %s ended", since, window)
	}
	if got := counterValue(t, reg, relay.ReconciledRowsMetric); got != 2+4 {
		t.Errorf("%s = %v, want 6 (a, then a and b)", relay.ReconciledRowsMetric, got)
	}
	e.queuedWithMessages(4)
	// The window holds: no further run.
	if got := e.runs(reg); got != 3 {
		t.Errorf("%v runs, want 3", got)
	}

	_, w := e.startWorker(cfg, 0) // on its own connection to the vhost, not through the proxy
	testutil.Eventually(t, func() bool { return e.countStatus(domain.StatusSent) == 4 }, waitTimeout)
	e.requireSentOnce(append(a, b...))
	e.requireCleanRelayLog()
	if !e.queueEmpty(w) {
		t.Error("the dispatch queue is not empty")
	}
}

// Start-up: rows left queued by a relay that died after its commit, their
// messages lost, are reconciled on the first connection of the relay
// binary's service and sent.
func TestE2E_ReconciliationAtStartup(t *testing.T) {
	const n = 3
	e := newEnv(t)
	cfg := e.config(nil)
	ids := e.create(n)
	// A lane transaction whose publish was confirmed, but whose messages are
	// not in the broker.
	ctx, cancel := e.ctx()
	defer cancel()
	lane := store.Lane{Channel: domain.ChannelSMS, Priority: domain.PriorityNormal}
	if _, err := e.store.PublishLane(ctx, lane, n, func(context.Context, []store.DueRow) ([]uuid.UUID, error) {
		return ids, nil
	}); err != nil {
		t.Fatalf("publish lane: %v", err)
	}
	if got := e.countStatus(domain.StatusQueued); got != n {
		t.Fatalf("%d rows queued, want %d", got, n)
	}

	_, w := e.startWorker(cfg, 0)
	e.startRelay(cfg)
	testutil.Eventually(t, func() bool { return e.countStatus(domain.StatusSent) == n }, waitTimeout)
	lines := logLines(e.logs, logReconciled)
	if len(lines) != 1 || lines[0]["reason"] != "first connection" || lines[0]["rows"] != float64(n) {
		t.Errorf("reconciliation lines %v, want one for the first connection with %d rows", lines, n)
	}
	e.requireSentOnce(ids)
	if !e.queueEmpty(w) {
		t.Error("the dispatch queue is not empty")
	}
}

// recoveryVars are the settings of the lease and final-write scenarios: a
// sweeper every 20 ms and a back-off of 50 ms, so that a swept row is
// published again at once.
func recoveryVars(concurrency string) map[string]string {
	return map[string]string{
		"WORKER_CONCURRENCY":   concurrency,
		"LEASE_SWEEP_INTERVAL": "20ms",
		"RETRY_NORMAL_BASE":    "50ms",
		"RETRY_NORMAL_CAP":     "50ms",
		"RETRY_MIN_DELAY":      "50ms",
	}
}

// expireLease back-dates the lease of the notification's attempt (test
// setup): the worker's configuration keeps the provider timeout inside the
// lease, so a send held at the provider cannot outlast the lease on its own.
func (e *env) expireLease(id uuid.UUID) {
	e.t.Helper()
	ctx, cancel := e.ctx()
	defer cancel()
	if _, err := e.pool.Exec(ctx, `UPDATE notifications SET lease_expires_at = now() - interval '1 millisecond' WHERE id = $1 AND status = 'processing'`, id); err != nil {
		e.t.Fatalf("expire lease: %v", err)
	}
}

// attemptOutcomes returns the outcomes of the notification's attempts, in
// order; an attempt in flight has an empty outcome.
func (e *env) attemptOutcomes(id uuid.UUID) []domain.Outcome {
	e.t.Helper()
	var out []domain.Outcome
	for _, a := range e.attempts(id) {
		out = append(out, a.Outcome)
	}
	return out
}

// Late success after the lease expired (transition 12, worker side): the
// send is held at the provider past its lease; the lease sweeper returns the
// row to retrying (attempt unknown) and the relay publishes it again; then
// the provider answers 202. The row ends sent through transition 12 from
// queued, the attempt row holds the true outcome, the provider received one
// request, and the copy the relay republished is delivered afterwards and
// dropped by the claim guard (acked, no new attempt). With one concurrency
// slot, held by the late send, the copy cannot be claimed before the late
// result lands.
func TestE2E_LateSuccessAfterLeaseSweep(t *testing.T) {
	e := newEnv(t)
	amqpProxy := testutil.NewProxy(t, e.amqpURL)
	tap := amqpProxy.Tap()
	cfg := e.config(recoveryVars("1"))
	logs := workerLogs(t)
	release := e.prov.block(t)
	w := e.startWorkerVia(cfg, amqpProxy.URL(), logs)
	e.startRelayWith(cfg, nil)

	id := e.create(1)[0]
	testutil.Eventually(t, func() bool { return e.prov.count() == 1 }, waitTimeout)
	e.expireLease(id)
	// Swept (attempt 1 unknown, retrying) and published again.
	testutil.Eventually(t, func() bool {
		o := e.attemptOutcomes(id)
		return e.get(id).Status == domain.StatusQueued && len(o) == 1 && o[0] == domain.OutcomeUnknown
	}, waitTimeout)

	release()
	e.waitStatus(id, domain.StatusSent)
	n := e.get(id)
	if n.AttemptCount != 1 || n.Failures != 1 {
		t.Errorf("attempt_count %d, failures %d; want 1 and 1 (the sweep consumed budget)", n.AttemptCount, n.Failures)
	}
	if o := e.attemptOutcomes(id); len(o) != 1 || o[0] != domain.OutcomeSuccess {
		t.Errorf("attempts %v, want one, corrected to success", o)
	}
	late := logLines(logs, logLateSuccess)
	if len(late) != 1 || late[0]["from"] != string(domain.StatusQueued) {
		t.Errorf("late-success lines %v, want one from queued", late)
	}
	// The republished copy is delivered and acked without a claim.
	testutil.Eventually(t, func() bool {
		d := deliveriesOf(tap, id)
		return len(d) == 2 && d[0].Answer == "ack" && d[1].Answer == "ack"
	}, waitTimeout)
	if got := e.get(id).AttemptCount; got != 1 {
		t.Errorf("attempt_count %d after the copy was delivered, want 1: the claim guard must drop it", got)
	}
	e.requireSentOnce([]uuid.UUID{id})
	requireCleanWire(t, tap)
	if !e.queueEmpty(w) {
		t.Error("the dispatch queue is not empty")
	}
}

// A newer attempt starts before the late result lands: the late success
// changes only its own attempt row (unknown → success); the notification
// stays with attempt 2, which then sends it through transition 4. The
// provider sees the key twice: the cost of a lease that expired while the
// provider was still working.
func TestE2E_LateResultAfterNewerAttempt(t *testing.T) {
	e := newEnv(t)
	cfg := e.config(recoveryVars("2"))
	release1 := e.prov.block(t)
	_, w := e.startWorker(cfg, 0)
	e.startRelayWith(cfg, nil)

	id := e.create(1)[0]
	testutil.Eventually(t, func() bool { return e.prov.count() == 1 }, waitTimeout)
	release2 := e.prov.block(t) // the next request waits on its own release
	e.expireLease(id)
	// Swept, published again, claimed by attempt 2, whose send is held.
	testutil.Eventually(t, func() bool { return e.prov.count() == 2 }, waitTimeout)
	before := e.get(id)
	if before.Status != domain.StatusProcessing || before.AttemptCount != 2 {
		t.Fatalf("status %s, attempt_count %d; want processing with attempt 2", before.Status, before.AttemptCount)
	}

	release1()
	testutil.Eventually(t, func() bool {
		o := e.attemptOutcomes(id)
		return len(o) == 2 && o[0] == domain.OutcomeSuccess
	}, waitTimeout)
	after := e.get(id)
	if after.Status != domain.StatusProcessing || after.AttemptCount != 2 || after.Version != before.Version {
		t.Errorf("after the late result: status %s, attempt_count %d, version %d; want processing, 2, %d unchanged",
			after.Status, after.AttemptCount, after.Version, before.Version)
	}
	if o := e.attemptOutcomes(id); o[1] != domain.OutcomeInFlight {
		t.Errorf("attempt 2 is %q, want in flight", o[1])
	}

	release2()
	e.waitStatus(id, domain.StatusSent)
	if o := e.attemptOutcomes(id); len(o) != 2 || o[0] != domain.OutcomeSuccess || o[1] != domain.OutcomeSuccess {
		t.Errorf("attempts %v, want two successes", o)
	}
	if n := len(logLines(e.logs, logLateSuccess)); n != 0 {
		t.Errorf("%d late-success lines, want none: a newer attempt had started", n)
	}
	if got := keys(e.prov.requests()); len(got) != 1 || got[id.String()] != 2 {
		t.Errorf("provider requests by key %v, want two for %s", got, id)
	}
	if !e.queueEmpty(w) {
		t.Error("the dispatch queue is not empty")
	}
}

// Final write during a database outage, inside the lease: the worker's
// connection to Postgres is cut while the send is held at the provider; the
// provider answers; the final write is retried and lands when the
// connection is back. The row ends sent through transition 4 with one
// attempt, and the provider saw one request.
func TestE2E_FinalWriteRetriedThroughOutage(t *testing.T) {
	const outage = 300 * time.Millisecond
	e := newEnv(t)
	db := testutil.NewProxy(t, e.pool.Config().ConnString())
	vars := recoveryVars("1")
	vars["CLAIM_RETRY_BASE"] = "10ms"
	vars["CLAIM_RETRY_MAX"] = "100ms"
	vars["FINAL_WRITE_RETRY_WINDOW"] = "10000ms"
	relayCfg := e.config(vars)
	vars["DATABASE_URL"] = db.URL()
	release := e.prov.block(t)
	_, w := e.startWorker(e.config(vars), 0)
	e.startRelayWith(relayCfg, nil)

	id := e.create(1)[0]
	testutil.Eventually(t, func() bool { return e.prov.count() == 1 }, waitTimeout)
	db.Break()
	release()
	testutil.Eventually(t, func() bool { return len(logLines(e.logs, logWriteFailed)) == 1 }, waitTimeout)
	cut := time.Now()
	testutil.Eventually(t, func() bool { return time.Since(cut) >= outage }, outage+waitTimeout)
	if got := e.get(id).Status; got != domain.StatusProcessing {
		t.Fatalf("status %s during the outage, want processing", got)
	}
	db.Restore(t)

	e.waitStatus(id, domain.StatusSent)
	n := e.get(id)
	if n.AttemptCount != 1 || n.Failures != 0 {
		t.Errorf("attempt_count %d, failures %d; want 1 and 0", n.AttemptCount, n.Failures)
	}
	if o := e.attemptOutcomes(id); len(o) != 1 || o[0] != domain.OutcomeSuccess {
		t.Errorf("attempts %v, want one success", o)
	}
	if got := len(logLines(e.logs, logWriteRetry)); got != 1 {
		t.Errorf("%d lines %q, want 1", got, logWriteRetry)
	}
	if got := len(logLines(e.logs, logLateSuccess)); got != 0 {
		t.Errorf("%d late-success lines, want none: the write landed inside the lease", got)
	}
	e.requireSentOnce([]uuid.UUID{id})
	if !e.queueEmpty(w) {
		t.Error("the dispatch queue is not empty")
	}
}

// Final write during a database outage that outlasts the lease: the lease
// sweeper (in the relay, whose database connection is up) takes the row and
// the relay publishes it again; the worker's final write lands when the
// connection is back, through transition 12; the copy is dropped by the
// claim guard, and the provider saw one request. Lease 2,500 ms, provider
// timeout 2,000 ms and write margin 100 ms give a send deadline of 400 ms.
func TestE2E_FinalWriteOutageOutlastsLease(t *testing.T) {
	e := newEnv(t)
	db := testutil.NewProxy(t, e.pool.Config().ConnString())
	vars := recoveryVars("1")
	vars["LEASE_DURATION"] = "2500ms"
	vars["PROVIDER_TIMEOUT"] = "2000ms"
	vars["CLAIM_RETRY_BASE"] = "10ms"
	vars["CLAIM_RETRY_MAX"] = "100ms"
	vars["FINAL_WRITE_RETRY_WINDOW"] = "20000ms"
	relayCfg := e.config(vars)
	vars["DATABASE_URL"] = db.URL()
	release := e.prov.block(t)
	_, w := e.startWorker(e.config(vars), 100*time.Millisecond)
	e.startRelayWith(relayCfg, nil)

	id := e.create(1)[0]
	testutil.Eventually(t, func() bool { return e.prov.count() == 1 }, waitTimeout)
	db.Break()
	release()
	testutil.Eventually(t, func() bool { return len(logLines(e.logs, logWriteFailed)) == 1 }, waitTimeout)
	// The lease ends during the outage: swept and published again.
	testutil.Eventually(t, func() bool {
		o := e.attemptOutcomes(id)
		return e.get(id).Status == domain.StatusQueued && len(o) == 1 && o[0] == domain.OutcomeUnknown
	}, waitTimeout)
	db.Restore(t)

	e.waitStatus(id, domain.StatusSent)
	n := e.get(id)
	if n.AttemptCount != 1 {
		t.Errorf("attempt_count %d, want 1: the copy must be dropped by the claim guard", n.AttemptCount)
	}
	if o := e.attemptOutcomes(id); len(o) != 1 || o[0] != domain.OutcomeSuccess {
		t.Errorf("attempts %v, want one, corrected to success", o)
	}
	late := logLines(e.logs, logLateSuccess)
	if len(late) != 1 || late[0]["from"] != string(domain.StatusQueued) {
		t.Errorf("late-success lines %v, want one from queued", late)
	}
	if got := len(logLines(e.logs, logWriteRetry)); got != 1 {
		t.Errorf("%d lines %q, want 1", got, logWriteRetry)
	}
	e.requireSentOnce([]uuid.UUID{id})
	if !e.queueEmpty(w) {
		t.Error("the dispatch queue is not empty")
	}
	if got := e.get(id).AttemptCount; got != 1 {
		t.Errorf("attempt_count %d after the queue drained, want 1", got)
	}
}
