//go:build integration

package integration

import (
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/config"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/queue"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/worker"
)

// The connection epoch (D76), the resubscription after a server-side cancel
// (D8) and the topology on every (re)connection (D2), through the wired
// roles. The worker's broker connection runs through a TCP proxy that reads
// the AMQP frames (testutil.AMQPTap): every ack, nack and reject is checked
// on the wire against the deliveries of its own channel session.

const (
	logAckStale  = "worker: ack not sent: the delivery is from an older connection epoch"
	logNackStale = "worker: nack not sent: the delivery is from an older connection epoch"
	logDropped   = "worker: deliveries from an older connection epoch dropped from the buffer"
	// logDroppedAtPush is the line for one such delivery dropped when the
	// consumer hands it to the worker.
	logDroppedAtPush = "worker: delivery from an older connection epoch dropped"
	logClaimFailed   = "worker: claim failed; retrying with back-off"
	logResubscribe   = "consumer cancelled by the broker; subscribing again"
)

// workerLogs returns a log buffer for a worker role; it is printed when the
// test fails.
func workerLogs(t *testing.T) *lockedBuffer {
	t.Helper()
	logs := &lockedBuffer{}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("worker logs:\n%s", logs.String())
		}
	})
	return logs
}

// startWorkerVia wires and runs the worker role as startWorker does, on a
// broker connection to amqpURL (a proxy), logging to logs.
func (e *env) startWorkerVia(cfg config.Config, amqpURL string, logs *lockedBuffer) *running {
	e.t.Helper()
	log := slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	conn, err := queue.Dial(amqpURL, 20*time.Millisecond, 1000, log)
	if err != nil {
		e.t.Fatalf("dial: %v", err)
	}
	e.t.Cleanup(func() { _ = conn.Close() })
	role, err := worker.NewRole(cfg, worker.Resources{
		Pool:     e.rolePool(cfg),
		Conn:     conn,
		Redis:    e.rdb,
		Registry: prometheus.NewRegistry(),
	}, log)
	if err != nil {
		e.t.Fatalf("worker role: %v", err)
	}
	return start(e.t, role.Run)
}

// logLines returns the JSON log lines with message msg.
func logLines(logs *lockedBuffer, msg string) []map[string]any {
	var out []map[string]any
	for line := range strings.SplitSeq(logs.String(), "\n") {
		var l map[string]any
		if json.Unmarshal([]byte(line), &l) == nil && l["msg"] == msg {
			out = append(out, l)
		}
	}
	return out
}

// droppedFromBuffer counts the deliveries of an older connection epoch that
// the worker forgot without an answer: the counts of the buffer-purge lines,
// and the deliveries it dropped when the consumer handed them over. A
// delivery the broker sent just before the connection broke can still be on
// its way to the buffer when the epoch advances; the worker then drops it at
// the hand-over, which is the same rule (D76) at another place. Seen in CI in
// P6: of four held deliveries three were purged from the buffer and one was
// dropped at the hand-over, 2.5 ms after it had been published.
func droppedFromBuffer(logs *lockedBuffer) int {
	n := len(logLines(logs, logDroppedAtPush))
	for _, l := range logLines(logs, logDropped) {
		if c, ok := l["count"].(float64); ok {
			n += int(c)
		}
	}
	return n
}

// deliveriesOf returns the deliveries of id seen on the wire.
func deliveriesOf(tap *testutil.AMQPTap, id uuid.UUID) []testutil.TapDelivery {
	var out []testutil.TapDelivery
	for _, d := range tap.Deliveries() {
		if d.Body == id.String() {
			out = append(out, d)
		}
	}
	return out
}

// answersOf returns the answers on the wire that name a delivery of id.
func answersOf(tap *testutil.AMQPTap, id uuid.UUID) []testutil.TapAnswer {
	var out []testutil.TapAnswer
	for _, a := range tap.Answers() {
		if a.Body == id.String() {
			out = append(out, a)
		}
	}
	return out
}

// unansweredOn returns the deliveries on TCP connection conn that have no
// answer on the wire.
func unansweredOn(tap *testutil.AMQPTap, conn int) []testutil.TapDelivery {
	var out []testutil.TapDelivery
	for _, d := range tap.Deliveries() {
		if d.Conn == conn && d.Answer == "" {
			out = append(out, d)
		}
	}
	return out
}

func deliveredOn(tap *testutil.AMQPTap, conn int, id string) bool {
	for _, d := range tap.Deliveries() {
		if d.Conn == conn && d.Body == id {
			return true
		}
	}
	return false
}

// requireCleanWire fails the test if an answer on the wire named no
// unanswered delivery of its own channel session (an ack, nack or reject
// with a delivery tag from an older epoch), or if the broker closed a channel
// or the connection (an answer with an unknown delivery tag closes the
// channel with 406 PRECONDITION_FAILED).
func requireCleanWire(t *testing.T, tap *testutil.AMQPTap) {
	t.Helper()
	v, closes := tap.Violations(), tap.BrokerCloses()
	if len(v) != 0 || len(closes) != 0 {
		t.Fatalf("answers on the wire that name no unanswered delivery of their own session:\n%s\nchannels and connections closed by the broker:\n%s",
			strings.Join(v, "\n"), strings.Join(closes, "\n"))
	}
}

// requireSentOnce checks that every id reached the provider exactly once.
func (e *env) requireSentOnce(ids []uuid.UUID) {
	e.t.Helper()
	byKey := keys(e.prov.requests())
	for _, id := range ids {
		if byKey[id.String()] != 1 {
			e.t.Errorf("notification %s reached the provider %d times, want once", id, byKey[id.String()])
		}
	}
	if n := e.prov.count(); n != len(ids) {
		e.t.Errorf("the provider received %d requests, want %d", n, len(ids))
	}
}

// TestWorker_ConnectionLossWithBufferedMessages (ADR-0010, D76): the worker's
// broker connection is broken while four sends are in flight at the provider
// (claimed and acked), two claims are in flight (committed in the database,
// their results held back by a paused database proxy, so not yet acked) and
// four deliveries wait unacknowledged in the local buffer. Then the
// connection is restored, the claims return, and the provider is released.
//
// Proved on the wire: no ack, nack or reject names a delivery of an older
// session (an ack of the two late claims with their old tags would name
// deliveries of the new session); every delivery is either acked once on the
// session it arrived on or delivered again; each notification is acked
// exactly once; nothing is nacked or rejected. The two late claims stand and
// their sends go ahead without an ack; the four buffered deliveries leave the
// buffer unanswered. Every notification ends sent and the provider received
// each idempotency key once: the copies of the two late claims are dropped by
// the claim guard, and the in-flight sends had been acked before the break,
// so the broker had nothing of theirs to redeliver. The queue ends empty with
// nothing left unacknowledged.
func TestWorker_ConnectionLossWithBufferedMessages(t *testing.T) {
	const (
		concurrency = 6 // = prefetch
		atProvider  = 4
		claiming    = concurrency - atProvider
		buffered    = 4
		total       = 12
	)
	e := newEnv(t)
	amqpProxy := testutil.NewProxy(t, e.amqpURL)
	tap := amqpProxy.Tap()
	db := testutil.NewProxy(t, e.pool.Config().ConnString())
	vars := map[string]string{"WORKER_CONCURRENCY": "6"}
	relayCfg := e.config(vars)
	vars["DATABASE_URL"] = db.URL()
	logs := workerLogs(t)
	release := e.prov.block(t)
	w := e.startWorkerVia(e.config(vars), amqpProxy.URL(), logs)
	e.startRelay(relayCfg) // on its own connections, not through the proxies

	ids := e.create(atProvider)
	testutil.Eventually(t, func() bool { return e.prov.count() == atProvider }, waitTimeout)
	// The database's answers to the worker are held back from now on.
	db.Pause()
	ids = append(ids, e.create(total-atProvider)...)
	// Two claims have committed; their results do not reach the worker.
	testutil.Eventually(t, func() bool { return e.countStatus(domain.StatusProcessing) == concurrency }, waitTimeout)
	testutil.Eventually(t, func() bool { return len(unansweredOn(tap, 1)) == claiming+buffered }, waitTimeout)
	held := unansweredOn(tap, 1)

	amqpProxy.Break()
	amqpProxy.Restore(t)
	// The worker reconnects and subscribes again; the broker delivers the
	// held messages again; the stale ones leave the buffer unanswered.
	testutil.Eventually(t, func() bool {
		for _, d := range held {
			if !deliveredOn(tap, 2, d.Body) {
				return false
			}
		}
		return true
	}, waitTimeout)
	testutil.Eventually(t, func() bool { return droppedFromBuffer(logs) == buffered }, waitTimeout)

	// The late claims return: their acks are not sent, their sends go ahead.
	db.Resume()
	if n := e.countStatus(domain.StatusSent); n != 0 {
		t.Fatalf("%d sent before the provider was released", n)
	}
	release()
	// Not fatal on a timeout: the wire tells what went wrong.
	if !testutil.Within(func() bool { return e.countStatus(domain.StatusSent) == total }, waitTimeout) {
		t.Errorf("%d of %d notifications sent; %d still queued", e.countStatus(domain.StatusSent), total, e.countStatus(domain.StatusQueued))
	}
	testutil.Within(func() bool { return len(tap.Answers()) >= total }, waitTimeout)

	requireCleanWire(t, tap)
	if n := len(logLines(logs, logAckStale)); n != claiming {
		t.Errorf("%d stale acks refused, want %d (the late claims')", n, claiming)
	}
	if tap.Conns() != 2 {
		t.Errorf("%d broker connections, want 2", tap.Conns())
	}
	for _, id := range ids {
		answers := answersOf(tap, id)
		if len(answers) != 1 || answers[0].Kind != "ack" {
			t.Errorf("notification %s: answers on the wire %+v, want exactly one ack", id, answers)
		}
	}
	// Every delivery on the first connection was acked there or delivered
	// again on the second; the held ones were not answered on either side
	// of the break with their old tags.
	for _, d := range tap.Deliveries() {
		if d.Conn == 1 && d.Answer == "" && !deliveredOn(tap, 2, d.Body) {
			t.Errorf("delivery %s (tag %d) on the first connection was neither acked there nor delivered again", d.Body, d.Tag)
		}
	}
	if n := len(unansweredOn(tap, 1)); n != claiming+buffered {
		t.Errorf("%d deliveries left unanswered on the first connection, want the %d held ones", n, claiming+buffered)
	}
	e.requireSentOnce(ids)
	if n := len(logLines(logs, logNackStale)); n != 0 {
		t.Errorf("%d stale nacks refused, want 0", n)
	}
	if !e.queueEmpty(w) {
		t.Error("the dispatch queue is not empty")
	}
	if len(unansweredOn(tap, 2)) != 0 {
		t.Errorf("deliveries left unanswered on the second connection: %+v", unansweredOn(tap, 2))
	}
}

// claimHeldAcrossReconnect sets up a delivery held through a long claim
// retry across a broker reconnect: the worker's database connection is cut,
// one notification is created, its claim fails and is retried in place;
// then the worker's broker connection is broken and restored. It returns
// when the message has been delivered again on the new connection (it waits
// in the buffer: the held dispatch has the only concurrency slot).
func claimHeldAcrossReconnect(t *testing.T) (e *env, db, amqpProxy *testutil.Proxy, tap *testutil.AMQPTap, logs *lockedBuffer, w *running, id uuid.UUID) {
	t.Helper()
	e = newEnv(t)
	db = testutil.NewProxy(t, e.pool.Config().ConnString())
	amqpProxy = testutil.NewProxy(t, e.amqpURL)
	tap = amqpProxy.Tap()
	vars := map[string]string{
		"WORKER_CONCURRENCY": "1",
		"CLAIM_RETRY_BASE":   "10ms",
		"CLAIM_RETRY_MAX":    "50ms",
	}
	relayCfg := e.config(vars)
	vars["DATABASE_URL"] = db.URL()
	logs = workerLogs(t)
	w = e.startWorkerVia(e.config(vars), amqpProxy.URL(), logs)
	e.startRelay(relayCfg)

	db.Break()
	id = e.create(1)[0]
	testutil.Eventually(t, func() bool { return len(logLines(logs, logClaimFailed)) == 1 }, waitTimeout)
	if len(deliveriesOf(tap, id)) != 1 {
		t.Fatalf("deliveries %+v before the break, want one", deliveriesOf(tap, id))
	}
	amqpProxy.Break()
	amqpProxy.Restore(t)
	testutil.Eventually(t, func() bool { return deliveredOn(tap, 2, id.String()) }, waitTimeout)
	return e, db, amqpProxy, tap, logs, w, id
}

// A delivery held through a long claim retry across the reconnect: the late
// claim succeeds, the ack for the old delivery is not sent (nothing is acked
// on the new connection for it), the notification is sent once, and the
// copy delivered on the new connection is acked there after the claim guard
// dropped it.
func TestWorker_ClaimHeldAcrossReconnect(t *testing.T) {
	e, db, _, tap, logs, w, id := claimHeldAcrossReconnect(t)
	db.Restore(t)
	e.waitStatus(id, domain.StatusSent)
	testutil.Within(func() bool { return len(answersOf(tap, id)) >= 1 }, waitTimeout)

	requireCleanWire(t, tap)
	ds := deliveriesOf(tap, id)
	if len(ds) != 2 || ds[0].Conn != 1 || ds[0].Answer != "" || ds[1].Conn != 2 || ds[1].Answer != "ack" {
		t.Errorf("deliveries on the wire %+v, want one unanswered on the first connection and one acked on the second", ds)
	}
	if n := len(logLines(logs, logAckStale)); n != 1 {
		t.Errorf("%d stale acks refused, want 1 (the late claim's)", n)
	}
	e.requireSentOnce([]uuid.UUID{id})
	if !e.queueEmpty(w) {
		t.Error("the dispatch queue is not empty")
	}
}

// Shutdown with a stale delivery in a dispatch: the claim retry ends at
// shutdown and its nack is not sent. The copy delivered on the new
// connection, still in the buffer, is nacked there, and the message waits in
// the queue.
func TestWorker_ShutdownWithStaleDeliveryInDispatch(t *testing.T) {
	e, _, _, tap, logs, w, id := claimHeldAcrossReconnect(t)
	w.cancel()
	if err := w.wait(t); err != nil {
		t.Fatalf("worker role: %v", err)
	}

	requireCleanWire(t, tap)
	if n := len(logLines(logs, logNackStale)); n != 1 {
		t.Errorf("%d stale nacks refused, want 1 (the held claim's)", n)
	}
	answers := answersOf(tap, id)
	if len(answers) != 1 || answers[0].Kind != "nack" || answers[0].Conn != 2 {
		t.Errorf("answers on the wire %+v, want one nack, on the second connection (the buffered copy)", answers)
	}
	// The nack is on the wire (asserted above); the broker makes the message
	// ready again a moment later, so the count is waited for, not read once.
	// (Read at once it was 0 in 1 of 60 runs on a loaded machine.)
	if !testutil.Within(func() bool { return e.ready() == 1 }, waitTimeout) {
		t.Errorf("%d messages ready after the shutdown, want 1", e.ready())
	}
	if n := e.prov.count(); n != 0 {
		t.Errorf("%d sends, want none", n)
	}
}

// D8: the broker cancels the worker's consumer when a delivery stays
// unacknowledged longer than the queue's x-consumer-timeout (here 1 s; the
// delivery is held by a claim that fails while the worker's database
// connection is cut). The consumer subscribes again on the same channel and
// the message is delivered again. When the database is back, the held claim
// succeeds, its ack is not sent (its subscription was cancelled), the
// notification is sent once, and the copy is acked.
func TestWorker_ServerCancelResubscribes(t *testing.T) {
	e := newEnv(t)
	db := testutil.NewProxy(t, e.pool.Config().ConnString())
	amqpProxy := testutil.NewProxy(t, e.amqpURL)
	tap := amqpProxy.Tap()
	vars := map[string]string{
		"WORKER_CONCURRENCY": "1",
		"CLAIM_RETRY_BASE":   "10ms",
		"CLAIM_RETRY_MAX":    "50ms",
		"CONSUMER_TIMEOUT":   "1000ms",
	}
	relayCfg := e.config(vars)
	vars["DATABASE_URL"] = db.URL()
	logs := workerLogs(t)
	w := e.startWorkerVia(e.config(vars), amqpProxy.URL(), logs)
	e.startRelay(relayCfg)

	db.Break()
	id := e.create(1)[0]
	testutil.Eventually(t, func() bool { return len(logLines(logs, logResubscribe)) >= 1 }, waitTimeout)
	testutil.Eventually(t, func() bool { return len(deliveriesOf(tap, id)) >= 2 }, waitTimeout)
	db.Restore(t)
	e.waitStatus(id, domain.StatusSent)
	testutil.Within(func() bool { return len(answersOf(tap, id)) >= 1 }, waitTimeout)

	requireCleanWire(t, tap)
	if tap.Conns() != 1 {
		t.Errorf("%d broker connections, want 1: the consumer subscribes again on the same channel", tap.Conns())
	}
	if a := answersOf(tap, id); len(a) != 1 || a[0].Kind != "ack" {
		t.Errorf("answers on the wire %+v, want one ack", a)
	}
	if n := len(logLines(logs, logAckStale)); n != 1 {
		t.Errorf("%d stale acks refused, want 1 (the held claim's)", n)
	}
	e.requireSentOnce([]uuid.UUID{id})
	if !e.queueEmpty(w) {
		t.Error("the dispatch queue is not empty")
	}
}

// D2 and D50: the broker comes back empty at the same address (the vhost is
// deleted and created again while the roles cannot reach it). The relay and
// the worker declare the topology on their new connections before they
// publish and consume: no lane fails on a missing exchange, and every
// notification, also those created during the outage, is sent once.
func TestE2E_BrokerComesBackEmpty(t *testing.T) {
	e := newEnv(t)
	proxy := testutil.NewProxy(t, e.amqpURL)
	cfg := e.config(map[string]string{"RABBITMQ_URL": proxy.URL()})
	logs := workerLogs(t)
	w := e.startWorkerVia(cfg, proxy.URL(), logs)
	e.startRelay(cfg)

	before := e.create(3)
	testutil.Eventually(t, func() bool { return e.countStatus(domain.StatusSent) == 3 }, waitTimeout)

	proxy.Break()
	testutil.WipeRabbitMQVhost(t, e.amqpURL)
	if testutil.RabbitMQQueueExists(t, e.amqpURL, queue.DispatchQueue(domain.ChannelSMS)) {
		t.Fatal("the wiped vhost still has the dispatch queue")
	}
	during := e.create(3)
	proxy.Restore(t)
	testutil.Eventually(t, func() bool { return e.countStatus(domain.StatusSent) == 6 }, waitTimeout)

	for line := range strings.SplitSeq(e.logs.String(), "\n") {
		if strings.Contains(line, "relay: lane failed") && (strings.Contains(line, "404") || strings.Contains(line, "NOT_FOUND")) {
			t.Errorf("a lane was published before the topology existed: %s", line)
		}
	}
	e.requireSentOnce(append(before, during...))
	// The admin channel died with the vhost.
	e.admin = adminChannel(t, e.amqpURL)
	if !e.queueEmpty(w) {
		t.Error("the dispatch queue is not empty")
	}
}

func adminChannel(t *testing.T, url string) *amqp.Channel {
	t.Helper()
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("admin dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("admin channel: %v", err)
	}
	return ch
}
