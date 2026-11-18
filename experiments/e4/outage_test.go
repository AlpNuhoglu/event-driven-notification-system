//go:build integration

// Scenarios 2 and 3: the broker is stopped for E4_OUTAGE (default 60s) and
// started again with the same data. A "relay" connection (confirm mode,
// NotifyPublish, NotifyReturn, mandatory publishes) and a "worker" connection
// (QoS prefetch 2, manual-ack consumer on a durable quorum queue) are left to
// the library's automatic recovery; the application does nothing on recovery.
// The test records, with timestamps, which notifications arrive and asserts
// what was observed about confirms, returns, consumer, QoS, stale acks and
// publishes during the outage.
//
// Run: go test -tags=integration -count=1 -v -timeout 20m -run TestBrokerRestart ./...
package e4

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestBrokerRestart(t *testing.T) {
	ctx := context.Background()
	outage := envDuration(t, "E4_OUTAGE", 60*time.Second)
	port := fixedHostPort(t)
	broker := startBroker(ctx, t, port)
	rec := newRecorder()
	amqp.SetLogger(libLogger{rec})
	t.Cleanup(func() { amqp.SetLogger(amqp.NullLogger{}) })
	rec.add("test", "broker up on fixed host port %d; outage=%s", port, outage)

	// Recovery enabled; retry budget must outlast the outage: the default
	// (5 x 5s) does not, see TestRecoveryGivesUp.
	const retries, interval = 40, 5 * time.Second

	// ---- relay ----
	relay, err := amqp.DialConfig(amqpURL(port), recoveryConfig(retries, interval))
	if err != nil {
		t.Fatal(err)
	}
	defer relay.Close()
	watchConn(rec, "relay.conn", relay)
	rch, err := relay.Channel()
	if err != nil {
		t.Fatal(err)
	}
	watchChan(rec, "relay.ch", rch)
	declareTopology(t, rch, "e4.x", map[string]string{"e4.work": "work", "e4.burst": "burst"})
	if err := rch.Confirm(false); err != nil {
		t.Fatal(err)
	}
	type conf struct {
		at time.Time
		c  amqp.Confirmation
	}
	var confMu sync.Mutex
	var confs []conf
	pubs := rch.NotifyPublish(make(chan amqp.Confirmation, 8192))
	go func() {
		for c := range pubs {
			confMu.Lock()
			confs = append(confs, conf{time.Now(), c})
			confMu.Unlock()
		}
		rec.add("relay.notifyPublish", "listener chan closed")
	}()
	returns := rch.NotifyReturn(make(chan amqp.Return, 16))
	go func() {
		for r := range returns {
			rec.add("relay.notifyReturn", "return %d %s key=%s body=%s", r.ReplyCode, r.ReplyText, r.RoutingKey, r.Body)
		}
		rec.add("relay.notifyReturn", "listener chan closed")
	}()
	confirmsAfter := func(since time.Time) []amqp.Confirmation {
		confMu.Lock()
		defer confMu.Unlock()
		var out []amqp.Confirmation
		for _, c := range confs {
			if c.at.After(since) {
				out = append(out, c.c)
			}
		}
		return out
	}
	publish := func(key, body string) (*amqp.DeferredConfirmation, error) {
		return rch.PublishWithDeferredConfirm("e4.x", key, true, false,
			amqp.Publishing{DeliveryMode: amqp.Persistent, Body: []byte(body)})
	}

	// ---- worker ----
	worker, err := amqp.DialConfig(amqpURL(port), recoveryConfig(retries, interval))
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	watchConn(rec, "worker.conn", worker)
	wch, err := worker.Channel()
	if err != nil {
		t.Fatal(err)
	}
	watchChan(rec, "worker.ch", wch)
	if err := wch.Qos(2, 0, false); err != nil {
		t.Fatal(err)
	}
	deliveries, err := wch.Consume("e4.work", "worker-1", false, false, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Four messages, prefetch 2. The worker acks m1, so it holds m2 (tag 2)
	// and m3 (tag 3) unacked when the broker stops; m4 stays ready.
	for i := 1; i <= 4; i++ {
		dc, err := publish("work", fmt.Sprintf("m%d", i))
		if err != nil || !dc.Wait() {
			t.Fatalf("pre-outage publish m%d: err=%v", i, err)
		}
	}
	recvPre := func() amqp.Delivery {
		d, ok := recvOne(deliveries, 10*time.Second)
		if !ok {
			t.Fatal("pre-outage delivery missing")
		}
		rec.add("worker.deliver", "pre-outage tag=%d body=%s redelivered=%v", d.DeliveryTag, d.Body, d.Redelivered)
		return d
	}
	m1, m2 := recvPre(), recvPre()
	if d, ok := recvOne(deliveries, 1500*time.Millisecond); ok {
		t.Fatalf("prefetch 2 not applied before outage: got third delivery %s", d.Body)
	}
	if err := m1.Ack(false); err != nil {
		t.Fatal(err)
	}
	m3 := recvPre()
	pre := []amqp.Delivery{m2, m3} // held unacked across the outage

	// ---- burst of publishes racing the broker stop (outstanding confirms) ----
	type burstPub struct {
		dc *amqp.DeferredConfirmation
	}
	var burstMu sync.Mutex
	var burst []burstPub
	var burstErr error
	var burstErrAt time.Time
	burstDone := make(chan struct{})
	go func() {
		defer close(burstDone)
		for i := 0; ; i++ {
			dc, err := publish("burst", fmt.Sprintf("b%d", i))
			if err != nil {
				burstErr, burstErrAt = err, time.Now()
				return
			}
			burstMu.Lock()
			burst = append(burst, burstPub{dc})
			burstMu.Unlock()
			if i%50 == 0 {
				time.Sleep(time.Millisecond) // rate limit (~50k/s at most) so the stop lands mid-stream
			}
		}
	}()
	for { // poll until the burst is well under way
		burstMu.Lock()
		n := len(burst)
		burstMu.Unlock()
		if n >= 1000 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	rec.add("test", "docker stop broker")
	if err := broker.Stop(ctx, nil); err != nil {
		t.Fatal(err)
	}
	stoppedAt := time.Now()
	rec.add("test", "broker stopped; outage begins")
	confirmsBetween := func(from, to time.Time) int {
		confMu.Lock()
		defer confMu.Unlock()
		n := 0
		for _, c := range confs {
			if !c.at.Before(from) && c.at.Before(to) {
				n++
			}
		}
		return n
	}
	<-burstDone
	rec.add("relay.burst", "publish loop ended after %d publishes: err=%v (errors.Is ErrClosed=%v) at %s",
		len(burst), burstErr, errors.Is(burstErr, amqp.ErrClosed), rec.stamp(burstErrAt))
	countBurst := func() (acked, nacked, pending int) {
		for _, b := range burst {
			select {
			case <-b.dc.Done():
				if b.dc.Acked() {
					acked++
				} else {
					nacked++
				}
			default:
				pending++
			}
		}
		return
	}
	a0, n0, p0 := countBurst()
	rec.add("relay.burst", "deferred confirmations at outage start: acked=%d nacked=%d pending=%d", a0, n0, p0)

	// ---- publishes DURING the outage ----
	var outageErrs []error
	for i := 0; i < 3; i++ {
		start := time.Now()
		dc, err := publish("work", fmt.Sprintf("during-outage-%d", i))
		rec.add("relay.publish", "during outage #%d: returned in %s err=%v dc=%v nextSeq=%d",
			i, time.Since(start), err, dc != nil, rch.GetNextPublishSeqNo())
		outageErrs = append(outageErrs, err)
	}
	// Channel() on the connection during the outage.
	if _, err := relay.Channel(); err != nil {
		rec.add("relay.conn", "Channel() during outage: err=%v", err)
	}

	// Deliberate outage period: the subject of the experiment.
	<-time.After(time.Until(stoppedAt.Add(outage)))
	a1, n1, p1 := countBurst()
	rec.add("relay.burst", "deferred confirmations at outage end: acked=%d nacked=%d pending=%d", a1, n1, p1)
	rec.add("test", "docker start broker (same container, same data)")
	if err := broker.Start(ctx); err != nil {
		t.Fatal(err)
	}
	rec.add("test", "broker started")

	relayOpen := rec.waitFor(t, "relay.conn.state", "reconnecting -> open", 3*time.Minute)
	workerOpen := rec.waitFor(t, "worker.conn.state", "reconnecting -> open", 3*time.Minute)
	rec.add("test", "relay reopened %s after stop, worker %s after stop",
		relayOpen.at.Sub(stoppedAt).Round(time.Millisecond), workerOpen.at.Sub(stoppedAt).Round(time.Millisecond))

	a2, n2, p2 := countBurst()
	rec.add("relay.burst", "deferred confirmations after recovery: acked=%d nacked=%d pending=%d", a2, n2, p2)
	rec.add("relay.notifyPublish", "confirmations received: total before recovery=%d, between stop and reopen=%d",
		confirmsBetween(time.Time{}, relayOpen.at), confirmsBetween(stoppedAt, relayOpen.at))
	rec.add("broker", "list_queues after restart (before new publishes):\n%s",
		rabbitctl(ctx, t, broker, "list_queues", "name", "type", "durable", "messages_ready", "messages_unacknowledged"))
	burstStored := queueMessages(ctx, t, broker, "e4.burst")
	rec.add("relay.burst", "e4.burst holds %d messages; %d were confirmed, %d resolved Acked()=false at reconnect -> %d of those were stored anyway",
		burstStored, a2, n2, burstStored-a2)

	// ---- scenario 3: relay after recovery, application did nothing ----
	seqAfter := rch.GetNextPublishSeqNo()
	rec.add("relay.ch", "GetNextPublishSeqNo after recovery = %d (before outage it was %d+)", seqAfter, len(burst)+3)
	recoveredAt := time.Now()
	dcUnroutable, err := publish("nowhere", "unroutable-after-recovery")
	if err != nil {
		t.Fatalf("publish after recovery: %v", err)
	}
	dcRoutable, err := publish("burst", "routable-after-recovery")
	if err != nil {
		t.Fatalf("publish after recovery: %v", err)
	}
	rec.add("relay.publish", "after recovery: unroutable dc.tag=%d acked=%v; routable dc.tag=%d acked=%v",
		dcUnroutable.DeliveryTag, dcUnroutable.Wait(), dcRoutable.DeliveryTag, dcRoutable.Wait())
	rec.waitFor(t, "relay.notifyReturn", "unroutable-after-recovery", 10*time.Second)
	var post []amqp.Confirmation
	deadline := time.Now().Add(10 * time.Second)
	for len(post) < 2 && time.Now().Before(deadline) {
		post = confirmsAfter(recoveredAt)
		time.Sleep(50 * time.Millisecond)
	}
	rec.add("relay.notifyPublish", "confirmations after recovery (pre-registered listener): %+v", post)

	// ---- scenario 3: worker after recovery ----
	var postDel []amqp.Delivery
	for {
		d, ok := recvOne(deliveries, 4*time.Second)
		if !ok {
			break
		}
		postDel = append(postDel, d)
		rec.add("worker.deliver", "post-recovery tag=%d body=%s redelivered=%v", d.DeliveryTag, d.Body, d.Redelivered)
	}
	rec.add("worker", "deliveries within 4s of quiet after recovery, without acking: %d", len(postDel))

	// The application now finishes m2, which it received BEFORE the outage,
	// and acks it through the old Delivery value (old tag 2).
	staleErr := pre[0].Ack(false)
	rec.add("worker.ack", "stale pre-outage delivery tag=%d body=%s Ack() err=%v", pre[0].DeliveryTag, pre[0].Body, staleErr)
	freed, gotFreed := recvOne(deliveries, 5*time.Second)
	if gotFreed {
		rec.add("worker.deliver", "after stale ack: tag=%d body=%s redelivered=%v", freed.DeliveryTag, freed.Body, freed.Redelivered)
		postDel = append(postDel, freed)
	}
	// Which message did the broker remove? Requeue everything still held on
	// the new channel and look at what comes back.
	if len(postDel) > 0 {
		if err := wch.Nack(postDel[len(postDel)-1].DeliveryTag, true, true); err != nil {
			t.Fatalf("nack multiple: %v", err)
		}
	}
	back := map[string]bool{}
	for {
		d, ok := recvOne(deliveries, 3*time.Second)
		if !ok {
			break
		}
		rec.add("worker.deliver", "after nack-requeue: tag=%d body=%s redelivered=%v", d.DeliveryTag, d.Body, d.Redelivered)
		back[string(d.Body)] = true
		if err := d.Ack(false); err != nil {
			t.Fatal(err)
		}
	}
	rec.add("worker", "bodies still in the queue after the stale ack of m2: %v", back)

	// A brand-new message reaches the pre-outage consumer.
	if dc, err := publish("work", "fresh-after-recovery"); err != nil || !dc.Wait() {
		t.Fatalf("publish fresh: %v", err)
	}
	fresh, gotFresh := recvOne(deliveries, 10*time.Second)
	if gotFresh {
		rec.add("worker.deliver", "tag=%d body=%s redelivered=%v", fresh.DeliveryTag, fresh.Body, fresh.Redelivered)
		_ = fresh.Ack(false)
	}

	// A stale tag that does not exist on the new channel: channel error 406.
	unknownErr := wch.Ack(999, false)
	rec.add("worker.ack", "Ack(999) on recovered channel: err=%v", unknownErr)
	rec.waitFor(t, "worker.ch.close", "PRECONDITION_FAILED", 10*time.Second)
	chReopen := rec.waitFor(t, "worker.ch.state", "reconnecting -> open", 30*time.Second)
	_ = chReopen
	if dc, err := publish("work", "after-channel-error"); err != nil || !dc.Wait() {
		t.Fatalf("publish after channel error: %v", err)
	}
	afterChErr, gotAfterChErr := recvOne(deliveries, 10*time.Second)
	if gotAfterChErr {
		rec.add("worker.deliver", "after channel-level recovery: tag=%d body=%s", afterChErr.DeliveryTag, afterChErr.Body)
		_ = afterChErr.Ack(false)
	}

	// Broker-side view of consumers and QoS after recovery.
	rec.add("broker", "list_consumers:\n%s", rabbitctl(ctx, t, broker, "list_consumers", "queue_name", "consumer_tag", "ack_required", "prefetch_count"))
	rec.add("broker", "list_channels:\n%s", rabbitctl(ctx, t, broker, "list_channels", "number", "confirm", "prefetch_count", "messages_unacknowledged"))

	// ---- assertions of what was observed ----
	if len(rec.all("relay.conn.state", "open -> reconnecting")) != 1 || len(rec.all("relay.conn.state", "reconnecting -> open")) != 1 {
		t.Errorf("relay connection: expected exactly one open->reconnecting and one reconnecting->open")
	}
	// NotifyClose receives the broker's 320 first; a failed reconnect attempt
	// (broker port open, broker not yet ready) can add a 501 EOF *Error.
	if es := rec.all("relay.conn.close", "received *Error"); len(es) == 0 || !contains(es[0].what, "(320)") {
		t.Errorf("relay connection NotifyClose: expected a 320 CONNECTION_FORCED first")
	}
	if len(rec.all("relay.conn.close", "listener chan closed")) != 0 || len(rec.all("relay.ch.close", "listener chan closed")) != 0 {
		t.Errorf("NotifyClose listener channels were closed during a successful recovery")
	}
	if len(rec.all("relay.ch.state", "reconnecting -> open")) != 1 {
		t.Errorf("relay channel did not report reconnecting -> open")
	}
	if !errors.Is(burstErr, amqp.ErrClosed) {
		t.Errorf("publish at broker stop: want ErrClosed, got %v", burstErr)
	}
	for i, err := range outageErrs {
		if !errors.Is(err, amqp.ErrClosed) {
			t.Errorf("publish #%d during outage: want ErrClosed, got %v", i, err)
		}
	}
	if p2 != 0 {
		t.Errorf("burst: %d deferred confirmations still pending after recovery", p2)
	}
	if burstStored <= a2 {
		t.Logf("NOTE: no unconfirmed burst message reached the queue this run (stored=%d acked=%d)", burstStored, a2)
	}
	// Observed: the channel's own state stays "open" for the whole outage and
	// only flips open->reconnecting->open at the moment of reconnection.
	if es := rec.all("relay.ch.state", "open -> reconnecting"); len(es) != 1 || es[0].at.Before(stoppedAt.Add(outage)) {
		t.Errorf("relay channel state: expected a single open->reconnecting only after the broker came back")
	}
	if seqAfter != 1 {
		t.Errorf("publish sequence after recovery: want reset to 1 (observed %d)", seqAfter)
	}
	if !dcUnroutable.Acked() || !dcRoutable.Acked() {
		t.Errorf("confirm mode not active after recovery")
	}
	if len(post) != 2 || post[0].DeliveryTag != 1 || post[1].DeliveryTag != 2 {
		t.Errorf("pre-registered NotifyPublish after recovery: want tags 1,2, got %+v", post)
	}
	if len(postDel) < 2 || string(postDel[0].Body) != "m2" || postDel[0].DeliveryTag != 1 ||
		string(postDel[1].Body) != "m3" || postDel[1].DeliveryTag != 2 {
		t.Errorf("post-recovery redeliveries: want m2 tag 1, m3 tag 2")
	}
	for _, d := range postDel[:min(2, len(postDel))] {
		if !d.Redelivered {
			t.Errorf("post-recovery delivery %s not marked redelivered", d.Body)
		}
	}
	if staleErr != nil {
		t.Errorf("stale ack returned %v", staleErr)
	}
	// Observed: prefetch still 2 (no third delivery until the stale ack freed a slot).
	if !gotFreed || string(freed.Body) != "m4" || len(postDel) != 3 {
		t.Errorf("prefetch/stale ack: want exactly 2 deliveries, then m4 after the stale ack")
	}
	// Observed: the stale ack of m2 (old tag 2) acked m3 (new tag 2).
	if !back["m2"] || back["m3"] || !back["m4"] {
		t.Errorf("stale ack: want m2 and m4 back in the queue and m3 gone, got %v", back)
	}
	if !gotFresh || string(fresh.Body) != "fresh-after-recovery" {
		t.Errorf("pre-outage consumer did not receive a new message after recovery")
	}
	if unknownErr != nil {
		t.Errorf("Ack(999) returned %v", unknownErr)
	}
	if !gotAfterChErr {
		t.Errorf("consumer not re-subscribed after channel-level recovery")
	}
}

func contains(s, sub string) bool { return strings.Contains(s, sub) }
