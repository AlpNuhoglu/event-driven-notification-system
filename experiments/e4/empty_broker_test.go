//go:build integration

// Scenario 4: the broker is REPLACED by a fresh, empty one at the same host
// port (container removed with its volumes, new container started). Does the
// library's topology recovery redeclare the exchange, the durable quorum
// queue, the binding, the QoS and the consumer? Topology declared on a channel
// that the application closed before the outage is checked separately.
//
// Run: go test -tags=integration -count=1 -v -timeout 20m -run TestEmptyBrokerReplacement ./...
package e4

import (
	"context"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestEmptyBrokerReplacement(t *testing.T) {
	ctx := context.Background()
	gap := envDuration(t, "E4_REPLACE_GAP", 10*time.Second)
	port := fixedHostPort(t)
	broker1 := startBroker(ctx, t, port)
	rec := newRecorder()
	amqp.SetLogger(libLogger{rec})
	t.Cleanup(func() { amqp.SetLogger(amqp.NullLogger{}) })
	rec.add("test", "broker #1 up on fixed host port %d; replacement gap=%s", port, gap)

	conn, err := amqp.DialConfig(amqpURL(port), recoveryConfig(40, 5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	watchConn(rec, "app.conn", conn)

	// Common start-up pattern: a short-lived channel only for declarations.
	setup, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	declareTopology(t, setup, "e4.tmp.x", map[string]string{"e4.tmp.q": "tmp"})
	if err := setup.Close(); err != nil {
		t.Fatal(err)
	}

	// Long-lived channel: declarations + QoS + confirm + consumer.
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	watchChan(rec, "app.ch", ch)
	declareTopology(t, ch, "e4.x", map[string]string{"e4.work": "work"})
	if err := ch.Qos(3, 0, false); err != nil {
		t.Fatal(err)
	}
	if err := ch.Confirm(false); err != nil {
		t.Fatal(err)
	}
	returns := ch.NotifyReturn(make(chan amqp.Return, 4))
	deliveries, err := ch.Consume("e4.work", "worker-1", false, false, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	topo := ch.TopologyConfiguration(true)
	var names []string
	for n := range topo.Exchanges {
		names = append(names, "exchange:"+n)
	}
	for n := range topo.Queues {
		names = append(names, "queue:"+n)
	}
	for _, b := range topo.Bindings {
		names = append(names, "binding:"+b.Exchange+"->"+b.Queue+"["+b.Key+"]")
	}
	rec.add("app.ch", "TopologyConfiguration(true) recorded by the library: %v qos=%+v", names, topo.Qos)

	rec.add("broker1", "list_queues:\n%s", rabbitctl(ctx, t, broker1, "list_queues", "name", "type", "durable", "consumers"))
	rec.add("test", "terminate broker #1 (container and volumes removed)")
	if err := broker1.Terminate(ctx); err != nil {
		t.Fatal(err)
	}
	removedAt := time.Now()
	rec.waitFor(t, "app.conn.state", "open -> reconnecting", 30*time.Second)
	<-time.After(time.Until(removedAt.Add(gap))) // deliberate replacement gap
	rec.add("test", "start broker #2 (fresh, empty) on the same host port")
	broker2 := startBroker(ctx, t, port)
	rec.add("broker2", "before recovery completes, list_queues:\n%s", rabbitctl(ctx, t, broker2, "list_queues", "name"))
	open := rec.waitFor(t, "app.conn.state", "reconnecting -> open", 3*time.Minute)
	rec.add("test", "connection reopened %s after removal", open.at.Sub(removedAt).Round(time.Millisecond))

	exchanges := rabbitctl(ctx, t, broker2, "list_exchanges", "name", "type", "durable")
	queues := rabbitctl(ctx, t, broker2, "list_queues", "name", "type", "durable", "consumers")
	bindings := rabbitctl(ctx, t, broker2, "list_bindings", "source_name", "destination_name", "routing_key")
	consumers := rabbitctl(ctx, t, broker2, "list_consumers", "queue_name", "consumer_tag", "ack_required", "prefetch_count")
	channels := rabbitctl(ctx, t, broker2, "list_channels", "number", "confirm", "prefetch_count")
	rec.add("broker2", "after recovery, list_exchanges (e4 only):\n%s", grepLines(exchanges, "e4."))
	rec.add("broker2", "after recovery, list_queues:\n%s", queues)
	rec.add("broker2", "after recovery, list_bindings (e4 only):\n%s", grepLines(bindings, "e4."))
	rec.add("broker2", "after recovery, list_consumers:\n%s", consumers)
	rec.add("broker2", "after recovery, list_channels:\n%s", channels)

	// End to end on the recovered channel: routed, confirmed, consumed.
	dc, err := ch.PublishWithDeferredConfirm("e4.x", "work", true, false, amqp.Publishing{Body: []byte("after-replacement")})
	if err != nil {
		t.Fatalf("publish after replacement: %v", err)
	}
	acked := dc.Wait()
	d, got := recvOne(deliveries, 10*time.Second)
	var returned bool
	select {
	case r := <-returns:
		returned = true
		rec.add("app.notifyReturn", "return %d %s", r.ReplyCode, r.ReplyText)
	default:
	}
	rec.add("app", "publish after replacement: acked=%v returned=%v delivered=%v body=%s", acked, returned, got, d.Body)
	if got {
		_ = d.Ack(false)
	}

	// ---- assertions of what was observed ----
	has := func(out string, fields ...string) bool {
		for _, l := range strings.Split(out, "\n") {
			ok := true
			f := strings.Fields(l)
			for i, want := range fields {
				if i >= len(f) || f[i] != want {
					ok = false
					break
				}
			}
			if ok {
				return true
			}
		}
		return false
	}
	if es := rec.all("app.conn.state", "reconnecting -> open"); len(es) != 1 || !strings.Contains(es[0].what, "skipped=0") {
		t.Errorf("want one reconnecting->open with no skipped entities")
	}
	if !has(exchanges, "e4.x", "direct", "true") {
		t.Errorf("exchange e4.x not redeclared")
	}
	// The consumers column of list_queues read 0 for the quorum queue right
	// after recovery while list_consumers already listed the consumer, so the
	// consumer is checked through list_consumers only.
	if !has(queues, "e4.work", "quorum", "true") {
		t.Errorf("durable quorum queue e4.work not redeclared")
	}
	if !has(bindings, "e4.x", "e4.work", "work") {
		t.Errorf("binding e4.x -> e4.work [work] not redeclared")
	}
	if !has(consumers, "e4.work", "worker-1", "true", "3") {
		t.Errorf("consumer worker-1 with prefetch 3 not restored")
	}
	if !acked || returned || !got || string(d.Body) != "after-replacement" {
		t.Errorf("end-to-end after replacement failed")
	}
	// Observed: topology declared on a channel the application closed is NOT recovered.
	if strings.Contains(queues, "e4.tmp.q") || strings.Contains(exchanges, "e4.tmp.x") {
		t.Errorf("topology of the closed setup channel was recovered (source says it is forgotten)")
	}
}

func grepLines(s, sub string) string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if strings.Contains(l, sub) {
			out = append(out, l)
		}
	}
	return strings.Join(out, "\n")
}
