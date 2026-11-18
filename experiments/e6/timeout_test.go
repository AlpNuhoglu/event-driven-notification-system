//go:build integration

// E6, part 2: does the quorum queue argument x-consumer-timeout override the
// node-wide consumer_timeout in both directions, and what does a delivery
// acknowledgement timeout look like to an amqp091-go consumer?
//
// The broker runs with consumer_timeout = E6_BROKER_TIMEOUT_MS (default 30 s).
// One unacked delivery is held on each of four durable quorum queues, in
// parallel subtests:
//
//	A: no argument                          -> node default applies
//	B: x-consumer-timeout = 24 h            -> must survive the node default
//	C: x-consumer-timeout = E6_SHORT_TIMEOUT_MS (default 10 s) and
//	   x-delivery-limit = 1, held through two timeouts and resubscribed on the
//	   same channel each time
//	D: queue argument 24 h, consumer argument x-consumer-timeout =
//	   E6_CONSUMER_ARG_TIMEOUT_MS (default 5 s)  (precedence check, extra)
//
// Run from experiments/e6:
//
//	go test -tags=integration -count=1 -v -run TestConsumerTimeout ./...
package e6

import (
	"context"
	"fmt"
	"io"
	"regexp"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	shortTimeoutMS = envMS("E6_SHORT_TIMEOUT_MS", 10000)
	consArgMS      = envMS("E6_CONSUMER_ARG_TIMEOUT_MS", 5000)
	// Extra deliberate hold beyond the node default for queue B; also the
	// allowed lateness of a timeout.
	marginMS = envMS("E6_MARGIN_MS", 10000)
)

func ms(n int) time.Duration { return time.Duration(n) * time.Millisecond }

// consumer is one channel with its notification channels.
type consumer struct {
	t       *testing.T
	queue   string
	ch      *amqp.Channel
	cancels chan string
	closes  chan *amqp.Error
}

func newConsumer(t *testing.T, conn *amqp.Connection, queue string, args amqp.Table) *consumer {
	t.Helper()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("channel: %v", err)
	}
	if err := ch.Qos(1, 0, false); err != nil {
		t.Fatalf("qos: %v", err)
	}
	if _, err := ch.QueueDeclare(queue, true, false, false, false, quorumArgs(args)); err != nil {
		t.Fatalf("declare %s: %v", queue, err)
	}
	c := &consumer{t: t, queue: queue, ch: ch,
		cancels: ch.NotifyCancel(make(chan string, 8)),
		closes:  ch.NotifyClose(make(chan *amqp.Error, 1))}
	err = ch.PublishWithContext(context.Background(), "", queue, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent, Body: []byte("e6 " + queue)})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	return c
}

// consume subscribes and waits (with a deadline) for the first delivery.
func (c *consumer) consume(tag string, args amqp.Table) (<-chan amqp.Delivery, amqp.Delivery, time.Time) {
	c.t.Helper()
	dels, err := c.ch.Consume(c.queue, tag, false, false, false, false, args)
	if err != nil {
		c.t.Fatalf("consume %s: %v", tag, err)
	}
	select {
	case d, ok := <-dels:
		if !ok {
			c.t.Fatalf("%s: delivery channel closed before first delivery", tag)
		}
		at := time.Now()
		logf(c.t, "%s: delivery tag=%d redelivered=%v headers=%v", tag, d.DeliveryTag, d.Redelivered, d.Headers)
		return dels, d, at
	case <-time.After(10 * time.Second):
		c.t.Fatalf("%s: no delivery within 10s", tag)
	}
	panic("unreachable")
}

// event is what ended (or did not end) a hold.
type event struct {
	kind    string // "cancel", "close" or "none"
	tag     string
	err     *amqp.Error
	elapsed time.Duration
}

// waitEvent waits until the server cancels the consumer or closes the channel,
// or until the deadline passes (kind "none").
func (c *consumer) waitEvent(since time.Time, deadline time.Duration) event {
	select {
	case tag := <-c.cancels:
		return event{kind: "cancel", tag: tag, elapsed: time.Since(since)}
	case e := <-c.closes:
		return event{kind: "close", err: e, elapsed: time.Since(since)}
	case <-time.After(deadline - time.Since(since)):
		return event{kind: "none", elapsed: time.Since(since)}
	}
}

// alive reports whether the channel still works, by a synchronous round trip,
// and returns the number of ready messages in the queue.
func (c *consumer) alive() (bool, int, error) {
	q, err := c.ch.QueueDeclarePassive(c.queue, true, false, false, false, nil)
	if err != nil {
		return false, 0, err
	}
	return true, q.Messages, nil
}

// deliveryChanClosed waits briefly for the client library to close the
// delivery channel of a cancelled consumer.
func deliveryChanClosed(dels <-chan amqp.Delivery) bool {
	select {
	case _, ok := <-dels:
		return !ok
	case <-time.After(2 * time.Second):
		return false
	}
}

func assertTimedOut(t *testing.T, ev event, tag string, wantMS int) {
	t.Helper()
	logf(t, "%s: event=%s tag=%q err=%v after %.3fs (configured %.1fs)",
		tag, ev.kind, ev.tag, ev.err, ev.elapsed.Seconds(), ms(wantMS).Seconds())
	if ev.kind != "cancel" || ev.tag != tag {
		t.Fatalf("%s: want basic.cancel for this tag, got %+v", tag, ev)
	}
	// Measured from receipt at the client, so allow 1 s below the setting.
	if ev.elapsed < ms(wantMS)-time.Second || ev.elapsed > ms(wantMS+marginMS) {
		t.Errorf("%s: timed out after %v, want within [%v, %v]", tag, ev.elapsed,
			ms(wantMS)-time.Second, ms(wantMS+marginMS))
	}
}

// checkCounts asserts what was observed on a delivery after `timeouts` earlier
// timeouts: redelivered flag set, x-acquired-count counts the timeouts, and no
// x-delivery-count header (the delivery count used by x-delivery-limit is not
// incremented by a timeout).
func checkCounts(t *testing.T, tag string, d amqp.Delivery, timeouts int) {
	t.Helper()
	if d.Redelivered != (timeouts > 0) {
		t.Errorf("%s: redelivered=%v after %d timeouts", tag, d.Redelivered, timeouts)
	}
	if timeouts > 0 && fmt.Sprint(d.Headers["x-acquired-count"]) != fmt.Sprint(timeouts) {
		t.Errorf("%s: x-acquired-count=%v, want %d", tag, d.Headers["x-acquired-count"], timeouts)
	}
	if v, ok := d.Headers["x-delivery-count"]; ok {
		t.Errorf("%s: unexpected x-delivery-count=%v", tag, v)
	}
}

func TestConsumerTimeout(t *testing.T) {
	logf(t, "broker node config: %s",
		strings.TrimSpace(rabbitmqctl(t, "eval", "application:get_env(rabbit, consumer_timeout).")))
	conn := dial(t)
	t.Run("hold", func(t *testing.T) { holdAll(t, conn) })
	printBrokerLog(t)
}

func holdAll(t *testing.T, conn *amqp.Connection) {
	t.Run("A_no_argument_uses_node_default", func(t *testing.T) {
		t.Parallel()
		c := newConsumer(t, conn, "e6-A", nil)
		dels, d, at := c.consume("e6-A-1", nil)
		ev := c.waitEvent(at, ms(brokerTimeoutMS+marginMS))
		assertTimedOut(t, ev, "e6-A-1", brokerTimeoutMS)
		logf(t, "e6-A-1: client closed delivery channel: %v", deliveryChanClosed(dels))
		ok, n, err := c.alive()
		logf(t, "e6-A: after cancel channel alive=%v ready=%d err=%v", ok, n, err)
		if !ok || n != 1 {
			t.Fatalf("want channel alive and the message back in the queue (ready=1)")
		}

		// The worker still holds the stale delivery and would ack it when the
		// circuit closes. Observed: the ack is silently ignored (no channel
		// error) and does NOT remove the returned message from the queue.
		ackErr := d.Ack(false)
		ok, n, err = c.alive() // synchronous round trip after the ack
		logf(t, "e6-A: ack of stale tag %d -> ackErr=%v; channel alive=%v ready=%d err=%v",
			d.DeliveryTag, ackErr, ok, n, err)
		if ackErr != nil || !ok || n != 1 {
			t.Errorf("stale ack: want no error, channel alive, message still ready; got ackErr=%v alive=%v ready=%d err=%v",
				ackErr, ok, n, err)
		}

		// Resubscribe on the same channel.
		_, d2, _ := c.consume("e6-A-2", nil)
		checkCounts(t, "e6-A-2", d2, 1)
		if err := d2.Ack(false); err != nil {
			t.Fatalf("ack: %v", err)
		}
		ok, n, err = c.alive()
		logf(t, "e6-A-2: acked; channel alive=%v ready=%d err=%v", ok, n, err)
		if !ok || n != 0 {
			t.Errorf("want queue empty after ack")
		}
	})

	t.Run("B_24h_argument_survives_node_default", func(t *testing.T) {
		t.Parallel()
		c := newConsumer(t, conn, "e6-B", amqp.Table{"x-consumer-timeout": int64(day)})
		_, d, at := c.consume("e6-B-1", nil)
		hold := ms(brokerTimeoutMS + marginMS) // deliberate: the subject of the test
		ev := c.waitEvent(at, hold)
		logf(t, "e6-B-1: event=%s tag=%q err=%v after holding %.3fs (node default %.1fs)",
			ev.kind, ev.tag, ev.err, ev.elapsed.Seconds(), ms(brokerTimeoutMS).Seconds())
		if ev.kind != "none" {
			t.Fatalf("queue argument 24h did not prevent the timeout: %+v", ev)
		}
		if err := d.Ack(false); err != nil {
			t.Fatalf("ack: %v", err)
		}
		ok, n, err := c.alive()
		logf(t, "e6-B-1: acked after %.3fs; channel alive=%v ready=%d err=%v",
			time.Since(at).Seconds(), ok, n, err)
		if !ok || n != 0 {
			t.Errorf("want channel alive and queue empty after ack")
		}
	})

	t.Run("C_short_argument_overrides_node_default", func(t *testing.T) {
		t.Parallel()
		// x-delivery-limit = 1: if timeouts counted as failed deliveries, the
		// message would be dropped before the third subscription.
		c := newConsumer(t, conn, "e6-C", amqp.Table{
			"x-consumer-timeout": int64(shortTimeoutMS), "x-delivery-limit": int64(1)})
		for i, tag := range []string{"e6-C-1", "e6-C-2"} {
			_, d, at := c.consume(tag, nil)
			checkCounts(t, tag, d, i)
			ev := c.waitEvent(at, ms(shortTimeoutMS+marginMS))
			assertTimedOut(t, ev, tag, shortTimeoutMS)
			ok, n, err := c.alive()
			logf(t, "%s: after cancel channel alive=%v ready=%d err=%v", tag, ok, n, err)
			if !ok || n != 1 {
				t.Fatalf("want channel alive and the message back in the queue")
			}
		}
		// Third subscription on the same channel: ack this time.
		_, d, _ := c.consume("e6-C-3", nil)
		checkCounts(t, "e6-C-3", d, 2)
		if err := d.Ack(false); err != nil {
			t.Fatalf("ack: %v", err)
		}
		ok, n, err := c.alive()
		logf(t, "e6-C-3: acked; channel alive=%v ready=%d err=%v", ok, n, err)
		if !ok || n != 0 {
			t.Errorf("want channel alive and queue empty after ack")
		}
	})

	t.Run("D_consumer_argument_overrides_queue_argument", func(t *testing.T) {
		t.Parallel()
		c := newConsumer(t, conn, "e6-D", amqp.Table{"x-consumer-timeout": int64(day)})
		_, _, at := c.consume("e6-D-1", amqp.Table{"x-consumer-timeout": int64(consArgMS)})
		ev := c.waitEvent(at, ms(consArgMS+marginMS))
		assertTimedOut(t, ev, "e6-D-1", consArgMS)
	})
}

var ansi = regexp.MustCompile(`\x1b\[[0-9;]*m`)

// printBrokerLog prints the broker's log lines about the e6 consumers.
func printBrokerLog(t *testing.T) {
	logs, err := broker.Logs(context.Background())
	if err == nil {
		buf := new(strings.Builder)
		_, _ = io.Copy(buf, logs)
		for _, line := range strings.Split(buf.String(), "\n") {
			if strings.Contains(line, "e6-") && (strings.Contains(line, "time") || strings.Contains(line, "cancel")) {
				logf(t, "broker log: %s", ansi.ReplaceAllString(strings.TrimSpace(line), ""))
			}
		}
	}
}
