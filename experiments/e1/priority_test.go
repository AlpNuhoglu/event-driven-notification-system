//go:build integration

// Experiment E1: strict message priority in a RabbitMQ 4.4.1 quorum queue.
//
// Checks, against rabbitmq:4.4.1-alpine started with testcontainers-go:
//   - amqp091-go cannot put an explicit priority 0 on the wire: Publishing{Priority: 0}
//     leaves the property out, so the broker treats it as "no priority" (sorted as 4);
//   - with a real explicit priority 0 (published through the management HTTP API),
//     one high (2) published after many lows (0) is consumed first;
//   - with priorities 0, 1 and 2 the consumed order is strict (all 2s, all 1s, all 0s),
//     FIFO inside each level, not a 2:1 ratio between two classes; same for 1, 2, 3;
//   - a message without a priority property sorts as priority 4 (relative to 0, 2, 3, 4, 5);
//   - a quorum queue needs no declaration argument; x-max-priority is rejected (406);
//   - priorities above 31 sort like 31;
//   - with prefetch > 1, the first delivery the broker sends after an ack is a newly published high;
//   - (extra) a nacked-and-requeued low is delivered before highs that were already waiting.
//
// amqp091-go publishes use publisher confirms; every test polls the queue's ready count
// before consuming, so the consumed order reflects the queue and not client buffering.
//
// Run from experiments/e1:
//
//	go test -tags=integration -count=1 -v ./...
package e1

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	tcrabbit "github.com/testcontainers/testcontainers-go/modules/rabbitmq"
)

const (
	rabbitImage  = "rabbitmq:4.4.1-alpine"
	pollDeadline = 10 * time.Second
	pollInterval = 20 * time.Millisecond
	// quietWindow is a deliberate observation period: the broker must send nothing beyond prefetch.
	quietWindow = 300 * time.Millisecond
	// noPriority asks publish to leave the priority property out.
	noPriority = -1
)

var (
	amqpURL            string
	httpURL            string
	httpUser, httpPass string
)

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	ctx := context.Background()
	c, err := tcrabbit.Run(ctx, rabbitImage)
	if c != nil {
		defer func() { _ = c.Terminate(ctx) }()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "start rabbitmq:", err)
		return 1
	}
	if amqpURL, err = c.AmqpURL(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "amqp url:", err)
		return 1
	}
	// The management plugin is only used to publish a message with an explicit priority 0,
	// which amqp091-go cannot do (see TestAmqp091PriorityZeroIsOmitted).
	if code, _, err := c.Exec(ctx, []string{"rabbitmq-plugins", "enable", "rabbitmq_management"}); err != nil || code != 0 {
		fmt.Fprintln(os.Stderr, "enable management plugin:", code, err)
		return 1
	}
	if httpURL, err = c.HttpURL(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "http url:", err)
		return 1
	}
	httpUser, httpPass = c.AdminUsername, c.AdminPassword
	if err := waitManagement(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return m.Run()
}

func waitManagement() error {
	deadline := time.Now().Add(60 * time.Second)
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for ; time.Now().Before(deadline); <-tick.C {
		req, _ := http.NewRequest(http.MethodGet, httpURL+"/api/overview", nil)
		req.SetBasicAuth(httpUser, httpPass)
		if resp, err := http.DefaultClient.Do(req); err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
	}
	return fmt.Errorf("management API not ready within 60s")
}

// ---------- helpers ----------

func dial(t *testing.T) *amqp.Connection {
	t.Helper()
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func channel(t *testing.T, conn *amqp.Connection) *amqp.Channel {
	t.Helper()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("channel: %v", err)
	}
	return ch
}

// declareQuorum declares a durable quorum queue with x-queue-type=quorum and no other argument.
func declareQuorum(t *testing.T, conn *amqp.Connection, name string) {
	t.Helper()
	ch := channel(t, conn)
	defer ch.Close()
	if _, err := ch.QueueDeclare(name, true, false, false, false, amqp.Table{"x-queue-type": "quorum"}); err != nil {
		t.Fatalf("declare %s: %v", name, err)
	}
}

// publisher publishes with amqp091-go in confirm mode and waits for each confirm.
type publisher struct {
	t       *testing.T
	ch      *amqp.Channel
	queue   string
	confirm chan amqp.Confirmation
}

func newPublisher(t *testing.T, conn *amqp.Connection, queue string) *publisher {
	t.Helper()
	ch := channel(t, conn)
	if err := ch.Confirm(false); err != nil {
		t.Fatalf("confirm mode: %v", err)
	}
	return &publisher{t: t, ch: ch, queue: queue, confirm: ch.NotifyPublish(make(chan amqp.Confirmation, 1))}
}

// publish sends one message.
//   - prio > 0: amqp091-go with Priority set;
//   - prio == 0: management HTTP API with properties.priority = 0 (explicit on the wire);
//   - prio == noPriority: amqp091-go with no priority property.
func (p *publisher) publish(body string, prio int) {
	p.t.Helper()
	switch {
	case prio == 0:
		publishHTTPPriorityZero(p.t, p.queue, body)
	case prio == noPriority:
		p.amqp(body, 0) // amqp091-go omits the property when Priority == 0
	default:
		p.amqp(body, uint8(prio))
	}
}

// amqp publishes through amqp091-go with Publishing.Priority = prio.
func (p *publisher) amqp(body string, prio uint8) {
	p.t.Helper()
	msg := amqp.Publishing{Body: []byte(body), DeliveryMode: amqp.Persistent, Priority: prio}
	if err := p.ch.PublishWithContext(context.Background(), "", p.queue, false, false, msg); err != nil {
		p.t.Fatalf("publish %s: %v", body, err)
	}
	select {
	case c := <-p.confirm:
		if !c.Ack {
			p.t.Fatalf("publish %s: nacked by broker", body)
		}
	case <-time.After(pollDeadline):
		p.t.Fatalf("publish %s: no confirm within %s", body, pollDeadline)
	}
}

// publishHTTPPriorityZero publishes via the management API with an explicit priority 0.
func publishHTTPPriorityZero(t *testing.T, queue, body string) {
	t.Helper()
	payload, _ := json.Marshal(map[string]any{
		"properties":       map[string]any{"priority": 0, "delivery_mode": 2},
		"routing_key":      queue,
		"payload":          body,
		"payload_encoding": "string",
	})
	req, _ := http.NewRequest(http.MethodPost, httpURL+"/api/exchanges/%2F/amq.default/publish", bytes.NewReader(payload))
	req.SetBasicAuth(httpUser, httpPass)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("http publish %s: %v", body, err)
	}
	defer resp.Body.Close()
	var out struct {
		Routed bool `json:"routed"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil || resp.StatusCode != http.StatusOK || !out.Routed {
		t.Fatalf("http publish %s: status %d routed=%v err=%v", body, resp.StatusCode, out.Routed, err)
	}
}

// waitReady polls a passive declare until the queue reports n ready messages.
func waitReady(t *testing.T, conn *amqp.Connection, queue string, n int) {
	t.Helper()
	deadline := time.Now().Add(pollDeadline)
	tick := time.NewTicker(pollInterval)
	defer tick.Stop()
	last := -1
	for ; time.Now().Before(deadline); <-tick.C {
		ch := channel(t, conn)
		q, err := ch.QueueDeclarePassive(queue, true, false, false, false, amqp.Table{"x-queue-type": "quorum"})
		_ = ch.Close()
		if err != nil {
			t.Fatalf("passive declare: %v", err)
		}
		if last = q.Messages; last == n {
			return
		}
	}
	t.Fatalf("queue %s: want %d ready messages, last seen %d", queue, n, last)
}

// drainGet consumes n messages with basic.get + manual ack, one at a time.
func drainGet(t *testing.T, ch *amqp.Channel, queue string, n int) []amqp.Delivery {
	t.Helper()
	out := make([]amqp.Delivery, 0, n)
	for len(out) < n {
		d, ok, err := ch.Get(queue, false)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if !ok {
			t.Fatalf("get: queue empty after %d of %d", len(out), n)
		}
		if err := d.Ack(false); err != nil {
			t.Fatalf("ack: %v", err)
		}
		out = append(out, d)
	}
	return out
}

// drainConsume consumes n messages with basic.consume, prefetch 1, manual ack.
func drainConsume(t *testing.T, ch *amqp.Channel, queue string, n int) []amqp.Delivery {
	t.Helper()
	if err := ch.Qos(1, 0, false); err != nil {
		t.Fatalf("qos: %v", err)
	}
	deliveries, err := ch.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	out := make([]amqp.Delivery, 0, n)
	for len(out) < n {
		select {
		case d := <-deliveries:
			if err := d.Ack(false); err != nil {
				t.Fatalf("ack: %v", err)
			}
			out = append(out, d)
		case <-time.After(pollDeadline):
			t.Fatalf("consume: got %d of %d", len(out), n)
		}
	}
	return out
}

func bodies(ds []amqp.Delivery) []string {
	s := make([]string, len(ds))
	for i, d := range ds {
		s[i] = string(d.Body)
	}
	return s
}

// shape renders the sequence of body labels (text before '-') as runs, e.g. "p2x100 p1x100 p0x100".
func shape(ds []amqp.Delivery) string {
	label := func(d amqp.Delivery) string { return strings.SplitN(string(d.Body), "-", 2)[0] }
	var parts []string
	for i := 0; i < len(ds); {
		j := i
		for j < len(ds) && label(ds[j]) == label(ds[i]) {
			j++
		}
		parts = append(parts, fmt.Sprintf("%sx%d", label(ds[i]), j-i))
		i = j
	}
	return strings.Join(parts, " ")
}

// assertExact checks the consumed bodies equal want, in order.
func assertExact(t *testing.T, got []amqp.Delivery, want []string) {
	t.Helper()
	g := bodies(got)
	if strings.Join(g, ",") != strings.Join(want, ",") {
		t.Fatalf("order mismatch\n got: %v\nwant: %v", g, want)
	}
}

// ---------- tests ----------

// amqp091-go v1.17.0 write.go sets the priority flag only when Priority > 0, so
// Publishing{Priority: 0} reaches the broker without a priority and sorts as 4.
func TestAmqp091PriorityZeroIsOmitted(t *testing.T) {
	conn := dial(t)
	q := "e1.amqp091-zero"
	declareQuorum(t, conn, q)
	p := newPublisher(t, conn, q)
	p.amqp("p2", 2)
	p.amqp("p3", 3)
	p.amqp("zero", 0) // intended as the lowest priority
	p.amqp("p5", 5)
	waitReady(t, conn, q, 4)

	got := drainGet(t, channel(t, conn), q, 4)
	t.Logf("order: %v", bodies(got))
	assertExact(t, got, []string{"p5", "zero", "p3", "p2"})
}

// Check 1: one high (2) after N explicit lows (0) comes out first.
func TestHighAfterManyLowsIsFirst(t *testing.T) {
	const n = 200
	conn := dial(t)
	q := "e1.high-after-lows"
	declareQuorum(t, conn, q)
	p := newPublisher(t, conn, q)
	for i := 0; i < n; i++ {
		p.publish(fmt.Sprintf("low-%03d", i), 0)
	}
	p.publish("high", 2)
	waitReady(t, conn, q, n+1)

	got := drainGet(t, channel(t, conn), q, n+1)
	t.Logf("first 3: %v; shape: %s", bodies(got[:3]), shape(got))
	want := []string{"high"}
	for i := 0; i < n; i++ {
		want = append(want, fmt.Sprintf("low-%03d", i))
	}
	assertExact(t, got, want)
}

// Check 2: strict order across three levels with FIFO inside a level.
func TestStrictThreeLevels(t *testing.T) {
	const perLevel = 100
	lowsFirst := func(i int) (int, int) { return i / perLevel, i % perLevel }
	roundRobin := func(i int) (int, int) { return i % 3, i / 3 }
	cases := []struct {
		name   string
		levels [3]int                       // low, normal, high
		order  func(i int) (level, seq int) // i in [0, 3*perLevel)
		drain  func(*testing.T, *amqp.Channel, string, int) []amqp.Delivery
	}{
		{"0-1-2/lows-first/get", [3]int{0, 1, 2}, lowsFirst, drainGet},
		{"0-1-2/lows-first/consume-prefetch1", [3]int{0, 1, 2}, lowsFirst, drainConsume},
		{"0-1-2/round-robin/get", [3]int{0, 1, 2}, roundRobin, drainGet},
		{"1-2-3/lows-first/get", [3]int{1, 2, 3}, lowsFirst, drainGet},
	}
	for ci, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn := dial(t)
			q := fmt.Sprintf("e1.strict-%d", ci)
			declareQuorum(t, conn, q)
			p := newPublisher(t, conn, q)
			for i := 0; i < 3*perLevel; i++ {
				level, seq := tc.order(i)
				prio := tc.levels[level]
				p.publish(fmt.Sprintf("p%d-%03d", prio, seq), prio)
			}
			waitReady(t, conn, q, 3*perLevel)

			got := tc.drain(t, channel(t, conn), q, 3*perLevel)
			t.Logf("consumed shape: %s", shape(got))
			var want []string
			for level := 2; level >= 0; level-- {
				for seq := 0; seq < perLevel; seq++ {
					want = append(want, fmt.Sprintf("p%d-%03d", tc.levels[level], seq))
				}
			}
			assertExact(t, got, want)
		})
	}
}

// Check 3: a message without priority sorts as priority 4.
func TestNoPriorityBehavesAsFour(t *testing.T) {
	conn := dial(t)
	q := "e1.no-priority"
	declareQuorum(t, conn, q)
	p := newPublisher(t, conn, q)
	// Published in rising order so priority, not FIFO, must explain the result.
	p.publish("p0", 0)
	p.publish("p2", 2)
	p.publish("p3", 3)
	p.publish("p4-before", 4)
	p.publish("none", noPriority)
	p.publish("p4-after", 4)
	p.publish("p5", 5)
	waitReady(t, conn, q, 7)

	got := drainGet(t, channel(t, conn), q, 7)
	t.Logf("order: %v", bodies(got))
	assertExact(t, got, []string{"p5", "p4-before", "none", "p4-after", "p3", "p2", "p0"})
}

// Check 4a: x-max-priority on a quorum queue. The 4.4 docs say "ignored"; the broker rejects it.
func TestXMaxPriorityRejected(t *testing.T) {
	conn := dial(t)
	ch := channel(t, conn)
	args := amqp.Table{"x-queue-type": "quorum", "x-max-priority": int32(2)}
	_, err := ch.QueueDeclare("e1.x-max-priority", true, false, false, false, args)
	t.Logf("declare with x-max-priority=2: err=%v", err)
	var amqpErr *amqp.Error
	if err == nil {
		t.Fatalf("declare with x-max-priority was accepted")
	}
	if !errors.As(err, &amqpErr) || amqpErr.Code != amqp.PreconditionFailed {
		t.Fatalf("want 406 PRECONDITION_FAILED, got %v", err)
	}
}

// Check 4b: priorities above 31 are clamped to 31 (FIFO with 31).
func TestPriorityClampedAt31(t *testing.T) {
	conn := dial(t)
	q := "e1.clamp"
	declareQuorum(t, conn, q)
	p := newPublisher(t, conn, q)
	p.publish("p2", 2)
	p.publish("p31", 31)
	p.publish("p200", 200)
	p.publish("p30", 30)
	waitReady(t, conn, q, 4)

	got := drainGet(t, channel(t, conn), q, 4)
	t.Logf("order: %v", bodies(got))
	assertExact(t, got, []string{"p31", "p200", "p30", "p2"})
}

// Check 5: with prefetch 3 and 3 unacked lows, the next delivery after an ack is a new high.
func TestNextDeliveryAfterAckIsHigh(t *testing.T) {
	conn := dial(t)
	q := "e1.prefetch"
	declareQuorum(t, conn, q)
	p := newPublisher(t, conn, q)
	for i := 0; i < 10; i++ {
		p.publish(fmt.Sprintf("low-%d", i), 0)
	}
	waitReady(t, conn, q, 10)

	cch := channel(t, conn)
	if err := cch.Qos(3, 0, false); err != nil {
		t.Fatalf("qos: %v", err)
	}
	deliveries, err := cch.Consume(q, "", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	next := func() amqp.Delivery {
		t.Helper()
		select {
		case d := <-deliveries:
			return d
		case <-time.After(pollDeadline):
			t.Fatalf("no delivery within %s", pollDeadline)
			return amqp.Delivery{}
		}
	}
	held := []amqp.Delivery{next(), next(), next()}
	t.Logf("prefetched and unacked: %v", bodies(held))
	select {
	case d := <-deliveries:
		t.Fatalf("broker exceeded prefetch 3: got %s", d.Body)
	case <-time.After(quietWindow):
	}

	p.publish("high", 2)
	waitReady(t, conn, q, 8) // 7 lows + high ready; 3 unacked
	if err := held[0].Ack(false); err != nil {
		t.Fatalf("ack: %v", err)
	}
	d := next()
	t.Logf("first delivery after ack: %s (priority %d)", d.Body, d.Priority)
	if string(d.Body) != "high" {
		t.Fatalf("want high after ack, got %s", d.Body)
	}
}

// Extra: a low (explicit 0) nacked with requeue while highs (2) are waiting.
// The 4.3+ docs say returned messages lose their priority and are requeued in return order.
func TestRequeuedLowVersusWaitingHighs(t *testing.T) {
	conn := dial(t)
	q := "e1.requeue"
	declareQuorum(t, conn, q)
	p := newPublisher(t, conn, q)
	p.publish("low", 0)
	waitReady(t, conn, q, 1)

	ch := channel(t, conn)
	d, ok, err := ch.Get(q, false)
	if err != nil || !ok {
		t.Fatalf("get low: ok=%v err=%v", ok, err)
	}
	for i := 0; i < 3; i++ {
		p.publish(fmt.Sprintf("high-%d", i), 2)
	}
	waitReady(t, conn, q, 3)
	if err := d.Nack(false, true); err != nil {
		t.Fatalf("nack: %v", err)
	}
	waitReady(t, conn, q, 4)

	got := drainGet(t, ch, q, 4)
	for _, g := range got {
		t.Logf("delivered %-6s redelivered=%v", g.Body, g.Redelivered)
	}
	assertExact(t, got, []string{"low", "high-0", "high-1", "high-2"})
}
