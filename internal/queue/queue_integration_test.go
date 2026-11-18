//go:build integration

package queue

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

func TestMain(m *testing.M) { testutil.Main(m) }

// Durations are parameters, in milliseconds.
const (
	waitTimeout     = 10_000 * time.Millisecond
	confirmTimeout  = 5_000 * time.Millisecond
	recoveryWait    = 20 * time.Millisecond
	consumerTimeout = 86_400_000 * time.Millisecond // 24 h
	testTraceParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
)

func discard() *slog.Logger { return slog.New(slog.DiscardHandler) }

// dial connects with recovery and closes the connection when the test ends.
func dial(t *testing.T, url string, retries int) *Connection {
	t.Helper()
	c, err := Dial(url, recoveryWait, retries, discard())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// setup returns a vhost URL with the topology declared and a connection to it.
func setup(t *testing.T) (string, *Connection) {
	t.Helper()
	url := testutil.NewRabbitMQVhost(t)
	c := dial(t, url, 1000)
	if err := c.DeclareTopology(consumerTimeout); err != nil {
		t.Fatalf("declare: %v", err)
	}
	return url, c
}

// raw opens a plain channel (no recovery) for inspecting the broker.
func raw(t *testing.T, url string) *amqp.Channel {
	t.Helper()
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("raw dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("raw channel: %v", err)
	}
	return ch
}

func newPublisher(t *testing.T, c *Connection, maxBatch int) *Publisher {
	t.Helper()
	p, err := NewPublisher(c, maxBatch)
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}
	return p
}

func ctxT(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	t.Cleanup(cancel)
	return ctx
}

func msg(c domain.Channel, p domain.Priority) Message {
	return Message{ID: uuid.New(), Channel: c, Priority: p, TraceParent: testTraceParent}
}

func ids(msgs []Message) []uuid.UUID {
	out := make([]uuid.UUID, len(msgs))
	for i, m := range msgs {
		out[i] = m.ID
	}
	return out
}

// ready returns the number of ready messages in a queue.
func ready(t *testing.T, ch *amqp.Channel, queue string) int {
	t.Helper()
	q, err := ch.QueueDeclarePassive(queue, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("passive declare %s: %v", queue, err)
	}
	return q.Messages
}

func get(t *testing.T, ch *amqp.Channel, queue string) amqp.Delivery {
	t.Helper()
	d, ok, err := ch.Get(queue, true)
	if err != nil || !ok {
		t.Fatalf("get %s: ok=%v err=%v", queue, ok, err)
	}
	return d
}

func closed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestDeclareTopology_TwiceQuorumWithConsumerTimeout(t *testing.T) {
	url, c := setup(t)
	if err := c.DeclareTopology(consumerTimeout); err != nil {
		t.Fatalf("second declaration: %v", err)
	}
	for _, ch := range domain.Channels() {
		q := testutil.RabbitMQQueue(t, url, DispatchQueue(ch))
		if q["type"] != "quorum" || q["durable"] != true {
			t.Errorf("%s: type %v durable %v, want a durable quorum queue", DispatchQueue(ch), q["type"], q["durable"])
		}
		args, _ := q["arguments"].(map[string]any)
		if args["x-consumer-timeout"] != float64(86_400_000) {
			t.Errorf("%s: x-consumer-timeout = %v, want 86400000", DispatchQueue(ch), args["x-consumer-timeout"])
		}
		if args["x-dead-letter-exchange"] != DeadLetterExchange {
			t.Errorf("%s: x-dead-letter-exchange = %v", DispatchQueue(ch), args["x-dead-letter-exchange"])
		}
		if _, ok := args["x-max-priority"]; ok {
			t.Errorf("%s: has x-max-priority", DispatchQueue(ch))
		}
		p := testutil.RabbitMQQueue(t, url, PoisonQueue(ch))
		if p["type"] != "classic" || p["durable"] != true {
			t.Errorf("%s: type %v durable %v, want a durable classic queue", PoisonQueue(ch), p["type"], p["durable"])
		}
	}
	// The bindings: a message for each routing key lands in its queue.
	pub := newPublisher(t, c, 10)
	msgs := []Message{msg(domain.ChannelSMS, domain.PriorityLow), msg(domain.ChannelEmail, domain.PriorityLow), msg(domain.ChannelPush, domain.PriorityLow)}
	published, err := pub.PublishBatch(ctxT(t), msgs, confirmTimeout)
	if err != nil || len(published) != 3 {
		t.Fatalf("publish: %v, %d published", err, len(published))
	}
	inspect := raw(t, url)
	for _, m := range msgs {
		if d := get(t, inspect, DispatchQueue(m.Channel)); d.MessageId != m.ID.String() {
			t.Errorf("%s holds %s, want %s", DispatchQueue(m.Channel), d.MessageId, m.ID)
		}
	}
}

func TestPublisher_MessageProperties(t *testing.T) {
	url, c := setup(t)
	pub := newPublisher(t, c, 10)
	with := msg(domain.ChannelSMS, domain.PriorityHigh)
	without := msg(domain.ChannelSMS, domain.PriorityNormal)
	without.TraceParent = ""
	published, err := pub.PublishBatch(ctxT(t), []Message{with, without}, confirmTimeout)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(published, []uuid.UUID{with.ID, without.ID}) {
		t.Fatalf("published %v", published)
	}
	inspect := raw(t, url)
	// Strict priority: the high message comes first.
	for _, want := range []Message{with, without} {
		d := get(t, inspect, DispatchQueue(domain.ChannelSMS))
		if d.MessageId != want.ID.String() || string(d.Body) != want.ID.String() {
			t.Errorf("MessageId %q body %q, want %s", d.MessageId, d.Body, want.ID)
		}
		if d.DeliveryMode != amqp.Persistent {
			t.Errorf("%s: delivery mode %d, want persistent", want.ID, d.DeliveryMode)
		}
		if d.Priority != want.Priority.AMQP() {
			t.Errorf("%s: priority %d, want %d", want.ID, d.Priority, want.Priority.AMQP())
		}
		if d.Exchange != DispatchExchange || d.RoutingKey != "sms" {
			t.Errorf("%s: exchange %q routing key %q", want.ID, d.Exchange, d.RoutingKey)
		}
		tp, ok := d.Headers["traceparent"]
		if want.TraceParent == "" && ok {
			t.Errorf("%s: traceparent header %v, want none", want.ID, tp)
		}
		if want.TraceParent != "" && tp != want.TraceParent {
			t.Errorf("%s: traceparent header %v, want %s", want.ID, tp, want.TraceParent)
		}
	}
}

// One batch mixes routable messages (sms), unroutable ones (push, unbound:
// returned) and rejected ones (email, routed only to a queue that refuses
// them: nacked). Only the sms ids are published; neither case is an error.
func TestPublisher_ReturnedAndNackedAreExcluded(t *testing.T) {
	url, c := setup(t)
	admin := raw(t, url)
	if err := admin.QueueUnbind(DispatchQueue(domain.ChannelPush), "push", DispatchExchange, nil); err != nil {
		t.Fatal(err)
	}
	if err := admin.QueueUnbind(DispatchQueue(domain.ChannelEmail), "email", DispatchExchange, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.QueueDeclare("refuse", true, false, false, false,
		amqp.Table{"x-max-length": int64(0), "x-overflow": "reject-publish"}); err != nil {
		t.Fatal(err)
	}
	if err := admin.QueueBind("refuse", "email", DispatchExchange, false, nil); err != nil {
		t.Fatal(err)
	}

	pub := newPublisher(t, c, 30)
	for round := range 20 {
		var msgs, want []Message
		for range 10 {
			s := msg(domain.ChannelSMS, domain.PriorityNormal)
			msgs = append(msgs, msg(domain.ChannelPush, domain.PriorityNormal), s, msg(domain.ChannelEmail, domain.PriorityNormal))
			want = append(want, s)
		}
		published, err := pub.PublishBatch(ctxT(t), msgs, confirmTimeout)
		if err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
		if !slices.Equal(published, ids(want)) {
			t.Fatalf("round %d: published %d ids, want exactly the %d sms ids", round, len(published), len(want))
		}
	}
	if n := ready(t, admin, DispatchQueue(domain.ChannelSMS)); n != 200 {
		t.Errorf("dispatch.sms holds %d messages, want 200", n)
	}
}

func TestPublisher_RejectsInvalidBatches(t *testing.T) {
	_, c := setup(t)
	pub := newPublisher(t, c, 2)
	three := []Message{msg(domain.ChannelSMS, domain.PriorityHigh), msg(domain.ChannelSMS, domain.PriorityHigh), msg(domain.ChannelSMS, domain.PriorityHigh)}
	if _, err := pub.PublishBatch(ctxT(t), three, confirmTimeout); err == nil {
		t.Error("a batch above the maximum was accepted")
	}
	bad := msg(domain.ChannelSMS, domain.Priority(3))
	if _, err := pub.PublishBatch(ctxT(t), []Message{bad}, confirmTimeout); err == nil {
		t.Error("an invalid priority was accepted")
	}
	if published, err := pub.PublishBatch(ctxT(t), nil, confirmTimeout); err != nil || len(published) != 0 {
		t.Errorf("empty batch: %v, %v", published, err)
	}
}

func TestPublisher_ConfirmTimeoutIsError(t *testing.T) {
	url, _ := setup(t)
	proxy := testutil.NewProxy(t, url)
	c := dial(t, proxy.URL(), 1000)
	pub := newPublisher(t, c, 10)

	proxy.Pause() // confirms never reach the client
	_, err := pub.PublishBatch(ctxT(t), []Message{msg(domain.ChannelSMS, domain.PriorityHigh)}, 200*time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want a confirm timeout, got %v", err)
	}
	proxy.Resume()

	// The publisher works again: the late confirm of the failed batch does
	// not leak into the next one.
	m := msg(domain.ChannelSMS, domain.PriorityHigh)
	published, err := pub.PublishBatch(ctxT(t), []Message{m}, confirmTimeout)
	if err != nil || !slices.Equal(published, []uuid.UUID{m.ID}) {
		t.Fatalf("after resume: %v, %v", published, err)
	}
}

func TestPublisher_ConnectionLostDuringBatchIsError(t *testing.T) {
	url, _ := setup(t)
	proxy := testutil.NewProxy(t, url)
	c := dial(t, proxy.URL(), 1000)
	pub := newPublisher(t, c, 10)

	proxy.Pause()
	before := proxy.BytesToBroker()
	errc := make(chan error, 1)
	go func() {
		_, err := pub.PublishBatch(context.Background(), []Message{msg(domain.ChannelSMS, domain.PriorityHigh)}, waitTimeout)
		errc <- err
	}()
	// The publish has reached the broker; its confirm is held back.
	testutil.Eventually(t, func() bool { return proxy.BytesToBroker() > before }, waitTimeout)
	proxy.Break()
	select {
	case err := <-errc:
		if err == nil {
			t.Fatal("a batch whose connection was lost succeeded")
		}
		if errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("want the loss reported before the confirm timeout, got %v", err)
		}
	case <-time.After(waitTimeout): // bound on the test, not a wait for a state
		t.Fatal("PublishBatch did not return after the connection was lost")
	}
	proxy.Resume()
	proxy.Restore(t)

	// Recovery reopens the connection and the publisher works again.
	testutil.Eventually(t, c.IsOpen, waitTimeout)
	m := msg(domain.ChannelSMS, domain.PriorityHigh)
	published, err := pub.PublishBatch(ctxT(t), []Message{m}, confirmTimeout)
	if err != nil || !slices.Equal(published, []uuid.UUID{m.ID}) {
		t.Fatalf("after recovery: %v, %v", published, err)
	}
}

func TestConnection_RecoveryThenGiveUp(t *testing.T) {
	url := testutil.NewRabbitMQVhost(t)
	proxy := testutil.NewProxy(t, url)
	c := dial(t, proxy.URL(), 3)
	if !c.IsOpen() {
		t.Fatal("a new connection is not open")
	}
	if c.Epoch() != 0 {
		t.Errorf("epoch %d, want 0", c.Epoch())
	}

	proxy.Break()
	testutil.Eventually(t, func() bool { return !c.IsOpen() }, waitTimeout)
	proxy.Restore(t)
	testutil.Eventually(t, c.IsOpen, waitTimeout)
	if closed(c.GaveUp()) {
		t.Fatal("GaveUp closed after a successful recovery")
	}

	proxy.Break() // and never restored: 3 attempts, then recovery gives up
	testutil.Eventually(t, func() bool { return closed(c.GaveUp()) }, waitTimeout)
	if c.Err() == nil {
		t.Error("recovery gave up without an error")
	}
	if c.IsOpen() {
		t.Error("a connection whose recovery gave up reports open")
	}
}

func TestConnection_CloseIsNotGiveUp(t *testing.T) {
	c, err := Dial(testutil.NewRabbitMQVhost(t), recoveryWait, 3, discard())
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	testutil.Eventually(t, func() bool { return !c.IsOpen() }, waitTimeout)
	if closed(c.GaveUp()) || c.Err() != nil {
		t.Errorf("Close reported as a give-up: %v", c.Err())
	}
}

func newConsumer(t *testing.T, c *Connection) *Consumer {
	t.Helper()
	cons, err := NewConsumer(c, domain.ChannelSMS, 5, discard())
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	return cons
}

// runConsumer runs cons until the returned stop function is called; stop
// returns Run's result.
func runConsumer(t *testing.T, cons *Consumer, handle Handler) (stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- cons.Run(ctx, handle) }()
	var once bool
	var result error
	stop = func() error {
		if !once {
			once = true
			cancel()
			result = <-done
		}
		return result
	}
	t.Cleanup(func() { _ = stop() })
	return stop
}

func recv(t *testing.T, ch <-chan Delivery) Delivery {
	t.Helper()
	select {
	case d := <-ch:
		return d
	case <-time.After(waitTimeout): // bound on the test, not a wait for a state
		t.Fatal("no delivery")
		return Delivery{}
	}
}

func TestConsumer_DeliveryAckNack(t *testing.T) {
	url, c := setup(t)
	m := msg(domain.ChannelSMS, domain.PriorityHigh)
	if _, err := newPublisher(t, c, 1).PublishBatch(ctxT(t), []Message{m}, confirmTimeout); err != nil {
		t.Fatal(err)
	}
	cons := newConsumer(t, c)
	got := make(chan Delivery, 10)
	stop := runConsumer(t, cons, func(d Delivery) { got <- d })

	d := recv(t, got)
	if d.ID != m.ID || d.Priority != 6 || d.Epoch != c.Epoch() {
		t.Fatalf("delivery id %s priority %d epoch %d, want %s 6 %d", d.ID, d.Priority, d.Epoch, m.ID, c.Epoch())
	}
	if err := d.Nack(); err != nil {
		t.Fatal(err)
	}
	again := recv(t, got) // requeued and delivered again
	if again.ID != m.ID {
		t.Fatalf("after nack: delivery %s, want %s", again.ID, m.ID)
	}
	if err := again.Ack(); err != nil {
		t.Fatal(err)
	}
	if err := stop(); err != nil {
		t.Fatalf("Run after cancel: %v", err)
	}
	// Closing the channel would requeue an unacknowledged delivery.
	if err := cons.Close(); err != nil {
		t.Fatal(err)
	}
	if n := ready(t, raw(t, url), DispatchQueue(domain.ChannelSMS)); n != 0 {
		t.Errorf("queue holds %d messages after the ack, want 0", n)
	}
}

func TestConsumer_InvalidBodyGoesToPoisonQueue(t *testing.T) {
	url, c := setup(t)
	admin := raw(t, url)
	if err := admin.PublishWithContext(ctxT(t), DispatchExchange, "sms", true, false,
		amqp.Publishing{Body: []byte("not-a-notification-id")}); err != nil {
		t.Fatal(err)
	}
	cons := newConsumer(t, c)
	handled := make(chan Delivery, 1)
	runConsumer(t, cons, func(d Delivery) { handled <- d })
	testutil.Eventually(t, func() bool { return ready(t, admin, PoisonQueue(domain.ChannelSMS)) == 1 }, waitTimeout)
	if len(handled) != 0 {
		t.Error("the handler received a delivery with an invalid body")
	}
}

func TestConsumer_StreamEndIsError(t *testing.T) {
	url, c := setup(t)
	cons := newConsumer(t, c)
	done := make(chan error, 1)
	go func() { done <- cons.Run(context.Background(), func(Delivery) {}) }()
	// Deleting the queue makes the broker cancel the consumer.
	admin := raw(t, url)
	testutil.Eventually(t, func() bool {
		q, err := admin.QueueDeclarePassive(DispatchQueue(domain.ChannelSMS), true, false, false, false, nil)
		return err == nil && q.Consumers == 1
	}, waitTimeout)
	if _, err := admin.QueueDelete(DispatchQueue(domain.ChannelSMS), false, false, false); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Error("Run returned nil after a server-side cancellation")
		}
	case <-time.After(waitTimeout): // bound on the test, not a wait for a state
		t.Fatal("Run did not return after a server-side cancellation")
	}
}
