//go:build integration

package queue

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync/atomic"
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
	if err := c.DeclareTopology(consumerTimeout); err != nil {
		t.Fatal(err)
	}
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
	if c.IsOpen() {
		t.Fatal("a connection without the topology reports open")
	}
	if n, _ := c.Opens(); n != 0 {
		t.Fatalf("opens %d before the topology is declared, want 0", n)
	}
	if err := c.DeclareTopology(consumerTimeout); err != nil {
		t.Fatal(err)
	}
	if !c.IsOpen() {
		t.Fatal("a new connection with the topology is not open")
	}
	n, next := c.Opens()
	if n != 1 || closed(next) {
		t.Fatalf("opens %d (next closed %v) after the first declaration, want 1 and an open signal", n, closed(next))
	}

	proxy.Break()
	testutil.Eventually(t, func() bool { return !c.IsOpen() }, waitTimeout)
	proxy.Restore(t)
	testutil.Eventually(t, c.IsOpen, waitTimeout)
	if closed(c.GaveUp()) {
		t.Fatal("GaveUp closed after a successful recovery")
	}
	if !closed(next) {
		t.Error("the opens signal read before the loss is not closed after the recovery")
	}
	if n, _ := c.Opens(); n != 2 {
		t.Errorf("opens %d after one recovery, want 2", n)
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
	return runConsumerWith(t, cons, handle, nil)
}

// runConsumerWith is runConsumer with Run's resubscribed callback.
func runConsumerWith(t *testing.T, cons *Consumer, handle Handler, resubscribed func()) (stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- cons.Run(ctx, handle, resubscribed) }()
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
	if d.ID != m.ID || d.Priority != 6 || d.Epoch != cons.Epoch() || d.Stale() {
		t.Fatalf("delivery id %s priority %d epoch %d stale %v, want %s 6 %d false", d.ID, d.Priority, d.Epoch, d.Stale(), m.ID, cons.Epoch())
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

// A body that holds an id in a spelling other than the canonical one (which
// the UUID library would accept) is as invalid as any other body; an upper
// case id in the canonical form is still a notification id.
func TestConsumer_IdInAnotherSpellingGoesToPoisonQueue(t *testing.T) {
	url, c := setup(t)
	admin := raw(t, url)
	id := uuid.Must(uuid.NewV7())
	s := id.String()
	refused := []string{"urn:uuid:" + s, "{" + s + "}", strings.ReplaceAll(s, "-", ""), "x" + s + "y"}
	for _, body := range append(refused, strings.ToUpper(s)) {
		if err := admin.PublishWithContext(ctxT(t), DispatchExchange, "sms", true, false,
			amqp.Publishing{Body: []byte(body)}); err != nil {
			t.Fatal(err)
		}
	}
	cons := newConsumer(t, c)
	handled := make(chan Delivery, len(refused)+1)
	runConsumer(t, cons, func(d Delivery) {
		handled <- d
		_ = d.Ack()
	})
	d := recv(t, handled)
	if d.ID != id {
		t.Errorf("handled id %s, want %s", d.ID, id)
	}
	testutil.Eventually(t, func() bool { return ready(t, admin, PoisonQueue(domain.ChannelSMS)) == len(refused) }, waitTimeout)
	if n := len(handled); n != 0 {
		t.Errorf("the handler received %d more deliveries, want 0", n)
	}
}

// A delivery stream that ends because the channel is closed for good (here
// closed under the consumer; the library does the same when recovery gives
// up) ends Run with an error.
func TestConsumer_ChannelClosedForGoodIsError(t *testing.T) {
	url, c := setup(t)
	cons := newConsumer(t, c)
	done := make(chan error, 1)
	go func() { done <- cons.Run(context.Background(), func(Delivery) {}, nil) }()
	admin := raw(t, url)
	testutil.Eventually(t, func() bool {
		q, err := admin.QueueDeclarePassive(DispatchQueue(domain.ChannelSMS), true, false, false, false, nil)
		return err == nil && q.Consumers == 1
	}, waitTimeout)
	if err := cons.ch.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Error("Run returned nil after its channel was closed for good")
		}
	case <-time.After(waitTimeout): // bound on the test, not a wait for a state
		t.Fatal("Run did not return after its channel was closed for good")
	}
}

// collect receives deliveries until want of them have arrived.
func collect(t *testing.T, ch <-chan Delivery, want int) []Delivery {
	t.Helper()
	out := make([]Delivery, 0, want)
	for len(out) < want {
		out = append(out, recv(t, ch))
	}
	return out
}

func byID(ds []Delivery) map[uuid.UUID]Delivery {
	out := map[uuid.UUID]Delivery{}
	for _, d := range ds {
		out[d.ID] = d
	}
	return out
}

// D76 without a connection loss: a channel error on the consumer's channel
// makes the library reopen that channel, and delivery tags restart at 1
// (experiment E4). The epoch advances once; the two deliveries held from
// before are stale (Ack and Nack send nothing); both messages are delivered
// again on a new subscription, with the new epoch, and acked. No answer
// names a delivery of another session on the wire, the epoch advances no
// further (a stale ack with an unknown tag would close the channel again),
// and the queue ends empty.
func TestConsumer_ChannelReopenAdvancesEpoch(t *testing.T) {
	url, _ := setup(t)
	proxy := testutil.NewProxy(t, url)
	tap := proxy.Tap()
	c := dial(t, proxy.URL(), 1000)
	if err := c.DeclareTopology(consumerTimeout); err != nil {
		t.Fatal(err)
	}
	msgs := []Message{msg(domain.ChannelSMS, domain.PriorityHigh), msg(domain.ChannelSMS, domain.PriorityHigh)}
	if _, err := newPublisher(t, c, 2).PublishBatch(ctxT(t), msgs, confirmTimeout); err != nil {
		t.Fatal(err)
	}
	cons := newConsumer(t, c)
	got := make(chan Delivery, 10)
	var resubscribed atomic.Int32
	stop := runConsumerWith(t, cons, func(d Delivery) { got <- d }, func() { resubscribed.Add(1) })
	held := collect(t, got, 2)
	for _, d := range held {
		if d.Epoch != 0 || d.Stale() {
			t.Fatalf("first delivery: epoch %d stale %v, want 0 and false", d.Epoch, d.Stale())
		}
	}

	// The broker answers 404 NOT_FOUND and closes the consumer's channel.
	if err := cons.ch.PublishWithContext(ctxT(t), "no-such-exchange", "sms", false, false, amqp.Publishing{Body: []byte("x")}); err != nil {
		t.Fatalf("publish to the missing exchange: %v", err)
	}
	again := collect(t, got, 2)
	if !c.IsOpen() {
		t.Error("the connection is not open: only the consumer's channel was reopened")
	}
	if e := cons.Epoch(); e != 1 {
		t.Fatalf("epoch %d after the channel was reopened, want 1", e)
	}
	for _, d := range held {
		if !d.Stale() {
			t.Errorf("a delivery from before the reopen is not stale")
		}
		if err := d.Ack(); !errors.Is(err, ErrStale) {
			t.Errorf("ack of a stale delivery: %v, want ErrStale", err)
		}
		if err := d.Nack(); !errors.Is(err, ErrStale) {
			t.Errorf("nack of a stale delivery: %v, want ErrStale", err)
		}
	}
	redelivered := byID(again)
	for _, m := range msgs {
		d, ok := redelivered[m.ID]
		if !ok || d.Epoch != 1 || !d.d.Redelivered {
			t.Fatalf("message %s again: %v, epoch %d, redelivered %v; want epoch 1, redelivered", m.ID, ok, d.Epoch, d.d.Redelivered)
		}
		if err := d.Ack(); err != nil {
			t.Fatalf("ack after the reopen: %v", err)
		}
	}
	if err := stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if e := cons.Epoch(); e != 1 {
		t.Errorf("epoch %d at the end, want 1: the channel was closed again", e)
	}
	if n := resubscribed.Load(); n < 1 {
		t.Errorf("resubscribed called %d times, want at least 1", n)
	}
	if v := tap.Violations(); len(v) != 0 {
		t.Errorf("answers on the wire that name no delivery of their session: %v", v)
	}
	acks := 0
	for _, a := range tap.Answers() {
		if a.Kind == "ack" {
			acks++
		}
	}
	if acks != 2 {
		t.Errorf("%d acks on the wire, want 2: %+v", acks, tap.Answers())
	}
	if err := cons.Close(); err != nil {
		t.Fatal(err)
	}
	if n := ready(t, raw(t, url), DispatchQueue(domain.ChannelSMS)); n != 0 {
		t.Errorf("queue holds %d messages, want 0", n)
	}
}

// D8: the broker cancels a consumer whose delivery stays unacknowledged for
// longer than the queue's x-consumer-timeout (experiment E6; here 1 s). The
// consumer subscribes again on the same channel, the epoch advances, the held
// delivery is stale, and the message is delivered again and acked.
func TestConsumer_ServerCancelResubscribes(t *testing.T) {
	const shortTimeout = 1_000 * time.Millisecond
	url := testutil.NewRabbitMQVhost(t)
	c := dial(t, url, 1000)
	if err := c.DeclareTopology(shortTimeout); err != nil {
		t.Fatal(err)
	}
	m := msg(domain.ChannelSMS, domain.PriorityNormal)
	if _, err := newPublisher(t, c, 1).PublishBatch(ctxT(t), []Message{m}, confirmTimeout); err != nil {
		t.Fatal(err)
	}
	cons := newConsumer(t, c)
	got := make(chan Delivery, 10)
	stop := runConsumer(t, cons, func(d Delivery) { got <- d })
	held := recv(t, got)
	if held.ID != m.ID || held.Epoch != 0 {
		t.Fatalf("first delivery %s epoch %d", held.ID, held.Epoch)
	}
	// Not acked: after the timeout the broker cancels the consumer.
	again := recv(t, got)
	if again.ID != m.ID || again.Epoch != 1 || !again.d.Redelivered {
		t.Fatalf("after the cancel: %s epoch %d redelivered %v; want %s, epoch 1, redelivered", again.ID, again.Epoch, again.d.Redelivered, m.ID)
	}
	if err := held.Ack(); !errors.Is(err, ErrStale) {
		t.Errorf("ack of the cancelled consumer's delivery: %v, want ErrStale", err)
	}
	if err := again.Ack(); err != nil {
		t.Fatal(err)
	}
	if err := stop(); err != nil {
		t.Fatalf("Run after a server-side cancel and the stop: %v", err)
	}
	if err := cons.Close(); err != nil {
		t.Fatal(err)
	}
	if n := ready(t, raw(t, url), DispatchQueue(domain.ChannelSMS)); n != 0 {
		t.Errorf("queue holds %d messages, want 0", n)
	}
}

// D2: a broker that comes back empty at the same address (its vhost deleted
// and created again) has the whole topology again as soon as the connection
// reports open, and IsOpen is false from the loss until then.
func TestConnection_TopologyDeclaredAgainOnEmptyBroker(t *testing.T) {
	url := testutil.NewRabbitMQVhost(t)
	proxy := testutil.NewProxy(t, url)
	c := dial(t, proxy.URL(), 1000)
	if err := c.DeclareTopology(consumerTimeout); err != nil {
		t.Fatal(err)
	}
	n, next := c.Opens()

	proxy.Break()
	testutil.Eventually(t, func() bool { return !c.IsOpen() }, waitTimeout)
	testutil.WipeRabbitMQVhost(t, url)
	if testutil.RabbitMQQueueExists(t, url, DispatchQueue(domain.ChannelSMS)) {
		t.Fatal("the wiped vhost still has the dispatch queue")
	}
	if c.IsOpen() || closed(next) {
		t.Fatal("open, or a reconnection signalled, while the broker is unreachable")
	}

	// Sample IsOpen from the restore on: the first true must find the
	// topology in place.
	var sawTopologyMissing atomic.Bool
	var firstOpen atomic.Bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		for !firstOpen.Load() {
			if c.IsOpen() {
				for _, ch := range domain.Channels() {
					if !testutil.RabbitMQQueueExists(t, url, DispatchQueue(ch)) || !testutil.RabbitMQQueueExists(t, url, PoisonQueue(ch)) {
						sawTopologyMissing.Store(true)
					}
				}
				firstOpen.Store(true)
			}
		}
	}()
	proxy.Restore(t)
	testutil.Eventually(t, firstOpen.Load, waitTimeout)
	<-done
	if sawTopologyMissing.Load() {
		t.Error("IsOpen was true before the topology existed on the recovered connection")
	}
	if !closed(next) {
		t.Error("no reconnection signalled")
	}
	if got, _ := c.Opens(); got != n+1 {
		t.Errorf("opens %d after one reconnection, want %d", got, n+1)
	}
	// The bindings too: a publish for each channel lands in its queue.
	pub := newPublisher(t, c, 3)
	msgs := []Message{msg(domain.ChannelSMS, domain.PriorityLow), msg(domain.ChannelEmail, domain.PriorityLow), msg(domain.ChannelPush, domain.PriorityLow)}
	published, err := pub.PublishBatch(ctxT(t), msgs, confirmTimeout)
	if err != nil || len(published) != 3 {
		t.Fatalf("publish after the wipe: %v, %d published", err, len(published))
	}
}

// A channel-level error from the broker (here: a publish to an exchange that
// does not exist) closes the publisher's channel. Whatever the client library
// does next, an unroutable message must never be reported as published
// afterwards: either the return listener still works on the channel the
// library reopened, or the batch fails with ErrPublisherChannelClosed (the
// relay exits on that error and is restarted).
func TestPublisher_AfterBrokerChannelErrorReturnsAreStillSeen(t *testing.T) {
	url, c := setup(t)
	admin := raw(t, url)
	if err := admin.QueueUnbind(DispatchQueue(domain.ChannelPush), "push", DispatchExchange, nil); err != nil {
		t.Fatal(err)
	}
	pub := newPublisher(t, c, 10)

	closed := pub.ch.NotifyClose(make(chan *amqp.Error, 4))
	// The broker answers 404 NOT_FOUND and closes the channel.
	if err := pub.ch.PublishWithContext(ctxT(t), "no-such-exchange", "sms", false, false, amqp.Publishing{Body: []byte("x")}); err != nil {
		t.Fatalf("publish to the missing exchange: %v", err)
	}
	var channelErr *amqp.Error
	testutil.Eventually(t, func() bool {
		select {
		case channelErr = <-closed:
			return true
		default:
			return false
		}
	}, waitTimeout)
	if channelErr == nil || channelErr.Code != amqp.NotFound {
		t.Fatalf("channel error = %v, want 404 NOT_FOUND", channelErr)
	}

	okBatches, closedBatches := 0, 0
	for round := range 30 {
		routable := msg(domain.ChannelSMS, domain.PriorityNormal)
		unroutable := msg(domain.ChannelPush, domain.PriorityNormal)
		published, err := pub.PublishBatch(ctxT(t), []Message{unroutable, routable}, confirmTimeout)
		switch {
		case errors.Is(err, ErrPublisherChannelClosed):
			closedBatches++
			if len(published) != 0 {
				t.Fatalf("round %d: %d ids reported with a closed channel", round, len(published))
			}
		case err != nil:
			// Any other error fails the whole batch too: nothing is reported.
			if len(published) != 0 {
				t.Fatalf("round %d: %d ids reported with error %v", round, len(published), err)
			}
		default:
			okBatches++
			if slices.Contains(published, unroutable.ID) {
				t.Fatalf("round %d: the unroutable message was reported as published after the channel error", round)
			}
			if !slices.Equal(published, []uuid.UUID{routable.ID}) {
				t.Fatalf("round %d: published %v, want exactly the routable id", round, published)
			}
		}
	}
	t.Logf("after the channel error: %d batches succeeded with the return seen, %d failed with ErrPublisherChannelClosed", okBatches, closedBatches)
	if okBatches+closedBatches == 0 {
		t.Error("no batch either succeeded or reported the closed channel")
	}
}

// When the publisher's channel is closed for good (here by closing it; the
// library does the same to every listener when recovery gives up), the return
// listener is closed too. Every batch then fails with
// ErrPublisherChannelClosed and reports nothing as published.
func TestPublisher_ClosedChannelFailsEveryBatch(t *testing.T) {
	_, c := setup(t)
	pub := newPublisher(t, c, 10)
	m := msg(domain.ChannelSMS, domain.PriorityHigh)
	if published, err := pub.PublishBatch(ctxT(t), []Message{m}, confirmTimeout); err != nil || len(published) != 1 {
		t.Fatalf("before the close: %v, %v", published, err)
	}
	if err := pub.ch.Close(); err != nil {
		t.Fatalf("close the publisher's channel: %v", err)
	}
	for i := range 3 {
		published, err := pub.PublishBatch(ctxT(t), []Message{msg(domain.ChannelSMS, domain.PriorityHigh)}, confirmTimeout)
		if !errors.Is(err, ErrPublisherChannelClosed) {
			t.Fatalf("batch %d: err = %v, want ErrPublisherChannelClosed", i, err)
		}
		if len(published) != 0 {
			t.Errorf("batch %d: %d ids reported with a closed channel", i, len(published))
		}
	}
	if !c.IsOpen() {
		t.Error("the connection is not open: only the channel was closed")
	}
}

// returnObservation is what the broker did with one message that a test
// returned to its dispatch queue again and again.
type returnObservation struct {
	deliveries    int    // deliveries the consumer received
	deliveryCount []any  // the x-delivery-count header of each delivery (nil: absent)
	redelivered   []bool // the redelivered flag of each delivery
	deadLettered  bool   // the message reached the channel's poison queue
}

// returnRepeatedly publishes one message through the project's publisher to
// the SMS dispatch queue of the project's topology and consumes it through
// the project's consumer. Each delivery is handed to ret, up to returns
// times; the delivery after that is acked. It stops early when the message
// reaches the poison queue instead of being delivered again.
func returnRepeatedly(t *testing.T, returns int, ret func(Delivery) error) returnObservation {
	t.Helper()
	url, c := setup(t)
	admin := raw(t, url)
	m := msg(domain.ChannelSMS, domain.PriorityNormal)
	if published, err := newPublisher(t, c, 1).PublishBatch(ctxT(t), []Message{m}, confirmTimeout); err != nil || len(published) != 1 {
		t.Fatalf("publish: %v, %d published", err, len(published))
	}
	cons := newConsumer(t, c)
	got := make(chan Delivery, 1)
	stop := runConsumer(t, cons, func(d Delivery) { got <- d })

	var obs returnObservation
	for {
		var d Delivery
		testutil.Eventually(t, func() bool {
			select {
			case d = <-got:
				return true
			default:
			}
			obs.deadLettered = ready(t, admin, PoisonQueue(domain.ChannelSMS)) > 0
			return obs.deadLettered
		}, waitTimeout)
		if obs.deadLettered {
			break
		}
		if d.ID != m.ID {
			t.Fatalf("delivery %d carries %s, want %s", obs.deliveries+1, d.ID, m.ID)
		}
		obs.deliveries++
		obs.deliveryCount = append(obs.deliveryCount, d.d.Headers["x-delivery-count"])
		obs.redelivered = append(obs.redelivered, d.d.Redelivered)
		if obs.deliveries > returns {
			if err := d.Ack(); err != nil {
				t.Fatalf("ack: %v", err)
			}
			break
		}
		if err := ret(d); err != nil {
			t.Fatalf("return %d: %v", obs.deliveries, err)
		}
	}
	if err := stop(); err != nil {
		t.Fatalf("consumer: %v", err)
	}
	if err := cons.Close(); err != nil {
		t.Fatal(err)
	}
	// After the close nothing is unacknowledged: an un-acked message would
	// be ready again now.
	obs.deadLettered = ready(t, admin, PoisonQueue(domain.ChannelSMS)) > 0
	left := ready(t, admin, DispatchQueue(domain.ChannelSMS))
	t.Logf("deliveries %d, dead-lettered %v, left in the dispatch queue %d", obs.deliveries, obs.deadLettered, left)
	t.Logf("x-delivery-count per delivery: %v", obs.deliveryCount)
	t.Logf("redelivered per delivery: %v", obs.redelivered)
	if !obs.deadLettered && left != 0 {
		t.Errorf("the dispatch queue holds %d messages after the ack, want 0", left)
	}
	return obs
}

// ADR-0001 (D6, open issue 7): a nack with requeue does not count toward a
// quorum queue's delivery limit (the broker default, 20). One message is
// nacked 25 times through the project's consumer: it is delivered every
// time, never dead-lettered, and the 26th delivery can be acked.
func TestConsumer_NackWithRequeueDoesNotCountTowardDeliveryLimit(t *testing.T) {
	const nacks = 25
	obs := returnRepeatedly(t, nacks, Delivery.Nack)
	if obs.deadLettered {
		t.Fatalf("the message was dead-lettered after %d deliveries", obs.deliveries)
	}
	if obs.deliveries != nacks+1 {
		t.Fatalf("%d deliveries, want %d (%d nacks, then the ack)", obs.deliveries, nacks+1, nacks)
	}
	// The broker counts no delivery: it sets no x-delivery-count header.
	for i, n := range obs.deliveryCount {
		if n != nil {
			t.Errorf("delivery %d carries x-delivery-count %v, want none", i+1, n)
		}
	}
}

// The control, which shows that the test can see dead-lettering: basic.reject
// with requeue counts toward the delivery limit (ADR-0001). With the default
// limit of 20, the message is delivered 21 times and the 21st reject
// dead-letters it to the poison queue.
func TestConsumer_RejectWithRequeueCountsTowardDeliveryLimit(t *testing.T) {
	const rejects = 25
	obs := returnRepeatedly(t, rejects, func(d Delivery) error { return d.d.Reject(true) })
	if !obs.deadLettered {
		t.Fatalf("a message rejected %d times was not dead-lettered", obs.deliveries)
	}
	if obs.deliveries != 21 {
		t.Errorf("dead-lettered after %d deliveries, want 21 (a delivery limit of 20)", obs.deliveries)
	}
}
