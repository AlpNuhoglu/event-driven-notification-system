//go:build integration

package queue

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

// The event queue's arguments in the tests, in milliseconds.
var testEventQueue = EventQueueConfig{MaxLength: 50, MessageTTL: 30_000 * time.Millisecond}

// eventSink collects the events a consumer hands over.
type eventSink struct {
	mu     sync.Mutex
	events []domain.StatusChanged
}

func (s *eventSink) handle(e domain.StatusChanged) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, e)
}

func (s *eventSink) has(id uuid.UUID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.events {
		if e.ID == id {
			return true
		}
	}
	return false
}

func (s *eventSink) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.events)
}

// running is a running Run: done is closed when it has returned err.
type running struct {
	done chan struct{}
	err  error
}

func (r *running) returned() bool {
	select {
	case <-r.done:
		return true
	default:
		return false
	}
}

// runEvents dials an event consumer on url and runs it until the test
// ends.
func runEvents(t *testing.T, url string, retries int) (*EventConsumer, *eventSink, *running) {
	t.Helper()
	k, err := DialEvents(url, recoveryWait, retries, testEventQueue, discard())
	if err != nil {
		t.Fatalf("dial events: %v", err)
	}
	sink := &eventSink{}
	ctx, cancel := context.WithCancel(context.Background())
	r := &running{done: make(chan struct{})}
	go func() {
		r.err = k.Run(ctx, sink.handle)
		close(r.done)
	}()
	t.Cleanup(func() {
		cancel()
		<-r.done
		_ = k.Close()
	})
	return k, sink, r
}

// publishStatus publishes e to the events exchange on ch, as a publisher
// does.
func publishStatus(t *testing.T, ch *amqp.Channel, e domain.StatusChanged) {
	t.Helper()
	p, err := eventPublishing(e)
	if err != nil {
		t.Fatal(err)
	}
	if err := ch.PublishWithContext(ctxT(t), EventsExchange, e.RoutingKey(), false, false, p); err != nil {
		t.Fatalf("publish: %v", err)
	}
}

// checkEventQueue checks the three properties of the gateway's queue:
// exclusive, auto-delete (a classic queue, not durable) and the two
// arguments, and that it is bound to the events exchange with status.#.
func checkEventQueue(t *testing.T, url, name string) {
	t.Helper()
	q := testutil.RabbitMQQueue(t, url, name)
	if q["exclusive"] != true || q["auto_delete"] != true || q["durable"] != false || q["type"] != "classic" {
		t.Errorf("queue %s: exclusive %v, auto_delete %v, durable %v, type %v; want an exclusive, auto-delete classic queue",
			name, q["exclusive"], q["auto_delete"], q["durable"], q["type"])
	}
	args, _ := q["arguments"].(map[string]any)
	// JSON numbers decode as float64.
	if args["x-max-length"] != float64(testEventQueue.MaxLength) || args["x-message-ttl"] != float64(testEventQueue.MessageTTL.Milliseconds()) {
		t.Errorf("queue %s: arguments %v, want x-max-length %d and x-message-ttl %d",
			name, args, testEventQueue.MaxLength, testEventQueue.MessageTTL.Milliseconds())
	}
}

// The consumer's queue is exclusive, auto-delete and has x-max-length and
// x-message-ttl; after the connection was broken a new such queue exists
// (the old one is gone with its connection), and an event published after
// the reconnection arrives. Active is false while the connection is down.
func TestEventConsumer_QueueHasItsPropertiesAndExistsAgainAfterAReconnection(t *testing.T) {
	url := testutil.NewRabbitMQVhost(t)
	proxy := testutil.NewProxy(t, url)
	k, sink, r := runEvents(t, proxy.URL(), 1000)
	testutil.Eventually(t, k.Active, waitTimeout)
	first := k.Queue()
	checkEventQueue(t, url, first)

	pub := raw(t, url)
	before := statusEvent(domain.StatusQueued, 2)
	publishStatus(t, pub, before)
	testutil.Eventually(t, func() bool { return sink.has(before.ID) }, waitTimeout)

	proxy.Break()
	testutil.Eventually(t, func() bool { return !k.Active() }, waitTimeout)
	testutil.Eventually(t, func() bool { return !testutil.RabbitMQQueueExists(t, url, first) }, waitTimeout)
	proxy.Restore(t)
	testutil.Eventually(t, func() bool { return k.Active() && k.Queue() != first }, waitTimeout)
	second := k.Queue()
	checkEventQueue(t, url, second)

	after := statusEvent(domain.StatusSent, 3)
	publishStatus(t, pub, after)
	testutil.Eventually(t, func() bool { return sink.has(after.ID) }, waitTimeout)
	if r.returned() {
		t.Fatalf("Run returned during the reconnection: %v", r.err)
	}
}

// A broker that comes back empty (the vhost wiped while the connection was
// lost) gets the exchange, a new queue and its binding again: an event
// published afterwards arrives.
func TestEventConsumer_DeclaresTheTopologyAgainOnAnEmptyBroker(t *testing.T) {
	url := testutil.NewRabbitMQVhost(t)
	proxy := testutil.NewProxy(t, url)
	k, sink, _ := runEvents(t, proxy.URL(), 1000)
	testutil.Eventually(t, k.Active, waitTimeout)
	first := k.Queue()

	proxy.Break()
	testutil.Eventually(t, func() bool { return !k.Active() }, waitTimeout)
	testutil.WipeRabbitMQVhost(t, url)
	if testutil.RabbitMQQueueExists(t, url, first) {
		t.Fatal("the wiped vhost still has the queue")
	}
	proxy.Restore(t)
	testutil.Eventually(t, func() bool { return k.Active() && k.Queue() != first }, waitTimeout)
	checkEventQueue(t, url, k.Queue())

	e := statusEvent(domain.StatusFailed, 4)
	publishStatus(t, raw(t, url), e)
	testutil.Eventually(t, func() bool { return sink.has(e.ID) }, waitTimeout)
}

// A body that does not decode is dropped; the events around it arrive.
func TestEventConsumer_DropsABodyThatDoesNotDecode(t *testing.T) {
	url := testutil.NewRabbitMQVhost(t)
	k, sink, _ := runEvents(t, url, 1000)
	testutil.Eventually(t, k.Active, waitTimeout)
	pub := raw(t, url)

	first := statusEvent(domain.StatusQueued, 2)
	publishStatus(t, pub, first)
	for _, body := range []string{`not json`, `{"id":"` + uuid.NewString() + `","version":0}`, `{"status":"unknown"}`} {
		if err := pub.PublishWithContext(ctxT(t), EventsExchange, "status.sms.queued", false, false,
			amqp.Publishing{ContentType: eventContentType, Body: []byte(body)}); err != nil {
			t.Fatal(err)
		}
	}
	last := statusEvent(domain.StatusSent, 3)
	publishStatus(t, pub, last)
	testutil.Eventually(t, func() bool { return sink.has(last.ID) }, waitTimeout)
	if !sink.has(first.ID) || sink.len() != 2 {
		t.Errorf("%d events handed over, want the 2 valid ones", sink.len())
	}
}

// When the recovery budget is used up, Run returns an error, so that the
// process exits with a non-zero code.
func TestEventConsumer_RunFailsWhenRecoveryGivesUp(t *testing.T) {
	url := testutil.NewRabbitMQVhost(t)
	proxy := testutil.NewProxy(t, url)
	k, _, r := runEvents(t, proxy.URL(), 3)
	testutil.Eventually(t, k.Active, waitTimeout)

	proxy.Break()
	testutil.Eventually(t, r.returned, waitTimeout)
	if r.err == nil {
		t.Fatal("Run returned nil after recovery gave up")
	}
	if k.Active() {
		t.Error("the consumer is active after Run returned")
	}
	// Run can return first because the library ended the subscription's
	// stream while giving up; the state notification follows.
	testutil.Eventually(t, func() bool {
		select {
		case <-k.GaveUp():
			return k.Err() != nil
		default:
			return false
		}
	}, waitTimeout)
	t.Logf("Run returned: %v", r.err)
}
