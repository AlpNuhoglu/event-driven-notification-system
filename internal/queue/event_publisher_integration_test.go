//go:build integration

package queue

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

// The status-event publisher against a real broker (ADR-0001 "Status
// events", ADR-0005, D11). Its connection runs through a testutil.Proxy
// where a test needs to stall or break it.

const (
	logEventsConnected   = "events: connected to rabbitmq; status events are published"
	logEventsLost        = "events: rabbitmq connection lost; status events are dropped until it is back"
	logEventsUnreachable = "events: rabbitmq unreachable; status events are dropped until it is back"
)

// logBuffer collects JSON log lines from several goroutines.
type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// lines returns the log lines with message msg and their levels.
func (b *logBuffer) lines(msg string) []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	var levels []string
	for _, line := range strings.Split(b.buf.String(), "\n") {
		var m map[string]any
		if json.Unmarshal([]byte(line), &m) != nil || m["msg"] != msg {
			continue
		}
		lvl, _ := m["level"].(string)
		levels = append(levels, lvl)
	}
	return levels
}

// atLevel counts the log lines at level.
func (b *logBuffer) atLevel(level string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return strings.Count(b.buf.String(), `"level":"`+level+`"`)
}

func (b *logBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// newEventPublisher makes a publisher to url that logs to the returned
// buffer at debug level. It is closed when the test ends; the log is
// printed when the test fails.
func newEventPublisher(t *testing.T, url string, bufferSize int, retry, flush time.Duration) (*EventPublisher, *logBuffer) {
	t.Helper()
	logs := &logBuffer{}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("publisher log:\n%s", logs.String())
		}
	})
	p, err := NewEventPublisher(EventPublisherConfig{
		URL: url, BufferSize: bufferSize, RetryInterval: retry, FlushTimeout: flush,
	}, slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	if err != nil {
		t.Fatalf("event publisher: %v", err)
	}
	t.Cleanup(p.Close)
	return p, logs
}

// isClosed reports whether Close has begun.
func (p *EventPublisher) isClosed() bool {
	p.closeMu.RLock()
	defer p.closeMu.RUnlock()
	return p.closed
}

// statusEvent returns an SMS event of a new notification with the status
// and version.
func statusEvent(s domain.Status, version int64) domain.StatusChanged {
	return domain.StatusChanged{
		ID:        uuid.New(),
		Channel:   domain.ChannelSMS,
		Priority:  domain.PriorityNormal,
		Status:    s,
		Version:   version,
		UpdatedAt: time.Now(),
	}
}

// received decodes the collector's messages.
func received(t *testing.T, c *testutil.Collector) []domain.StatusChanged {
	t.Helper()
	var out []domain.StatusChanged
	for _, d := range c.Messages() {
		e, err := DecodeEvent(d.Body)
		if err != nil {
			t.Fatalf("decode %s: %v", d.Body, err)
		}
		out = append(out, e)
	}
	return out
}

// publishUntilReceived publishes a new event every poll until the collector
// has received one of them, and returns how many it published. It is how a
// test waits for a publisher to be connected again: an event that finds no
// connection is dropped, not held.
func publishUntilReceived(t *testing.T, p *EventPublisher, c *testutil.Collector) int {
	t.Helper()
	var sent []uuid.UUID
	testutil.Eventually(t, func() bool {
		for _, e := range received(t, c) {
			for _, id := range sent {
				if e.ID == id {
					return true
				}
			}
		}
		e := statusEvent(domain.StatusQueued, 2)
		sent = append(sent, e.ID)
		p.Publish(e)
		return false
	}, waitTimeout)
	return len(sent)
}

// Making a publisher does no I/O and starts nothing; the first event dials,
// declares the exchange on a broker that has none, and is published with the
// event's routing key, the event's name as type, JSON and transient
// delivery.
func TestEventPublisher_LazyDeclaresExchangeAndPublishes(t *testing.T) {
	url := testutil.NewRabbitMQVhost(t)
	p, logs := newEventPublisher(t, url, 8, recoveryWait, waitTimeout)
	if p.dials.Load() != 0 || testutil.RabbitMQExchangeExists(t, url, EventsExchange) {
		t.Fatal("making the publisher dialled or declared")
	}

	first := statusEvent(domain.StatusQueued, 2)
	p.Publish(first) // routed nowhere: the exchange has no binding yet
	testutil.Eventually(t, func() bool { return p.Stats().Published == 1 }, waitTimeout)
	if !testutil.RabbitMQExchangeExists(t, url, EventsExchange) {
		t.Fatal("the publisher did not declare the exchange")
	}

	c := testutil.NewCollector(t, url, EventsExchange, EventsBindingKey)
	e := statusEvent(domain.StatusFailed, 5)
	reason := domain.ReasonExpired
	e.FailureReason = &reason
	p.Publish(e)
	testutil.Eventually(t, func() bool { return len(c.Messages()) == 1 }, waitTimeout)
	d := c.Messages()[0]
	if d.RoutingKey != "status.sms.failed" || d.Type != EventType || d.ContentType != "application/json" ||
		d.DeliveryMode != amqp.Transient || d.Exchange != EventsExchange {
		t.Errorf("message: exchange %q, routing key %q, type %q, content type %q, delivery mode %d",
			d.Exchange, d.RoutingKey, d.Type, d.ContentType, d.DeliveryMode)
	}
	got := received(t, c)[0]
	if got.ID != e.ID || got.Status != e.Status || got.Version != e.Version || got.FailureReason == nil || *got.FailureReason != reason {
		t.Errorf("event %+v, want %+v", got, e)
	}
	if s := p.Stats(); s != (EventStats{Published: 2}) {
		t.Errorf("stats %+v, want 2 published", s)
	}
	if p.dials.Load() != 1 {
		t.Errorf("%d dials, want 1", p.dials.Load())
	}
	if l := logs.lines(logEventsConnected); len(l) != 1 || l[0] != "INFO" {
		t.Errorf("connected lines %v, want one at INFO", l)
	}
}

// A broker that stays down: every event is dropped and counted, nothing is
// held, the publisher dials no more often than its retry interval, and the
// outage produces one warn line, not one per event or per attempt.
func TestEventPublisher_BrokerDownDropsLogsOnceAndPacesDials(t *testing.T) {
	url := "amqp://guest:guest@" + testutil.UnusedAddr(t) + "/"
	const n = 100

	// An interval longer than the test: one dial for every event.
	slow, slowLogs := newEventPublisher(t, url, n, time.Hour, waitTimeout)
	for range n {
		slow.Publish(statusEvent(domain.StatusQueued, 2))
	}
	testutil.Eventually(t, func() bool { return slow.Stats().DroppedNotConnected == n }, waitTimeout)
	if d := slow.dials.Load(); d != 1 {
		t.Errorf("%d dials for %d events within the retry interval, want 1", d, n)
	}
	if s := slow.Stats(); s != (EventStats{DroppedNotConnected: n}) {
		t.Errorf("stats %+v, want %d dropped as not connected", s, n)
	}

	// A short interval: it dials again, and keeps quiet about it.
	fast, fastLogs := newEventPublisher(t, url, n, recoveryWait, waitTimeout)
	testutil.Eventually(t, func() bool {
		fast.Publish(statusEvent(domain.StatusQueued, 2))
		return fast.dials.Load() >= 5
	}, waitTimeout)

	for name, logs := range map[string]*logBuffer{"one dial": slowLogs, "many dials": fastLogs} {
		if l := logs.lines(logEventsUnreachable); len(l) != 1 || l[0] != "WARN" {
			t.Errorf("%s: unreachable lines %v, want one at WARN", name, l)
		}
		if w := logs.atLevel("WARN"); w != 1 {
			t.Errorf("%s: %d warn lines, want 1", name, w)
		}
		if logs.atLevel("ERROR") != 0 || logs.atLevel("INFO") != 0 {
			t.Errorf("%s: info or error lines while the broker is down:\n%s", name, logs.String())
		}
	}
}

// After the connection is broken, events are dropped while the broker is
// unreachable and published again once it is back, without a restart; the
// loss and the new connection are one line each.
func TestEventPublisher_ReconnectsAfterConnectionLoss(t *testing.T) {
	url := testutil.NewRabbitMQVhost(t)
	proxy := testutil.NewProxy(t, url)
	c := testutil.NewCollector(t, url, EventsExchange, EventsBindingKey)
	p, logs := newEventPublisher(t, proxy.URL(), 8, recoveryWait, waitTimeout)

	publishUntilReceived(t, p, c)
	proxy.Break()
	testutil.Eventually(t, func() bool { return len(logs.lines(logEventsLost)) == 1 }, waitTimeout)
	before := p.Stats()
	for range 3 {
		p.Publish(statusEvent(domain.StatusQueued, 2))
	}
	testutil.Eventually(t, func() bool { return p.Stats().DroppedNotConnected >= before.DroppedNotConnected+3 }, waitTimeout)

	proxy.Restore(t)
	publishUntilReceived(t, p, c)

	if l := logs.lines(logEventsLost); len(l) != 1 || l[0] != "WARN" {
		t.Errorf("lost lines %v, want one at WARN", l)
	}
	if l := logs.lines(logEventsConnected); len(l) != 2 {
		t.Errorf("%d connected lines, want 2 (the first connection and the new one)", len(l))
	}
	if w := logs.atLevel("WARN"); w != 1 {
		t.Errorf("%d warn lines for one outage, want 1:\n%s", w, logs.String())
	}
	if s := p.Stats(); s.DroppedBufferFull != 0 {
		t.Errorf("stats %+v: nothing should find the buffer full", s)
	}
}

// A broker that came back empty (its vhost deleted and created again, which
// closes the connection) has the exchange again once an event is published
// after the reconnection.
func TestEventPublisher_ExchangeDeclaredAgainOnEmptyBroker(t *testing.T) {
	url := testutil.NewRabbitMQVhost(t)
	p, logs := newEventPublisher(t, url, 8, recoveryWait, waitTimeout)
	p.Publish(statusEvent(domain.StatusQueued, 2))
	testutil.Eventually(t, func() bool { return p.Stats().Published == 1 }, waitTimeout)

	testutil.WipeRabbitMQVhost(t, url)
	if testutil.RabbitMQExchangeExists(t, url, EventsExchange) {
		t.Fatal("the wiped vhost still has the exchange")
	}
	testutil.Eventually(t, func() bool { return len(logs.lines(logEventsLost)) == 1 }, waitTimeout)
	testutil.Eventually(t, func() bool {
		p.Publish(statusEvent(domain.StatusQueued, 2))
		return testutil.RabbitMQExchangeExists(t, url, EventsExchange)
	}, waitTimeout)

	c := testutil.NewCollector(t, url, EventsExchange, EventsBindingKey)
	publishUntilReceived(t, p, c)
}

// An exchange deleted under an open connection: the next publish closes the
// channel (404), the publisher drops the connection, dials again and
// declares the exchange, and later events are routed again.
func TestEventPublisher_ExchangeDeclaredAgainAfterItWasDeleted(t *testing.T) {
	url := testutil.NewRabbitMQVhost(t)
	p, logs := newEventPublisher(t, url, 8, recoveryWait, waitTimeout)
	p.Publish(statusEvent(domain.StatusQueued, 2))
	testutil.Eventually(t, func() bool { return p.Stats().Published == 1 }, waitTimeout)

	if err := raw(t, url).ExchangeDelete(EventsExchange, false, false); err != nil {
		t.Fatalf("delete exchange: %v", err)
	}
	testutil.Eventually(t, func() bool {
		p.Publish(statusEvent(domain.StatusQueued, 2))
		return len(logs.lines(logEventsConnected)) == 2 && testutil.RabbitMQExchangeExists(t, url, EventsExchange)
	}, waitTimeout)

	c := testutil.NewCollector(t, url, EventsExchange, EventsBindingKey)
	publishUntilReceived(t, p, c)
	if l := logs.lines(logEventsLost); len(l) != 1 {
		t.Errorf("lost lines %v, want one", l)
	}
}

// awaitStalledDial publishes one event through a paused proxy and waits
// until the publisher is inside its dial: it has taken that event from the
// buffer and sent the protocol header, and the broker's answer is held back.
func awaitStalledDial(t *testing.T, p *EventPublisher, proxy *testutil.Proxy) domain.StatusChanged {
	t.Helper()
	proxy.Pause()
	e := statusEvent(domain.StatusQueued, 2)
	p.Publish(e)
	testutil.Eventually(t, func() bool { return proxy.BytesToBroker() > 0 }, waitTimeout)
	return e
}

// With the connection stalled, more events than the buffer holds: every
// Publish returns at once, the surplus is dropped and counted, and the
// buffered events go out when the connection moves again.
func TestEventPublisher_FullBufferDropsWithoutBlocking(t *testing.T) {
	const size, extra = 4, 3
	url := testutil.NewRabbitMQVhost(t)
	proxy := testutil.NewProxy(t, url)
	c := testutil.NewCollector(t, url, EventsExchange, EventsBindingKey)
	p, _ := newEventPublisher(t, proxy.URL(), size, recoveryWait, waitTimeout)
	first := awaitStalledDial(t, p, proxy)

	evs := make([]domain.StatusChanged, size+extra)
	for i := range evs {
		evs[i] = statusEvent(domain.StatusQueued, 2)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, e := range evs {
			p.Publish(e)
		}
	}()
	// A Publish that waited for room would wait for the stalled dial.
	testutil.Eventually(t, func() bool { return closed(done) }, waitTimeout)
	if s := p.Stats(); s != (EventStats{DroppedBufferFull: extra}) {
		t.Fatalf("stats %+v while stalled, want %d dropped for a full buffer", s, extra)
	}

	proxy.Resume()
	want := append([]domain.StatusChanged{first}, evs[:size]...)
	testutil.Eventually(t, func() bool { return len(c.Messages()) == len(want) }, waitTimeout)
	for i, got := range received(t, c) {
		if got.ID != want[i].ID {
			t.Errorf("event %d is %s, want %s", i, got.ID, want[i].ID)
		}
	}
	if s := p.Stats(); s != (EventStats{Published: uint64(len(want)), DroppedBufferFull: extra}) {
		t.Errorf("stats %+v", s)
	}
}

// Close gives the buffered events their chance: with the dial stalled and
// events waiting, Close does not return before they are sent once the
// connection moves again within the flush time.
func TestEventPublisher_CloseSendsBufferedEvents(t *testing.T) {
	url := testutil.NewRabbitMQVhost(t)
	proxy := testutil.NewProxy(t, url)
	c := testutil.NewCollector(t, url, EventsExchange, EventsBindingKey)
	p, _ := newEventPublisher(t, proxy.URL(), 8, recoveryWait, waitTimeout)
	first := awaitStalledDial(t, p, proxy)
	want := []domain.StatusChanged{first}
	for range 3 {
		e := statusEvent(domain.StatusSent, 4)
		p.Publish(e)
		want = append(want, e)
	}

	closedAt := make(chan struct{})
	go func() {
		defer close(closedAt)
		p.Close()
	}()
	testutil.Eventually(t, p.isClosed, waitTimeout)
	proxy.Resume()
	testutil.Eventually(t, func() bool { return closed(closedAt) }, waitTimeout)

	if s := p.Stats(); s != (EventStats{Published: uint64(len(want))}) {
		t.Errorf("stats %+v after Close, want %d published", s, len(want))
	}
	testutil.Eventually(t, func() bool { return len(c.Messages()) == len(want) }, waitTimeout)
	for i, got := range received(t, c) {
		if got.ID != want[i].ID {
			t.Errorf("event %d is %s, want %s", i, got.ID, want[i].ID)
		}
	}
	p.Publish(statusEvent(domain.StatusSent, 5))
	if s := p.Stats(); s.DroppedNotConnected != 1 {
		t.Errorf("an event after Close: stats %+v, want it dropped as not connected", s)
	}
}

// Close does not hang on a broker that does not answer: after the flush
// time the stalled dial is cut, Close returns and every event is accounted
// for as dropped.
func TestEventPublisher_CloseDoesNotHangOnStalledBroker(t *testing.T) {
	const flush = 200 * time.Millisecond
	url := testutil.NewRabbitMQVhost(t)
	proxy := testutil.NewProxy(t, url)
	// The dial's own timeout (connection_timeout) is far longer than the
	// test: only Close can end it.
	p, _ := newEventPublisher(t, proxy.URL()+"?connection_timeout=600000", 8, recoveryWait, flush)
	awaitStalledDial(t, p, proxy)
	for range 3 {
		p.Publish(statusEvent(domain.StatusSent, 4))
	}

	start := time.Now()
	closedAt := make(chan time.Duration, 1)
	go func() {
		p.Close()
		closedAt <- time.Since(start)
	}()
	var took time.Duration
	if !testutil.Within(func() bool {
		select {
		case took = <-closedAt:
			return true
		default:
			return false
		}
	}, waitTimeout) {
		t.Fatalf("Close did not return within %s with a flush time of %s", waitTimeout, flush)
	}
	if took < flush {
		t.Errorf("Close returned after %s, before the flush time of %s", took, flush)
	}
	if s := p.Stats(); s != (EventStats{DroppedNotConnected: 4}) {
		t.Errorf("stats %+v, want all 4 events dropped as not connected", s)
	}
}

// Close on a publisher that never received an event returns at once and
// dials nothing; an event after it is dropped and counted.
func TestEventPublisher_CloseUnusedDialsNothing(t *testing.T) {
	p, _ := newEventPublisher(t, "amqp://guest:guest@"+testutil.UnusedAddr(t)+"/", 8, recoveryWait, waitTimeout)
	p.Close()
	p.Publish(statusEvent(domain.StatusQueued, 2))
	if s := p.Stats(); s != (EventStats{DroppedNotConnected: 1}) || p.dials.Load() != 0 {
		t.Errorf("stats %+v, %d dials; want one dropped as not connected and no dial", s, p.dials.Load())
	}
}

// A URL that cannot be parsed does not fail the constructor (a role must
// start whatever the broker's address); it is reported like an unreachable
// broker when the first event is sent.
func TestEventPublisher_UnparsableURLIsAnUnreachableBroker(t *testing.T) {
	p, logs := newEventPublisher(t, "not a url", 8, time.Hour, waitTimeout)
	p.Publish(statusEvent(domain.StatusQueued, 2))
	testutil.Eventually(t, func() bool { return p.Stats().DroppedNotConnected == 1 }, waitTimeout)
	if l := logs.lines(logEventsUnreachable); len(l) != 1 {
		t.Errorf("unreachable lines %v, want one", l)
	}
}
