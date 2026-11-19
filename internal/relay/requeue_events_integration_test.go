//go:build integration

package relay

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/queue"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

// requeue-stale publishes the events of its transitions (8, queued →
// pending) to the events exchange, like the relay role, and its output is
// unchanged.
func TestRequeueStaleCommand_PublishesEvents(t *testing.T) {
	st, pool := newStore(t)
	amqpURL := testutil.NewRabbitMQVhost(t)
	col := testutil.NewCollector(t, amqpURL, queue.EventsExchange, queue.EventsBindingKey)
	ids := queueRows(t, st, pool, 3, 600_000)

	code, out, errOut := runRelayBinary(t, map[string]string{
		"DATABASE_URL": pool.Config().ConnString(),
		"RABBITMQ_URL": amqpURL,
	}, "requeue-stale")
	if code != 0 || out != "requeue-stale: 3 notifications returned to pending\n" || errOut != "" {
		t.Fatalf("exit %d, stdout %q, stderr %q", code, out, errOut)
	}
	// The command closed its publisher before it returned, so its events
	// are with the broker; the collector's consumer may still be reading.
	testutil.Eventually(t, func() bool { return len(col.Messages()) == len(ids) }, waitTimeout)
	want := map[uuid.UUID]bool{}
	for _, id := range ids {
		want[id] = true
	}
	for _, d := range col.Messages() {
		e, err := queue.DecodeEvent(d.Body)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if !want[e.ID] || e.Status != domain.StatusPending || e.Version != 3 || d.RoutingKey != "status.sms.pending" {
			t.Errorf("event %s %s v%d (%s), want one of the requeued rows pending at version 3", e.ID, e.Status, e.Version, d.RoutingKey)
		}
		delete(want, e.ID)
	}
	if len(want) != 0 {
		t.Errorf("no event for %v", want)
	}
}

// requeue-stale with a broker that accepts the connection and never answers
// still exits 0 with its usual output, after the publisher's flush time
// (EVENT_FLUSH_TIMEOUT), not after the dial's own timeout.
func TestRequeueStaleCommand_StalledBrokerDoesNotHoldIt(t *testing.T) {
	const flush = 200 * time.Millisecond
	st, pool := newStore(t)
	proxy := testutil.NewProxy(t, testutil.NewRabbitMQVhost(t))
	proxy.Pause()
	queueRows(t, st, pool, 2, 600_000)

	start := time.Now()
	code, out, errOut := runRelayBinary(t, map[string]string{
		"DATABASE_URL":        pool.Config().ConnString(),
		"RABBITMQ_URL":        proxy.URL() + "?connection_timeout=600000",
		"EVENT_FLUSH_TIMEOUT": "200ms",
	}, "requeue-stale")
	took := time.Since(start)
	if code != 0 || out != "requeue-stale: 2 notifications returned to pending\n" || errOut != "" {
		t.Fatalf("exit %d, stdout %q, stderr %q", code, out, errOut)
	}
	if proxy.BytesToBroker() == 0 {
		t.Error("the command did not try to publish its events")
	}
	if took < flush || took > waitTimeout {
		t.Errorf("the command took %s; want at least the flush time of %s and far less than the dial timeout", took, flush)
	}
}
