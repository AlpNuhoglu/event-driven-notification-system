//go:build integration

package integration

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/queue"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/relay"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

// One notification on an idle system reaches sent through the whole path:
// store, relay, broker, worker, provider.
//
// The relay publishes inside its lane transaction, so the worker usually
// receives the message before the relay has committed. With a claim guard of
// status = 'queued' only, such a claim sees the committed 'pending' row,
// matches zero rows without waiting, and the message is acked and dropped:
// the row stays queued and this test fails.
func TestE2E_IdleSingleNotificationIsSent(t *testing.T) {
	e := newEnv(t)
	cfg := e.config(nil)
	_, w := e.startWorker(cfg, 0)
	e.startRelay(cfg)

	id := e.create(1)[0]
	e.waitStatus(id, domain.StatusSent)

	n := e.get(id)
	if n.AttemptCount != 1 || n.Failures != 0 || n.ProviderMessageID == nil || *n.ProviderMessageID == "" {
		t.Errorf("attempt_count %d, failures %d, provider_message_id set %v",
			n.AttemptCount, n.Failures, n.ProviderMessageID != nil)
	}
	a := e.attempts(id)
	if len(a) != 1 || a[0].AttemptNo != 1 || a[0].Outcome != domain.OutcomeSuccess {
		t.Errorf("attempts = %+v, want one success", a)
	}

	reqs := e.prov.requests()
	if len(reqs) != 1 {
		t.Fatalf("provider received %d requests, want 1", len(reqs))
	}
	r := reqs[0]
	if got := fieldNames(r.body); !slices.Equal(got, []string{"channel", "content", "to"}) {
		t.Errorf("body fields = %v, want exactly to, channel, content", got)
	}
	var to, channel, content string
	for name, dst := range map[string]*string{"to": &to, "channel": &channel, "content": &content} {
		if err := json.Unmarshal(r.body[name], dst); err != nil {
			t.Errorf("field %s: %v", name, err)
		}
	}
	// Compared without printing: the test fails without echoing the value.
	if to != testRecipient {
		t.Errorf("to is not the real recipient (masked form: %v, length %d)", to == maskedRecipient, len(to))
	}
	if channel != string(domain.ChannelSMS) || content != testContent {
		t.Errorf("channel %q, content matches %v", channel, content == testContent)
	}
	if r.key != id.String() {
		t.Errorf("idempotency key %q, want the notification id %s", r.key, id)
	}

	if !e.queueEmpty(w) {
		t.Error("the dispatch queue is not empty")
	}
}

// The race of decision D1, forced end to end: the worker has the message and
// its claim is waiting on the relay's row lock before the relay commits.
// After the commit the claim proceeds and the notification is sent once.
//
// TestE2E_IdleSingleNotificationIsSent runs the same path without forcing the
// order; there the relay's commit can win the race (on a fast machine it
// always does), so that test alone does not show that the claim waits. This
// one holds the relay's transaction open until the claim is seen waiting.
// With a claim guard of status = 'queued' only, the claim matches zero rows
// without waiting, the message is acked and dropped, and the wait below
// times out.
func TestE2E_WorkerFasterThanRelayCommit(t *testing.T) {
	e := newEnv(t)
	cfg := e.config(nil)
	_, w := e.startWorker(cfg, 0)

	hp := &holdingPublisher{published: make(chan struct{}), release: make(chan struct{})}
	e.startRelayWith(cfg, func(p relay.Publisher) relay.Publisher { hp.Publisher = p; return hp })
	t.Cleanup(hp.Release) // before the relay stops, if the test ends early

	id := e.create(1)[0]
	testutil.Eventually(t, func() bool { return closedChan(hp.published) }, waitTimeout)
	// The message is published and the row is still pending and locked.
	testutil.Eventually(t, func() bool { return e.claimLockWaiters() == 1 }, waitTimeout)
	if got := e.get(id).Status; got != domain.StatusPending {
		t.Fatalf("status %s while the relay's transaction is open, want pending", got)
	}
	hp.Release()

	e.waitStatus(id, domain.StatusSent)
	n := e.get(id)
	if n.AttemptCount != 1 || n.Failures != 0 || n.QueuedAt == nil {
		t.Errorf("attempt_count %d, failures %d, queued_at set %v", n.AttemptCount, n.Failures, n.QueuedAt != nil)
	}
	if got := keys(e.prov.requests()); len(got) != 1 || got[id.String()] != 1 {
		t.Errorf("provider requests by key = %v, want one for %s", got, id)
	}
	if !e.queueEmpty(w) {
		t.Error("the dispatch queue is not empty")
	}
}

// The same notification published twice (a second relay cycle after a
// rollback) is sent once: the second claim matches zero rows, its delivery is
// acked and dropped, and the queue ends empty.
func TestE2E_DuplicateMessageIsSentOnce(t *testing.T) {
	e := newEnv(t)
	cfg := e.config(nil)
	_, w := e.startWorker(cfg, 0)
	pub := e.publisher()

	id := e.create(1)[0]
	msg := queue.Message{ID: id, Channel: domain.ChannelSMS, Priority: domain.PriorityNormal, TraceParent: testTraceParent}
	for range 2 {
		ctx, cancel := e.ctx()
		published, err := pub.PublishBatch(ctx, []queue.Message{msg}, confirmTimeout)
		cancel()
		if err != nil || len(published) != 1 {
			t.Fatalf("publish: %v, %d confirmed", err, len(published))
		}
	}

	e.waitStatus(id, domain.StatusSent)
	// Both deliveries were taken: none is ready.
	testutil.Eventually(t, func() bool { return e.ready() == 0 }, waitTimeout)
	// ... and none is held: the second was acked, not kept or requeued.
	if !e.queueEmpty(w) {
		t.Error("the dispatch queue is not empty")
	}
	if got := keys(e.prov.requests()); len(got) != 1 || got[id.String()] != 1 {
		t.Errorf("provider requests by key = %v, want one for %s", got, id)
	}
	if n := e.get(id); n.AttemptCount != 1 || n.Failures != 0 {
		t.Errorf("attempt_count %d, failures %d", n.AttemptCount, n.Failures)
	}
	if a := e.attempts(id); len(a) != 1 || a[0].Outcome != domain.OutcomeSuccess {
		t.Errorf("attempts = %+v", a)
	}
}
