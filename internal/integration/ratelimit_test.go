//go:build integration

package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/queue"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/ratewindow"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/relay"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

// relayHold is how long the relay's lane transaction stays open after the
// publish, counted from the first claim seen waiting on a row lock (so at
// least this long after the first message reached the worker). ADR-0007 §5
// shows that 30 ms already exceeds the limit without the missed-slot rule;
// the hold is longer so that the scenario does so by a clear margin.
const relayHold = 100 * time.Millisecond

// batchSize is the number of notifications that become due together.
const batchSize = 200

// holdingPublisher is the relay's publisher with its first batch held: after
// PublishBatch returns, it reports the publish and waits for the release
// before the relay may go on to commit.
type holdingPublisher struct {
	relay.Publisher

	once      sync.Once
	published chan struct{}
	release   chan struct{}
	released  sync.Once
}

func (p *holdingPublisher) PublishBatch(ctx context.Context, msgs []queue.Message, timeout time.Duration) ([]uuid.UUID, error) {
	ids, err := p.Publisher.PublishBatch(ctx, msgs, timeout)
	first := false
	p.once.Do(func() { first = true })
	if first {
		close(p.published)
		<-p.release
	}
	return ids, err
}

func (p *holdingPublisher) Release() { p.released.Do(func() { close(p.release) }) }

// D77: a batch becomes due at once on an idle channel and the relay commits
// late. The worker receives the messages before the commit; their claims
// wait on the relay's row locks while the pacer keeps reserving the following
// slots. When the relay commits, the claims whose slots passed long ago must
// not send at once (the missed-slot rule): otherwise they leave together next
// to slots used on time, and a one-second window at the provider holds more
// than 100 sends. Real time, at the default 97 msg/s: about two seconds.
func TestE2E_LateRelayCommitKeepsRateLimit(t *testing.T) {
	e := newEnv(t)
	cfg := e.config(map[string]string{"RELAY_BATCH_SIZE": "200"})
	if cfg.RateLimit.PerSecond != 97 {
		t.Fatalf("rate %d, want the default 97", cfg.RateLimit.PerSecond)
	}
	ids := e.create(batchSize)
	role, _ := e.startWorker(cfg, 0)

	hp := &holdingPublisher{published: make(chan struct{}), release: make(chan struct{})}
	e.startRelayWith(cfg, func(p relay.Publisher) relay.Publisher { hp.Publisher = p; return hp })
	// Registered after the relay, so it runs before the relay is stopped and
	// its pool closed, if the test ends early.
	t.Cleanup(hp.Release)

	testutil.Eventually(t, func() bool { return closedChan(hp.published) }, waitTimeout)
	testutil.Eventually(t, func() bool { return e.claimLockWaiters() >= 1 }, waitTimeout)
	firstWaiter := time.Now()
	testutil.Eventually(t, func() bool { return time.Since(firstWaiter) >= relayHold }, waitTimeout)
	waiters := e.claimLockWaiters()
	hp.Release()

	testutil.Eventually(t, func() bool { return e.countStatus(domain.StatusSent) == batchSize }, waitTimeout)

	reqs := e.prov.requests()
	byKey := keys(reqs)
	for _, id := range ids {
		if byKey[id.String()] != 1 {
			t.Errorf("notification %s reached the provider %d times", id, byKey[id.String()])
		}
	}
	if len(reqs) != batchSize {
		t.Errorf("provider received %d requests, want %d", len(reqs), batchSize)
	}
	arrivals := make([]time.Time, len(reqs))
	for i, r := range reqs {
		arrivals[i] = r.at
	}
	maxWindow := ratewindow.MaxInWindow(arrivals, time.Second)
	missed := role.Worker(domain.ChannelSMS).MissedSlots()
	t.Logf("claims waiting at release: %d; max sends in a one-second window: %d; missed slots: %d",
		waiters, maxWindow, missed)
	if maxWindow > 100 {
		t.Errorf("a one-second window holds %d sends, more than 100", maxWindow)
	}
	if missed == 0 {
		t.Error("no slot was missed: the late commit did not happen as intended")
	}
}

func closedChan(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
