//go:build integration

package relay

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/queue"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

func TestMain(m *testing.M) { testutil.Main(m) }

// Durations are parameters, in milliseconds.
const (
	waitTimeout     = 10_000 * time.Millisecond
	recoveryWait    = 20 * time.Millisecond
	consumerTimeout = 86_400_000 * time.Millisecond
	testTTL         = 3_600_000 * time.Millisecond
	testLease       = 60_000 * time.Millisecond
)

var integrationConfig = Config{
	TickInterval:       10 * time.Millisecond,
	BatchSize:          200,
	ConfirmTimeout:     5_000 * time.Millisecond,
	HWMHigh:            500,
	HWMNormal:          1000,
	HWMLow:             500,
	LeaseSweepInterval: 10 * time.Millisecond,
	LeaseSweepBatch:    500,

	ExpiryInterval:             10 * time.Millisecond,
	ExpiryBatch:                1000,
	IdempotencyCleanupInterval: 10 * time.Millisecond,
}

// zeroDelays makes a retried row due at once.
var zeroDelays = store.Profiles{
	domain.PriorityHigh: {}, domain.PriorityNormal: {}, domain.PriorityLow: {},
}

// broker is a vhost with the topology declared.
type broker struct {
	url   string
	admin *amqp.Channel // plain channel for inspection
}

func newBroker(t *testing.T) broker {
	t.Helper()
	url := testutil.NewRabbitMQVhost(t)
	c := connect(t, url)
	if err := c.DeclareTopology(consumerTimeout); err != nil {
		t.Fatalf("declare: %v", err)
	}
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("admin dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("admin channel: %v", err)
	}
	return broker{url: url, admin: ch}
}

func connect(t *testing.T, url string) *queue.Connection {
	t.Helper()
	c, err := queue.Dial(url, recoveryWait, 1000, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// newRelay returns a relay on its own broker connection to url, with the
// topology declared on that connection, as relay.Service does: until then
// the connection does not count as open.
func newRelay(t *testing.T, st *store.Store, url string, cfg Config) (*Relay, *queue.Connection, *recorder) {
	t.Helper()
	c := connect(t, url)
	if err := c.DeclareTopology(consumerTimeout); err != nil {
		t.Fatalf("declare: %v", err)
	}
	pub, err := queue.NewPublisher(c, cfg.BatchSize)
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}
	rec := &recorder{}
	return New(st, pub, c, rec, slog.New(slog.DiscardHandler), cfg, func() float64 { return 0.5 }, nil), c, rec
}

func newStore(t *testing.T) (*store.Store, *pgxpool.Pool) {
	t.Helper()
	pool := testutil.NewPool(t, store.OpenPool)
	return store.New(pool, zeroDelays), pool
}

func ctxT(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	t.Cleanup(cancel)
	return ctx
}

// create inserts n pending rows in a lane and returns their ids.
func create(t *testing.T, st *store.Store, c domain.Channel, p domain.Priority, n int) []uuid.UUID {
	t.Helper()
	recipient := domain.Recipient("test@example.com")
	if c == domain.ChannelSMS {
		recipient = domain.Recipient("+905551234567")
	}
	ids := make([]uuid.UUID, n)
	for i := range ids {
		row, err := st.CreateNotification(ctxT(t), store.NewNotification{
			Channel:       c,
			Priority:      p,
			Recipient:     recipient,
			Content:       "Your code is 1234",
			TTL:           testTTL,
			MaxFailures:   5,
			CorrelationID: "corr-relay",
			TraceParent:   "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		})
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		ids[i] = row.ID
	}
	return ids
}

func status(t *testing.T, st *store.Store, id uuid.UUID) domain.Status {
	t.Helper()
	n, err := st.GetNotification(ctxT(t), id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	return n.Status
}

func requireStatus(t *testing.T, st *store.Store, ids []uuid.UUID, want domain.Status) {
	t.Helper()
	for _, id := range ids {
		if got := status(t, st, id); got != want {
			t.Errorf("row %s is %s, want %s", id, got, want)
		}
	}
}

// drain takes every message out of a queue.
func drain(t *testing.T, ch *amqp.Channel, queue string) []amqp.Delivery {
	t.Helper()
	var out []amqp.Delivery
	for {
		d, ok, err := ch.Get(queue, true)
		if err != nil {
			t.Fatalf("get %s: %v", queue, err)
		}
		if !ok {
			return out
		}
		out = append(out, d)
	}
}

func eventIDs(evs []domain.StatusChanged) map[uuid.UUID]bool {
	out := make(map[uuid.UUID]bool, len(evs))
	for _, e := range evs {
		out[e.ID] = true
	}
	return out
}

// TestRelay_UnroutablePublishStaysPending: with no binding for a routing key
// the broker returns the publish, and its row stays pending; no such row is
// ever queued. In a batch that mixes returned and confirmed messages, exactly
// the confirmed ones become queued. Run with -count=100: it rests on the
// client handing over each return before its confirm (U8).
func TestRelay_UnroutablePublishStaysPending(t *testing.T) {
	b := newBroker(t)
	st, _ := newStore(t)
	// The relay declares the topology on its connection; the binding is
	// removed afterwards.
	r, _, rec := newRelay(t, st, b.url, integrationConfig)
	if err := b.admin.QueueUnbind(queue.DispatchQueue(domain.ChannelPush), "push", queue.DispatchExchange, nil); err != nil {
		t.Fatal(err)
	}

	// A whole lane is unroutable; the lanes of the other channels are not.
	push := create(t, st, domain.ChannelPush, domain.PriorityHigh, 5)
	sms := create(t, st, domain.ChannelSMS, domain.PriorityHigh, 5)
	runCycle(t, r, ctxT(t))
	requireStatus(t, st, push, domain.StatusPending)
	requireStatus(t, st, sms, domain.StatusQueued)
	evs := eventIDs(rec.events())
	for _, id := range push {
		if evs[id] {
			t.Errorf("event for the returned row %s", id)
		}
	}
	if got := len(drain(t, b.admin, queue.DispatchQueue(domain.ChannelSMS))); got != len(sms) {
		t.Errorf("dispatch.sms holds %d messages, want %d", got, len(sms))
	}

	// One batch mixes returned and confirmed messages: the rows of one lane
	// are published alternately with an unbound and a bound routing key.
	mixed := create(t, st, domain.ChannelEmail, domain.PriorityNormal, 10)
	pub, err := queue.NewPublisher(connect(t, b.url), 10)
	if err != nil {
		t.Fatal(err)
	}
	routed := map[uuid.UUID]bool{}
	_, err = st.PublishLane(ctxT(t), store.Lane{Channel: domain.ChannelEmail, Priority: domain.PriorityNormal}, 10,
		func(ctx context.Context, rows []store.DueRow) ([]uuid.UUID, error) {
			msgs := make([]queue.Message, len(rows))
			for i, row := range rows {
				c := domain.ChannelPush // unbound: returned
				if i%2 == 1 {
					c = domain.ChannelEmail
					routed[row.ID] = true
				}
				msgs[i] = queue.Message{ID: row.ID, Channel: c, Priority: domain.PriorityNormal}
			}
			return pub.PublishBatch(ctx, msgs, integrationConfig.ConfirmTimeout)
		})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range mixed {
		want := domain.StatusPending
		if routed[id] {
			want = domain.StatusQueued
		}
		if got := status(t, st, id); got != want {
			t.Errorf("mixed batch: row %s is %s, want %s", id, got, want)
		}
	}

	// Once the binding exists again, a later cycle publishes the pending rows.
	if err := b.admin.QueueBind(queue.DispatchQueue(domain.ChannelPush), "push", queue.DispatchExchange, false, nil); err != nil {
		t.Fatal(err)
	}
	runCycle(t, r, ctxT(t))
	requireStatus(t, st, push, domain.StatusQueued)
}

// TestRelay_PrioritySetOnEveryMessage: every published message carries the
// AMQP priority 6 - db_priority, for all three priorities.
func TestRelay_PrioritySetOnEveryMessage(t *testing.T) {
	b := newBroker(t)
	st, _ := newStore(t)
	r, _, _ := newRelay(t, st, b.url, integrationConfig)
	amqpPriority := map[domain.Priority]uint8{domain.PriorityHigh: 6, domain.PriorityNormal: 5, domain.PriorityLow: 4}
	want := map[string]uint8{}
	for _, c := range domain.Channels() {
		for _, p := range domain.Priorities() {
			for _, id := range create(t, st, c, p, 3) {
				want[id.String()] = amqpPriority[p]
			}
		}
	}
	runCycle(t, r, ctxT(t))

	seen := 0
	for _, c := range domain.Channels() {
		for _, d := range drain(t, b.admin, queue.DispatchQueue(c)) {
			w, ok := want[d.MessageId]
			if !ok {
				t.Errorf("unexpected message %q", d.MessageId)
				continue
			}
			if d.Priority != w {
				t.Errorf("message %s: priority %d, want %d", d.MessageId, d.Priority, w)
			}
			if d.RoutingKey != string(c) {
				t.Errorf("message %s: routing key %q, want %q", d.MessageId, d.RoutingKey, c)
			}
			seen++
		}
	}
	if seen != len(want) {
		t.Errorf("%d messages, want %d", seen, len(want))
	}
}

// TestRelay_TwoRelaysNeverPublishSameRow: two relays with their own broker
// connections run their cycles at the same time on the same rows. Every row
// is published exactly once and ends queued with version 2.
func TestRelay_TwoRelaysNeverPublishSameRow(t *testing.T) {
	b := newBroker(t)
	st, _ := newStore(t)
	cfg := integrationConfig
	cfg.BatchSize = 7 // many small lane transactions, so the two relays interleave
	r1, _, _ := newRelay(t, st, b.url, cfg)
	r2, _, _ := newRelay(t, st, b.url, cfg)
	var all []uuid.UUID
	for _, c := range domain.Channels() {
		for _, p := range domain.Priorities() {
			all = append(all, create(t, st, c, p, 20)...)
		}
	}

	pending := func() int {
		n := 0
		for _, id := range all {
			if status(t, st, id) == domain.StatusPending {
				n++
			}
		}
		return n
	}
	for round := 0; pending() > 0; round++ {
		if round > 20 {
			t.Fatal("rows still pending after 20 rounds")
		}
		var wg sync.WaitGroup
		wg.Go(func() { runCycle(t, r1, context.Background()) })
		wg.Go(func() { runCycle(t, r2, context.Background()) })
		wg.Wait()
	}

	deliveries := map[string]int{}
	for _, c := range domain.Channels() {
		for _, d := range drain(t, b.admin, queue.DispatchQueue(c)) {
			deliveries[d.MessageId]++
		}
	}
	for _, id := range all {
		if n := deliveries[id.String()]; n != 1 {
			t.Errorf("row %s published %d times, want 1", id, n)
		}
		n, err := st.GetNotification(ctxT(t), id)
		if err != nil {
			t.Fatal(err)
		}
		if n.Status != domain.StatusQueued || n.Version != 2 {
			t.Errorf("row %s: %s version %d, want queued version 2", id, n.Status, n.Version)
		}
	}
	if len(deliveries) != len(all) {
		t.Errorf("%d distinct messages, want %d", len(deliveries), len(all))
	}
}

// TestRelay_LaneHWMHoldsOnRealRows: a single relay never takes a lane above
// its high-water mark, however many cycles run.
func TestRelay_LaneHWMHoldsOnRealRows(t *testing.T) {
	b := newBroker(t)
	st, _ := newStore(t)
	cfg := integrationConfig
	cfg.BatchSize = 3
	cfg.HWMHigh, cfg.HWMNormal, cfg.HWMLow = 4, 7, 2
	r, _, _ := newRelay(t, st, b.url, cfg)
	for _, p := range domain.Priorities() {
		create(t, st, domain.ChannelSMS, p, 10)
	}
	for range 6 {
		runCycle(t, r, ctxT(t))
	}
	counts, err := st.QueuedCounts(ctxT(t))
	if err != nil {
		t.Fatal(err)
	}
	for p, want := range map[domain.Priority]int{domain.PriorityHigh: 4, domain.PriorityNormal: 7, domain.PriorityLow: 2} {
		if got := counts[store.Lane{Channel: domain.ChannelSMS, Priority: p}]; got != want {
			t.Errorf("lane sms/%s: %d queued, want %d", p, got, want)
		}
	}
	if got := len(drain(t, b.admin, queue.DispatchQueue(domain.ChannelSMS))); got != 13 {
		t.Errorf("dispatch.sms holds %d messages, want 13", got)
	}
}

// TestRelay_ConfirmTimeoutLeavesRowsPending: the confirms do not arrive in
// time; the lane rolls back and every row stays pending.
func TestRelay_ConfirmTimeoutLeavesRowsPending(t *testing.T) {
	b := newBroker(t)
	proxy := testutil.NewProxy(t, b.url)
	st, _ := newStore(t)
	cfg := integrationConfig
	cfg.ConfirmTimeout = 200 * time.Millisecond
	r, _, rec := newRelay(t, st, proxy.URL(), cfg)
	ids := create(t, st, domain.ChannelSMS, domain.PriorityHigh, 5)

	proxy.Pause() // confirms are held back
	runCycle(t, r, ctxT(t))
	requireStatus(t, st, ids, domain.StatusPending)
	if n := len(rec.events()); n != 0 {
		t.Errorf("%d events after a failed lane, want 0", n)
	}

	proxy.Resume()
	runCycle(t, r, ctxT(t))
	requireStatus(t, st, ids, domain.StatusQueued)
}

// TestRelay_ConnectionLostDuringBatchLeavesRowsPending: the connection drops
// while the lane waits for its confirms; the lane rolls back and every row
// stays pending. After recovery the rows are published.
func TestRelay_ConnectionLostDuringBatchLeavesRowsPending(t *testing.T) {
	b := newBroker(t)
	proxy := testutil.NewProxy(t, b.url)
	st, _ := newStore(t)
	r, conn, rec := newRelay(t, st, proxy.URL(), integrationConfig)
	ids := create(t, st, domain.ChannelSMS, domain.PriorityHigh, 5)

	proxy.Pause()
	before := proxy.BytesToBroker()
	done := make(chan struct{})
	go func() {
		defer close(done)
		runCycle(t, r, context.Background())
	}()
	testutil.Eventually(t, func() bool { return proxy.BytesToBroker() > before }, waitTimeout)
	proxy.Break()
	select {
	case <-done:
	case <-time.After(waitTimeout): // bound on the test, not a wait for a state
		t.Fatal("the cycle did not end after the connection was lost")
	}
	requireStatus(t, st, ids, domain.StatusPending)
	if n := len(rec.events()); n != 0 {
		t.Errorf("%d events after a failed lane, want 0", n)
	}

	proxy.Resume()
	proxy.Restore(t)
	testutil.Eventually(t, conn.IsOpen, waitTimeout)
	runCycle(t, r, ctxT(t))
	requireStatus(t, st, ids, domain.StatusQueued)
}

// TestRelay_LeaseSweeperRecoversExpiredLease: the sweeper loop moves a
// processing row whose lease has expired to retrying and emits its event.
func TestRelay_LeaseSweeperRecoversExpiredLease(t *testing.T) {
	b := newBroker(t)
	st, pool := newStore(t)
	r, _, rec := newRelay(t, st, b.url, integrationConfig)
	id := create(t, st, domain.ChannelSMS, domain.PriorityHigh, 1)[0]
	runCycle(t, r, ctxT(t))
	if _, _, err := st.Claim(ctxT(t), id, testLease, "mock"); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if _, err := pool.Exec(ctxT(t),
		`UPDATE notifications SET lease_expires_at = now() - interval '1 millisecond' WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.RunLeaseSweeper(ctx) }()
	testutil.Eventually(t, func() bool {
		for _, e := range rec.events() {
			if e.ID == id && e.Status == domain.StatusRetrying {
				return true
			}
		}
		return false
	}, waitTimeout)
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := status(t, st, id); got != domain.StatusRetrying {
		t.Errorf("row is %s, want retrying", got)
	}
}
