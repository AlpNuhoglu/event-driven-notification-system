//go:build integration

package gateway

import (
	"context"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/queue"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

// Durations are parameters, in milliseconds.
const (
	testTTL       = 3_600_000 * time.Millisecond
	recoveryWait  = 20 * time.Millisecond
	eventQueueTTL = 30_000 * time.Millisecond
)

func ctxT(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	t.Cleanup(cancel)
	return ctx
}

func newNotification() store.NewNotification {
	return store.NewNotification{
		Channel:       domain.ChannelSMS,
		Priority:      domain.PriorityNormal,
		Recipient:     domain.Recipient("+905551234567"),
		Content:       "Your code is 1234",
		TTL:           testTTL,
		MaxFailures:   5,
		CorrelationID: "corr-gateway",
		TraceParent:   "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	}
}

func createNotification(t *testing.T, s *store.Store) store.Notification {
	t.Helper()
	n, err := s.CreateNotification(ctxT(t), newNotification())
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	return n
}

func createBatch(t *testing.T, s *store.Store, n int) (uuid.UUID, []uuid.UUID) {
	t.Helper()
	items := make([]store.NewNotification, n)
	for i := range items {
		items[i] = newNotification()
	}
	res, err := s.CreateBatch(ctxT(t), nil, store.NewBatch{Total: n, CorrelationID: "corr-batch", Items: items},
		func(store.BatchResult) (store.StoredResponse, error) {
			return store.StoredResponse{Status: 202, Body: []byte(`{}`)}, nil
		})
	if err != nil || res.Outcome != store.OutcomeCreated || res.Accepted != n {
		t.Fatalf("create batch: outcome %v, error %v", res.Outcome, err)
	}
	ids := make([]uuid.UUID, n)
	for i, it := range res.Items {
		ids[i] = it.ID
	}
	return res.BatchID, ids
}

// readOnlyStore returns a store on a read-only pool of the database of url,
// as the role opens it.
func readOnlyStore(t *testing.T, url string) *store.Store {
	t.Helper()
	pool, err := store.OpenPoolWith(ctxT(t), url, store.ReadOnly)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return store.New(pool, nil)
}

// A client that subscribes after the notification was created receives the
// row's current state and version as the snapshot, read from Postgres.
func TestGateway_SnapshotAfterCreationHasTheRowsStateAndVersion(t *testing.T) {
	url := testutil.NewMigratedDatabase(t)
	pool, err := store.OpenPool(ctxT(t), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	rw := store.New(pool, nil)
	pending := createNotification(t, rw)
	moved := createNotification(t, rw)
	_, cancelled, err := rw.CancelNotification(ctxT(t), moved.ID)
	if err != nil {
		t.Fatal(err)
	}

	s := startServer(t, testOptions(readOnlyStore(t, url)))
	c := dial(t, s.url, nil)
	c.subscribe([]uuid.UUID{pending.ID, moved.ID}, nil)
	got := map[uuid.UUID]message{}
	for range 2 {
		m := c.nextStatus()
		got[m.ID] = m
	}
	if m := got[pending.ID]; m.Status != "pending" || m.Version != pending.Version || !m.Updated.Equal(pending.UpdatedAt) ||
		m.BatchID != nil || m.Reason != nil {
		t.Errorf("snapshot of the pending row %+v, want pending at version %d", m, pending.Version)
	}
	if m := got[moved.ID]; m.Status != "cancelled" || m.Version != cancelled.Version || !m.Updated.Equal(cancelled.UpdatedAt) {
		t.Errorf("snapshot of the cancelled row %+v, want cancelled at version %d", m, cancelled.Version)
	}
}

// A batch subscription yields one message per notification of the batch;
// with one of them also named by id in the same request, still one each.
func TestGateway_BatchSubscriptionYieldsOneMessagePerNotification(t *testing.T) {
	url := testutil.NewMigratedDatabase(t)
	pool, err := store.OpenPool(ctxT(t), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	rw := store.New(pool, nil)
	batch, ids := createBatch(t, rw, 4)
	createNotification(t, rw) // outside the batch

	s := startServer(t, testOptions(readOnlyStore(t, url)))
	for _, named := range [][]uuid.UUID{nil, {ids[2]}} {
		c := dial(t, s.url, nil)
		c.subscribe(named, &batch)
		got := map[uuid.UUID]int{}
		for range ids {
			m := c.nextStatus()
			if m.BatchID == nil || *m.BatchID != batch || m.Status != "pending" {
				t.Errorf("message %+v, want a pending notification of the batch", m)
			}
			got[m.ID]++
		}
		// A marker for the batch, dispatched after the snapshot, is the
		// next message: the snapshot had no more.
		marker := event(uuid.New(), &batch, domain.StatusSent, 1)
		s.g.Dispatch(marker)
		if m := c.nextStatus(); m.ID != marker.ID {
			t.Errorf("named %v: message %+v after %d snapshot messages, want the marker", named, m, len(ids))
		}
		for _, id := range ids {
			if got[id] != 1 {
				t.Errorf("named %v: %d snapshot messages for %s, want 1", named, got[id], id)
			}
		}
	}
}

// transitionAfterRead is the seam of the snapshot read: it performs the
// real read, then makes one transition of the row commit and has its event
// dispatched through the gateway before the read returns. The transition
// therefore commits after the snapshot was read, and its event is
// dispatched before the subscribe request has finished.
type transitionAfterRead struct {
	Snapshots
	once  sync.Once
	after func(ctx context.Context)
}

func (r *transitionAfterRead) SnapshotByIDs(ctx context.Context, ids []uuid.UUID) ([]domain.StatusChanged, error) {
	snap, err := r.Snapshots.SnapshotByIDs(ctx, ids)
	r.once.Do(func() { r.after(ctx) })
	return snap, err
}

// The ordering the snapshot rests on (ADR-0005 "Snapshot on subscribe"):
// the subscription is registered first, then Postgres is read. A transition
// that commits after the read, and whose event is dispatched before the
// subscribe request has finished, must reach the client. Read before the
// registration, it is dispatched to nobody and the client never learns of
// it. "Never" is decided by a marker event that both orders deliver: the
// test reads up to it.
func TestGateway_SubscriptionIsRegisteredBeforeTheSnapshotIsRead(t *testing.T) {
	url := testutil.NewMigratedDatabase(t)
	pool, err := store.OpenPool(ctxT(t), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	rw := store.New(pool, nil)
	n := createNotification(t, rw)

	var g *Gateway
	var committed domain.StatusChanged
	seam := &transitionAfterRead{Snapshots: readOnlyStore(t, url), after: func(ctx context.Context) {
		_, ev, err := rw.CancelNotification(ctx, n.ID)
		if err != nil {
			t.Errorf("cancel after the snapshot read: %v", err)
			return
		}
		committed = ev
		g.Dispatch(ev)
	}}
	s := startServer(t, testOptions(seam))
	g = s.g
	c := dial(t, s.url, nil)
	marker := uuid.New()
	c.subscribe([]uuid.UUID{n.ID, marker}, nil)

	// Up to the snapshot (the live event may come before or after it);
	// the snapshot is sent after the registration in either order, so a
	// marker dispatched now reaches the client in either order.
	var versions []int64
	for !slices.Contains(versions, n.Version) {
		m := c.nextStatus()
		versions = append(versions, m.Version)
	}
	g.Dispatch(event(marker, nil, domain.StatusSent, 1))
	for {
		m := c.nextStatus()
		if m.ID == marker {
			break
		}
		versions = append(versions, m.Version)
	}

	if committed.Version <= n.Version {
		t.Fatalf("the transition after the read has version %d, want more than the snapshot's %d", committed.Version, n.Version)
	}
	if !slices.Contains(versions, committed.Version) {
		t.Fatalf("the client never received version %d (%s), which committed after the snapshot read and was "+
			"dispatched before the subscribe request finished; it received versions %v up to the marker. "+
			"The subscription must be registered before the snapshot is read", committed.Version, committed.Status, versions)
	}
}

// The role's wiring: Run serves the gateway with the snapshot from a
// read-only pool and the events of a real consumer. An event published to
// the exchange after the snapshot reaches the subscribed client, and
// cancelling Run closes the connection with 1001.
func TestRun_DeliversPublishedEventsAndClosesOnShutdown(t *testing.T) {
	url := testutil.NewMigratedDatabase(t)
	pool, err := store.OpenPool(ctxT(t), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	rw := store.New(pool, nil)
	n := createNotification(t, rw)

	amqpURL := testutil.NewRabbitMQVhost(t)
	events, err := queue.DialEvents(amqpURL, recoveryWait, 1000,
		queue.EventQueueConfig{MaxLength: 100, MessageTTL: eventQueueTTL}, discard())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = events.Close() })

	g := New(testOptions(readOnlyStore(t, url)))
	ln, err := (&net.ListenConfig{}).Listen(ctxT(t), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, ln, g, events, waitTimeout, discard()) }()
	testutil.Eventually(t, events.Active, waitTimeout)

	c := dial(t, "ws://"+ln.Addr().String()+Path, nil)
	c.subscribe([]uuid.UUID{n.ID}, nil)
	if m := c.nextStatus(); m.ID != n.ID || m.Version != n.Version {
		t.Fatalf("snapshot %+v, want version %d", m, n.Version)
	}

	_, ev, err := rw.CancelNotification(ctxT(t), n.ID)
	if err != nil {
		t.Fatal(err)
	}
	publishEvent(t, amqpURL, ev)
	if m := c.nextStatus(); m.ID != n.ID || m.Version != ev.Version || m.Status != "cancelled" {
		t.Fatalf("live message %+v, want cancelled at version %d", m, ev.Version)
	}

	cancel()
	if status, _ := c.closeStatus(); status != websocket.StatusGoingAway {
		t.Errorf("close status %d, want %d (going away)", status, websocket.StatusGoingAway)
	}
	if err := within(t, done, waitTimeout, "Run"); err != nil {
		t.Errorf("Run: %v", err)
	}
	if events.Active() {
		t.Error("the consumer is still active after Run returned")
	}
}

// publishEvent publishes e to the events exchange as a publisher does.
func publishEvent(t *testing.T, amqpURL string, e domain.StatusChanged) {
	t.Helper()
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	body, err := queue.EncodeEvent(e)
	if err != nil {
		t.Fatal(err)
	}
	if err := ch.PublishWithContext(ctxT(t), queue.EventsExchange, e.RoutingKey(), false, false,
		amqp.Publishing{ContentType: "application/json", Type: queue.EventType, Body: body}); err != nil {
		t.Fatal(err)
	}
}
