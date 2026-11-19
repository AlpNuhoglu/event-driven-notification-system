//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/api"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/config"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/queue"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

// Status events through the wired roles (ADR-0005 "Status events", D11):
// the relay and the worker role publish through the publishers their
// services build, the API through one made from the same configuration, and
// a queue of the test's own, bound with status.#, receives what reaches the
// events exchange.

// eventPublisher returns the publisher every role builds from cfg. It is
// closed when the test ends.
func (e *env) eventPublisher(cfg config.Config) *queue.EventPublisher {
	e.t.Helper()
	ev, err := queue.NewEventPublisher(queue.EventPublisherConfigFrom(cfg), e.log)
	if err != nil {
		e.t.Fatalf("event publisher: %v", err)
	}
	e.t.Cleanup(ev.Close)
	return ev
}

// eventsAPI serves the API role's handler on this test's database, as
// api.Service wires it, with ev as its event publisher.
func (e *env) eventsAPI(cfg config.Config, ev *queue.EventPublisher) *httptest.Server {
	e.t.Helper()
	srv := httptest.NewServer(api.NewHandler(api.Options{
		Repository:           store.New(e.pool, store.ProfilesFrom(cfg.Retry)),
		DB:                   e.pool,
		Logger:               e.log,
		Retry:                cfg.Retry,
		MaxBodyBytes:         cfg.API.MaxBodyBytes,
		ExpiresAtMinLead:     cfg.API.ExpiresAtMinLead,
		ScheduledAtTolerance: cfg.API.SchedulePastTolerance,
		ScheduledAtMaxAhead:  cfg.API.ScheduleMaxHorizon,
		Events:               ev,
	}))
	// Registered after the publisher's Close, so it runs before it.
	e.t.Cleanup(func() {
		srv.Client().CloseIdleConnections()
		srv.Close()
	})
	return srv
}

// receivedEvents decodes what the collector has received.
func receivedEvents(t *testing.T, c *testutil.Collector) []domain.StatusChanged {
	t.Helper()
	var out []domain.StatusChanged
	for _, d := range c.Messages() {
		ev, err := queue.DecodeEvent(d.Body)
		if err != nil {
			t.Fatalf("decode %s: %v", d.Body, err)
		}
		if want := ev.RoutingKey(); d.RoutingKey != want {
			t.Errorf("event %s v%d arrived with routing key %q, want %q", ev.ID, ev.Version, d.RoutingKey, want)
		}
		out = append(out, ev)
	}
	return out
}

// eventsOf returns the events of id by version; a version seen twice is
// reported.
func eventsOf(t *testing.T, all []domain.StatusChanged, id uuid.UUID) map[int64]domain.StatusChanged {
	t.Helper()
	out := map[int64]domain.StatusChanged{}
	for _, ev := range all {
		if ev.ID != id {
			continue
		}
		if _, dup := out[ev.Version]; dup {
			t.Errorf("notification %s: version %d published twice", id, ev.Version)
		}
		out[ev.Version] = ev
	}
	return out
}

// createScheduledVia creates an SMS notification through the API, due an
// hour from now, so that no relay picks it up, and returns its id.
func (e *env) createScheduledVia(srv *httptest.Server) uuid.UUID {
	e.t.Helper()
	when := time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)
	status, body := e.call(srv, http.MethodPost, "/v1/notifications",
		`{"channel":"sms","recipient":"`+testRecipient+`","content":"`+testContent+`","scheduled_at":"`+when+`"}`)
	if status != http.StatusAccepted {
		e.t.Fatalf("create: status %d", status)
	}
	var id uuid.UUID
	if err := json.Unmarshal(body["id"], &id); err != nil {
		e.t.Fatalf("create: id: %v", err)
	}
	return id
}

// transition names the transition of ADR-0004 that took a notification
// from one status to the next. swept marks the notification whose attempt
// the lease sweeper closed: its processing → retrying or failed is 7, not
// 5 or 6.
func transition(from, to domain.Status, swept bool) int {
	waiting := from == domain.StatusPending || from == domain.StatusRetrying || from == domain.StatusQueued
	switch {
	case from == domain.StatusPending && to == domain.StatusQueued:
		return 1
	case from == domain.StatusRetrying && to == domain.StatusQueued:
		return 2
	case waiting && to == domain.StatusProcessing:
		return 3
	case from == domain.StatusProcessing && to == domain.StatusSent:
		return 4
	case from == domain.StatusProcessing && (to == domain.StatusRetrying || to == domain.StatusFailed) && swept:
		return 7
	case from == domain.StatusProcessing && to == domain.StatusRetrying:
		return 5
	case from == domain.StatusProcessing && to == domain.StatusFailed:
		return 6
	case from == domain.StatusQueued && to == domain.StatusPending:
		return 8
	case waiting && to == domain.StatusCancelled:
		return 9
	case waiting && to == domain.StatusFailed:
		return 10
	case from == domain.StatusFailed && to == domain.StatusPending:
		return 11
	case to == domain.StatusSent: // from pending, queued, retrying, failed or cancelled
		return 12
	}
	return 0
}

// The plan's P5 test: all 12 transitions of ADR-0004 produce an event on
// the events exchange, published by the role that performs them (relay: 1,
// 2, 7, 8, 10; worker: 3, 4, 5, 6, 12; api: 9, 11), each with the
// notification's id, the status the transition leads to and the row's
// version after it.
//
// Seven notifications take the paths that between them perform every
// transition. For each one, the events must form an unbroken chain of
// versions from the first transition to the row's current version (every
// transition increments the version by one), ending at the row's current
// status; each step of the chain is then named by its transition. A
// provider retry may add steps (a second 500, for example); the chain is
// checked whatever its length. The table of transition → event is logged.
func TestEvents_AllTwelveTransitionsReachTheExchange(t *testing.T) {
	e := newEnv(t)
	col := testutil.NewCollector(t, e.amqpURL, queue.EventsExchange, queue.EventsBindingKey)
	vars := recoveryVars("1") // one send at a time; a swept row is published again at once
	vars["EXPIRY_INTERVAL"] = "20ms"
	cfg := e.config(vars)
	srv := e.eventsAPI(cfg, e.eventPublisher(cfg))

	// Statuses that a test step, not a role, gave a notification: the event
	// chain starts after them.
	setBy := map[uuid.UUID]map[int64]domain.Status{}

	// 8, then 1, 3 and 4: a row queued by a relay that died after its
	// commit, its message lost; the relay's startup reconciliation returns
	// it to pending, then it is queued again and sent.
	reconciled := e.create(1)[0]
	ctx, cancel := e.ctx()
	defer cancel()
	lane := store.Lane{Channel: domain.ChannelSMS, Priority: domain.PriorityNormal}
	if _, err := e.store.PublishLane(ctx, lane, 1, func(context.Context, []store.DueRow) ([]uuid.UUID, error) {
		return []uuid.UUID{reconciled}, nil
	}); err != nil {
		t.Fatalf("publish lane: %v", err)
	}
	setBy[reconciled] = map[int64]domain.Status{1: domain.StatusPending, 2: domain.StatusQueued}
	e.startWorker(cfg, 0)
	e.startRelay(cfg)
	e.waitStatus(reconciled, domain.StatusSent)

	// 5 and 2: the provider answers 500 once, the worker schedules a retry,
	// the relay queues it again, and the second attempt is sent.
	e.prov.answer(http.StatusInternalServerError)
	before := e.prov.count()
	retried := e.create(1)[0]
	testutil.Eventually(t, func() bool { return e.prov.count() > before }, waitTimeout)
	e.prov.answer(http.StatusAccepted)
	e.waitStatus(retried, domain.StatusSent)

	// 6: the provider rejects the message (400): failed at once.
	e.prov.answer(http.StatusBadRequest)
	redriven := e.create(1)[0]
	e.waitStatus(redriven, domain.StatusFailed)
	e.prov.answer(http.StatusAccepted)

	// 11: the API redrives it; it is sent this time.
	if status, body := e.call(srv, http.MethodPost, "/v1/notifications/"+redriven.String()+"/redrive", ""); status != http.StatusOK {
		t.Fatalf("redrive: status %d: %v", status, body)
	}
	e.waitStatus(redriven, domain.StatusSent)

	// 7 and 12: the send is held at the provider past its lease; the lease
	// sweeper returns the row to retrying (7), the relay queues it again,
	// and then the provider's answer lands as a late success (12).
	release := e.prov.block(t)
	before = e.prov.count()
	late := e.create(1)[0]
	testutil.Eventually(t, func() bool { return e.prov.count() > before }, waitTimeout)
	e.expireLease(late)
	testutil.Eventually(t, func() bool {
		o := e.attemptOutcomes(late)
		return e.get(late).Status == domain.StatusQueued && len(o) == 1 && o[0] == domain.OutcomeUnknown
	}, waitTimeout)
	release()
	e.waitStatus(late, domain.StatusSent)

	// 9: the API cancels a notification that is not due yet.
	cancelled := e.createScheduledVia(srv)
	if status, body := e.call(srv, http.MethodPost, "/v1/notifications/"+cancelled.String()+"/cancel", ""); status != http.StatusOK {
		t.Fatalf("cancel: status %d: %v", status, body)
	}

	// 10: a notification not due for an hour expires in a moment; the
	// relay's expiry pass fails it.
	now := e.dbNow()
	due, expires := now.Add(time.Hour), now.Add(300*time.Millisecond)
	ctx2, cancel2 := e.ctx()
	defer cancel2()
	row, err := e.store.CreateNotification(ctx2, store.NewNotification{
		Channel: domain.ChannelSMS, Priority: domain.PriorityNormal,
		Recipient: domain.Recipient(testRecipient), Content: testContent,
		ScheduledAt: &due, ExpiresAt: &expires, TTL: testTTL, MaxFailures: 5,
		CorrelationID: "corr-integration", TraceParent: testTraceParent,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	expired := row.ID
	e.waitStatus(expired, domain.StatusFailed)

	ids := map[string]uuid.UUID{
		"reconciled": reconciled, "retried": retried, "redriven": redriven, "late": late,
		"cancelled": cancelled, "expired": expired,
	}
	// Every role publishes after its commit, so the last events can arrive
	// a moment after the rows changed.
	complete := func() bool {
		all := receivedEvents(t, col)
		for _, id := range ids {
			got := eventsOf(t, all, id)
			for v := int64(2); v <= e.get(id).Version; v++ {
				if _, test := setBy[id][v]; test {
					continue
				}
				if _, ok := got[v]; !ok {
					return false
				}
			}
		}
		return true
	}
	if !testutil.Within(complete, waitTimeout) {
		t.Errorf("not every transition's event arrived within %s", waitTimeout)
	}

	all := receivedEvents(t, col)
	seen := map[int][]string{}
	for name, id := range ids {
		row := e.get(id)
		got := eventsOf(t, all, id)
		status := map[int64]domain.Status{1: domain.StatusPending}
		for v, s := range setBy[id] {
			status[v] = s
		}
		for v := int64(2); v <= row.Version; v++ {
			if _, test := setBy[id][v]; test {
				continue
			}
			ev, ok := got[v]
			if !ok {
				t.Errorf("%s (%s): no event for version %d", name, id, v)
				continue
			}
			if ev.Channel != domain.ChannelSMS || ev.Priority != domain.PriorityNormal {
				t.Errorf("%s v%d: channel %s, priority %s", name, v, ev.Channel, ev.Priority)
			}
			if (ev.Status == domain.StatusFailed) != (ev.FailureReason != nil) {
				t.Errorf("%s v%d: status %s with failure reason %v", name, v, ev.Status, ev.FailureReason)
			}
			status[v] = ev.Status
			n := transition(status[v-1], ev.Status, id == late)
			if n == 0 {
				t.Errorf("%s v%d: %s → %s is no transition", name, v, status[v-1], ev.Status)
				continue
			}
			seen[n] = append(seen[n], fmt.Sprintf("%s v%d %s → %s", name, v, status[v-1], ev.Status))
		}
		if want := int(row.Version-1) - setAfterCreate(setBy[id]); len(got) != want {
			t.Errorf("%s: %d events, want %d (one per transition a role performed up to version %d)", name, len(got), want, row.Version)
		}
		if status[row.Version] != row.Status {
			t.Errorf("%s: last event says %s, the row is %s", name, status[row.Version], row.Status)
		}
	}
	var table strings.Builder
	for n := 1; n <= 12; n++ {
		fmt.Fprintf(&table, "  transition %2d: ", n)
		if len(seen[n]) == 0 {
			table.WriteString("NO EVENT\n")
			t.Errorf("transition %d: no event on the exchange", n)
			continue
		}
		table.WriteString(strings.Join(seen[n], "; ") + "\n")
	}
	t.Logf("events seen on %s, by transition:\n%s", queue.EventsExchange, table.String())
}

// setAfterCreate counts the versions after the creation that a test step,
// not a role, produced.
func setAfterCreate(set map[int64]domain.Status) int {
	n := 0
	for v := range set {
		if v > 1 {
			n++
		}
	}
	return n
}

// stalledPublisher returns a publisher made from cfg, whose RABBITMQ_URL is
// a proxy in front of the test's vhost, with its first dial stalled: the
// proxy holds the broker's answer back, so the publisher's goroutine waits
// inside the dial with one event, and every later event stays in the
// buffer.
func (e *env) stalledPublisher(cfg config.Config, proxy *testutil.Proxy) (*queue.EventPublisher, domain.StatusChanged) {
	e.t.Helper()
	ev := e.eventPublisher(cfg)
	proxy.Pause()
	first := domain.StatusChanged{
		ID: uuid.New(), Channel: domain.ChannelSMS, Priority: domain.PriorityNormal,
		Status: domain.StatusQueued, Version: 2, UpdatedAt: time.Now(),
	}
	ev.Publish(first)
	testutil.Eventually(e.t, func() bool { return proxy.BytesToBroker() > 0 }, waitTimeout)
	return ev, first
}

// A full buffer drops events without blocking the transition (plan, P5): the
// API's publisher is stalled with its buffer full; more events are dropped
// and counted, each Publish returning at once, and a cancel through the API
// completes meanwhile, its event dropped and counted too. When the
// connection moves again, what was buffered goes out, and nothing else.
func TestEvents_FullBufferDoesNotBlockACancel(t *testing.T) {
	const size, extra = 4, 3
	e := newEnv(t)
	col := testutil.NewCollector(t, e.amqpURL, queue.EventsExchange, queue.EventsBindingKey)
	proxy := testutil.NewProxy(t, e.amqpURL)
	cfg := e.config(map[string]string{"RABBITMQ_URL": proxy.URL(), "EVENT_BUFFER_SIZE": fmt.Sprint(size)})
	ev, first := e.stalledPublisher(cfg, proxy)
	srv := e.eventsAPI(cfg, ev)

	filler := make([]domain.StatusChanged, size+extra)
	for i := range filler {
		filler[i] = domain.StatusChanged{
			ID: uuid.New(), Channel: domain.ChannelSMS, Priority: domain.PriorityNormal,
			Status: domain.StatusQueued, Version: 2, UpdatedAt: time.Now(),
		}
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, x := range filler {
			ev.Publish(x)
		}
	}()
	testutil.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, waitTimeout)
	if s := ev.Stats(); s != (queue.EventStats{DroppedBufferFull: extra}) {
		t.Fatalf("stats %+v with the buffer full, want %d dropped", s, extra)
	}

	id := e.createScheduledVia(srv)
	if status, body := e.call(srv, http.MethodPost, "/v1/notifications/"+id.String()+"/cancel", ""); status != http.StatusOK {
		t.Fatalf("cancel: status %d: %v", status, body)
	}
	if got := e.get(id).Status; got != domain.StatusCancelled {
		t.Fatalf("status %s after the cancel, want cancelled", got)
	}
	if s := ev.Stats(); s != (queue.EventStats{DroppedBufferFull: extra + 1}) {
		t.Errorf("stats %+v after the cancel, want its event dropped too", s)
	}

	proxy.Resume()
	want := append([]domain.StatusChanged{first}, filler[:size]...)
	testutil.Eventually(t, func() bool { return ev.Stats().Published == uint64(len(want)) }, waitTimeout)
	testutil.Eventually(t, func() bool { return len(col.Messages()) == len(want) }, waitTimeout)
	for i, got := range receivedEvents(t, col) {
		if got.ID != want[i].ID {
			t.Errorf("event %d is %s, want %s", i, got.ID, want[i].ID)
		}
	}
}

// The worker role's shutdown gives its buffered events their chance: the
// events of a send are held while the events connection is stalled; the
// role is stopped and the connection moves again; when Run has returned,
// both events (processing and sent) have been published.
func TestEvents_WorkerShutdownSendsBufferedEvents(t *testing.T) {
	e := newEnv(t)
	col := testutil.NewCollector(t, e.amqpURL, queue.EventsExchange, queue.EventsBindingKey)
	proxy := testutil.NewProxy(t, e.amqpURL)
	relayCfg := e.config(nil)
	// The worker's dispatch connection is the test's own (startWorker);
	// RABBITMQ_URL reaches only its event publisher. The flush time is
	// longer than the test needs.
	workerCfg := e.config(map[string]string{"RABBITMQ_URL": proxy.URL(), "EVENT_FLUSH_TIMEOUT": millis(waitTimeout)})
	proxy.Pause()
	role, w := e.startWorker(workerCfg, 0)
	e.startRelay(relayCfg)

	id := e.create(1)[0]
	e.waitStatus(id, domain.StatusSent)
	testutil.Eventually(t, func() bool { return proxy.BytesToBroker() > 0 }, waitTimeout)
	if s := role.EventStats(); s.Published != 0 {
		t.Fatalf("stats %+v while the events connection is stalled", s)
	}

	w.cancel()
	proxy.Resume()
	if err := w.wait(t); err != nil {
		t.Fatalf("worker role returned %v", err)
	}
	if s := role.EventStats(); s != (queue.EventStats{Published: 2}) {
		t.Errorf("stats %+v after the stop, want the 2 events published", s)
	}
	testutil.Eventually(t, func() bool {
		var statuses []domain.Status
		for _, ev := range receivedEvents(t, col) {
			if ev.ID == id {
				statuses = append(statuses, ev.Status)
			}
		}
		return slices.Contains(statuses, domain.StatusProcessing) && slices.Contains(statuses, domain.StatusSent)
	}, waitTimeout)
}

// The worker role's shutdown does not hang on an events broker that does not
// answer: with the events connection stalled for good, Run returns after
// the flush time and the events are counted as dropped.
func TestEvents_WorkerShutdownDoesNotHangOnStalledBroker(t *testing.T) {
	const flush = 300 * time.Millisecond
	e := newEnv(t)
	proxy := testutil.NewProxy(t, e.amqpURL)
	relayCfg := e.config(nil)
	// The dial's own timeout is far longer than the test: only the flush
	// time ends it.
	workerCfg := e.config(map[string]string{
		"RABBITMQ_URL":        proxy.URL() + "?connection_timeout=600000",
		"EVENT_FLUSH_TIMEOUT": millis(flush),
	})
	proxy.Pause()
	role, w := e.startWorker(workerCfg, 0)
	e.startRelay(relayCfg)

	id := e.create(1)[0]
	e.waitStatus(id, domain.StatusSent)
	testutil.Eventually(t, func() bool { return proxy.BytesToBroker() > 0 }, waitTimeout)

	start := time.Now()
	w.cancel()
	if err := w.wait(t); err != nil {
		t.Fatalf("worker role returned %v", err)
	}
	if took := time.Since(start); took < flush {
		t.Errorf("the role stopped after %s, before the flush time of %s", took, flush)
	}
	if s := role.EventStats(); s != (queue.EventStats{DroppedNotConnected: 2}) {
		t.Errorf("stats %+v, want the 2 events dropped as not connected", s)
	}
}
