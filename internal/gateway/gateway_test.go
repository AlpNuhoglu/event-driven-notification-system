package gateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

// A snapshot message and a live event have the same shape, every member
// present: batch_id and failure_reason are null when there is none.
func TestGateway_StatusMessagesCarryEveryMember(t *testing.T) {
	batch := uuid.New()
	plain := event(uuid.New(), nil, domain.StatusPending, 1)
	failed := event(uuid.New(), &batch, domain.StatusFailed, 7)
	reason := domain.FailureReason("max_failures")
	failed.FailureReason = &reason
	s := startServer(t, testOptions(newFakeSnapshots(plain, failed)))
	c := dial(t, s.url, nil)

	c.subscribe([]uuid.UUID{plain.ID}, nil)
	m := c.nextStatus()
	if m.ID != plain.ID || m.Status != "pending" || m.Version != 1 || m.BatchID != nil || m.Reason != nil ||
		!m.Updated.Equal(plain.UpdatedAt) {
		t.Errorf("snapshot message %+v, want the row of %s at version 1", m, plain.ID)
	}
	wantMembers := []string{"type", "id", "batch_id", "status", "version", "failure_reason", "updated_at"}
	for _, k := range wantMembers {
		if _, ok := m.raw[k]; !ok {
			t.Errorf("member %q is missing", k)
		}
	}
	if len(m.raw) != len(wantMembers) {
		t.Errorf("the message has %d members, want %d", len(m.raw), len(wantMembers))
	}
	if string(m.raw["batch_id"]) != "null" || string(m.raw["failure_reason"]) != "null" {
		t.Errorf("batch_id %s and failure_reason %s, want null", m.raw["batch_id"], m.raw["failure_reason"])
	}
	if string(m.raw["updated_at"]) != `"2026-11-19T12:00:00.123456Z"` {
		t.Errorf("updated_at %s, want RFC 3339 in UTC", m.raw["updated_at"])
	}

	c.subscribe(nil, &batch)
	m = c.nextStatus()
	if m.ID != failed.ID || m.BatchID == nil || *m.BatchID != batch || m.Reason == nil || *m.Reason != "max_failures" || m.Version != 7 {
		t.Errorf("snapshot message %+v, want the failed row with its batch and reason", m)
	}

	live := event(plain.ID, nil, domain.StatusQueued, 2)
	s.g.Dispatch(live)
	m = c.nextStatus()
	if m.ID != plain.ID || m.Status != "queued" || m.Version != 2 || len(m.raw) != len(wantMembers) {
		t.Errorf("live message %+v, want queued at version 2 with every member", m)
	}
}

// A subscription to an id without a row is registered and gets no
// snapshot: the first message is the next live event.
func TestGateway_SubscriptionToAnUnknownIDIsRegisteredWithoutSnapshot(t *testing.T) {
	s := startServer(t, testOptions(newFakeSnapshots()))
	c := dial(t, s.url, nil)
	id := uuid.New()
	c.subscribe([]uuid.UUID{id}, nil)
	testutil.Eventually(t, func() bool { return s.subscribers(id) == 1 }, waitTimeout)

	s.g.Dispatch(event(id, nil, domain.StatusPending, 1))
	if m := c.nextStatus(); m.ID != id || m.Version != 1 {
		t.Errorf("first message %+v, want the live event of %s", m, id)
	}
}

// A notification named by id and through its batch in one request is sent
// once in the snapshot, and a live event for it reaches the connection once.
func TestGateway_IDAndBatchOfTheSameNotificationAreServedOnce(t *testing.T) {
	batch := uuid.New()
	a := event(uuid.New(), &batch, domain.StatusPending, 1)
	b := event(uuid.New(), &batch, domain.StatusQueued, 2)
	s := startServer(t, testOptions(newFakeSnapshots(a, b)))
	c := dial(t, s.url, nil)

	c.subscribe([]uuid.UUID{a.ID, a.ID}, &batch)
	got := map[uuid.UUID]int{}
	for range 2 {
		got[c.nextStatus().ID]++
	}
	if got[a.ID] != 1 || got[b.ID] != 1 {
		t.Fatalf("snapshot messages per id %v, want one for each of the two notifications", got)
	}

	s.g.Dispatch(event(a.ID, &batch, domain.StatusQueued, 2))
	marker := event(b.ID, &batch, domain.StatusSent, 9)
	s.g.Dispatch(marker)
	if m := c.nextStatus(); m.ID != a.ID || m.Version != 2 {
		t.Fatalf("message %+v, want the live event of %s", m, a.ID)
	}
	if m := c.nextStatus(); m.ID != marker.ID || m.Version != marker.Version {
		t.Errorf("message %+v after the live event, want the marker: the event arrived twice", m)
	}
}

// A live event reaches every connection subscribed to it.
func TestGateway_EventReachesEverySubscribedConnection(t *testing.T) {
	s := startServer(t, testOptions(newFakeSnapshots()))
	id := uuid.New()
	batch := uuid.New()
	byID, byBatch := dial(t, s.url, nil), dial(t, s.url, nil)
	byID.subscribe([]uuid.UUID{id}, nil)
	byBatch.subscribe(nil, &batch)
	testutil.Eventually(t, func() bool { return s.subscribers(id) == 1 && s.batchSubscribers(batch) == 1 }, waitTimeout)

	s.g.Dispatch(event(id, &batch, domain.StatusSent, 5))
	for name, c := range map[string]*wsClient{"by id": byID, "by batch": byBatch} {
		if m := c.nextStatus(); m.ID != id || m.Version != 5 {
			t.Errorf("%s: message %+v, want the event", name, m)
		}
	}
}

// Subscribing again to an id sends its snapshot again and does not count
// twice; a request that would exceed the limit registers nothing, and one
// that reaches it exactly is accepted.
func TestGateway_SubscriptionLimitIsAllOrNothing(t *testing.T) {
	a, b, c3, d := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	batch := uuid.New()
	opts := testOptions(newFakeSnapshots(event(a, nil, domain.StatusPending, 1)))
	opts.MaxSubscriptions = 3
	s := startServer(t, opts)
	c := dial(t, s.url, nil)

	c.subscribe([]uuid.UUID{a, b}, nil)
	if m := c.nextStatus(); m.ID != a {
		t.Fatalf("snapshot %+v, want %s", m, a)
	}
	c.subscribe([]uuid.UUID{a}, nil) // again: its snapshot again, still two subscriptions
	if m := c.nextStatus(); m.ID != a {
		t.Fatalf("snapshot of the repeated subscription %+v, want %s", m, a)
	}

	c.subscribe([]uuid.UUID{c3}, &batch) // 2 + 2 > 3
	e := c.nextError()
	if e.Code != codeTooManySubscriptions {
		t.Fatalf("error %q, want %s", e.Code, codeTooManySubscriptions)
	}
	s.g.Dispatch(event(c3, &batch, domain.StatusQueued, 2))
	s.g.Dispatch(event(uuid.New(), &batch, domain.StatusQueued, 2))
	s.g.Dispatch(event(a, nil, domain.StatusQueued, 2)) // marker
	if m := c.nextStatus(); m.ID != a || m.Version != 2 {
		t.Fatalf("message %+v, want the marker of %s: the refused request registered something", m, a)
	}

	c.subscribe([]uuid.UUID{a, d}, nil) // 2 + 1 = 3: accepted; a's snapshot again
	if m := c.nextStatus(); m.ID != a {
		t.Fatalf("snapshot %+v, want %s", m, a)
	}
	s.g.Dispatch(event(d, nil, domain.StatusQueued, 2))
	if m := c.nextStatus(); m.ID != d {
		t.Fatalf("message %+v, want the event of %s: the request that reaches the limit was refused", m, d)
	}
	c.subscribe([]uuid.UUID{c3}, nil) // 3 + 1 > 3
	if e := c.nextError(); e.Code != codeTooManySubscriptions {
		t.Fatalf("error %q, want %s", e.Code, codeTooManySubscriptions)
	}
}

// After an unsubscribe no further event of the id or the batch arrives;
// unsubscribing from something not subscribed is not an error, and nothing
// answers an unsubscribe.
func TestGateway_UnsubscribeStopsEvents(t *testing.T) {
	s := startServer(t, testOptions(newFakeSnapshots()))
	c := dial(t, s.url, nil)
	id, kept, batch := uuid.New(), uuid.New(), uuid.New()
	c.subscribe([]uuid.UUID{id, kept}, &batch)
	testutil.Eventually(t, func() bool { return s.subscribers(id) == 1 && s.batchSubscribers(batch) == 1 }, waitTimeout)

	c.unsubscribe([]uuid.UUID{id, uuid.New()}, &batch)
	c.unsubscribe(nil, &batch) // no longer subscribed: ignored
	testutil.Eventually(t, func() bool { return s.subscribers(id) == 0 && s.batchSubscribers(batch) == 0 }, waitTimeout)

	s.g.Dispatch(event(id, nil, domain.StatusQueued, 2))
	s.g.Dispatch(event(uuid.New(), &batch, domain.StatusQueued, 2))
	s.g.Dispatch(event(kept, nil, domain.StatusQueued, 2)) // marker
	if m := c.nextStatus(); m.ID != kept {
		t.Errorf("message %+v, want the marker of %s: an event after the unsubscribe arrived", m, kept)
	}
	if n := s.subscribers(kept); n != 1 {
		t.Errorf("%d subscribers of the id still subscribed, want 1", n)
	}
}

// Every error code, each with a message that repeats nothing the client
// sent; the connection stays open after each.
func TestGateway_ErrorsNameTheProblemWithoutRepeatingTheRequest(t *testing.T) {
	const secret = "s3cr3t"
	valid := uuid.New().String()
	cases := []struct {
		name string
		typ  websocket.MessageType
		data string
		code string
	}{
		{"not JSON", websocket.MessageText, "not json " + secret, codeInvalidMessage},
		{"not an object", websocket.MessageText, `["` + secret + `"]`, codeInvalidMessage},
		{"null", websocket.MessageText, `null`, codeInvalidMessage},
		{"unknown action", websocket.MessageText, `{"action":"` + secret + `","notification_ids":["` + valid + `"]}`, codeInvalidMessage},
		{"action of the wrong type", websocket.MessageText, `{"action":["` + secret + `"],"batch_id":"` + valid + `"}`, codeInvalidMessage},
		{"no action", websocket.MessageText, `{"notification_ids":["` + valid + `"],"x":"` + secret + `"}`, codeInvalidMessage},
		{"neither member", websocket.MessageText, `{"action":"subscribe","x":"` + secret + `"}`, codeInvalidMessage},
		{"empty list", websocket.MessageText, `{"action":"subscribe","notification_ids":[]}`, codeInvalidMessage},
		{"ids not a list", websocket.MessageText, `{"action":"subscribe","notification_ids":"` + secret + `"}`, codeInvalidMessage},
		{"id not a string", websocket.MessageText, `{"action":"subscribe","notification_ids":[{"` + secret + `":1}]}`, codeInvalidMessage},
		{"batch not a string", websocket.MessageText, `{"action":"subscribe","batch_id":["` + secret + `"]}`, codeInvalidMessage},
		{"binary frame", websocket.MessageBinary, `{"action":"subscribe","notification_ids":["` + valid + `"],"x":"` + secret + `"}`, codeInvalidMessage},
		{"id not a UUID", websocket.MessageText, `{"action":"subscribe","notification_ids":["` + valid + `","` + secret + `"]}`, codeInvalidID},
		{"id in another form", websocket.MessageText, `{"action":"subscribe","notification_ids":["urn:uuid:` + valid + `"]}`, codeInvalidID},
		{"batch not a UUID", websocket.MessageText, `{"action":"unsubscribe","batch_id":"` + secret + `"}`, codeInvalidID},
	}
	snapshots := newFakeSnapshots()
	opts := testOptions(snapshots)
	opts.MaxSubscriptions = 1
	s := startServer(t, opts)
	c := dial(t, s.url, nil)

	check := func(name, wantCode string, sent []byte, e message) {
		t.Helper()
		if e.Code != wantCode {
			t.Errorf("%s: code %q (%q), want %q", name, e.Code, e.Message, wantCode)
		}
		if e.Message == "" || strings.Contains(e.Message, secret) || bytes.Contains(sent, []byte(e.Message)) {
			t.Errorf("%s: message %q is empty or repeats the request", name, e.Message)
		}
		if strings.Contains(e.Message, valid) {
			t.Errorf("%s: message %q repeats an id of the request", name, e.Message)
		}
		if len(e.raw) != 3 {
			t.Errorf("%s: the error message has %d members, want type, code and message", name, len(e.raw))
		}
	}
	for _, tc := range cases {
		c.sendRaw(tc.typ, []byte(tc.data))
		check(tc.name, tc.code, []byte(tc.data), c.nextError())
	}

	two := subscription("subscribe", []uuid.UUID{uuid.MustParse(valid), uuid.New()}, nil)
	c.send(two)
	check("over the limit", codeTooManySubscriptions, nil, c.nextError())

	// The snapshot read fails: the error is reported without the cause, and
	// the subscription stays registered.
	snapshots.setErr(errors.New("database says " + secret + " " + valid))
	c.subscribe([]uuid.UUID{uuid.MustParse(valid)}, nil)
	check("snapshot unavailable", codeSnapshotUnavailable, nil, c.nextError())
	s.g.Dispatch(event(uuid.MustParse(valid), nil, domain.StatusQueued, 2))
	if m := c.nextStatus(); m.ID.String() != valid || m.Version != 2 {
		t.Errorf("message %+v, want the live event of the subscription whose snapshot failed", m)
	}
}

// A client that does not read while events are dispatched fills its send
// queue and is closed with status 1013 (try again later). The hub is not
// blocked by it: another connection receives every event, each before the
// next is dispatched, up to the overflow and after it.
func TestGateway_FullSendQueueClosesTheConnectionAndDoesNotBlockTheHub(t *testing.T) {
	opts := testOptions(newFakeSnapshots())
	opts.SendQueueSize = 8
	// The slow client's blocked write must last until the test reads.
	opts.WriteTimeout = 60_000 * time.Millisecond
	s := startServer(t, opts, smallWriteBuffer)
	id := uuid.New()
	slow := dial(t, s.url, smallReadBuffer())
	fast := dial(t, s.url, nil)
	slow.subscribe([]uuid.UUID{id}, nil)
	fast.subscribe([]uuid.UUID{id}, nil)
	testutil.Eventually(t, func() bool { return s.subscribers(id) == 2 }, waitTimeout)

	versions := make(chan int64, 1024)
	go func() {
		defer close(versions)
		for {
			m, err := fast.read(waitTimeout)
			if err != nil {
				return
			}
			versions <- m.Version
		}
	}()
	// The events are dispatched one at a time: the fast connection's queue
	// never holds more than one, while the slow one's fills.
	const maxEvents = 100_000
	var v int64
	dispatch := func() {
		t.Helper()
		v++
		s.g.Dispatch(event(id, nil, domain.StatusQueued, v))
		if got := within(t, versions, waitTimeout, "the other connection"); got != v {
			t.Fatalf("the other connection received version %d, want %d", got, v)
		}
	}
	for s.g.SlowClientsClosed() == 0 {
		if v == maxEvents {
			t.Fatalf("no send queue overflowed in %d events", maxEvents)
		}
		dispatch()
	}
	overflowAt := v
	for range 3 {
		dispatch()
	}

	status, read := slow.closeStatus()
	if status != websocket.StatusTryAgainLater {
		t.Errorf("the slow connection ended with status %d, want %d (try again later)", status, websocket.StatusTryAgainLater)
	}
	if int64(read) >= overflowAt {
		t.Errorf("the slow connection read %d messages, want fewer than the %d dispatched up to the overflow", read, overflowAt)
	}
	t.Logf("the slow connection's queue overflowed at event %d; it read %d messages before the close frame", overflowAt, read)
	testutil.Eventually(t, func() bool { return s.g.Connections() == 1 && s.subscribers(id) == 1 }, waitTimeout)
	if n := s.g.SlowClientsClosed(); n != 1 {
		t.Errorf("%d connections closed as slow, want 1", n)
	}
}

// Without patterns only the same origin is allowed; a pattern allows the
// hosts it matches; a request without an Origin header (a client that is
// not a browser) is always accepted. A refused origin is answered 403
// before the upgrade.
func TestGateway_OriginIsCheckedBeforeTheUpgrade(t *testing.T) {
	cases := []struct {
		name     string
		patterns []string
		origin   string // "self": the server's own origin
		allowed  bool
	}{
		{"no Origin header", nil, "", true},
		{"same origin", nil, "self", true},
		{"cross origin without patterns", nil, "https://app.example.com", false},
		{"cross origin matching a pattern", []string{"*.example.com"}, "https://app.example.com", true},
		{"cross origin matching no pattern", []string{"*.example.com"}, "https://app.example.org", false},
		{"no Origin header with patterns", []string{"*.example.com"}, "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := testOptions(newFakeSnapshots())
			opts.OriginPatterns = tc.patterns
			s := startServer(t, opts)
			hdr := http.Header{}
			switch tc.origin {
			case "":
			case "self":
				hdr.Set("Origin", "http://"+s.srv.Listener.Addr().String())
			default:
				hdr.Set("Origin", tc.origin)
			}
			ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
			defer cancel()
			ws, resp, err := websocket.Dial(ctx, s.url, &websocket.DialOptions{HTTPHeader: hdr})
			if resp != nil && resp.Body != nil {
				_ = resp.Body.Close()
			}
			if !tc.allowed {
				if err == nil {
					_ = ws.CloseNow()
					t.Fatal("the connection was accepted")
				}
				if resp == nil || resp.StatusCode != http.StatusForbidden {
					t.Fatalf("refusal: response %v, error %v; want 403", resp, err)
				}
				if n := s.g.Connections(); n != 0 {
					t.Errorf("%d connections, want 0", n)
				}
				return
			}
			if err != nil {
				t.Fatalf("refused: %v", err)
			}
			c := &wsClient{t: t, ws: ws}
			t.Cleanup(func() { _ = ws.CloseNow() })
			id := uuid.New()
			c.subscribe([]uuid.UUID{id}, nil)
			testutil.Eventually(t, func() bool { return s.subscribers(id) == 1 }, waitTimeout)
		})
	}
}

// Shutdown sends every connection a close frame with status 1001 (going
// away) and waits for them to end; a connection that arrives afterwards is
// closed the same way at once.
func TestGateway_ShutdownSendsGoingAwayToEveryConnection(t *testing.T) {
	s := startServer(t, testOptions(newFakeSnapshots()))
	subscribed, idle := dial(t, s.url, nil), dial(t, s.url, nil)
	id := uuid.New()
	subscribed.subscribe([]uuid.UUID{id}, nil)
	testutil.Eventually(t, func() bool { return s.g.Connections() == 2 && s.subscribers(id) == 1 }, waitTimeout)

	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
		defer cancel()
		done <- s.g.Shutdown(ctx)
	}()
	// The clients answer the close frame when they read it.
	for name, c := range map[string]*wsClient{"subscribed": subscribed, "idle": idle} {
		if status, _ := c.closeStatus(); status != websocket.StatusGoingAway {
			t.Errorf("%s connection: close status %d, want %d (going away)", name, status, websocket.StatusGoingAway)
		}
	}
	if err := within(t, done, waitTimeout, "Shutdown"); err != nil {
		t.Errorf("Shutdown: %v", err)
	}
	if n := s.g.Connections(); n != 0 {
		t.Errorf("%d connections after Shutdown, want 0", n)
	}

	late := dial(t, s.url, nil)
	if status, _ := late.closeStatus(); status != websocket.StatusGoingAway {
		t.Errorf("a connection after Shutdown: close status %d, want %d", status, websocket.StatusGoingAway)
	}
}

// A client that does not answer pings is disconnected; one that answers
// stays connected across several pings.
func TestGateway_ClientThatDoesNotAnswerPingsIsDisconnected(t *testing.T) {
	opts := testOptions(newFakeSnapshots())
	opts.PingInterval = 50 * time.Millisecond
	s := startServer(t, opts)

	var pings atomic.Int64
	answering := dial(t, s.url, &websocket.DialOptions{OnPingReceived: func(context.Context, []byte) bool {
		pings.Add(1)
		return true
	}})
	// A client answers a ping only while it reads.
	msgs := make(chan message, 16)
	go func() {
		for {
			m, err := answering.read(waitTimeout)
			if err != nil {
				return
			}
			msgs <- m
		}
	}()
	dial(t, s.url, nil) // never reads: never answers
	testutil.Eventually(t, func() bool { return s.g.Connections() == 2 }, waitTimeout)

	testutil.Eventually(t, func() bool { return s.g.Connections() == 1 }, waitTimeout)
	testutil.Eventually(t, func() bool { return pings.Load() >= 3 }, waitTimeout)
	id := uuid.New()
	answering.subscribe([]uuid.UUID{id}, nil)
	testutil.Eventually(t, func() bool { return s.subscribers(id) == 1 }, waitTimeout)
	s.g.Dispatch(event(id, nil, domain.StatusSent, 4))
	if m := within(t, msgs, waitTimeout, "the answering client"); m.ID != id {
		t.Errorf("message %+v, want the event of %s", m, id)
	}
}

// A request of a thousand ids, with whitespace, fits the read limit; a
// message far beyond it closes the connection with status 1009.
func TestGateway_ReadLimitFitsAThousandIDs(t *testing.T) {
	s := startServer(t, testOptions(newFakeSnapshots()))
	c := dial(t, s.url, nil)
	ids := make([]string, 1000)
	for i := range ids {
		ids[i] = uuid.NewString()
	}
	req := "{\n  \"action\": \"subscribe\",\n  \"notification_ids\": [\n    \"" +
		strings.Join(ids, "\",\n    \"") + "\"\n  ]\n}"
	t.Logf("request of %d bytes against a limit of %d", len(req), s.g.readLimit)
	c.sendRaw(websocket.MessageText, []byte(req))
	last := uuid.MustParse(ids[len(ids)-1])
	testutil.Eventually(t, func() bool { return s.subscribers(last) == 1 }, waitTimeout)
	s.g.Dispatch(event(last, nil, domain.StatusQueued, 2))
	if m := c.nextStatus(); m.ID != last {
		t.Errorf("message %+v, want the event of the last id", m)
	}

	big := fmt.Sprintf(`{"action":"subscribe","notification_ids":[%q],"pad":%q}`, ids[0], strings.Repeat("x", int(s.g.readLimit)))
	c.sendRaw(websocket.MessageText, []byte(big))
	if status, _ := c.closeStatus(); status != websocket.StatusMessageTooBig {
		t.Errorf("close status %d, want %d (message too big)", status, websocket.StatusMessageTooBig)
	}
}
