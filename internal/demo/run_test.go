package demo

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
)

// wsEvent is one message the fake gateway sends for the notification at
// position idx of the subscription.
type wsEvent struct {
	idx     int
	status  string
	version int64
	reason  string
}

// fakeStack is an API and a gateway in one test server set.
type fakeStack struct {
	mu       sync.Mutex
	ids      []string        // in creation order
	keys     map[string]bool // idempotency keys seen
	requests []recordedReq
	final    map[string]string // status answered by GET, by id; default sent
	// failAt is the creation position of the notification GET reports as
	// failed; -1 for none.
	failAt int
	// script is what the gateway sends after a subscription.
	script func(n int) []wsEvent

	api *httptest.Server
	gw  *httptest.Server
}

type recordedReq struct {
	method, path, key string
	body              []byte
}

func newFakeStack(t *testing.T, script func(n int) []wsEvent) *fakeStack {
	t.Helper()
	f := &fakeStack{keys: map[string]bool{}, final: map[string]string{}, script: script, failAt: -1}
	f.api = httptest.NewServer(http.HandlerFunc(f.serveAPI))
	f.gw = httptest.NewServer(http.HandlerFunc(f.serveWS))
	t.Cleanup(f.api.Close)
	t.Cleanup(f.gw.Close)
	return f
}

func (f *fakeStack) serveAPI(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	defer f.mu.Unlock()
	key := r.Header.Get("Idempotency-Key")
	f.requests = append(f.requests, recordedReq{r.Method, r.URL.Path, key, body})
	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/v1/notifications":
		id := f.newID(key, w)
		_, _ = fmt.Fprintf(w, `{"id":%q}`, id)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/notifications/batch":
		id := f.newID(key, w)
		_, _ = fmt.Fprintf(w, `{"batch_id":%q,"accepted":[{"index":0,"id":%q}],"rejected":[]}`, uuid.NewString(), id)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/notifications/"):
		id := strings.TrimPrefix(r.URL.Path, "/v1/notifications/")
		st := f.final[id]
		if st == "" {
			st = "sent"
		}
		var reason any
		if st == "failed" {
			reason = "max_failures"
		}
		b, _ := json.Marshal(map[string]any{"id": id, "status": st, "attempt_count": 1, "failure_reason": reason})
		_, _ = w.Write(b)
	case r.URL.Path == "/v1/metrics/summary":
		_, _ = w.Write([]byte(`{"generated_at":"2026-01-01T00:00:00Z","window":"5m","channels":[
			{"channel":"sms","attempts":{"success":2,"retryable":1},"success_rate":0.6667},
			{"channel":"email","attempts":{"success":0},"success_rate":null}]}`))
	default:
		http.NotFound(w, r)
	}
}

// newID answers 202 and returns the id: a new one, or on a key seen before
// the first id of the run in order, marking the answer as a replay.
func (f *fakeStack) newID(key string, w http.ResponseWriter) string {
	if f.keys[key] {
		w.Header().Set("Idempotent-Replayed", "true")
	}
	f.keys[key] = true
	id := uuid.NewString()
	f.ids = append(f.ids, id)
	if len(f.ids)-1 == f.failAt {
		f.final[id] = "failed"
	}
	w.WriteHeader(http.StatusAccepted)
	return id
}

func (f *fakeStack) serveWS(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer func() { _ = c.CloseNow() }()
	ctx := r.Context()
	_, data, err := c.Read(ctx)
	if err != nil {
		return
	}
	var req struct {
		IDs []string `json:"notification_ids"`
	}
	_ = json.Unmarshal(data, &req)
	for _, e := range f.script(len(req.IDs)) {
		m := map[string]any{"type": "status_changed", "id": req.IDs[e.idx], "status": e.status, "version": e.version, "failure_reason": nil}
		if e.reason != "" {
			m["failure_reason"] = e.reason
		}
		b, _ := json.Marshal(m)
		if err := c.Write(ctx, websocket.MessageText, b); err != nil {
			return
		}
	}
	_, _, _ = c.Read(ctx) // stay open until the client leaves
}

func (f *fakeStack) options(count int) Options {
	return Options{
		API:         f.api.URL,
		Gateway:     "ws" + strings.TrimPrefix(f.gw.URL, "http"),
		Count:       count,
		Timeout:     5 * time.Second,
		ProviderURL: "http://provider.invalid:8090/",
		Prober:      &Prober{Dial: func(context.Context, string, bool) error { return nil }},
		Now:         func() time.Time { return time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC) },
	}
}

// allSent is a script in which every notification goes pending, then sent.
func allSent(n int) []wsEvent {
	var ev []wsEvent
	for i := range n {
		ev = append(ev, wsEvent{i, "pending", 1, ""}, wsEvent{i, "sent", 4, ""})
	}
	return ev
}

func assertNoRecipient(t *testing.T, out string) {
	t.Helper()
	for _, v := range []string{fakePhone, fakeEmail, fakePush, "demo message"} {
		if strings.Contains(out, v) {
			t.Errorf("the output contains %q:\n%s", v, out)
		}
	}
}

func run(t *testing.T, o Options) (int, string) {
	t.Helper()
	var buf bytes.Buffer
	code := Run(context.Background(), o, &buf)
	return code, buf.String()
}

func TestRunCreatesOnlyFakeValuesThroughBothEndpointsWithKeys(t *testing.T) {
	f := newFakeStack(t, allSent)
	code, out := run(t, f.options(5))
	if code != 0 {
		t.Fatalf("exit %d:\n%s", code, out)
	}
	batches, singles := 0, 0
	channels := map[string]bool{}
	for _, r := range f.requests {
		if r.method != http.MethodPost {
			continue
		}
		if r.key == "" {
			t.Errorf("%s has no Idempotency-Key", r.path)
		}
		var items []createRequest
		if r.path == "/v1/notifications/batch" {
			batches++
			var b struct {
				Notifications []createRequest `json:"notifications"`
			}
			if err := json.Unmarshal(r.body, &b); err != nil {
				t.Fatal(err)
			}
			items = b.Notifications
		} else {
			singles++
			var one createRequest
			if err := json.Unmarshal(r.body, &one); err != nil {
				t.Fatal(err)
			}
			items = []createRequest{one}
		}
		for _, it := range items {
			channels[it.Channel] = true
			want := map[string]string{"sms": fakePhone, "email": fakeEmail, "push": fakePush}[it.Channel]
			if it.Recipient != want || !strings.HasPrefix(it.Content, "demo message ") {
				t.Errorf("item %+v is not made of the fake values", it)
			}
		}
	}
	if batches != 1 || singles != 4 || len(channels) != 3 {
		t.Fatalf("batches %d, singles %d, channels %v", batches, singles, channels)
	}
	assertNoRecipient(t, out)
	if !strings.Contains(out, "+90555***4567") {
		t.Errorf("the masked recipient is not shown:\n%s", out)
	}
}

func TestRunPrintsEventsInArrivalOrderAndIgnoresAnOlderVersion(t *testing.T) {
	f := newFakeStack(t, func(n int) []wsEvent {
		ev := []wsEvent{
			{0, "pending", 1, ""},
			{1, "pending", 1, ""},
			{0, "sent", 3, ""},
			{0, "processing", 2, ""}, // late: older than the sent that is held
			{0, "sent", 3, ""},       // the same version again
		}
		for i := 1; i < n; i++ {
			ev = append(ev, wsEvent{i, "sent", 2, ""})
		}
		return ev
	})
	code, out := run(t, f.options(2))
	if code != 0 {
		t.Fatalf("exit %d:\n%s", code, out)
	}
	if strings.Contains(out, "processing") {
		t.Errorf("an older version was printed:\n%s", out)
	}
	id0, id1 := shortID(f.ids[0]), shortID(f.ids[1])
	var lines []string
	for _, l := range strings.Split(out, "\n") {
		if strings.HasPrefix(l, "12:00:00.000") {
			lines = append(lines, l)
		}
	}
	want := []struct{ id, status string }{{id0, "pending"}, {id1, "pending"}, {id0, "sent"}, {id1, "sent"}}
	if len(lines) != len(want) {
		t.Fatalf("event lines = %d, want %d:\n%s", len(lines), len(want), out)
	}
	for i, w := range want {
		if !strings.Contains(lines[i], w.id) || !strings.Contains(lines[i], w.status) {
			t.Errorf("line %d = %q, want id %s status %s", i, lines[i], w.id, w.status)
		}
	}
	assertNoRecipient(t, out)
}

func TestRunEndsAsSoonAsEveryNotificationIsFinal(t *testing.T) {
	// The fake sends nothing after the finals and keeps the connection open:
	// a run that waited for more would last until the timeout.
	f := newFakeStack(t, allSent)
	o := f.options(3)
	o.Timeout = time.Minute
	start := time.Now()
	code, out := run(t, o)
	if code != 0 || time.Since(start) > 20*time.Second {
		t.Fatalf("exit %d after %s:\n%s", code, time.Since(start), out)
	}
	if !strings.Contains(out, "success_rate: 67%") || !strings.Contains(out, "retryable 1, success 2") {
		t.Errorf("the metrics summary is not printed:\n%s", out)
	}
	for _, want := range []string{"Grafana", "Prometheus", "Jaeger", "attempts 1"} {
		if !strings.Contains(out, want) {
			t.Errorf("the output lacks %q:\n%s", want, out)
		}
	}
}

func TestRunWithAFailedNotificationExitsWithOne(t *testing.T) {
	f := newFakeStack(t, func(n int) []wsEvent {
		ev := allSent(n)
		return append(ev[:len(ev)-1:len(ev)-1], wsEvent{n - 1, "failed", 5, "max_failures"})
	})
	f.failAt = 1 // GET reports the second notification as failed too
	code, out := run(t, f.options(2))
	if code != 1 {
		t.Fatalf("exit %d, want 1:\n%s", code, out)
	}
	if !strings.Contains(out, "failed") || !strings.Contains(out, "max_failures") {
		t.Errorf("the failure is not shown:\n%s", out)
	}
}

func TestRunTimeoutNamesWhatIsStillPending(t *testing.T) {
	f := newFakeStack(t, func(n int) []wsEvent {
		ev := []wsEvent{{n - 1, "queued", 2, ""}}
		for i := 0; i < n-1; i++ {
			ev = append(ev, wsEvent{i, "sent", 3, ""})
		}
		return ev
	})
	o := f.options(3)
	o.Timeout = 300 * time.Millisecond
	code, out := run(t, o)
	if code != 1 {
		t.Fatalf("exit %d, want 1:\n%s", code, out)
	}
	want := fmt.Sprintf("still pending: %s (queued)", shortID(f.ids[2]))
	if !strings.Contains(out, want) {
		t.Errorf("the output lacks %q:\n%s", want, out)
	}
	assertNoRecipient(t, out)
}

func TestRunSecondTimeSaysItReplayed(t *testing.T) {
	f := newFakeStack(t, allSent)
	if code, out := run(t, f.options(3)); code != 0 || strings.Contains(out, "replay") {
		t.Fatalf("first run: exit %d:\n%s", code, out)
	}
	code, out := run(t, f.options(3))
	if code != 0 || !strings.Contains(out, "3 of 3 requests were replays") {
		t.Fatalf("second run: exit %d:\n%s", code, out)
	}
}

func TestRunRefusesACountOutsideOneToTenBeforeSendingAnything(t *testing.T) {
	for _, n := range []int{11, 100, 0, -1} {
		f := newFakeStack(t, allSent)
		dialed := false
		o := f.options(n)
		o.Prober = &Prober{Dial: func(context.Context, string, bool) error { dialed = true; return nil }}
		code, out := run(t, o)
		if code == 0 || !strings.Contains(out, "--count must be between 1 and 10") {
			t.Errorf("count %d: exit %d:\n%s", n, code, out)
		}
		if len(f.requests) != 0 || dialed {
			t.Errorf("count %d: something was sent (requests %d, dialed %v)", n, len(f.requests), dialed)
		}
	}
}

func TestRunWithAnUnreachableProviderSendsNothing(t *testing.T) {
	f := newFakeStack(t, allSent)
	o := f.options(3)
	o.Prober = &Prober{Dial: func(context.Context, string, bool) error { return fmt.Errorf("refused") }}
	code, out := run(t, o)
	if code != 1 || len(f.requests) != 0 || !strings.Contains(out, "nothing was sent") {
		t.Fatalf("exit %d, requests %d:\n%s", code, len(f.requests), out)
	}
}

func TestMainRefusesACountAboveTenBeforeReadingTheConfiguration(t *testing.T) {
	var buf bytes.Buffer
	code := Main(context.Background(), []string{"--count", "11"}, func(string) (string, bool) { return "", false }, &buf)
	if code != 2 || !strings.Contains(buf.String(), "--count must be between 1 and 10") {
		t.Fatalf("exit %d:\n%s", code, buf.String())
	}
}
