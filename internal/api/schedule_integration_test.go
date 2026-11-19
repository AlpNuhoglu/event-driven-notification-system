//go:build integration

package api

import (
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

// schedRow is the scheduling part of a stored notification.
type schedRow struct {
	createdAt, dueAt, expiresAt time.Time
	scheduledAt                 *time.Time
}

func (a *testAPI) schedRow(t *testing.T, id string) schedRow {
	t.Helper()
	var r schedRow
	err := a.pool.QueryRow(ctxT(t), `
		SELECT created_at, due_at, scheduled_at, expires_at FROM notifications WHERE id = $1`, id).
		Scan(&r.createdAt, &r.dueAt, &r.scheduledAt, &r.expiresAt)
	if err != nil {
		t.Fatalf("read row: %v", err)
	}
	return r
}

// responseTime reads a time member of a response; nil for null.
func responseTime(t *testing.T, body map[string]any, name string) *time.Time {
	t.Helper()
	v, ok := body[name]
	if !ok {
		t.Fatalf("no %s in the response", name)
	}
	if v == nil {
		return nil
	}
	s, _ := v.(string)
	ts, err := time.Parse(time.RFC3339Nano, s)
	if err != nil || ts.Location() != time.UTC {
		t.Fatalf("%s = %q, want RFC 3339 in UTC", name, s)
	}
	return &ts
}

// TestCreate_Scheduled checks the stored row and the response of each case
// of a single create, on the real clock: the stored values do not depend on
// the API's clock agreeing with the database's.
func TestCreate_Scheduled(t *testing.T) {
	a := newTestAPI(t)
	normalTTL := testRetry().Normal.TTL
	highTTL := testRetry().High.TTL
	create := func(t *testing.T, extra string) (string, map[string]any) {
		t.Helper()
		r := a.post(item(extra))
		if r.status != http.StatusAccepted {
			t.Fatalf("status %d: %s", r.status, r.body)
		}
		body := r.object(t)
		return body["id"].(string), body
	}

	t.Run("scheduled ahead: due at scheduled_at, default expiry scheduled_at + TTL", func(t *testing.T) {
		when := time.Now().Add(time.Hour).Truncate(time.Microsecond).In(time.FixedZone("+03", 3*3600))
		id, body := create(t, `,"scheduled_at":"`+rfc(when)+`"`)
		r := a.schedRow(t, id)
		if r.scheduledAt == nil || !r.scheduledAt.Equal(when) || !r.dueAt.Equal(when) {
			t.Errorf("scheduled_at %v, due_at %v, want both %v", r.scheduledAt, r.dueAt, when)
		}
		if !r.expiresAt.Equal(when.Add(normalTTL)) {
			t.Errorf("expires_at %v, want %v", r.expiresAt, when.Add(normalTTL))
		}
		if got := responseTime(t, body, "scheduled_at"); got == nil || !got.Equal(when) {
			t.Errorf("response scheduled_at %v, want %v", got, when)
		}
		if got := responseTime(t, body, "expires_at"); !got.Equal(when.Add(normalTTL)) {
			t.Errorf("response expires_at %v", got)
		}
		if body["status"] != "pending" {
			t.Errorf("status %v", body["status"])
		}
	})

	t.Run("the TTL is the priority's", func(t *testing.T) {
		when := time.Now().Add(time.Hour).Truncate(time.Microsecond)
		id, _ := create(t, `,"priority":"high","scheduled_at":"`+rfc(when)+`"`)
		if r := a.schedRow(t, id); !r.expiresAt.Equal(when.Add(highTTL)) {
			t.Errorf("expires_at %v, want %v", r.expiresAt, when.Add(highTTL))
		}
	})

	t.Run("an explicit expires_at is kept", func(t *testing.T) {
		when := time.Now().Add(time.Hour).Truncate(time.Microsecond)
		exp := when.Add(2 * time.Minute)
		id, body := create(t, `,"scheduled_at":"`+rfc(when)+`","expires_at":"`+rfc(exp)+`"`)
		r := a.schedRow(t, id)
		if !r.dueAt.Equal(when) || !r.expiresAt.Equal(exp) {
			t.Errorf("due_at %v, expires_at %v; want %v, %v", r.dueAt, r.expiresAt, when, exp)
		}
		if got := responseTime(t, body, "expires_at"); !got.Equal(exp) {
			t.Errorf("response expires_at %v, want %v", got, exp)
		}
	})

	t.Run("an explicit expires_at without scheduled_at", func(t *testing.T) {
		exp := time.Now().Add(10 * time.Minute).Truncate(time.Microsecond)
		id, body := create(t, `,"expires_at":"`+rfc(exp)+`"`)
		r := a.schedRow(t, id)
		if r.scheduledAt != nil || !r.dueAt.Equal(r.createdAt) || !r.expiresAt.Equal(exp) {
			t.Errorf("row %+v, want scheduled_at NULL, due_at = created_at, expires_at %v", r, exp)
		}
		if responseTime(t, body, "scheduled_at") != nil {
			t.Errorf("response scheduled_at %v, want null", body["scheduled_at"])
		}
	})

	t.Run("inside the tolerance: stored as NULL and due now", func(t *testing.T) {
		id, body := create(t, `,"scheduled_at":"`+rfc(time.Now().Add(-10*time.Second))+`"`)
		r := a.schedRow(t, id)
		if r.scheduledAt != nil || !r.dueAt.Equal(r.createdAt) || !r.expiresAt.Equal(r.createdAt.Add(normalTTL)) {
			t.Errorf("row %+v, want scheduled_at NULL, due_at = created_at, expires_at = created_at + TTL", r)
		}
		if responseTime(t, body, "scheduled_at") != nil {
			t.Errorf("response scheduled_at %v, want null", body["scheduled_at"])
		}
	})

	t.Run("scheduled_at null is absent", func(t *testing.T) {
		id, body := create(t, `,"template":null,"scheduled_at":null`)
		r := a.schedRow(t, id)
		if r.scheduledAt != nil || !r.dueAt.Equal(r.createdAt) {
			t.Errorf("row %+v", r)
		}
		if body["scheduled_at"] != nil {
			t.Errorf("response scheduled_at %v", body["scheduled_at"])
		}
	})

	t.Run("refusals store nothing", func(t *testing.T) {
		before := a.count(t)
		now := time.Now()
		for _, tt := range []struct {
			extra string
			codes []string
		}{
			{`,"scheduled_at":"` + rfc(now.Add(-time.Hour)) + `"`, []string{"scheduled_at:in_the_past"}},
			{`,"scheduled_at":"` + rfc(now.Add(31*24*time.Hour)) + `"`, []string{"scheduled_at:too_far_ahead"}},
			{`,"scheduled_at":"2026-11-19T10:00:00"`, []string{"scheduled_at:invalid_value"}},
			{`,"scheduled_at":"` + rfc(now.Add(time.Hour)) + `","expires_at":"` + rfc(now.Add(time.Hour+time.Second)) + `"`, []string{"expires_at:too_soon"}},
			{`,"expires_at":null`, []string{"expires_at:invalid_type"}},
			{`,"scheduled_at":"` + rfc(now.Add(-time.Hour)) + `","expires_at":"x"`, []string{"scheduled_at:in_the_past", "expires_at:invalid_value"}},
		} {
			expectCodes(t, a.post(item(tt.extra)), http.StatusUnprocessableEntity, tt.codes...)
		}
		if got := a.count(t); got != before {
			t.Errorf("rows %d, want %d", got, before)
		}
	})
}

// TestCreate_ScheduleBoundariesThroughHTTP runs the boundaries on an injected
// API clock: the handler reads its Options.Now once per request.
func TestCreate_ScheduleBoundariesThroughHTTP(t *testing.T) {
	now := time.Now().Add(time.Hour).Truncate(time.Microsecond).UTC()
	a := newTestAPIOn(t, testutil.NewPool(t, store.OpenPool), &syncBuffer{}, func(o *Options) {
		o.Now = func() time.Time { return now }
		o.ScheduledAtTolerance = 30_000 * time.Millisecond
		o.ScheduledAtMaxAhead = 3_600_000 * time.Millisecond
		o.ExpiresAtMinLead = 30_000 * time.Millisecond
	})
	post := func(extra string) response { return a.post(item(extra)) }

	r := post(`,"scheduled_at":"` + rfc(now.Add(-30*time.Second)) + `"`)
	if r.status != http.StatusAccepted || r.object(t)["scheduled_at"] != nil {
		t.Errorf("now - tolerance: %d %s, want 202 with scheduled_at null", r.status, r.body)
	}
	expectCodes(t, post(`,"scheduled_at":"`+rfc(now.Add(-30*time.Second-time.Microsecond))+`"`),
		http.StatusUnprocessableEntity, "scheduled_at:in_the_past")

	r = post(`,"scheduled_at":"` + rfc(now.Add(time.Hour)) + `"`)
	if r.status != http.StatusAccepted {
		t.Fatalf("now + horizon: %d %s", r.status, r.body)
	}
	if got := responseTime(t, r.object(t), "scheduled_at"); got == nil || !got.Equal(now.Add(time.Hour)) {
		t.Errorf("now + horizon: scheduled_at %v", got)
	}
	expectCodes(t, post(`,"scheduled_at":"`+rfc(now.Add(time.Hour+time.Microsecond))+`"`),
		http.StatusUnprocessableEntity, "scheduled_at:too_far_ahead")

	start := now.Add(10 * time.Minute)
	r = post(`,"scheduled_at":"` + rfc(start) + `","expires_at":"` + rfc(start.Add(30*time.Second)) + `"`)
	if r.status != http.StatusAccepted {
		t.Errorf("expires_at at start + lead: %d %s", r.status, r.body)
	}
	expectCodes(t, post(`,"scheduled_at":"`+rfc(start)+`","expires_at":"`+rfc(start.Add(30*time.Second-time.Microsecond))+`"`),
		http.StatusUnprocessableEntity, "expires_at:too_soon")
}

// A batch judges every item at the same instant; an item in the past is
// rejected and the others are stored with their schedule.
func TestBatch_Scheduled(t *testing.T) {
	a := newTestAPI(t)
	now := time.Now()
	when := now.Add(time.Hour).Truncate(time.Microsecond)
	exp := when.Add(time.Hour)
	body := batchBody(
		item(`,"scheduled_at":"`+rfc(when)+`"`),                              // 0 scheduled
		item(`,"scheduled_at":"`+rfc(now.Add(-time.Hour))+`"`),               // 1 in the past
		item(`,"scheduled_at":"`+rfc(now.Add(-5*time.Second))+`"`),           // 2 inside the tolerance
		item(`,"scheduled_at":"`+rfc(when)+`","expires_at":"`+rfc(exp)+`"`),  // 3 explicit expiry
		item(`,"scheduled_at":"`+rfc(when)+`","expires_at":"`+rfc(when)+`"`), // 4 too soon
		item(`,"scheduled_at":"`+rfc(now.Add(40*24*time.Hour))+`"`),          // 5 too far
	)
	r := a.do(http.MethodPost, pathBatch, body, nil)
	if r.status != http.StatusAccepted {
		t.Fatalf("status %d: %s", r.status, r.body)
	}
	d := decodeBatch(t, r)
	if got := d.acceptedIndexes(); !slices.Equal(got, []int{0, 2, 3}) {
		t.Fatalf("accepted %v", got)
	}
	want := []string{"1:scheduled_at:in_the_past", "4:expires_at:too_soon", "5:scheduled_at:too_far_ahead"}
	if got := d.rejectedCodes(); !slices.Equal(got, want) {
		t.Errorf("rejected %v, want %v", got, want)
	}
	ttl := testRetry().Normal.TTL
	ids := map[int]string{}
	for _, it := range d.Accepted {
		ids[it.Index] = it.ID
	}
	if r := a.schedRow(t, ids[0]); r.scheduledAt == nil || !r.scheduledAt.Equal(when) || !r.dueAt.Equal(when) || !r.expiresAt.Equal(when.Add(ttl)) {
		t.Errorf("item 0: %+v", r)
	}
	if r := a.schedRow(t, ids[2]); r.scheduledAt != nil || !r.dueAt.Equal(r.createdAt) || !r.expiresAt.Equal(r.createdAt.Add(ttl)) {
		t.Errorf("item 2: %+v", r)
	}
	if r := a.schedRow(t, ids[3]); !r.dueAt.Equal(when) || !r.expiresAt.Equal(exp) {
		t.Errorf("item 3: %+v", r)
	}
	g := a.get("/v1/notifications/" + ids[0])
	if got := responseTime(t, g.object(t), "scheduled_at"); got == nil || !got.Equal(when) {
		t.Errorf("get scheduled_at %v", got)
	}
}

// A scheduled notification is pending until it is due, so it can be
// cancelled before its time (transition 9); the answer keeps its
// scheduled_at.
func TestCancel_Scheduled(t *testing.T) {
	a := newTestAPI(t)
	r := a.post(item(`,"scheduled_at":"` + rfc(time.Now().Add(time.Hour)) + `"`))
	if r.status != http.StatusAccepted {
		t.Fatalf("status %d: %s", r.status, r.body)
	}
	id := r.object(t)["id"].(string)
	c := a.do(http.MethodPost, "/v1/notifications/"+id+"/cancel", "", nil)
	if c.status != http.StatusOK {
		t.Fatalf("cancel: %d %s", c.status, c.body)
	}
	n := c.object(t)["notification"].(map[string]any)
	if n["status"] != "cancelled" || n["scheduled_at"] == nil {
		t.Errorf("cancelled notification %v", n)
	}
}

// A request that was accepted is answered from its Idempotency-Key when it
// is retried, also when the clock has moved so far that its scheduled_at or
// expires_at would be refused now: the rules judged against the clock must
// not turn a retry into a 422. A request that was never accepted keeps its
// validation answer and stores no key.
func TestCreate_ClockRulesDoNotBreakTheReplay(t *testing.T) {
	var mu sync.Mutex
	now := time.Now().Truncate(time.Microsecond).UTC()
	advance := func(d time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		now = now.Add(d)
	}
	a := newTestAPIOn(t, testutil.NewPool(t, store.OpenPool), &syncBuffer{}, func(o *Options) {
		o.Now = func() time.Time {
			mu.Lock()
			defer mu.Unlock()
			return now
		}
	})
	replayed := func(t *testing.T, first, again response, idField string) {
		t.Helper()
		if first.status != http.StatusAccepted || first.header.Get("Idempotent-Replayed") != "" {
			t.Fatalf("first request: %d %s", first.status, first.body)
		}
		if again.status != http.StatusAccepted || again.header.Get("Idempotent-Replayed") != "true" {
			t.Fatalf("retry: %d (Idempotent-Replayed %q) %s; want the stored 202",
				again.status, again.header.Get("Idempotent-Replayed"), again.body)
		}
		if a, b := first.object(t)[idField], again.object(t)[idField]; a == nil || a != b {
			t.Errorf("retry answered %s %v, want %v", idField, b, a)
		}
	}

	t.Run("single, scheduled_at inside the tolerance", func(t *testing.T) {
		body := item(`,"scheduled_at":"` + rfc(now.Add(-10*time.Second)) + `"`)
		first := a.postKey("/v1/notifications", body, "clock-single-1", "")
		before := a.count(t)
		advance(time.Minute)
		// Without the key the same body is refused now: the rule did move.
		expectCodes(t, a.post(body), http.StatusUnprocessableEntity, "scheduled_at:in_the_past")
		replayed(t, first, a.postKey("/v1/notifications", body, "clock-single-1", ""), "id")
		if got := a.count(t); got != before {
			t.Errorf("the retry stored %d notification(s)", got-before)
		}
	})

	t.Run("single, expires_at just far enough ahead", func(t *testing.T) {
		body := item(`,"expires_at":"` + rfc(now.Add(31*time.Second)) + `"`)
		first := a.postKey("/v1/notifications", body, "clock-single-2", "")
		advance(5 * time.Second)
		expectCodes(t, a.post(body), http.StatusUnprocessableEntity, "expires_at:too_soon")
		replayed(t, first, a.postKey("/v1/notifications", body, "clock-single-2", ""), "id")
	})

	t.Run("batch whose items are all refused by the clock now", func(t *testing.T) {
		body := batchOf(
			item(`,"scheduled_at":"`+rfc(now.Add(-10*time.Second))+`"`),
			item(`,"expires_at":"`+rfc(now.Add(31*time.Second))+`"`),
		)
		first := a.postKey("/v1/notifications/batch", body, "clock-batch-1", "")
		advance(time.Minute)
		if r := a.do(http.MethodPost, "/v1/notifications/batch", body, nil); r.status != http.StatusUnprocessableEntity {
			t.Fatalf("without the key: %d %s, want 422", r.status, r.body)
		}
		replayed(t, first, a.postKey("/v1/notifications/batch", body, "clock-batch-1", ""), "batch_id")
	})

	t.Run("a request that was never accepted", func(t *testing.T) {
		keys := a.rows(t, "idempotency_keys")
		body := item(`,"scheduled_at":"` + rfc(now.Add(-time.Hour)) + `"`)
		for range 2 {
			expectCodes(t, a.postKey("/v1/notifications", body, "clock-never", ""),
				http.StatusUnprocessableEntity, "scheduled_at:in_the_past")
		}
		if got := a.rows(t, "idempotency_keys"); got != keys {
			t.Errorf("a refused request stored %d key(s)", got-keys)
		}
	})

	t.Run("a key that is live for another request", func(t *testing.T) {
		accepted := a.postKey("/v1/notifications", item(""), "clock-other", "")
		if accepted.status != http.StatusAccepted {
			t.Fatalf("first request: %d %s", accepted.status, accepted.body)
		}
		// Another body under the same key that fails a clock rule: the
		// validation answer stands (it names what is wrong with this body).
		expectCodes(t, a.postKey("/v1/notifications", item(`,"scheduled_at":"`+rfc(now.Add(-time.Hour))+`"`), "clock-other", ""),
			http.StatusUnprocessableEntity, "scheduled_at:in_the_past")
	})
}
