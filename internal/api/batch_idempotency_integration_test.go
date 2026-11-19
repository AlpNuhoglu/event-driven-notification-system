//go:build integration

package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

const (
	pathCreate = "/v1/notifications"
	pathBatch  = "/v1/notifications/batch"
	waitT      = 10_000 * time.Millisecond
	// itBigBody fits a batch of 1,001 items.
	itBigBody = 1 << 20
)

// item is a valid create request with extra members appended.
func item(extra string) string {
	return `{"channel":"sms","recipient":"+905551234567","content":"hello"` + extra + `}`
}

// Invalid items.
const (
	faxItem          = `{"channel":"fax","recipient":"+905551234567","content":"hello"}`
	emptyContentItem = `{"channel":"sms","recipient":"+905551234567","content":""}`
)

func batchBody(items ...string) string {
	return `{"notifications":[` + strings.Join(items, ",") + `]}`
}

// postKey posts body to path with the Idempotency-Key key ("" for none) and
// the X-Request-ID reqID ("" for none).
func (a *testAPI) postKey(path, body, key, reqID string) response {
	a.t.Helper()
	return a.do(http.MethodPost, path, body, map[string][]string{"Idempotency-Key": {key}, "X-Request-ID": {reqID}})
}

func (a *testAPI) rows(t *testing.T, table string) int {
	t.Helper()
	var n int
	if err := a.pool.QueryRow(ctxT(t), `SELECT count(*) FROM `+table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// counts returns the number of rows of notifications, batches and
// idempotency_keys.
func (a *testAPI) counts(t *testing.T) [3]int {
	t.Helper()
	return [3]int{a.rows(t, "notifications"), a.rows(t, "batches"), a.rows(t, "idempotency_keys")}
}

func jsonEqual(t *testing.T, a, b []byte) bool {
	t.Helper()
	var va, vb any
	if err := json.Unmarshal(a, &va); err != nil {
		t.Fatalf("decode %s: %v", a, err)
	}
	if err := json.Unmarshal(b, &vb); err != nil {
		t.Fatalf("decode %s: %v", b, err)
	}
	return reflect.DeepEqual(va, vb)
}

type fieldErr struct {
	Field      string  `json:"field"`
	Code       string  `json:"code"`
	Message    string  `json:"message"`
	ExistingID *string `json:"existing_id"`
}

type rejectedItem struct {
	Index  int        `json:"index"`
	Errors []fieldErr `json:"errors"`
}

// batchDoc decodes both the 202 body and the 422 problem of the batch endpoint.
type batchDoc struct {
	BatchID  string `json:"batch_id"`
	Accepted []struct {
		Index int    `json:"index"`
		ID    string `json:"id"`
	} `json:"accepted"`
	Rejected []rejectedItem   `json:"rejected"`
	Detail   string           `json:"detail"`
	Type     string           `json:"type"`
	Errors   *json.RawMessage `json:"errors"`
}

func decodeBatch(t *testing.T, r response) batchDoc {
	t.Helper()
	var d batchDoc
	if err := json.Unmarshal(r.body, &d); err != nil {
		t.Fatalf("decode %s: %v", r.body, err)
	}
	return d
}

func (d batchDoc) acceptedIndexes() []int {
	out := []int{}
	for _, a := range d.Accepted {
		out = append(out, a.Index)
	}
	return out
}

// rejectedCodes lists "index:field:code" per error.
func (d batchDoc) rejectedCodes() []string {
	out := []string{}
	for _, r := range d.Rejected {
		for _, e := range r.Errors {
			out = append(out, fmt.Sprintf("%d:%s:%s", r.Index, e.Field, e.Code))
		}
	}
	return out
}

// N concurrent identical requests with one key: exactly one creates, every
// answer is 202 with the same body, and all but one are replays.
func TestIdempotencyKey_ConcurrentSameKey(t *testing.T) {
	const callers = 8
	for _, tc := range []struct {
		name, path, body string
		want             [3]int // notifications, batches, keys
	}{
		{"single", pathCreate, item(`,"dedupe_key":"order-c"`), [3]int{1, 0, 1}},
		{"batch", pathBatch, batchBody(item(""), item(`,"priority":"low"`), faxItem), [3]int{2, 1, 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := newTestAPI(t)
			results := make([]response, callers)
			start := make(chan struct{})
			var wg sync.WaitGroup
			for i := range callers {
				wg.Go(func() {
					<-start
					results[i] = a.postKey(tc.path, tc.body, "key-concurrent", fmt.Sprintf("req-%d", i))
				})
			}
			close(start)
			wg.Wait()

			replayed := 0
			for i, r := range results {
				if r.status != http.StatusAccepted {
					t.Fatalf("caller %d: status %d: %s", i, r.status, r.body)
				}
				if !jsonEqual(t, r.body, results[0].body) || r.header.Get("Location") != results[0].header.Get("Location") {
					t.Errorf("caller %d: %s %s, want %s %s", i, r.header.Get("Location"), r.body,
						results[0].header.Get("Location"), results[0].body)
				}
				if got := r.header.Get("X-Request-ID"); got != fmt.Sprintf("req-%d", i) {
					t.Errorf("caller %d: X-Request-ID %q", i, got)
				}
				switch v := r.header.Values("Idempotent-Replayed"); {
				case len(v) == 1 && v[0] == "true":
					replayed++
				case len(v) != 0:
					t.Errorf("caller %d: Idempotent-Replayed %q", i, v)
				}
			}
			if replayed != callers-1 {
				t.Errorf("%d replays, want %d", replayed, callers-1)
			}
			if got := a.counts(t); got != tc.want {
				t.Errorf("notifications, batches, keys = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestIdempotencyKey_Replay(t *testing.T) {
	a := newTestAPI(t)
	for _, tc := range []struct{ name, path, body, location string }{
		{"single", pathCreate, validBody, "/v1/notifications/"},
		{"batch", pathBatch, batchBody(item(""), faxItem), "/v1/batches/"},
	} {
		key := "key-replay-" + tc.name
		first := a.postKey(tc.path, tc.body, key, "first-"+tc.name)
		if first.status != http.StatusAccepted || first.header.Get("Idempotent-Replayed") != "" {
			t.Fatalf("%s: first %d %v", tc.name, first.status, first.header)
		}
		before := a.counts(t)
		// Same request with other key order and whitespace: the same hash.
		var v any
		if err := json.Unmarshal([]byte(tc.body), &v); err != nil {
			t.Fatal(err)
		}
		reordered, err := json.MarshalIndent(v, "", "   ")
		if err != nil {
			t.Fatal(err)
		}
		again := a.postKey(tc.path, string(reordered), key, "second-"+tc.name)
		if again.status != http.StatusAccepted || again.header.Get("Idempotent-Replayed") != "true" {
			t.Fatalf("%s: replay %d %v: %s", tc.name, again.status, again.header, again.body)
		}
		if !jsonEqual(t, again.body, first.body) {
			t.Errorf("%s: replayed %s, want %s", tc.name, again.body, first.body)
		}
		loc := first.header.Get("Location")
		if !strings.HasPrefix(loc, tc.location) || again.header.Get("Location") != loc {
			t.Errorf("%s: Location %q then %q", tc.name, loc, again.header.Get("Location"))
		}
		if again.header.Get("X-Request-ID") != "second-"+tc.name {
			t.Errorf("%s: X-Request-ID %q, want the current request's", tc.name, again.header.Get("X-Request-ID"))
		}
		if after := a.counts(t); after != before {
			t.Errorf("%s: replay stored something: %v -> %v", tc.name, before, after)
		}
		// The key is ignored on GET.
		g := a.do(http.MethodGet, loc, "", map[string][]string{"Idempotency-Key": {"bad key"}})
		if g.status != http.StatusOK || g.header.Get("Idempotent-Replayed") != "" {
			t.Errorf("%s: GET with a key: %d %v", tc.name, g.status, g.header)
		}
	}
}

func TestIdempotencyKey_ExpiredKeyIsTakenOver(t *testing.T) {
	const ttl = 50 * time.Millisecond
	a := newTestAPIOn(t, testutil.NewPool(t, store.OpenPool), &syncBuffer{}, func(o *Options) { o.IdempotencyKeyTTL = ttl })
	first := a.postKey(pathCreate, validBody, "key-expiring", "")
	if first.status != http.StatusAccepted {
		t.Fatalf("first: %d", first.status)
	}
	var created, expires time.Time
	if err := a.pool.QueryRow(ctxT(t), `SELECT created_at, expires_at FROM idempotency_keys`).Scan(&created, &expires); err != nil {
		t.Fatal(err)
	}
	if expires.Sub(created) != ttl {
		t.Errorf("retention %s, want %s", expires.Sub(created), ttl)
	}
	testutil.Eventually(t, func() bool {
		var expired bool
		err := a.pool.QueryRow(ctxT(t), `SELECT expires_at <= now() FROM idempotency_keys`).Scan(&expired)
		return err == nil && expired
	}, waitT)

	second := a.postKey(pathCreate, validBody, "key-expiring", "")
	if second.status != http.StatusAccepted || second.header.Get("Idempotent-Replayed") != "" {
		t.Fatalf("second: %d %v", second.status, second.header)
	}
	firstID, secondID := first.object(t)["id"], second.object(t)["id"]
	if firstID == secondID {
		t.Errorf("same id %v: the expired key was replayed", firstID)
	}
	var stored []byte
	if err := a.pool.QueryRow(ctxT(t), `SELECT response_body FROM idempotency_keys`).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if !jsonEqual(t, stored, second.body) {
		t.Errorf("stored response %s, want the second's", stored)
	}
	if got := a.counts(t); got != [3]int{2, 0, 1} {
		t.Errorf("counts %v", got)
	}
}

func TestIdempotencyKey_Reused(t *testing.T) {
	a := newTestAPI(t)
	const key = "key-reused-7f3a"
	if r := a.postKey(pathCreate, item(""), key, ""); r.status != http.StatusAccepted {
		t.Fatalf("first: %d", r.status)
	}
	checkReused := func(name string, r response) {
		t.Helper()
		if r.status != http.StatusUnprocessableEntity {
			t.Fatalf("%s: status %d: %s", name, r.status, r.body)
		}
		if got := r.problemCodes(t); len(got) != 1 || got[0] != "Idempotency-Key:idempotency_key_reused" {
			t.Errorf("%s: codes %v", name, got)
		}
		if !strings.HasSuffix(r.object(t)["type"].(string), "#idempotency-key-reused") {
			t.Errorf("%s: type %v", name, r.object(t)["type"])
		}
		if strings.Contains(string(r.body), key) {
			t.Errorf("%s: key echoed: %s", name, r.body)
		}
	}
	checkReused("different body", a.postKey(pathCreate, item(`,"priority":"high"`), key, ""))
	// The same notification through the other endpoint: the hash covers the
	// method and the path, so the key belongs to the first request only.
	checkReused("other endpoint", a.postKey(pathBatch, batchBody(item("")), key, ""))

	const batchKey = "key-reused-batch"
	if r := a.postKey(pathBatch, batchBody(item("")), batchKey, ""); r.status != http.StatusAccepted {
		t.Fatalf("batch: %d", r.status)
	}
	checkReused("batch, different body", a.postKey(pathBatch, batchBody(item(""), item("")), batchKey, ""))
	checkReused("batch key on the single endpoint", a.postKey(pathCreate, item(""), batchKey, ""))
	if got := a.counts(t); got != [3]int{2, 1, 2} {
		t.Errorf("counts %v, want 2 notifications, 1 batch, 2 keys", got)
	}
}

// A request that stores nothing stores no key either, so the same key works
// for the corrected request.
func TestIdempotencyKey_NothingStoredWithoutCreation(t *testing.T) {
	a := newTestAPI(t)
	if r := a.post(item(`,"dedupe_key":"held-1"`)); r.status != http.StatusAccepted {
		t.Fatalf("setup: %d", r.status)
	}
	for _, tc := range []struct {
		name, path, failing string
		status              int
		fixed               string
	}{
		{"validation", pathCreate, faxItem, 422, item("")},
		{"409 duplicate", pathCreate, item(`,"dedupe_key":"held-1"`), 409, item(`,"dedupe_key":"held-2"`)},
		{"batch rejected in validation", pathBatch, batchBody(faxItem, item(`,"extra":1`)), 422, batchBody(item(""))},
		{"batch of duplicates", pathBatch, batchBody(item(`,"dedupe_key":"held-1"`)), 422, batchBody(item(`,"dedupe_key":"held-3"`))},
		{"top level of the batch", pathBatch, `{"notifications":[]}`, 422, batchBody(item(""))},
	} {
		key := "key-" + strings.ReplaceAll(tc.name, " ", "-")
		before := a.counts(t)
		r := a.postKey(tc.path, tc.failing, key, "")
		if r.status != tc.status {
			t.Fatalf("%s: status %d, want %d: %s", tc.name, r.status, tc.status, r.body)
		}
		if after := a.counts(t); after != before {
			t.Errorf("%s: stored something: %v -> %v", tc.name, before, after)
		}
		if fixed := a.postKey(tc.path, tc.fixed, key, ""); fixed.status != http.StatusAccepted || fixed.header.Get("Idempotent-Replayed") != "" {
			t.Errorf("%s: corrected request with the same key: %d %s", tc.name, fixed.status, fixed.body)
		}
	}
}

func TestIdempotencyKey_InvalidHeader(t *testing.T) {
	a := newTestAPI(t)
	for _, path := range []string{pathCreate, pathBatch} {
		body := item("")
		if path == pathBatch {
			body = batchBody(item(""))
		}
		for name, values := range map[string][]string{
			"empty":       {emptyHeaderValue},
			"256 bytes":   {strings.Repeat("k", 256)},
			"with space":  {"key with-space"},
			"non-ASCII":   {"key-é"},
			"sent twice":  {"key-a", "key-b"},
			"twice, same": {"key-a", "key-a"},
		} {
			r := a.do(http.MethodPost, path, body, map[string][]string{"Idempotency-Key": values})
			if r.status != http.StatusBadRequest {
				t.Errorf("%s %s: status %d: %s", path, name, r.status, r.body)
				continue
			}
			if got := r.problemCodes(t); len(got) != 1 || got[0] != "Idempotency-Key:invalid_idempotency_key" {
				t.Errorf("%s %s: codes %v", path, name, got)
			}
			for _, v := range values {
				if v != emptyHeaderValue && strings.Contains(string(r.body), v) {
					t.Errorf("%s %s: value echoed: %s", path, name, r.body)
				}
			}
		}
		// Order of checks: media type, then the body, then the key.
		bad := map[string][]string{"Idempotency-Key": {"bad key"}}
		if r := a.do(http.MethodPost, path, body, map[string][]string{"Idempotency-Key": {"bad key"}, "Content-Type": {"text/plain"}}); r.status != http.StatusUnsupportedMediaType {
			t.Errorf("%s: 415 before the key: %d", path, r.status)
		}
		if r := a.do(http.MethodPost, path, `{"extra":1}`, bad); r.status != http.StatusUnprocessableEntity {
			t.Errorf("%s: body before the key: %d", path, r.status)
		}
		// The longest valid key (one per endpoint: a key belongs to one request).
		longest := strings.Repeat("~", 254) + string(path[len(path)-1])
		if r := a.do(http.MethodPost, path, body, map[string][]string{"Idempotency-Key": {longest}}); r.status != http.StatusAccepted {
			t.Errorf("%s: 255-byte key: %d %s", path, r.status, r.body)
		}
	}
	if got := a.counts(t); got != [3]int{2, 1, 2} {
		t.Errorf("counts %v, want only the two 255-byte-key requests stored", got)
	}
}

func TestCreate_DedupeConflict(t *testing.T) {
	a := newTestAPI(t)
	first := a.post(item(`,"dedupe_key":"order-42"`))
	if first.status != http.StatusAccepted {
		t.Fatalf("first: %d", first.status)
	}
	id := first.object(t)["id"].(string)
	r := a.post(item(`,"dedupe_key":"order-42","priority":"low"`))
	if r.status != http.StatusConflict {
		t.Fatalf("status %d: %s", r.status, r.body)
	}
	var p struct {
		Type   string     `json:"type"`
		Errors []fieldErr `json:"errors"`
	}
	if err := json.Unmarshal(r.body, &p); err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(p.Type, "#duplicate") || len(p.Errors) != 1 || p.Errors[0].Field != "dedupe_key" ||
		p.Errors[0].Code != "duplicate" || p.Errors[0].ExistingID == nil || *p.Errors[0].ExistingID != id {
		t.Errorf("problem %s, want duplicate of %s", r.body, id)
	}
	if strings.Contains(string(r.body), "order-42") {
		t.Errorf("dedupe_key echoed: %s", r.body)
	}
	if n := a.count(t); n != 1 {
		t.Errorf("%d notifications", n)
	}
	// Another key is not a conflict.
	if r := a.post(item(`,"dedupe_key":"order-43"`)); r.status != http.StatusAccepted {
		t.Errorf("other key: %d", r.status)
	}
}

// Partial success: valid items are stored, invalid ones, duplicates of
// stored rows and duplicates inside the batch are rejected at their index.
func TestBatch_PartialSuccess(t *testing.T) {
	a := newTestAPI(t)
	held := a.post(item(`,"dedupe_key":"held"`))
	if held.status != http.StatusAccepted {
		t.Fatalf("setup: %d", held.status)
	}
	heldID := held.object(t)["id"].(string)

	body := batchBody(
		item(`,"dedupe_key":"new-0"`), // 0 accepted, normal
		faxItem,                       // 1 invalid
		item(`,"dedupe_key":"held"`),  // 2 duplicate of a stored row
		item(`,"priority":"high"`),    // 3 accepted, high
		item(`,"dedupe_key":"new-0"`), // 4 duplicate inside the batch
		item(`,"extra":true`),         // 5 unknown field in an item
		`{"channel":"email","recipient":"user@example.com","content":"hi","priority":"low"}`, // 6 accepted, low
		`"not an object"`,       // 7 invalid
		item(`,"dedupe_key":5`), // 8 wrong type
		item(`,"dedupe_key":"`+strings.Repeat("k", 256)+`"`), // 9 too long
	)
	r := a.do(http.MethodPost, pathBatch, body, map[string][]string{"X-Request-ID": {"req-batch"}, "traceparent": {traceparent}})
	if r.status != http.StatusAccepted {
		t.Fatalf("status %d: %s", r.status, r.body)
	}
	d := decodeBatch(t, r)
	if got := d.acceptedIndexes(); !slices.Equal(got, []int{0, 3, 6}) {
		t.Errorf("accepted indexes %v", got)
	}
	wantRejected := []string{
		"1:channel:invalid_value", "2:dedupe_key:duplicate", "4:dedupe_key:duplicate_in_batch",
		"5:extra:unknown_field", "7::invalid_type", "8:dedupe_key:invalid_type", "9:dedupe_key:too_long",
	}
	if got := d.rejectedCodes(); !slices.Equal(got, wantRejected) {
		t.Errorf("rejected %v, want %v", got, wantRejected)
	}
	for _, rj := range d.Rejected {
		if rj.Index == 2 && (rj.Errors[0].ExistingID == nil || *rj.Errors[0].ExistingID != heldID) {
			t.Errorf("item 2: existing_id %v, want %s", rj.Errors[0].ExistingID, heldID)
		}
	}
	if r.header.Get("Location") != "/v1/batches/"+d.BatchID {
		t.Errorf("Location %q, batch %s", r.header.Get("Location"), d.BatchID)
	}

	// Each row: its priority's TTL and budget, the request's correlation id
	// and trace parent, the batch id.
	limits := map[int]struct {
		ttl time.Duration
		max int
	}{0: {testRetry().Normal.TTL, testRetry().Normal.MaxFailures}, 3: {testRetry().High.TTL, testRetry().High.MaxFailures}, 6: {testRetry().Low.TTL, testRetry().Low.MaxFailures}}
	for _, acc := range d.Accepted {
		row := a.row(t, acc.ID)
		want := limits[acc.Index]
		if row.ttl != want.ttl || row.maxFailures != want.max || row.correlationID != "req-batch" || row.traceParent != traceparent {
			t.Errorf("item %d: row %+v, want ttl %s max_failures %d", acc.Index, row, want.ttl, want.max)
		}
		var batchID string
		if err := a.pool.QueryRow(ctxT(t), `SELECT batch_id::text FROM notifications WHERE id = $1`, acc.ID).Scan(&batchID); err != nil || batchID != d.BatchID {
			t.Errorf("item %d: batch_id %q (%v)", acc.Index, batchID, err)
		}
	}
	if row := a.row(t, d.Accepted[2].ID); row.recipient != "user@example.com" {
		t.Errorf("recipient %q", row.recipient)
	}

	g := a.get("/v1/batches/" + d.BatchID)
	if g.status != http.StatusOK {
		t.Fatalf("get batch: %d %s", g.status, g.body)
	}
	got := g.object(t)
	if got["id"] != d.BatchID || got["total"] != float64(10) || got["accepted"] != float64(3) || got["rejected"] != float64(7) {
		t.Errorf("batch %v", got)
	}
	wantCounts := map[string]any{
		"pending": float64(3), "queued": float64(0), "processing": float64(0), "retrying": float64(0),
		"sent": float64(0), "failed": float64(0), "cancelled": float64(0),
	}
	if !reflect.DeepEqual(got["status_counts"], wantCounts) {
		t.Errorf("status_counts %v, want %v", got["status_counts"], wantCounts)
	}
	var batchCorr string
	if err := a.pool.QueryRow(ctxT(t), `SELECT correlation_id FROM batches`).Scan(&batchCorr); err != nil || batchCorr != "req-batch" {
		t.Errorf("batch correlation_id %q (%v)", batchCorr, err)
	}
}

func TestBatch_StatusCountsMove(t *testing.T) {
	a := newTestAPI(t)
	r := a.do(http.MethodPost, pathBatch, batchBody(item(""), item("")), nil)
	if r.status != http.StatusAccepted {
		t.Fatalf("status %d", r.status)
	}
	d := decodeBatch(t, r)
	// Cancel does not exist yet; the row is moved with raw SQL.
	if _, err := a.pool.Exec(ctxT(t), `UPDATE notifications SET status = 'cancelled' WHERE id = $1`, d.Accepted[0].ID); err != nil {
		t.Fatal(err)
	}
	counts := a.get("/v1/batches/" + d.BatchID).object(t)["status_counts"].(map[string]any)
	if counts["pending"] != float64(1) || counts["cancelled"] != float64(1) {
		t.Errorf("status_counts %v", counts)
	}
}

func TestBatch_Thousand(t *testing.T) {
	a := newTestAPIOn(t, testutil.NewPool(t, store.OpenPool), &syncBuffer{}, func(o *Options) { o.MaxBodyBytes = itBigBody })
	items := make([]string, 1001)
	for i := range items {
		items[i] = item(fmt.Sprintf(`,"dedupe_key":"bulk-%04d"`, i))
	}
	r := a.do(http.MethodPost, pathBatch, batchBody(items[:1000]...), nil)
	if r.status != http.StatusAccepted {
		t.Fatalf("1,000 items: %d %s", r.status, r.body)
	}
	if got := rawField(t, r.body, "rejected"); got != "[]" {
		t.Errorf("rejected = %s, want []", got)
	}
	if d := decodeBatch(t, r); len(d.Accepted) != 1000 || d.Accepted[999].Index != 999 {
		t.Errorf("%d accepted", len(d.Accepted))
	}
	if got := a.counts(t); got != [3]int{1000, 1, 0} {
		t.Errorf("counts %v", got)
	}

	r = a.do(http.MethodPost, pathBatch, batchBody(items...), nil)
	if r.status != http.StatusUnprocessableEntity {
		t.Fatalf("1,001 items: %d", r.status)
	}
	if got := r.problemCodes(t); len(got) != 1 || got[0] != "notifications:too_many_items" {
		t.Errorf("codes %v", got)
	}
	if got := a.counts(t); got != [3]int{1000, 1, 0} {
		t.Errorf("1,001 items stored something: %v", got)
	}
}

func TestBatch_AllRejected(t *testing.T) {
	a := newTestAPI(t)
	if r := a.post(item(`,"dedupe_key":"held"`)); r.status != http.StatusAccepted {
		t.Fatalf("setup: %d", r.status)
	}
	for _, tc := range []struct {
		name  string
		body  string
		codes []string
	}{
		{
			"in validation", batchBody(faxItem, item(`,"x":1`), `[]`),
			[]string{"0:channel:invalid_value", "1:x:unknown_field", "2::invalid_type"},
		},
		{
			"duplicates and invalid items", batchBody(item(`,"dedupe_key":"held"`), emptyContentItem, item(`,"dedupe_key":"held"`)),
			[]string{"0:dedupe_key:duplicate", "1:content:empty", "2:dedupe_key:duplicate_in_batch"},
		},
	} {
		r := a.do(http.MethodPost, pathBatch, tc.body, nil)
		if r.status != http.StatusUnprocessableEntity {
			t.Fatalf("%s: status %d: %s", tc.name, r.status, r.body)
		}
		d := decodeBatch(t, r)
		if got := d.rejectedCodes(); !slices.Equal(got, tc.codes) {
			t.Errorf("%s: rejected %v, want %v", tc.name, got, tc.codes)
		}
		if d.Errors != nil || !strings.HasSuffix(d.Type, "#validation") ||
			d.Detail != "All 3 item(s) were rejected; no batch was created." {
			t.Errorf("%s: problem %s", tc.name, r.body)
		}
		if got := a.counts(t); got != [3]int{1, 0, 0} {
			t.Errorf("%s: counts %v", tc.name, got)
		}
	}
}

func TestBatch_TopLevelStrictness(t *testing.T) {
	a := newTestAPI(t)
	for _, tc := range []struct {
		name, body string
		status     int
		codes      []string
	}{
		{"unknown top-level field", `{"notifications":[` + item("") + `],"extra":1}`, 422, []string{"extra:unknown_field"}},
		{"no notifications", `{}`, 422, []string{"notifications:required"}},
		{"not an array", `{"notifications":{}}`, 422, []string{"notifications:invalid_type"}},
		{"empty array", `{"notifications":[]}`, 422, []string{"notifications:empty"}},
		{"not an object", `[]`, 400, []string{":not_an_object"}},
		{"malformed", `{"notifications":[`, 400, []string{":malformed_json"}},
	} {
		r := a.do(http.MethodPost, pathBatch, tc.body, nil)
		if r.status != tc.status {
			t.Errorf("%s: status %d: %s", tc.name, r.status, r.body)
			continue
		}
		if got := r.problemCodes(t); !slices.Equal(got, tc.codes) {
			t.Errorf("%s: codes %v, want %v", tc.name, got, tc.codes)
		}
		if strings.Contains(string(r.body), `"rejected"`) {
			t.Errorf("%s: top-level problem with rejected: %s", tc.name, r.body)
		}
	}
	if r := a.do(http.MethodPost, pathBatch, batchBody(item("")), map[string][]string{"Content-Type": {"text/plain"}}); r.status != http.StatusUnsupportedMediaType {
		t.Errorf("text/plain: %d", r.status)
	}
	big := batchBody(item(`,"x":"` + strings.Repeat("a", itMaxBody) + `"`))
	if r := a.do(http.MethodPost, pathBatch, big, nil); r.status != http.StatusRequestEntityTooLarge {
		t.Errorf("large body: %d", r.status)
	}
	if got := a.counts(t); got != [3]int{0, 0, 0} {
		t.Errorf("counts %v", got)
	}
}

func TestGetBatch_UnknownAndInvalidID(t *testing.T) {
	a := newTestAPI(t)
	if r := a.get("/v1/batches/0193f0d2-7c1e-7b3a-9d41-5be0a1c2e3f4"); r.status != http.StatusNotFound {
		t.Errorf("unknown: %d", r.status)
	}
	r := a.get("/v1/batches/abc")
	if r.status != http.StatusBadRequest {
		t.Fatalf("invalid: %d", r.status)
	}
	if got := r.problemCodes(t); len(got) != 1 || got[0] != "id:invalid_uuid" {
		t.Errorf("codes %v", got)
	}
}
