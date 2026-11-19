//go:build integration

package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
)

// listPage is one page of GET /v1/notifications, as the tests read it.
type listPage struct {
	Data []struct {
		ID        string `json:"id"`
		Recipient string `json:"recipient"`
		Status    string `json:"status"`
		Channel   string `json:"channel"`
		Priority  string `json:"priority"`
		BatchID   string `json:"batch_id"`
		CreatedAt string `json:"created_at"`
	} `json:"data"`
	NextCursor *string `json:"next_cursor"`
}

func (p listPage) ids() []string {
	ids := make([]string, len(p.Data))
	for i, d := range p.Data {
		ids[i] = d.ID
	}
	return ids
}

func (a *testAPI) list(t *testing.T, query string) listPage {
	t.Helper()
	path := "/v1/notifications"
	if query != "" {
		path += "?" + query
	}
	res := a.get(path)
	if res.status != http.StatusOK {
		t.Fatalf("GET %s: status %d: %s", path, res.status, res.body)
	}
	var p listPage
	if err := json.Unmarshal(res.body, &p); err != nil {
		t.Fatalf("decode %s: %v", res.body, err)
	}
	if rawMember(t, res.body, "data") == "null" {
		t.Fatalf("data is null: %s", res.body)
	}
	return p
}

// walk follows next_cursor from the query until it is null and returns the
// ids in order and the number of pages.
func (a *testAPI) walk(t *testing.T, query string) ([]string, int) {
	t.Helper()
	var ids []string
	cursor := ""
	for pages := 1; pages <= 1000; pages++ {
		q := query
		if cursor != "" {
			q += "&cursor=" + url.QueryEscape(cursor)
		}
		p := a.list(t, q)
		ids = append(ids, p.ids()...)
		if p.NextCursor == nil {
			return ids, pages
		}
		cursor = *p.NextCursor
	}
	t.Fatal("the walk does not end")
	return nil, 0
}

// seedList creates n notifications of rotating channel, priority and status,
// each with its own created_at (one second apart, the first at base), and
// returns their ids newest first.
func (a *testAPI) seedList(t *testing.T, n int, base time.Time) []string {
	t.Helper()
	statuses := []string{"pending", "cancelled", "pending", "cancelled"}
	ids := make([]string, n)
	for i := range n {
		row, err := a.store.CreateNotification(ctxT(t), store.NewNotification{
			Channel:       domain.Channels()[i%3],
			Priority:      domain.Priorities()[i%3],
			Recipient:     domain.Recipient(phone),
			Content:       "secret content",
			TTL:           itKeyTTL,
			MaxFailures:   5,
			CorrelationID: "seed",
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := a.pool.Exec(ctxT(t), `UPDATE notifications SET created_at = $2, status = $3 WHERE id = $1`,
			row.ID, base.Add(time.Duration(i)*time.Second), statuses[i%len(statuses)]); err != nil {
			t.Fatal(err)
		}
		ids[i] = row.ID.String()
	}
	slices.Reverse(ids)
	return ids
}

func TestListAPI_WalkWithLimitAndCursor(t *testing.T) {
	a := newTestAPI(t)
	want := a.seedList(t, 23, time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC))

	got, pages := a.walk(t, "limit=5")
	if pages != 5 || !slices.Equal(got, want) {
		t.Errorf("%d pages, ids %v; want 5 pages and %v", pages, got, want)
	}
	// A limit that divides the rows: the last page is full and has no cursor.
	got, pages = a.walk(t, "limit=23")
	if pages != 1 || !slices.Equal(got, want) {
		t.Errorf("limit 23: %d pages", pages)
	}
	if p := a.list(t, "limit=23"); p.NextCursor != nil {
		t.Errorf("next_cursor %q on the last page, want null", *p.NextCursor)
	}
	got, pages = a.walk(t, "limit=1")
	if pages != 23 || !slices.Equal(got, want) {
		t.Errorf("limit 1: %d pages", pages)
	}
}

func TestListAPI_DefaultLimitAndNextCursorNull(t *testing.T) {
	a := newTestAPI(t)
	a.seedList(t, 55, time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC))
	first := a.list(t, "")
	if len(first.Data) != 50 || first.NextCursor == nil {
		t.Fatalf("first page: %d rows, next_cursor %v", len(first.Data), first.NextCursor)
	}
	second := a.list(t, "cursor="+url.QueryEscape(*first.NextCursor))
	if len(second.Data) != 5 || second.NextCursor != nil {
		t.Errorf("second page: %d rows, next_cursor %v", len(second.Data), second.NextCursor)
	}
	res := a.get("/v1/notifications?cursor=" + url.QueryEscape(*first.NextCursor))
	if got := rawMember(t, res.body, "next_cursor"); got != "null" {
		t.Errorf("next_cursor %s, want null", got)
	}
	if p := a.list(t, "limit=200"); len(p.Data) != 55 || p.NextCursor != nil {
		t.Errorf("limit 200: %d rows", len(p.Data))
	}
}

func TestListAPI_EmptyList(t *testing.T) {
	a := newTestAPI(t)
	res := a.get("/v1/notifications")
	if res.status != http.StatusOK || rawMember(t, res.body, "data") != "[]" || rawMember(t, res.body, "next_cursor") != "null" {
		t.Errorf("status %d: %s", res.status, res.body)
	}
}

func TestListAPI_Filters(t *testing.T) {
	a := newTestAPI(t)
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	ids := a.seedList(t, 24, base)
	// ids is newest first: ids[k] was created at base + (23-k) seconds.
	if res := a.post(`{"channel":"push","recipient":"token-1","content":"x"}`); res.status != http.StatusAccepted {
		t.Fatalf("create: %d %s", res.status, res.body)
	}
	res := a.do(http.MethodPost, "/v1/notifications/batch", `{"notifications":[{"channel":"sms","recipient":"+905551234567","content":"a"},{"channel":"email","recipient":"user@example.com","content":"b"}]}`, nil)
	if res.status != http.StatusAccepted {
		t.Fatalf("batch: %d %s", res.status, res.body)
	}
	batchID := res.object(t)["batch_id"].(string)

	only := func(name, query string, check func(d struct {
		ID, Recipient, Status, Channel, Priority, BatchID, CreatedAt string
	}) bool,
	) {
		t.Helper()
		got, _ := a.walk(t, query+"&limit=7")
		all, _ := a.walk(t, "limit=7")
		var want []string
		for _, d := range a.list(t, "limit=200").Data {
			if check(struct{ ID, Recipient, Status, Channel, Priority, BatchID, CreatedAt string }(d)) {
				want = append(want, d.ID)
			}
		}
		if len(all) != 24+1+2 {
			t.Fatalf("%s: unfiltered walk has %d rows", name, len(all))
		}
		if len(want) == 0 {
			t.Fatalf("%s: the test selects nothing", name)
		}
		if !slices.Equal(got, want) {
			t.Errorf("%s: got %d rows %v, want %d rows %v", name, len(got), got, len(want), want)
		}
	}
	type row = struct{ ID, Recipient, Status, Channel, Priority, BatchID, CreatedAt string }
	only("status", "status=cancelled", func(d row) bool { return d.Status == "cancelled" })
	only("channel", "channel=email", func(d row) bool { return d.Channel == "email" })
	only("priority", "priority=low", func(d row) bool { return d.Priority == "low" })
	only("batch_id", "batch_id="+batchID, func(d row) bool { return d.BatchID == batchID })
	only("status and channel", "status=pending&channel=sms", func(d row) bool { return d.Status == "pending" && d.Channel == "sms" })
	only("everything", "status=pending&channel=sms&priority=normal&batch_id="+batchID,
		func(d row) bool {
			return d.Status == "pending" && d.Channel == "sms" && d.Priority == "normal" && d.BatchID == batchID
		})

	// The range [from, to) on created_at, with offsets other than Z.
	from := base.Add(5 * time.Second)
	to := base.Add(10 * time.Second)
	zone := time.FixedZone("", 3*3600)
	q := url.Values{"created_from": {from.In(zone).Format(time.RFC3339)}, "created_to": {to.In(zone).Format(time.RFC3339)}, "limit": {"3"}}
	got, _ := a.walk(t, q.Encode())
	if want := ids[23-9 : 23-5+1]; !slices.Equal(got, want) { // created at +9s down to +5s
		t.Errorf("range: got %v, want %v", got, want)
	}
	// from after to: an empty page, not an error.
	q = url.Values{"created_from": {to.Format(time.RFC3339)}, "created_to": {from.Format(time.RFC3339)}}
	if p := a.list(t, q.Encode()); len(p.Data) != 0 || p.NextCursor != nil {
		t.Errorf("from after to: %d rows", len(p.Data))
	}
	// A batch id that no batch has: an empty page.
	if p := a.list(t, "batch_id="+uuid.NewString()); len(p.Data) != 0 {
		t.Errorf("unknown batch: %d rows", len(p.Data))
	}
}

func TestListAPI_InvalidQueries(t *testing.T) {
	a := newTestAPI(t)
	a.seedList(t, 3, time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC))
	good := a.list(t, "limit=1")
	cursor := *good.NextCursor
	tests := []struct {
		name, query string
		want        []string
	}{
		{"unknown parameter", "colour=red", []string{"colour:unknown_parameter"}},
		{"a parameter of another operation", "id=" + uuid.NewString(), []string{"id:unknown_parameter"}},
		{"unknown name is shortened", strings.Repeat("n", 200) + "=1", []string{strings.Repeat("n", 64) + ":unknown_parameter"}},
		{"status twice", "status=pending&status=sent", []string{"status:duplicate_parameter"}},
		{"channel twice", "channel=sms&channel=sms", []string{"channel:duplicate_parameter"}},
		{"priority twice", "priority=low&priority=high", []string{"priority:duplicate_parameter"}},
		{"batch_id twice", "batch_id=" + uuid.NewString() + "&batch_id=" + uuid.NewString(), []string{"batch_id:duplicate_parameter"}},
		{"created_from twice", "created_from=2026-01-01T00:00:00Z&created_from=2026-01-02T00:00:00Z", []string{"created_from:duplicate_parameter"}},
		{"created_to twice", "created_to=2026-01-01T00:00:00Z&created_to=2026-01-02T00:00:00Z", []string{"created_to:duplicate_parameter"}},
		{"limit twice", "limit=1&limit=2", []string{"limit:duplicate_parameter"}},
		{"cursor twice", "cursor=" + cursor + "&cursor=" + cursor, []string{"cursor:duplicate_parameter"}},
		{"status not an enum value", "status=done", []string{"status:invalid_parameter"}},
		{"status in capitals", "status=PENDING", []string{"status:invalid_parameter"}},
		{"empty status", "status=", []string{"status:invalid_parameter"}},
		{"channel not an enum value", "channel=fax", []string{"channel:invalid_parameter"}},
		{"priority not an enum value", "priority=urgent", []string{"priority:invalid_parameter"}},
		{"batch_id not a UUID", "batch_id=123", []string{"batch_id:invalid_parameter"}},
		{"empty batch_id", "batch_id=", []string{"batch_id:invalid_parameter"}},
		{"created_from without an offset", "created_from=2026-01-01T00:00:00", []string{"created_from:invalid_parameter"}},
		{"created_to a date", "created_to=2026-01-01", []string{"created_to:invalid_parameter"}},
		{"empty created_from", "created_from=", []string{"created_from:invalid_parameter"}},
		{"limit not an integer", "limit=many", []string{"limit:invalid_parameter"}},
		{"limit a decimal", "limit=2.5", []string{"limit:invalid_parameter"}},
		{"limit zero", "limit=0", []string{"limit:invalid_parameter"}},
		{"limit negative", "limit=-5", []string{"limit:invalid_parameter"}},
		{"limit over the maximum", "limit=201", []string{"limit:invalid_parameter"}},
		{"empty limit", "limit=", []string{"limit:invalid_parameter"}},
		{"cursor that does not decode", "cursor=not-a-cursor", []string{"cursor:invalid_cursor"}},
		{"cursor of the wrong length", "cursor=AAAA", []string{"cursor:invalid_cursor"}},
		{"a very long cursor", "cursor=" + strings.Repeat("A", 1000), []string{"cursor:invalid_cursor"}},
		{"empty cursor", "cursor=", []string{"cursor:invalid_parameter"}},
		{"one error per problem", "limit=0&status=x&zzz=1&cursor=%21", []string{"status:invalid_parameter", "limit:invalid_parameter", "cursor:invalid_cursor", "zzz:unknown_parameter"}},
	}
	for _, tt := range tests {
		res := a.get("/v1/notifications?" + tt.query)
		if res.status != http.StatusBadRequest {
			t.Errorf("%s: status %d: %s", tt.name, res.status, res.body)
			continue
		}
		if got := res.problemCodes(t); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%s: errors %v, want %v", tt.name, got, tt.want)
		}
		var p struct{ Type string }
		_ = json.Unmarshal(res.body, &p)
		if !strings.HasSuffix(p.Type, "#malformed-request") {
			t.Errorf("%s: type %q", tt.name, p.Type)
		}
		for _, secret := range []string{"done", "PENDING", "many", "2.5", "not-a-cursor", "2026-01-01"} {
			if strings.Contains(tt.query, secret) && strings.Contains(string(res.body), secret) {
				t.Errorf("%s: the submitted value %q is in the response: %s", tt.name, secret, res.body)
			}
		}
	}
}

func TestListAPI_AttemptsRead(t *testing.T) {
	a := newTestAPI(t)
	id := uuid.MustParse(a.post(validBody).object(t)["id"].(string))
	other := uuid.MustParse(a.post(validBody).object(t)["id"].(string))

	// Without attempts: an empty array.
	res := a.get("/v1/notifications/" + id.String() + "/attempts")
	if res.status != http.StatusOK || rawMember(t, res.body, "data") != "[]" {
		t.Fatalf("no attempts: %d %s", res.status, res.body)
	}

	// Two attempts: one retryable, one still in flight. (Inserted in reverse
	// order: the answer is in attempt order.)
	start := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	if _, err := a.pool.Exec(ctxT(t), `
		INSERT INTO notification_attempts (notification_id, attempt_no, channel, provider, outcome, started_at, finished_at, http_status, latency_ms, error, provider_message_id)
		VALUES ($1, 2, 'sms', 'mock', 'in_flight', $2, NULL, NULL, NULL, NULL, NULL),
		       ($1, 1, 'sms', 'mock', 'retryable', $3, $2, 503, 12, 'service unavailable', NULL),
		       ($4, 1, 'sms', 'mock', 'success', $3, $2, 200, 5, NULL, 'pm-1')`,
		id, start.Add(time.Second), start, other); err != nil {
		t.Fatal(err)
	}
	res = a.get("/v1/notifications/" + id.String() + "/attempts")
	if res.status != http.StatusOK {
		t.Fatalf("status %d: %s", res.status, res.body)
	}
	var body struct {
		Data []struct {
			AttemptNo  int     `json:"attempt_no"`
			Provider   string  `json:"provider"`
			Outcome    string  `json:"outcome"`
			HTTPStatus *int    `json:"http_status"`
			LatencyMS  *int    `json:"latency_ms"`
			Error      *string `json:"error"`
			StartedAt  string  `json:"started_at"`
			FinishedAt *string `json:"finished_at"`
		} `json:"data"`
	}
	if err := json.Unmarshal(res.body, &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Data) != 2 {
		t.Fatalf("%d attempts: %s", len(body.Data), res.body)
	}
	first, second := body.Data[0], body.Data[1]
	if first.AttemptNo != 1 || first.Outcome != "retryable" || first.HTTPStatus == nil || *first.HTTPStatus != 503 ||
		first.LatencyMS == nil || *first.LatencyMS != 12 || first.Error == nil || *first.Error != "service unavailable" ||
		first.FinishedAt == nil || first.StartedAt != "2026-03-01T12:00:00Z" {
		t.Errorf("first attempt %+v", first)
	}
	if second.AttemptNo != 2 || second.Outcome != "in_flight" || second.FinishedAt != nil || second.HTTPStatus != nil {
		t.Errorf("second attempt %+v", second)
	}
	// The members the contract requires are present as null, not absent.
	for _, m := range []string{"error", "finished_at", "http_status", "latency_ms", "provider_message_id"} {
		if !strings.Contains(string(res.body), `"`+m+`":null`) {
			t.Errorf("no %s:null in %s", m, res.body)
		}
	}

	res = a.get("/v1/notifications/" + id.String() + "/attempts?limit=1")
	if res.status != http.StatusBadRequest || fmt.Sprint(res.problemCodes(t)) != "[limit:unknown_parameter]" {
		t.Errorf("query parameter: %d %s", res.status, res.body)
	}
	if res := a.get("/v1/notifications/" + uuid.NewString() + "/attempts"); res.status != http.StatusNotFound {
		t.Errorf("unknown id: status %d: %s", res.status, res.body)
	}
	res = a.get("/v1/notifications/nope/attempts")
	if res.status != http.StatusBadRequest || fmt.Sprint(res.problemCodes(t)) != "[id:invalid_uuid]" {
		t.Errorf("invalid id: %d %s", res.status, res.body)
	}
}

// The recipient is in the items of the listing and in no log line.
func TestListAPI_RecipientInItemsButNotInLogs(t *testing.T) {
	a := newTestAPI(t)
	a.seedList(t, 3, time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC))
	p := a.list(t, "limit=2")
	if len(p.Data) != 2 {
		t.Fatalf("%d items", len(p.Data))
	}
	for _, d := range p.Data {
		if d.Recipient != phone {
			t.Errorf("recipient %q, want the real value", d.Recipient)
		}
	}
	a.get("/v1/notifications?cursor=bad&" + url.Values{"zz": {phone}}.Encode()) // an error answer, too
	a.get("/v1/notifications/" + p.Data[0].ID + "/attempts")
	logs := a.logs.String()
	if logs == "" {
		t.Fatal("no log lines")
	}
	for _, secret := range []string{phone, strings.TrimPrefix(phone, "+"), "secret content"} {
		if strings.Contains(logs, secret) {
			t.Errorf("the log contains %q:\n%s", secret, logs)
		}
	}
}
