package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
)

var validCursor = domain.Cursor{CreatedAt: time.UnixMicro(1_700_000_000_000_000), ID: uuid.MustParse("0193f0d2-7c1e-7b3a-9d41-5be0a1c2e3f4")}.Encode()

func TestParseListQuery_Valid(t *testing.T) {
	id := uuid.MustParse("0193f0d2-7c1e-7b3a-9d41-5be0a1c2e3f4")
	cur := domain.Cursor{CreatedAt: time.UnixMicro(1_700_000_000_123_456).UTC(), ID: id}
	st, ch, pr := domain.StatusFailed, domain.ChannelPush, domain.PriorityLow
	from := time.Date(2026, 1, 2, 1, 4, 5, 0, time.UTC)
	to := time.Date(2026, 1, 3, 0, 0, 0, 1000, time.UTC)

	tests := []struct {
		name, query string
		want        store.ListFilter
	}{
		{"nothing", "", store.ListFilter{Limit: 50}},
		{"limit bounds", "limit=1", store.ListFilter{Limit: 1}},
		{"limit max", "limit=200", store.ListFilter{Limit: 200}},
		{
			"all", "status=failed&channel=push&priority=low&batch_id=" + id.String() +
				"&created_from=2026-01-02T03:04:05%2B02:00&created_to=2026-01-03T00:00:00.0000005Z&limit=7&cursor=" + cur.Encode(),
			store.ListFilter{Status: &st, Channel: &ch, Priority: &pr, BatchID: &id, CreatedFrom: &from, CreatedTo: &to, After: &cur, Limit: 7},
		},
		{
			"fraction rounds up to the microsecond", "created_from=2026-01-02T00:00:00.0000001Z",
			store.ListFilter{CreatedFrom: ptr(time.Date(2026, 1, 2, 0, 0, 0, 1000, time.UTC)), Limit: 50},
		},
		{
			"microsecond kept", "created_to=2026-01-02T00:00:00.000001Z",
			store.ListFilter{CreatedTo: ptr(time.Date(2026, 1, 2, 0, 0, 0, 1000, time.UTC)), Limit: 50},
		},
		{
			"from after to is not an error", "created_from=2026-02-01T00:00:00Z&created_to=2026-01-01T00:00:00Z",
			store.ListFilter{
				CreatedFrom: ptr(time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)),
				CreatedTo:   ptr(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)), Limit: 50,
			},
		},
	}
	for _, tt := range tests {
		got, errs := parseListQuery(tt.query)
		if len(errs) != 0 {
			t.Errorf("%s: errors %+v", tt.name, errs)
			continue
		}
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%s:\n got %+v\nwant %+v", tt.name, got, tt.want)
		}
	}
}

func ptr[T any](v T) *T { return &v }

func TestParseListQuery_Invalid(t *testing.T) {
	id := uuid.NewString()
	long := strings.Repeat("a", 300)
	tests := []struct {
		name, query string
		want        []string // "field:code"
	}{
		{"unknown", "foo=1", []string{"foo:unknown_parameter"}},
		{"unknown name truncated", long + "=1", []string{strings.Repeat("a", maxEchoedFieldName) + ":unknown_parameter"}},
		{"unknown names sorted", "zeta=1&alpha=2", []string{"alpha:unknown_parameter", "zeta:unknown_parameter"}},
		{"unknown repeated once", "foo=1&foo=2", []string{"foo:unknown_parameter"}},
		{"name case matters", "Status=sent", []string{"Status:unknown_parameter"}},
		{"duplicate", "status=sent&status=failed", []string{"status:duplicate_parameter"}},
		{"duplicate with an invalid value", "limit=x&limit=y", []string{"limit:duplicate_parameter"}},
		{"duplicate empty", "channel=&channel=", []string{"channel:duplicate_parameter"}},
		{"bad status", "status=done", []string{"status:invalid_parameter"}},
		{"status case", "status=SENT", []string{"status:invalid_parameter"}},
		{"empty status", "status=", []string{"status:invalid_parameter"}},
		{"bad channel", "channel=fax", []string{"channel:invalid_parameter"}},
		{"bad priority", "priority=urgent", []string{"priority:invalid_parameter"}},
		{"numeric priority", "priority=1", []string{"priority:invalid_parameter"}},
		{"bad batch_id", "batch_id=not-a-uuid", []string{"batch_id:invalid_parameter"}},
		{"batch_id without dashes", "batch_id=" + strings.ReplaceAll(id, "-", ""), []string{"batch_id:invalid_parameter"}},
		{"batch_id urn", "batch_id=urn:uuid:" + id, []string{"batch_id:invalid_parameter"}},
		{"empty batch_id", "batch_id=", []string{"batch_id:invalid_parameter"}},
		{"created_from without offset", "created_from=2026-01-02T03:04:05", []string{"created_from:invalid_parameter"}},
		{"created_from date only", "created_from=2026-01-02", []string{"created_from:invalid_parameter"}},
		{"created_to garbage", "created_to=yesterday", []string{"created_to:invalid_parameter"}},
		{"empty created_to", "created_to=", []string{"created_to:invalid_parameter"}},
		{"limit zero", "limit=0", []string{"limit:invalid_parameter"}},
		{"limit 201", "limit=201", []string{"limit:invalid_parameter"}},
		{"limit negative", "limit=-1", []string{"limit:invalid_parameter"}},
		{"limit plus", "limit=%2B5", []string{"limit:invalid_parameter"}},
		{"limit text", "limit=ten", []string{"limit:invalid_parameter"}},
		{"limit float", "limit=5.0", []string{"limit:invalid_parameter"}},
		{"limit huge", "limit=99999999999999999999", []string{"limit:invalid_parameter"}},
		{"limit leading zeros", "limit=0005", nil},
		{"empty limit", "limit=", []string{"limit:invalid_parameter"}},
		{"empty cursor", "cursor=", []string{"cursor:invalid_parameter"}},
		{"bad cursor", "cursor=!!!", []string{"cursor:invalid_cursor"}},
		{"short cursor", "cursor=AAAA", []string{"cursor:invalid_cursor"}},
		{"long cursor", "cursor=" + long, []string{"cursor:invalid_cursor"}},
		{"padded cursor", "cursor=" + validCursor + "%3D", []string{"cursor:invalid_cursor"}},
		{
			"several problems in parameter order", "zzz=1&cursor=x&limit=0&status=no",
			[]string{"status:invalid_parameter", "limit:invalid_parameter", "cursor:invalid_cursor", "zzz:unknown_parameter"},
		},
		{"malformed query string", "status=sent&a=%zz", []string{":invalid_parameter"}},
	}
	for _, tt := range tests {
		_, errs := parseListQuery(tt.query)
		var got []string
		for _, e := range errs {
			got = append(got, e.Field+":"+e.Code)
			// No message contains a submitted value.
			vals, _ := url.ParseQuery(tt.query)
			for _, vs := range vals {
				for _, v := range vs {
					if len(v) >= 3 && strings.Contains(e.Message, v) {
						t.Errorf("%s: message %q contains the submitted value %q", tt.name, e.Message, v)
					}
				}
			}
		}
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%s: errors %v, want %v", tt.name, got, tt.want)
		}
	}
}

// The same problems come back over HTTP, whether the generated wrapper
// refuses the value (a typed parameter that does not bind) or the guard does.
// The repository is never called.
func TestListNotifications_BadQuery(t *testing.T) {
	repo := &listRepo{}
	c, logs := newListAPI(t, repo)
	for _, tt := range []struct {
		query string
		want  []string
	}{
		{"foo=bar", []string{"foo:unknown_parameter"}},
		{"status=a&status=b", []string{"status:duplicate_parameter"}},
		{"limit=notanumber", []string{"limit:invalid_parameter"}},
		{"limit=", []string{"limit:invalid_parameter"}},
		{"limit=201", []string{"limit:invalid_parameter"}},
		{"batch_id=zzz", []string{"batch_id:invalid_parameter"}},
		{"created_from=nope&created_to=nope", []string{"created_from:invalid_parameter", "created_to:invalid_parameter"}},
		{"created_from=2026-01-01T00:00:00", []string{"created_from:invalid_parameter"}},
		{"status=SENT", []string{"status:invalid_parameter"}},
		{"cursor=%00", []string{"cursor:invalid_cursor"}},
		{"cursor=", []string{"cursor:invalid_parameter"}},
		{"limit=x&foo=1&cursor=bad", []string{"limit:invalid_parameter", "cursor:invalid_cursor", "foo:unknown_parameter"}},
	} {
		res := c.get("/v1/notifications?" + tt.query)
		if res.status != http.StatusBadRequest {
			t.Errorf("%s: status %d: %s", tt.query, res.status, res.body)
			continue
		}
		if got := res.problemCodes(t); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%s: errors %v, want %v", tt.query, got, tt.want)
		}
		if !strings.Contains(string(res.body), "malformed-request") {
			t.Errorf("%s: not a malformed-request problem: %s", tt.query, res.body)
		}
	}
	if repo.calls != 0 {
		t.Errorf("repository called %d times for invalid queries", repo.calls)
	}
	if strings.Contains(logs.String(), "notanumber") {
		t.Errorf("a submitted value is in the log: %s", logs.String())
	}
}

func TestListNotifications_PathParameterKeepsItsAnswer(t *testing.T) {
	c, _ := newListAPI(t, &listRepo{})
	res := c.get("/v1/notifications/not-a-uuid/attempts")
	if res.status != http.StatusBadRequest {
		t.Fatalf("status %d: %s", res.status, res.body)
	}
	if got := res.problemCodes(t); !reflect.DeepEqual(got, []string{"id:invalid_uuid"}) {
		t.Errorf("errors %v", got)
	}
}

func TestListNotifications_PassesFilterAndShapesPage(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 123456000, time.UTC)
	rows := []store.Notification{
		{ID: uuid.Must(uuid.NewV7()), Channel: domain.ChannelSMS, Priority: domain.PriorityNormal, Recipient: "+905551234567", Status: domain.StatusPending, Version: 1, ExpiresAt: now, CreatedAt: now, UpdatedAt: now},
	}
	next := domain.Cursor{CreatedAt: now, ID: rows[0].ID}
	repo := &listRepo{rows: rows, next: &next}
	c, _ := newListAPI(t, repo)

	res := c.get("/v1/notifications?limit=1&status=pending&cursor=" + next.Encode())
	if res.status != http.StatusOK {
		t.Fatalf("status %d: %s", res.status, res.body)
	}
	if repo.filter.Limit != 1 || repo.filter.Status == nil || *repo.filter.Status != domain.StatusPending ||
		repo.filter.After == nil || *repo.filter.After != next {
		t.Errorf("filter %+v", repo.filter)
	}
	if got := rawMember(t, res.body, "next_cursor"); got != `"`+next.Encode()+`"` {
		t.Errorf("next_cursor %s", got)
	}

	repo.rows, repo.next = nil, nil
	res = c.get("/v1/notifications")
	if res.status != http.StatusOK {
		t.Fatalf("status %d: %s", res.status, res.body)
	}
	if got := rawMember(t, res.body, "data"); got != "[]" {
		t.Errorf("data %s, want []", got)
	}
	if got := rawMember(t, res.body, "next_cursor"); got != "null" {
		t.Errorf("next_cursor %s, want null", got)
	}
	if repo.filter.Limit != defaultListLimit {
		t.Errorf("default limit %d", repo.filter.Limit)
	}
}

func TestListNotifications_StoreErrors(t *testing.T) {
	repo := &listRepo{}
	repo.err = context.DeadlineExceeded
	c, _ := newListAPI(t, repo)
	if res := c.get("/v1/notifications"); res.status != http.StatusServiceUnavailable {
		t.Errorf("list: status %d", res.status)
	}
	if res := c.get("/v1/notifications/" + uuid.NewString() + "/attempts"); res.status != http.StatusServiceUnavailable {
		t.Errorf("attempts: status %d", res.status)
	}
}

func TestListAttempts_UnknownNotification(t *testing.T) {
	c, _ := newFakeAPI(t, &fakeRepo{})
	res := c.get("/v1/notifications/" + uuid.NewString() + "/attempts")
	if res.status != http.StatusNotFound {
		t.Errorf("status %d: %s", res.status, res.body)
	}
}

// rawMember returns the raw JSON of one member of a response body.
func rawMember(t *testing.T, body []byte, name string) string {
	t.Helper()
	var m map[string]json.RawMessage
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("decode %s: %v", body, err)
	}
	v, ok := m[name]
	if !ok {
		t.Fatalf("no %q in %s", name, body)
	}
	return string(v)
}

func TestListNotificationAttempts_RejectsQueryParameters(t *testing.T) {
	repo := &listRepo{}
	c, _ := newListAPI(t, repo)
	base := "/v1/notifications/" + uuid.NewString() + "/attempts"
	for _, tt := range []struct {
		query string
		want  []string
	}{
		{"limit=5", []string{"limit:unknown_parameter"}},
		{"cursor=x&a=1&a=2", []string{"a:unknown_parameter", "cursor:unknown_parameter"}},
		{"a=%zz", []string{":invalid_parameter"}},
	} {
		res := c.get(base + "?" + tt.query)
		if res.status != http.StatusBadRequest {
			t.Errorf("%s: status %d: %s", tt.query, res.status, res.body)
			continue
		}
		if got := res.problemCodes(t); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%s: errors %v, want %v", tt.query, got, tt.want)
		}
	}
	if res := c.get(base); res.status != http.StatusOK {
		t.Errorf("no query: status %d: %s", res.status, res.body)
	}
}

// Every operation under /v1/ but the listing defines no query parameter and
// rejects any; the health endpoints and the spec do not.
func TestUndefinedQueryProblem(t *testing.T) {
	for _, tt := range []struct {
		route, query string
		want         []string
	}{
		{"GET /v1/notifications/{id}", "status=sent", []string{"status:unknown_parameter"}},
		{"POST /v1/notifications", "dry_run=1", []string{"dry_run:unknown_parameter"}},
		{"POST /v1/notifications/{id}/cancel", "force=true&a=1", []string{"a:unknown_parameter", "force:unknown_parameter"}},
		{"GET /v1/batches/{id}", "a=%zz", []string{":invalid_parameter"}},
		{"GET /v1/notifications/{id}", "", nil},
		{"GET /v1/notifications", "status=sent", nil},
		{"GET /healthz", "probe=1", nil},
		{"GET /openapi.yaml", "v=2", nil},
		{"", "a=1", nil}, // no route matched: the router answers 404 or 405
	} {
		p := undefinedQueryProblem(tt.route, tt.query)
		if tt.want == nil {
			if p != nil {
				t.Errorf("%s ?%s: unexpected problem", tt.route, tt.query)
			}
			continue
		}
		if p == nil || p.kind.status != http.StatusBadRequest {
			t.Errorf("%s ?%s: no 400 problem", tt.route, tt.query)
			continue
		}
		var got []string
		for _, e := range p.errors {
			got = append(got, e.Field+":"+e.Code)
		}
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%s ?%s: errors %v, want %v", tt.route, tt.query, got, tt.want)
		}
	}
}

// The rule is applied by the router, before the handler.
func TestRouter_RejectsUndefinedQueryParameters(t *testing.T) {
	c, _ := newListAPI(t, &listRepo{})
	if res := c.get("/v1/notifications/" + uuid.NewString() + "?expand=attempts"); res.status != http.StatusBadRequest {
		t.Errorf("read with a query: status %d: %s", res.status, res.body)
	} else if got := res.problemCodes(t); !reflect.DeepEqual(got, []string{"expand:unknown_parameter"}) {
		t.Errorf("read with a query: errors %v", got)
	}
	if res := c.get("/healthz?probe=1"); res.status != http.StatusOK {
		t.Errorf("healthz with a query: status %d", res.status)
	}
}
