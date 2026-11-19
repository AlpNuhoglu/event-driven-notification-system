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
)

// templatePage is one page of GET /v1/templates, as the tests read it.
type templatePage struct {
	Data []struct {
		Name          string `json:"name"`
		Channel       string `json:"channel"`
		LatestVersion int    `json:"latest_version"`
		IsActive      bool   `json:"is_active"`
	} `json:"data"`
	NextCursor *string `json:"next_cursor"`
}

func (p templatePage) names() []string {
	names := make([]string, len(p.Data))
	for i, d := range p.Data {
		names[i] = d.Name
	}
	return names
}

func (a *testAPI) listTemplates(t *testing.T, query string) templatePage {
	t.Helper()
	path := "/v1/templates"
	if query != "" {
		path += "?" + query
	}
	res := a.get(path)
	wantStatus(t, res, http.StatusOK)
	if rawMember(t, res.body, "data") == "null" {
		t.Fatalf("data is null: %s", res.body)
	}
	var p templatePage
	if err := json.Unmarshal(res.body, &p); err != nil {
		t.Fatalf("decode %s: %v", res.body, err)
	}
	return p
}

func (a *testAPI) createTpl(t *testing.T, name string) {
	t.Helper()
	wantStatus(t, a.do(http.MethodPost, "/v1/templates", tplBody(name, "sms", "hello"), nil), http.StatusCreated)
}

func TestListTemplatesAPI_PaginationWalk(t *testing.T) {
	a := newTestAPI(t)
	var want []string
	for i := range 25 {
		name := fmt.Sprintf("t%02d", i*2) // t00, t02, ... t48: room for names in between
		a.createTpl(t, name)
		want = append(want, name)
	}

	var got []string
	cursor := ""
	pages := 0
	var late string
	for {
		pages++
		if pages > 10 {
			t.Fatal("the walk does not end")
		}
		q := "limit=7"
		if cursor != "" {
			q += "&cursor=" + url.QueryEscape(cursor)
		}
		p := a.listTemplates(t, q)
		got = append(got, p.names()...)
		last := pages == 4
		if last != (p.NextCursor == nil) {
			t.Fatalf("page %d: next_cursor %v, null expected exactly on page 4", pages, p.NextCursor)
		}
		if pages == 2 {
			// During the walk: one name that sorts after the cursor (appears)
			// and one before it (does not).
			late = "t41"
			a.createTpl(t, late)
			a.createTpl(t, "t01x")
			want = append(want, late)
			slices.Sort(want)
		}
		if p.NextCursor == nil {
			break
		}
		cursor = *p.NextCursor
	}
	// 14 names are read before the late ones are created; 11 + t41 remain.
	if !slices.Equal(got, want) {
		t.Errorf("walk returned\n%v\nwant\n%v", got, want)
	}
	if slices.Contains(got, "t01x") {
		t.Error("a template created before the cursor was listed")
	}
}

func TestListTemplatesAPI_ExactPages(t *testing.T) {
	a := newTestAPI(t)
	var want []string
	for i := range 25 {
		name := fmt.Sprintf("n%02d", i)
		a.createTpl(t, name)
		want = append(want, name)
	}
	var got []string
	cursor, pages := "", 0
	for {
		pages++
		q := "limit=7"
		if cursor != "" {
			q += "&cursor=" + url.QueryEscape(cursor)
		}
		p := a.listTemplates(t, q)
		got = append(got, p.names()...)
		wantLen := 7
		if pages == 4 {
			wantLen = 4
		}
		if len(p.Data) != wantLen {
			t.Fatalf("page %d has %d items, want %d", pages, len(p.Data), wantLen)
		}
		if (p.NextCursor == nil) != (pages == 4) {
			t.Fatalf("page %d: next_cursor %v", pages, p.NextCursor)
		}
		if p.NextCursor == nil {
			break
		}
		if want := encodeTemplateCursor(p.Data[len(p.Data)-1].Name); *p.NextCursor != want {
			t.Fatalf("next_cursor %q, want %q", *p.NextCursor, want)
		}
		cursor = *p.NextCursor
	}
	if !slices.Equal(got, want) || pages != 4 {
		t.Errorf("%d pages, names %v", pages, got)
	}
}

func TestListTemplatesAPI_LimitBounds(t *testing.T) {
	a := newTestAPI(t)
	for _, n := range []string{"b", "a", "c"} {
		a.createTpl(t, n)
	}
	p := a.listTemplates(t, "limit=1")
	if !slices.Equal(p.names(), []string{"a"}) || p.NextCursor == nil {
		t.Errorf("limit=1: %v next %v", p.names(), p.NextCursor)
	}
	p = a.listTemplates(t, "limit=200")
	if !slices.Equal(p.names(), []string{"a", "b", "c"}) || p.NextCursor != nil {
		t.Errorf("limit=200: %v next %v", p.names(), p.NextCursor)
	}
	// The default limit is 50: 55 templates make a first page of 50 and a cursor.
	for i := range 52 {
		a.createTpl(t, fmt.Sprintf("d%02d", i))
	}
	p = a.listTemplates(t, "")
	if len(p.Data) != 50 || p.NextCursor == nil {
		t.Errorf("default limit: %d items, next %v", len(p.Data), p.NextCursor)
	}
}

func TestListTemplatesAPI_EmptyList(t *testing.T) {
	a := newTestAPI(t)
	res := a.get("/v1/templates")
	wantStatus(t, res, http.StatusOK)
	var got any
	if err := json.Unmarshal(res.body, &got); err != nil {
		t.Fatal(err)
	}
	if want := map[string]any{"data": []any{}, "next_cursor": nil}; !reflect.DeepEqual(got, want) {
		t.Errorf("body %s, want {\"data\": [], \"next_cursor\": null}", res.body)
	}
}

func TestListTemplatesAPI_ItemShapeAndInactive(t *testing.T) {
	a := newTestAPI(t)
	a.createTpl(t, "one")
	wantStatus(t, a.do(http.MethodPost, "/v1/templates/one/versions", versionBody("bye"), nil), http.StatusCreated)
	a.createTpl(t, "two")
	wantStatus(t, a.do(http.MethodPost, "/v1/templates/two/deactivate", "", nil), http.StatusOK)

	res := a.get("/v1/templates")
	wantStatus(t, res, http.StatusOK)
	var body struct {
		Data []map[string]any `json:"data"`
	}
	if err := json.Unmarshal(res.body, &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Data) != 2 {
		t.Fatalf("items: %s", res.body)
	}
	want := []map[string]any{
		{"name": "one", "channel": "sms", "latest_version": float64(2), "is_active": true},
		{"name": "two", "channel": "sms", "latest_version": float64(1), "is_active": false},
	}
	for i, item := range body.Data {
		if !reflect.DeepEqual(item, want[i]) {
			t.Errorf("item %d = %v, want exactly %v", i, item, want[i])
		}
	}
}

func TestListTemplatesAPI_BadQuery(t *testing.T) {
	a := newTestAPI(t)
	a.createTpl(t, "otp")
	for _, tt := range []struct {
		query string
		want  []string
	}{
		{"foo=bar", []string{"foo:unknown_parameter"}},
		{"channel=sms", []string{"channel:unknown_parameter"}},
		{"limit=1&limit=2", []string{"limit:duplicate_parameter"}},
		{"cursor=YQ&cursor=YQ", []string{"cursor:duplicate_parameter"}},
		{"limit=0", []string{"limit:invalid_parameter"}},
		{"limit=201", []string{"limit:invalid_parameter"}},
		{"limit=ten", []string{"limit:invalid_parameter"}},
		{"limit=", []string{"limit:invalid_parameter"}},
		{"cursor=", []string{"cursor:invalid_parameter"}},
		{"cursor=%00", []string{"cursor:invalid_cursor"}},
		{"cursor=YWI%3D", []string{"cursor:invalid_cursor"}},
		{"cursor=" + strings.Repeat("a", 300), []string{"cursor:invalid_cursor"}},
		{"cursor=" + encodeTemplateCursor("Not-A-Name"), []string{"cursor:invalid_cursor"}},
		{"limit=x&foo=1&cursor=bad!", []string{"limit:invalid_parameter", "cursor:invalid_cursor", "foo:unknown_parameter"}},
	} {
		res := a.get("/v1/templates?" + tt.query)
		wantStatus(t, res, http.StatusBadRequest)
		if got := res.problemCodes(t); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%s: errors %v, want %v", tt.query, got, tt.want)
		}
	}
}
