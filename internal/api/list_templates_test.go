package api

import (
	"encoding/base64"
	"reflect"
	"strings"
	"testing"
)

func TestTemplateCursor_RoundTrip(t *testing.T) {
	for _, name := range []string{"a", "0", "otp", "a-b_c9", strings.Repeat("z", 64)} {
		c := encodeTemplateCursor(name)
		if strings.ContainsAny(c, "=+/") {
			t.Errorf("%q: cursor %q is padded or not URL-safe", name, c)
		}
		got, ok := decodeTemplateCursor(c)
		if !ok || got != name {
			t.Errorf("%q: decoded %q, %v", name, got, ok)
		}
	}
}

func TestTemplateCursor_Refused(t *testing.T) {
	enc := func(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }
	for name, c := range map[string]string{
		"empty":                "",
		"padded":               base64.URLEncoding.EncodeToString([]byte("ab")), // YWI=; the canonical spelling is YWI
		"standard alphabet":    base64.StdEncoding.EncodeToString([]byte("a-b?>")),
		"non-canonical bits":   "YR", // decodes to "a" with trailing bits set; "YQ" is the canonical spelling
		"not base64":           "%%%",
		"newline inside":       "YW\nI",
		"not a name (upper)":   enc("Otp"),
		"not a name (dot)":     enc("a.b"),
		"not a name (leading)": enc("-a"),
		"not a name (space)":   enc("a b"),
		"not a name (utf-8)":   enc("ş"),
		"not a name (nul)":     enc("a\x00"),
		"over-long name":       enc(strings.Repeat("a", 65)),
		"over-long cursor":     strings.Repeat("a", maxCursorChars+1),
	} {
		if got, ok := decodeTemplateCursor(c); ok {
			t.Errorf("%s: %q accepted as %q", name, c, got)
		}
	}
	if name, ok := decodeTemplateCursor("YWI"); !ok || name != "ab" {
		t.Errorf("canonical cursor refused: %q, %v", name, ok)
	}
}

func TestParseTemplateListQuery(t *testing.T) {
	cursor := encodeTemplateCursor("otp")
	for _, tt := range []struct {
		name, query string
		want        templateListQuery
		codes       []string
	}{
		{"nothing", "", templateListQuery{Limit: 50}, nil},
		{"limit 1", "limit=1", templateListQuery{Limit: 1}, nil},
		{"limit 200", "limit=200", templateListQuery{Limit: 200}, nil},
		{"cursor and limit", "limit=7&cursor=" + cursor, templateListQuery{After: "otp", Limit: 7}, nil},
		{"limit 0", "limit=0", templateListQuery{}, []string{"limit:invalid_parameter"}},
		{"limit 201", "limit=201", templateListQuery{}, []string{"limit:invalid_parameter"}},
		{"limit negative", "limit=-1", templateListQuery{}, []string{"limit:invalid_parameter"}},
		{"limit text", "limit=ten", templateListQuery{}, []string{"limit:invalid_parameter"}},
		{"limit decimal", "limit=1.5", templateListQuery{}, []string{"limit:invalid_parameter"}},
		{"limit empty", "limit=", templateListQuery{}, []string{"limit:invalid_parameter"}},
		{"limit twice", "limit=1&limit=2", templateListQuery{}, []string{"limit:duplicate_parameter"}},
		{"cursor twice", "cursor=" + cursor + "&cursor=" + cursor, templateListQuery{}, []string{"cursor:duplicate_parameter"}},
		{"cursor empty", "cursor=", templateListQuery{}, []string{"cursor:invalid_parameter"}},
		{"cursor garbage", "cursor=%00", templateListQuery{}, []string{"cursor:invalid_cursor"}},
		{"cursor padded", "cursor=YWI%3D", templateListQuery{}, []string{"cursor:invalid_cursor"}},
		{"cursor not a name", "cursor=" + encodeTemplateCursor("A"), templateListQuery{}, []string{"cursor:invalid_cursor"}},
		{"cursor over-long", "cursor=" + strings.Repeat("a", maxCursorChars+1), templateListQuery{}, []string{"cursor:invalid_cursor"}},
		{"unknown", "status=active", templateListQuery{}, []string{"status:unknown_parameter"}},
		{"a notification filter is unknown here", "channel=sms", templateListQuery{}, []string{"channel:unknown_parameter"}},
		{"unknown sorted", "z=1&a=1", templateListQuery{}, []string{"a:unknown_parameter", "z:unknown_parameter"}},
		{"bad query string", "limit=%zz", templateListQuery{}, []string{":invalid_parameter"}},
		{"all problems", "x=1&limit=0&cursor=%00", templateListQuery{}, []string{"limit:invalid_parameter", "cursor:invalid_cursor", "x:unknown_parameter"}},
	} {
		got, errs := parseTemplateListQuery(tt.query)
		var codes []string
		for _, e := range errs {
			codes = append(codes, e.Field+":"+string(e.Code))
		}
		if !reflect.DeepEqual(codes, tt.codes) {
			t.Errorf("%s: errors %v, want %v", tt.name, codes, tt.codes)
			continue
		}
		if tt.codes == nil && got != tt.want {
			t.Errorf("%s: query %+v, want %+v", tt.name, got, tt.want)
		}
	}
}

func TestUndefinedQueryProblem_TemplateListingDefinesItsOwn(t *testing.T) {
	if p := undefinedQueryProblem(routeListTemplates, "limit=5"); p != nil {
		t.Error("GET /v1/templates is refused its own parameters by the router")
	}
	if p := undefinedQueryProblem("GET /v1/templates/{name}", "limit=5"); p == nil {
		t.Error("GET /v1/templates/{name} accepts a query string")
	}
	if p := undefinedQueryProblem("POST /v1/templates", "limit=5"); p == nil {
		t.Error("POST /v1/templates accepts a query string")
	}
}
