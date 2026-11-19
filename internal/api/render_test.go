package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
)

// tplItem is a create request of channel sms that uses a template; use is
// the template member's JSON.
func tplItem(use string) string {
	return `{"channel":"sms","recipient":"+905551234567","template":` + use + `}`
}

// The template member of a create is checked without the database: its
// shape, the name, the version and the variables object. The values of the
// variables are checked by the template stage, which knows the declaration.
func TestValidateCreate_TemplateMember(t *testing.T) {
	long := strings.Repeat("z", 100)
	tests := []struct {
		name  string
		body  string
		codes []string // nil: valid
	}{
		{"name only", tplItem(`{"name":"otp"}`), nil},
		{"version and variables", tplItem(`{"name":"otp","version":3,"variables":{"code":482913,"x":"y","b":true}}`), nil},
		{"largest version", tplItem(`{"name":"otp","version":2147483647}`), nil},
		{"empty variables", tplItem(`{"name":"otp","variables":{}}`), nil},
		{"a value of any JSON type passes this stage", tplItem(`{"name":"otp","variables":{"a":{"b":1},"c":null}}`), nil},
		{"template null with content", `{"channel":"sms","recipient":"+905551234567","content":"x","template":null}`, nil},

		{"neither content nor template", `{"channel":"sms","recipient":"+905551234567"}`, []string{"content:required"}},
		{"template null without content", `{"channel":"sms","recipient":"+905551234567","template":null}`, []string{"content:required"}},
		{"both", `{"channel":"sms","recipient":"+905551234567","content":"x","template":{"name":"otp"}}`, []string{"template:conflicts_with_content"}},
		{"both, content invalid", `{"channel":"sms","recipient":"+905551234567","content":"","template":{"name":"otp"}}`, []string{"content:empty", "template:conflicts_with_content"}},
		{"template repeated", `{"channel":"sms","recipient":"+905551234567","template":{"name":"a"},"template":{"name":"b"}}`, []string{"template:duplicate_field"}},
		{"template a string", tplItem(`"otp"`), []string{"template:invalid_type"}},
		{"template an array", tplItem(`[]`), []string{"template:invalid_type"}},
		{"template a number", tplItem(`1`), []string{"template:invalid_type"}},

		{"name missing", tplItem(`{}`), []string{"template.name:required"}},
		{"name not a string", tplItem(`{"name":5}`), []string{"template.name:invalid_type"}},
		{"name null", tplItem(`{"name":null}`), []string{"template.name:invalid_type"}},
		{"name in upper case", tplItem(`{"name":"OTP"}`), []string{"template.name:invalid_value"}},
		{"name too long", tplItem(`{"name":"` + strings.Repeat("a", 65) + `"}`), []string{"template.name:invalid_value"}},
		{"name repeated", tplItem(`{"name":"a","name":"b"}`), []string{"template.name:duplicate_field"}},

		{"version a string", tplItem(`{"name":"otp","version":"3"}`), []string{"template.version:invalid_type"}},
		{"version a fraction", tplItem(`{"name":"otp","version":1.5}`), []string{"template.version:invalid_type"}},
		{"version with an exponent", tplItem(`{"name":"otp","version":1e2}`), []string{"template.version:invalid_type"}},
		{"version null", tplItem(`{"name":"otp","version":null}`), []string{"template.version:invalid_type"}},
		{"version 0", tplItem(`{"name":"otp","version":0}`), []string{"template.version:invalid_value"}},
		{"version negative", tplItem(`{"name":"otp","version":-1}`), []string{"template.version:invalid_value"}},
		{"version over int4", tplItem(`{"name":"otp","version":2147483648}`), []string{"template.version:invalid_value"}},
		{"version far over int4", tplItem(`{"name":"otp","version":99999999999999999999}`), []string{"template.version:invalid_value"}},

		{"variables an array", tplItem(`{"name":"otp","variables":[]}`), []string{"template.variables:invalid_type"}},
		{"variables null", tplItem(`{"name":"otp","variables":null}`), []string{"template.variables:invalid_type"}},
		{"variables a string", tplItem(`{"name":"otp","variables":"code"}`), []string{"template.variables:invalid_type"}},
		{"a variable repeated", tplItem(`{"name":"otp","variables":{"a":1,"b":2,"a":3,"a":4}}`), []string{"template.variables.a:duplicate_field"}},
		{"variables repeated", tplItem(`{"name":"otp","variables":{},"variables":{}}`), []string{"template.variables:duplicate_field"}},

		{"unknown member", tplItem(`{"name":"otp","subject":"x"}`), []string{"template.subject:unknown_field"}},
		{"unknown member reported once", tplItem(`{"name":"otp","x":1,"x":2}`), []string{"template.x:unknown_field"}},
		{"unknown member echoed cut", tplItem(`{"name":"otp","` + long + `":1}`), []string{"template." + long[:maxEchoedFieldName] + ":unknown_field"}},
		{
			"every member invalid, in member order then unknown members",
			tplItem(`{"zeta":1,"variables":[],"version":0,"name":"-x"}`),
			[]string{"template.name:invalid_value", "template.version:invalid_value", "template.variables:invalid_type", "template.zeta:unknown_field"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := checkCreate([]byte(tt.body), domain.PhonePossible)
			if got := codes(p); !slices.Equal(got, tt.codes) {
				t.Fatalf("codes %v, want %v", got, tt.codes)
			}
			// An item of a batch is checked the same way.
			in, bp := validateBatch([]byte(batchOf(tt.body)), domain.PhonePossible)
			if bp != nil {
				t.Fatalf("batch: %v", codes(bp))
			}
			if got := rejectedCodes(in); !slices.Equal(got, prefixed("0/", tt.codes)) {
				t.Errorf("batch item codes %v, want %v", got, tt.codes)
			}
		})
	}
	// The message of a missing message names both fields.
	p := checkCreate([]byte(`{"channel":"sms","recipient":"+905551234567"}`), domain.PhonePossible)
	if m := p.errors[0].Message; m != "content or template is required" {
		t.Errorf("message %q", m)
	}
}

func prefixed(prefix string, list []string) []string {
	var out []string
	for _, s := range list {
		out = append(out, prefix+s)
	}
	return out
}

// The parsed template use keeps the variables as the raw JSON the client
// sent: a number is not turned into a float64.
func TestValidateCreate_TemplateParsed(t *testing.T) {
	body := tplItem(`{"name":"otp","version":7,"variables":{"code":482913,"long":12345678901234567890123,"s":"x"}}`)
	req, p := validateCreate([]byte(body), domain.PhonePossible)
	if p != nil {
		t.Fatalf("problem %v", codes(p))
	}
	u := req.template
	if u == nil || u.name != "otp" || u.version != 7 || req.request.Content != nil {
		t.Fatalf("use = %+v, content %v", u, req.request.Content)
	}
	if string(u.vars["code"]) != "482913" || string(u.vars["long"]) != "12345678901234567890123" || string(u.vars["s"]) != `"x"` {
		t.Errorf("vars = %v", u.vars)
	}
	vars, err := encodeTemplateVars(u.rawVars)
	if err != nil {
		t.Fatal(err)
	}
	if string(vars) != `{"code":482913,"long":12345678901234567890123,"s":"x"}` {
		t.Errorf("template_vars = %s", vars)
	}
	if vars, _ := encodeTemplateVars(nil); string(vars) != `{}` {
		t.Errorf("template_vars without variables = %s", vars)
	}
}

// The body of a preview has the members version and variables only.
func TestValidatePreview(t *testing.T) {
	for _, tt := range []struct {
		name   string
		body   string
		status int
		codes  []string
	}{
		{"empty object", `{}`, 0, nil},
		{"version and variables", `{"version":2,"variables":{"a":1}}`, 0, nil},
		{"name is not a member", `{"name":"x"}`, 422, []string{"name:unknown_field"}},
		{"version a string", `{"version":"2"}`, 422, []string{"version:invalid_type"}},
		{"version 0", `{"version":0}`, 422, []string{"version:invalid_value"}},
		{"variables an array", `{"variables":[]}`, 422, []string{"variables:invalid_type"}},
		{"a variable repeated", `{"variables":{"a":1,"a":2}}`, 422, []string{"variables.a:duplicate_field"}},
		{"version repeated", `{"version":1,"version":2}`, 422, []string{"version:duplicate_field"}},
		{"not an object", `[]`, 400, []string{":not_an_object"}},
		{"malformed", `{`, 400, []string{":malformed_json"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, p := validatePreview([]byte(tt.body))
			if tt.status == 0 {
				if p != nil {
					t.Fatalf("problem %v", codes(p))
				}
				return
			}
			if p == nil || p.kind.status != tt.status || !slices.Equal(codes(p), tt.codes) {
				t.Fatalf("got %v, want %d %v", p, tt.status, tt.codes)
			}
		})
	}
}

// fakeTemplates is a template store that counts its reads. Each name maps
// to its versions, the first being version 1.
type fakeTemplates struct {
	versions map[string][]string // bodies
	vars     map[string][]string // required variables of every version
	channel  domain.Channel
	inactive map[string]bool
	err      error
	// latestReads and versionReads count the calls.
	latestReads, versionReads int
}

func (f *fakeTemplates) vt(name string, version int) store.VersionedTemplate {
	return store.VersionedTemplate{
		Template: store.Template{Name: name, Channel: f.channel, IsActive: !f.inactive[name], LatestVersion: len(f.versions[name])},
		Version: store.TemplateVersion{
			Version: version, Body: f.versions[name][version-1],
			Variables: f.vars[name], OptionalVariables: []string{},
		},
	}
}

func (f *fakeTemplates) GetTemplate(_ context.Context, name string) (store.VersionedTemplate, error) {
	f.latestReads++
	if f.err != nil {
		return store.VersionedTemplate{}, f.err
	}
	if len(f.versions[name]) == 0 {
		return store.VersionedTemplate{}, store.ErrTemplateNotFound
	}
	return f.vt(name, len(f.versions[name])), nil
}

func (f *fakeTemplates) GetTemplateVersion(_ context.Context, name string, version int) (store.VersionedTemplate, error) {
	f.versionReads++
	if f.err != nil {
		return store.VersionedTemplate{}, f.err
	}
	switch {
	case len(f.versions[name]) == 0:
		return store.VersionedTemplate{}, store.ErrTemplateNotFound
	case version > len(f.versions[name]):
		return store.VersionedTemplate{}, store.ErrTemplateVersionNotFound
	}
	return f.vt(name, version), nil
}

func newFakeTemplates() *fakeTemplates {
	return &fakeTemplates{
		versions: map[string][]string{
			"otp":     {"Old {{ .code }}", "Your code is {{ .code }}"},
			"welcome": {"Hello {{ .name }}"},
		},
		vars:     map[string][]string{"otp": {"code"}, "welcome": {"name"}},
		channel:  domain.ChannelSMS,
		inactive: map[string]bool{},
	}
}

// batchFrom validates a batch body that must pass its top level.
func batchFrom(t *testing.T, items ...string) batchInput {
	t.Helper()
	in, p := validateBatch([]byte(batchOf(items...)), domain.PhonePossible)
	if p != nil {
		t.Fatalf("problem %v", codes(p))
	}
	return in
}

const cacheTimeout = 1_000 * time.Millisecond

// 1,000 items naming two templates load each once (ADR-0008 §8); every item
// that leaves the version out gets the latest version, and a pinned latest
// version needs no other load.
func TestTemplateCache_OneLoadPerTemplate(t *testing.T) {
	f := newFakeTemplates()
	items := make([]string, 0, maxBatchItems)
	for i := range maxBatchItems {
		switch i % 3 {
		case 0:
			items = append(items, tplItem(`{"name":"otp","variables":{"code":482913}}`))
		case 1:
			items = append(items, tplItem(`{"name":"welcome","variables":{"name":"Ada"}}`))
		default:
			items = append(items, tplItem(`{"name":"otp","version":2,"variables":{"code":1}}`))
		}
	}
	in, p := prepareBatch(context.Background(), newTemplateCache(f, cacheTimeout), batchFrom(t, items...))
	if p != nil {
		t.Fatalf("problem %v", p)
	}
	if f.latestReads != 2 || f.versionReads != 0 {
		t.Errorf("reads: latest %d, pinned %d; want 2 and 0", f.latestReads, f.versionReads)
	}
	if len(in.valid) != maxBatchItems || len(in.rejected) != 0 {
		t.Fatalf("valid %d, rejected %v", len(in.valid), rejectedCodes(in))
	}
	for _, it := range in.valid {
		r := it.rendered
		switch it.index % 3 {
		case 0:
			if r.name != "otp" || r.version != 2 || r.content != "Your code is 482913" || string(r.vars) != `{"code":482913}` {
				t.Fatalf("item %d: %+v", it.index, r)
			}
		case 1:
			if r.name != "welcome" || r.version != 1 || r.content != "Hello Ada" {
				t.Fatalf("item %d: %+v", it.index, r)
			}
		}
	}

	// An unknown name is looked up once, and so is a missing version.
	f = newFakeTemplates()
	in, p = prepareBatch(context.Background(), newTemplateCache(f, cacheTimeout), batchFrom(t,
		slices.Repeat([]string{tplItem(`{"name":"nope"}`), tplItem(`{"name":"otp","version":9}`)}, maxBatchItems/2)...))
	if p != nil {
		t.Fatalf("problem %v", p)
	}
	if f.latestReads != 1 || f.versionReads != 1 || len(in.valid) != 0 || len(in.rejected) != maxBatchItems {
		t.Errorf("reads: latest %d, pinned %d; valid %d, rejected %d", f.latestReads, f.versionReads, len(in.valid), len(in.rejected))
	}
	if got := rejectedCodes(in)[:2]; !slices.Equal(got, []string{"0/template.name:template_not_found", "1/template.version:template_version_not_found"}) {
		t.Errorf("codes %v", got)
	}
}

// The dedupe keys are claimed after the template stage: an item that fails
// to render does not make a later valid item with its key a duplicate, and
// rejected stays in ascending order of index.
func TestPrepareBatch_ClaimsDedupeKeysAfterTheTemplateStage(t *testing.T) {
	withKey := func(item, key string) string {
		return strings.TrimSuffix(item, "}") + `,"dedupe_key":"` + key + `"}`
	}
	in := batchFrom(t,
		withKey(tplItem(`{"name":"otp"}`), "k"),                            // 0: misses code
		`{"channel":"fax","recipient":"x","content":"x","dedupe_key":"k"}`, // 1: invalid
		withKey(tplItem(`{"name":"otp","variables":{"code":"1"}}`), "k"),   // 2: keeps k
		withKey(itemA, "k"), // 3: duplicate of 2
		withKey(tplItem(`{"name":"otp","variables":{"code":"1","x":"2"}}`), "j"), // 4: unknown variable
		withKey(itemA, "j"), // 5: keeps j
	)
	out, p := prepareBatch(context.Background(), newTemplateCache(newFakeTemplates(), cacheTimeout), in)
	if p != nil {
		t.Fatalf("problem %v", p)
	}
	if got := validIndexes(out); !slices.Equal(got, []int{2, 5}) {
		t.Errorf("valid %v, want [2 5]", got)
	}
	want := []string{
		"0/template.variables.code:missing_variable",
		"1/channel:invalid_value",
		"3/dedupe_key:duplicate_in_batch",
		"4/template.variables.x:unknown_variable",
	}
	if got := rejectedCodes(out); !slices.Equal(got, want) {
		t.Errorf("rejected %v, want %v", got, want)
	}
	// The input's rejected list is not changed.
	if got := rejectedCodes(in); !slices.Equal(got, []string{"1/channel:invalid_value"}) {
		t.Errorf("input rejected changed: %v", got)
	}
}

// The template stage's own errors.
func TestRenderForCreate_Errors(t *testing.T) {
	f := newFakeTemplates()
	f.versions["mail"] = []string{"<p>{{ .name }}</p>"}
	f.vars["mail"] = []string{"name"}
	f.versions["off"] = []string{"x"}
	f.inactive["off"] = true
	f.versions["empty"] = []string{"{{ .v }}"}
	f.vars["empty"] = []string{"v"}
	f.versions["long"] = []string{strings.Repeat("ş", 700) + "{{ .v }}"}
	f.vars["long"] = []string{"v"}
	longName := strings.Repeat("v", 100)
	for _, tt := range []struct {
		name    string
		channel domain.Channel
		use     string
		codes   []string
	}{
		{"unknown name", domain.ChannelSMS, `{"name":"nope"}`, []string{"template.name:template_not_found"}},
		{"unknown version", domain.ChannelSMS, `{"name":"otp","version":3}`, []string{"template.version:template_version_not_found"}},
		{"inactive", domain.ChannelSMS, `{"name":"off"}`, []string{"template.name:template_inactive"}},
		{"inactive, pinned", domain.ChannelSMS, `{"name":"off","version":1}`, []string{"template.name:template_inactive"}},
		{"another channel", domain.ChannelEmail, `{"name":"otp","variables":{"code":"1"}}`, []string{"template.name:template_channel_mismatch"}},
		{"missing variable", domain.ChannelSMS, `{"name":"otp"}`, []string{"template.variables.code:missing_variable"}},
		{
			"value problems, sorted by name", domain.ChannelSMS, `{"name":"otp","variables":{"code":{"a":1},"zz":1,"` + longName + `":2}}`,
			[]string{"template.variables.code:invalid_type", "template.variables." + longName[:maxEchoedFieldName] + ":unknown_variable", "template.variables.zz:unknown_variable"},
		},
		{"value too long", domain.ChannelSMS, `{"name":"otp","variables":{"code":"` + strings.Repeat("a", 1025) + `"}}`, []string{"template.variables.code:too_long"}},
		{"value with NUL", domain.ChannelSMS, `{"name":"otp","variables":{"code":"a\u0000"}}`, []string{"template.variables.code:invalid_character"}},
		{"value null", domain.ChannelSMS, `{"name":"otp","variables":{"code":null}}`, []string{"template.variables.code:invalid_type"}},
		{"renders to nothing", domain.ChannelSMS, `{"name":"empty","variables":{"v":""}}`, []string{"template:empty"}},
		{"renders over the SMS limit", domain.ChannelSMS, `{"name":"long","variables":{"v":"x"}}`, []string{"template:too_many_segments"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req, p := validateCreate([]byte(tplItem(tt.use)), domain.PhonePossible)
			if p != nil {
				t.Fatalf("problem %v", codes(p))
			}
			r, errs, p := newTemplateCache(f, cacheTimeout).renderForCreate(context.Background(), tt.channel, req.template)
			if p != nil || r != nil {
				t.Fatalf("problem %v, rendered %+v", p, r)
			}
			var got []string
			for _, e := range errs {
				got = append(got, e.Field+":"+e.Code)
				if strings.Contains(e.Message, "482913") || strings.Contains(e.Message, "aaaa") {
					t.Errorf("message leaks a value: %q", e.Message)
				}
			}
			if !slices.Equal(got, tt.codes) {
				t.Errorf("codes %v, want %v", got, tt.codes)
			}
		})
	}

	// A pinned old version renders the old body.
	req, _ := validateCreate([]byte(tplItem(`{"name":"otp","version":1,"variables":{"code":7}}`)), domain.PhonePossible)
	r, errs, p := newTemplateCache(f, cacheTimeout).renderForCreate(context.Background(), domain.ChannelSMS, req.template)
	if p != nil || len(errs) > 0 || r.content != "Old 7" || r.version != 1 {
		t.Errorf("pinned: %+v %v %v", r, errs, p)
	}
}

// A failure that is not the client's fails the whole request: the store
// (503 when it is unavailable, 500 otherwise) and a stored body that does
// not compile (500).
func TestRenderForCreate_RequestFailures(t *testing.T) {
	use := tplItem(`{"name":"otp","variables":{"code":"1"}}`)
	for _, tt := range []struct {
		name   string
		mod    func(*fakeTemplates)
		status int
	}{
		{"database unavailable", func(f *fakeTemplates) { f.err = context.DeadlineExceeded }, http.StatusServiceUnavailable},
		{"database error", func(f *fakeTemplates) { f.err = errors.New("scan failed") }, http.StatusInternalServerError},
		{"stored body does not compile", func(f *fakeTemplates) { f.versions["otp"][1] = `{{ printf "%d" 1 }}` }, http.StatusInternalServerError},
		{"stored declaration does not match the body", func(f *fakeTemplates) { f.vars["otp"] = []string{"other"} }, http.StatusInternalServerError},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeTemplates()
			tt.mod(f)
			in, p := prepareBatch(context.Background(), newTemplateCache(f, cacheTimeout), batchFrom(t, use, itemA))
			if p == nil || p.kind.status != tt.status || p.cause == nil {
				t.Fatalf("problem %+v, want %d with a cause; input %+v", p, tt.status, in)
			}
		})
	}
}

// The rendered text of an e-mail template is escaped; the same value through
// an SMS template is not.
func TestRenderEntry_EscapesForEmailOnly(t *testing.T) {
	const value = `<b>"Ada" & co</b>`
	vars := map[string]json.RawMessage{"name": json.RawMessage(`"<b>\"Ada\" & co</b>"`)}
	for channel, want := range map[domain.Channel]string{
		domain.ChannelEmail: "<p>&lt;b&gt;&#34;Ada&#34; &amp; co&lt;/b&gt;</p>",
		domain.ChannelSMS:   "<p>" + value + "</p>",
	} {
		f := newFakeTemplates()
		f.channel = channel
		f.versions["mail"] = []string{"<p>{{ .name }}</p>"}
		f.vars["mail"] = []string{"name"}
		e := newTemplateCache(f, cacheTimeout).load(context.Background(), "mail", 0)
		content, errs, p := renderEntry(e, vars, createRenderFields)
		if p != nil || len(errs) > 0 || content != want {
			t.Errorf("%s: %q %v %v, want %q", channel, content, errs, p, want)
		}
	}
}

// The members the validator accepts are exactly those of the template use
// and of the preview body in api/openapi.yaml, and both are strict.
func TestTemplateUse_MatchesSpec(t *testing.T) {
	doc := loadContract(t).doc
	for _, tc := range []struct {
		schema string
		want   []string
	}{
		{"TemplateUse", []string{memberName, memberVariables, memberVersion}},
		{"TemplatePreviewRequest", []string{memberVariables, memberVersion}},
	} {
		s := doc.Components.Schemas[tc.schema].Value
		var props []string
		for name := range s.Properties {
			props = append(props, name)
		}
		slices.Sort(props)
		if !slices.Equal(props, tc.want) {
			t.Errorf("%s: properties %v, validator %v", tc.schema, props, tc.want)
		}
		if s.AdditionalProperties.Has == nil || *s.AdditionalProperties.Has {
			t.Errorf("%s: additionalProperties must be false", tc.schema)
		}
		v := s.Properties[memberVersion].Value
		if v.Min == nil || *v.Min != 1 || v.Format != "int32" {
			t.Errorf("%s: version bounds min %v format %q", tc.schema, v.Min, v.Format)
		}
	}
	if req := doc.Components.Schemas["TemplateUse"].Value.Required; !slices.Equal(req, []string{memberName}) {
		t.Errorf("TemplateUse required = %v", req)
	}
}
