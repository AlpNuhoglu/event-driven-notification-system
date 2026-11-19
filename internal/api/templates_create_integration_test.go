//go:build integration

package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

// mkTemplate creates a template through the API.
func (a *testAPI) mkTemplate(t *testing.T, name, channel, body string, vars, optional []string) {
	t.Helper()
	b, err := json.Marshal(map[string]any{
		"name": name, "channel": channel, "body": body,
		"variables": nonNilList(vars), "optional_variables": nonNilList(optional),
	})
	if err != nil {
		t.Fatal(err)
	}
	wantStatus(t, a.do(http.MethodPost, "/v1/templates", string(b), nil), http.StatusCreated)
}

// mkVersion adds a version through the API.
func (a *testAPI) mkVersion(t *testing.T, name, body string, vars []string) {
	t.Helper()
	b, err := json.Marshal(map[string]any{"body": body, "variables": nonNilList(vars)})
	if err != nil {
		t.Fatal(err)
	}
	wantStatus(t, a.do(http.MethodPost, "/v1/templates/"+name+"/versions", string(b), nil), http.StatusCreated)
}

// fromTemplate is a create request of channel that uses template (its JSON).
func fromTemplate(channel, recipient, use string) string {
	return `{"channel":"` + channel + `","recipient":"` + recipient + `","template":` + use + `}`
}

// tplRow is what the template tests read back from a notification row.
type tplRow struct {
	content  string
	name     *string
	version  *int32
	vars     map[string]any // decoded with UseNumber
	encoding *string
	segments *int16
}

func (a *testAPI) tplRow(t *testing.T, id string) tplRow {
	t.Helper()
	var r tplRow
	var vars []byte
	err := a.pool.QueryRow(ctxT(t), `
		SELECT content, template_name, template_version, template_vars, sms_encoding, sms_segments
		FROM notifications WHERE id = $1`, id).Scan(&r.content, &r.name, &r.version, &vars, &r.encoding, &r.segments)
	if err != nil {
		t.Fatalf("read row: %v", err)
	}
	if vars != nil {
		dec := json.NewDecoder(bytes.NewReader(vars))
		dec.UseNumber()
		if err := dec.Decode(&r.vars); err != nil {
			t.Fatalf("decode template_vars %s: %v", vars, err)
		}
	}
	return r
}

// provenance returns the template member of a notification body.
func provenance(t *testing.T, body []byte) map[string]any {
	t.Helper()
	m, _ := (response{body: body}).object(t)["template"].(map[string]any)
	return m
}

func TestTemplateCreate_Single(t *testing.T) {
	a := newTestAPI(t)
	a.mkTemplate(t, "otp", "sms", "Kodunuz {{ .code }}{{ if .ref }} ref {{ .ref }}{{ end }}, {{ .name }}", []string{"code", "name"}, []string{"ref"})

	res := a.post(fromTemplate("sms", phone,
		`{"name":"otp","variables":{"code":482913,"ref":12345678901234567890123,"name":"Şule"}}`))
	wantStatus(t, res, http.StatusAccepted)
	if p := provenance(t, res.body); p["name"] != "otp" || p["version"] != float64(1) {
		t.Errorf("template = %v", p)
	}
	if _, ok := res.object(t)["content"]; ok {
		t.Error("content echoed")
	}
	id := res.object(t)["id"].(string)
	want := "Kodunuz 482913 ref 12345678901234567890123, Şule"
	r := a.tplRow(t, id)
	if r.content != want || r.name == nil || *r.name != "otp" || r.version == nil || *r.version != 1 {
		t.Fatalf("row %+v", r)
	}
	// The numbers are stored as sent, not as float64.
	if r.vars["code"] != json.Number("482913") || r.vars["ref"] != json.Number("12345678901234567890123") || r.vars["name"] != "Şule" || len(r.vars) != 3 {
		t.Errorf("template_vars = %v", r.vars)
	}
	// The SMS encoding and segments are those of the rendered text: Ş puts
	// it into UCS-2.
	if r.encoding == nil || *r.encoding != "UCS-2" || r.segments == nil || *r.segments != 1 {
		t.Errorf("sms %v %v", r.encoding, r.segments)
	}
	sms, _ := res.object(t)["sms"].(map[string]any)
	if sms["encoding"] != "UCS-2" || sms["segments"] != float64(1) {
		t.Errorf("sms = %v", sms)
	}
	// The read shows the provenance; a notification with content has none.
	if p := provenance(t, a.get("/v1/notifications/"+id).body); p["name"] != "otp" || p["version"] != float64(1) {
		t.Errorf("get: template = %v", p)
	}
	plain := a.post(validBody)
	if rawField(t, plain.body, "template") != "null" {
		t.Errorf("content create: template = %s", rawField(t, plain.body, "template"))
	}

	// No variables at all is stored as {}.
	a.mkTemplate(t, "fixed", "push", "Hello", nil, nil)
	res = a.post(fromTemplate("push", "device-token-1", `{"name":"fixed"}`))
	wantStatus(t, res, http.StatusAccepted)
	if r := a.tplRow(t, res.object(t)["id"].(string)); r.content != "Hello" || r.vars == nil || len(r.vars) != 0 || r.encoding != nil {
		t.Errorf("row %+v", r)
	}
	// A template null is a create with content.
	res = a.post(`{"channel":"sms","recipient":"` + phone + `","content":"x","template":null}`)
	wantStatus(t, res, http.StatusAccepted)
	if r := a.tplRow(t, res.object(t)["id"].(string)); r.name != nil || r.vars != nil {
		t.Errorf("row %+v", r)
	}
}

// An e-mail template escapes a variable that holds markup; the same value
// through an SMS template is stored as it is.
func TestTemplateCreate_EmailEscapes(t *testing.T) {
	a := newTestAPI(t)
	a.mkTemplate(t, "mail", "email", "<p>Hello {{ .name }}</p>", []string{"name"}, nil)
	a.mkTemplate(t, "text", "sms", "Hello {{ .name }}", []string{"name"}, nil)
	vars := `{"name":"<b>Ada</b> & \"co\""}`
	m := a.post(fromTemplate("email", email, `{"name":"mail","variables":`+vars+`}`))
	wantStatus(t, m, http.StatusAccepted)
	s := a.post(fromTemplate("sms", phone, `{"name":"text","variables":`+vars+`}`))
	wantStatus(t, s, http.StatusAccepted)
	if got := a.tplRow(t, m.object(t)["id"].(string)).content; got != "<p>Hello &lt;b&gt;Ada&lt;/b&gt; &amp; &#34;co&#34;</p>" {
		t.Errorf("e-mail content %q", got)
	}
	if got := a.tplRow(t, s.object(t)["id"].(string)).content; got != `Hello <b>Ada</b> & "co"` {
		t.Errorf("sms content %q", got)
	}
}

// Every error of a create from a template; nothing is stored.
func TestTemplateCreate_Errors(t *testing.T) {
	a := newTestAPI(t)
	a.mkTemplate(t, "otp", "sms", "Code {{ .code }}", []string{"code"}, nil)
	a.mkTemplate(t, "opt", "sms", "{{ .v }}", nil, []string{"v"})
	a.mkTemplate(t, "big", "sms", strings.Repeat("a", 1600)+"{{ .v }}", []string{"v"}, nil)
	// 120 times a value of 1,024 characters is over the e-mail limit; 600
	// times is over the engine's output limit (512 KiB).
	a.mkTemplate(t, "wide", "email", strings.Repeat("{{.v}}", 120), []string{"v"}, nil)
	a.mkTemplate(t, "huge", "email", strings.Repeat("{{.v}}", 600), []string{"v"}, nil)
	a.mkTemplate(t, "huge-sms", "sms", strings.Repeat("{{.v}}", 600), []string{"v"}, nil)
	kb := strings.Repeat("b", 1024)
	a.mkTemplate(t, "off", "sms", "x", nil, nil)
	wantStatus(t, a.do(http.MethodPost, "/v1/templates/off/deactivate", "", nil), http.StatusOK)
	sms := func(use string) string { return fromTemplate("sms", phone, use) }
	for _, tt := range []struct {
		name, body string
		codes      []string
	}{
		{"unknown template", sms(`{"name":"nope"}`), []string{"template.name:template_not_found"}},
		{"unknown version", sms(`{"name":"otp","version":2,"variables":{"code":1}}`), []string{"template.version:template_version_not_found"}},
		{"inactive", sms(`{"name":"off"}`), []string{"template.name:template_inactive"}},
		{"inactive, pinned", sms(`{"name":"off","version":1}`), []string{"template.name:template_inactive"}},
		{"another channel", fromTemplate("email", email, `{"name":"otp","variables":{"code":1}}`), []string{"template.name:template_channel_mismatch"}},
		{"missing variable", sms(`{"name":"otp"}`), []string{"template.variables.code:missing_variable"}},
		{"unknown variable", sms(`{"name":"otp","variables":{"code":1,"cdoe":2}}`), []string{"template.variables.cdoe:unknown_variable"}},
		{"value of a wrong type", sms(`{"name":"otp","variables":{"code":[1]}}`), []string{"template.variables.code:invalid_type"}},
		{"value too long", sms(`{"name":"otp","variables":{"code":"` + strings.Repeat("9", 1025) + `"}}`), []string{"template.variables.code:too_long"}},
		{"value with U+0000", sms(`{"name":"otp","variables":{"code":"1\u0000"}}`), []string{"template.variables.code:invalid_character"}},
		{"renders to nothing", sms(`{"name":"opt"}`), []string{"template:empty"}},
		{"renders over the SMS limit", sms(`{"name":"big","variables":{"v":"x"}}`), []string{"template:too_many_segments"}},
		{"renders over the e-mail limit", fromTemplate("email", email, `{"name":"wide","variables":{"v":"`+kb+`"}}`), []string{"template:too_long"}},
		{"renders over the output limit, e-mail", fromTemplate("email", email, `{"name":"huge","variables":{"v":"`+kb+`"}}`), []string{"template:too_long"}},
		{"renders over the output limit, SMS", sms(`{"name":"huge-sms","variables":{"v":"` + kb + `"}}`), []string{"template:too_many_segments"}},
		{"content and template", `{"channel":"sms","recipient":"` + phone + `","content":"x","template":{"name":"otp"}}`, []string{"template:conflicts_with_content"}},
		{"neither", `{"channel":"sms","recipient":"` + phone + `"}`, []string{"content:required"}},
		{"unknown member", sms(`{"name":"otp","vars":{}}`), []string{"template.vars:unknown_field"}},
		{"version not an integer", sms(`{"name":"otp","version":"1"}`), []string{"template.version:invalid_type"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			res := a.do(http.MethodPost, pathCreate, tt.body, nil)
			wantStatus(t, res, http.StatusUnprocessableEntity)
			wantCodes(t, res, tt.codes...)
		})
	}
	if n := a.count(t); n != 0 {
		t.Errorf("%d rows stored", n)
	}
}

// Inactive is refused even with a pinned version, and the template can be
// used again after activate. A pinned old version renders the old body after
// a new version exists.
func TestTemplateCreate_ActivationAndVersions(t *testing.T) {
	a := newTestAPI(t)
	a.mkTemplate(t, "otp", "sms", "Old {{ .code }}", []string{"code"}, nil)
	a.mkVersion(t, "otp", "New {{ .code }}", []string{"code"})

	create := func(use string) response { return a.post(fromTemplate("sms", phone, use)) }
	latest := create(`{"name":"otp","variables":{"code":1}}`)
	wantStatus(t, latest, http.StatusAccepted)
	pinned := create(`{"name":"otp","version":1,"variables":{"code":1}}`)
	wantStatus(t, pinned, http.StatusAccepted)
	if p := provenance(t, latest.body); p["version"] != float64(2) {
		t.Errorf("latest: %v", p)
	}
	if p := provenance(t, pinned.body); p["version"] != float64(1) {
		t.Errorf("pinned: %v", p)
	}
	if c := a.tplRow(t, latest.object(t)["id"].(string)).content; c != "New 1" {
		t.Errorf("latest content %q", c)
	}
	if c := a.tplRow(t, pinned.object(t)["id"].(string)).content; c != "Old 1" {
		t.Errorf("pinned content %q", c)
	}

	wantStatus(t, a.do(http.MethodPost, "/v1/templates/otp/deactivate", "", nil), http.StatusOK)
	for _, use := range []string{`{"name":"otp","variables":{"code":1}}`, `{"name":"otp","version":1,"variables":{"code":1}}`} {
		res := create(use)
		wantStatus(t, res, http.StatusUnprocessableEntity)
		wantCodes(t, res, "template.name:template_inactive")
	}
	// Existing notifications are not affected.
	if c := a.tplRow(t, pinned.object(t)["id"].(string)).content; c != "Old 1" {
		t.Errorf("content after deactivation %q", c)
	}
	wantStatus(t, a.do(http.MethodPost, "/v1/templates/otp/activate", "", nil), http.StatusOK)
	wantStatus(t, create(`{"name":"otp","version":1,"variables":{"code":1}}`), http.StatusAccepted)
}

// A batch: an item whose template fails is rejected with its errors and the
// others are stored; the item that fails does not claim its dedupe_key.
func TestTemplateCreate_Batch(t *testing.T) {
	a := newTestAPIOn(t, testutil.NewPool(t, store.OpenPool), &syncBuffer{}, func(o *Options) { o.MaxBodyBytes = itBigBody })
	a.mkTemplate(t, "otp", "sms", "Code {{ .code }}", []string{"code"}, nil)
	a.mkTemplate(t, "mail", "email", "<p>{{ .name }}</p>", []string{"name"}, nil)
	withKey := func(item, key string) string { return strings.TrimSuffix(item, "}") + `,"dedupe_key":"` + key + `"}` }
	res := a.do(http.MethodPost, pathBatch, batchBody(
		withKey(fromTemplate("sms", phone, `{"name":"otp","variables":{"code":482913}}`), "k0"), // 0
		withKey(fromTemplate("sms", phone, `{"name":"otp"}`), "k1"),                             // 1: misses code
		withKey(item(""), "k1"), // 2: k1 is free
		fromTemplate("email", email, `{"name":"mail","variables":{"name":"Ada"}}`), // 3
		fromTemplate("sms", phone, `{"name":"nope"}`),                              // 4
		faxItem,                 // 5
		withKey(item(""), "k0"), // 6: duplicate of 0
	), nil)
	wantStatus(t, res, http.StatusAccepted)
	d := decodeBatch(t, res)
	if got := d.acceptedIndexes(); !slices.Equal(got, []int{0, 2, 3}) {
		t.Errorf("accepted %v", got)
	}
	want := []string{
		"1:template.variables.code:missing_variable",
		"4:template.name:template_not_found",
		"5:channel:invalid_value",
		"6:dedupe_key:duplicate_in_batch",
	}
	if got := d.rejectedCodes(); !slices.Equal(got, want) {
		t.Errorf("rejected %v, want %v", got, want)
	}
	byIndex := map[int]string{}
	for _, acc := range d.Accepted {
		byIndex[acc.Index] = acc.ID
	}
	r0 := a.tplRow(t, byIndex[0])
	if r0.content != "Code 482913" || *r0.name != "otp" || *r0.version != 1 || r0.vars["code"] != json.Number("482913") ||
		*r0.encoding != "GSM-7" || *r0.segments != 1 {
		t.Errorf("item 0: %+v", r0)
	}
	if r3 := a.tplRow(t, byIndex[3]); r3.content != "<p>Ada</p>" || *r3.name != "mail" || r3.encoding != nil {
		t.Errorf("item 3: %+v", r3)
	}
	if r2 := a.tplRow(t, byIndex[2]); r2.name != nil || r2.content != "hello" {
		t.Errorf("item 2: %+v", r2)
	}
	var total, accepted, rejected int
	if err := a.pool.QueryRow(ctxT(t), `SELECT total, accepted, rejected FROM batches WHERE id = $1`, d.BatchID).
		Scan(&total, &accepted, &rejected); err != nil {
		t.Fatal(err)
	}
	if total != 7 || accepted != 3 || rejected != 4 {
		t.Errorf("batch counts %d %d %d", total, accepted, rejected)
	}

	// Every item rejected by the template stage: 422 with rejected, nothing
	// stored.
	before := a.counts(t)
	res = a.do(http.MethodPost, pathBatch, batchBody(fromTemplate("sms", phone, `{"name":"otp"}`), fromTemplate("sms", phone, `{"name":"nope"}`)), nil)
	wantStatus(t, res, http.StatusUnprocessableEntity)
	if got := decodeBatch(t, res).rejectedCodes(); !slices.Equal(got, []string{"0:template.variables.code:missing_variable", "1:template.name:template_not_found"}) {
		t.Errorf("rejected %v", got)
	}
	if after := a.counts(t); after != before {
		t.Errorf("stored something: %v -> %v", before, after)
	}
}

// A keyed create from a template is replayed after the template was
// deactivated, single and batch; the same key with another body is a 422
// of a reused key. Without the key, the deactivation is a 422.
func TestTemplateCreate_ReplayAfterDeactivation(t *testing.T) {
	a := newTestAPI(t)
	a.mkTemplate(t, "otp", "sms", "Code {{ .code }}", []string{"code"}, nil)
	single := fromTemplate("sms", phone, `{"name":"otp","variables":{"code":482913}}`)
	for _, tc := range []struct{ name, path, body, location string }{
		{"single", pathCreate, single, "/v1/notifications/"},
		{"batch", pathBatch, batchBody(single, faxItem), "/v1/batches/"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wantStatus(t, a.do(http.MethodPost, "/v1/templates/otp/activate", "", nil), http.StatusOK)
			key := "tpl-replay-" + tc.name
			first := a.postKey(tc.path, tc.body, key, "")
			wantStatus(t, first, http.StatusAccepted)
			wantStatus(t, a.do(http.MethodPost, "/v1/templates/otp/deactivate", "", nil), http.StatusOK)
			before := a.counts(t)

			again := a.postKey(tc.path, tc.body, key, "")
			wantStatus(t, again, http.StatusAccepted)
			if again.header.Get("Idempotent-Replayed") != "true" || !jsonEqual(t, again.body, first.body) {
				t.Fatalf("replay %v: %s, want %s", again.header, again.body, first.body)
			}
			if loc := again.header.Get("Location"); loc != first.header.Get("Location") || !strings.HasPrefix(loc, tc.location) {
				t.Errorf("Location %q", loc)
			}
			reused := a.postKey(tc.path, strings.Replace(tc.body, "482913", "482914", 1), key, "")
			wantStatus(t, reused, http.StatusUnprocessableEntity)
			wantCodes(t, reused, "Idempotency-Key:idempotency_key_reused")
			if !strings.HasSuffix(reused.object(t)["type"].(string), "#idempotency-key-reused") {
				t.Errorf("type %v", reused.object(t)["type"])
			}
			// Without the key the deactivation applies.
			plain := a.do(http.MethodPost, tc.path, tc.body, nil)
			wantStatus(t, plain, http.StatusUnprocessableEntity)
			if after := a.counts(t); after != before {
				t.Errorf("stored something: %v -> %v", before, after)
			}
		})
	}
}

func TestTemplatePreview(t *testing.T) {
	logs := &syncBuffer{}
	a := newTestAPIOn(t, testutil.NewPool(t, store.OpenPool), logs)
	a.mkTemplate(t, "otp", "sms", "Kodunuz {{ .code }}, {{ .name }}", []string{"code", "name"}, nil)
	a.mkVersion(t, "otp", "Code {{ .code }} {{ .name }}", []string{"code", "name"})
	a.mkTemplate(t, "mail", "email", "<p>Hello {{ .name | default \"Customer\" }}</p>", nil, []string{"name"})
	a.mkTemplate(t, "opt", "push", "{{ .v }}", nil, []string{"v"})
	a.mkTemplate(t, "big", "sms", strings.Repeat("a", 1600)+"{{ .v }}", []string{"v"}, nil)
	const canary = "canary-value-5d1a"

	preview := func(name, body string) response {
		return a.do(http.MethodPost, "/v1/templates/"+name+"/preview", body, nil)
	}
	// SMS: the latest version, with its encoding and segments.
	res := preview("otp", `{"variables":{"code":482913,"name":"`+canary+`"}}`)
	wantStatus(t, res, http.StatusOK)
	if o := res.object(t); o["content"] != "Code 482913 "+canary || o["encoding"] != "GSM-7" || o["segments"] != float64(1) {
		t.Errorf("latest: %s", res.body)
	}
	// A pinned version; UCS-2 for its Turkish text.
	res = preview("otp", `{"version":1,"variables":{"code":1,"name":"Şule"}}`)
	wantStatus(t, res, http.StatusOK)
	if o := res.object(t); o["content"] != "Kodunuz 1, Şule" || o["encoding"] != "UCS-2" || o["segments"] != float64(1) {
		t.Errorf("pinned: %s", res.body)
	}
	// E-mail: escaped, no SMS fields; an optional variable left out.
	res = preview("mail", `{"variables":{"name":"<i>Ada</i>"}}`)
	wantStatus(t, res, http.StatusOK)
	if o := res.object(t); o["content"] != "<p>Hello &lt;i&gt;Ada&lt;/i&gt;</p>" || o["encoding"] != nil || o["segments"] != nil {
		t.Errorf("mail: %s", res.body)
	}
	// An empty body, without a Content-Type, is a preview without variables.
	res = a.do(http.MethodPost, "/v1/templates/mail/preview", "", nil)
	wantStatus(t, res, http.StatusOK)
	if o := res.object(t); o["content"] != "<p>Hello Customer</p>" {
		t.Errorf("empty body: %s", res.body)
	}
	// An inactive template can be previewed.
	wantStatus(t, a.do(http.MethodPost, "/v1/templates/mail/deactivate", "", nil), http.StatusOK)
	wantStatus(t, preview("mail", `{}`), http.StatusOK)

	for _, tt := range []struct {
		name, tpl, body string
		status          int
		codes           []string
	}{
		{"unknown name", "nope", `{}`, 404, nil},
		{"name that cannot exist", "No_pe", `{}`, 404, nil},
		{"unknown version", "otp", `{"version":3}`, 404, nil},
		{"missing variable", "otp", `{"variables":{"code":1}}`, 422, []string{"variables.name:missing_variable"}},
		{"unknown variable", "otp", `{"variables":{"code":1,"name":"x","nmae":"` + canary + `"}}`, 422, []string{"variables.nmae:unknown_variable"}},
		{"value of a wrong type", "otp", `{"variables":{"code":{},"name":"x"}}`, 422, []string{"variables.code:invalid_type"}},
		{"renders to nothing", "opt", `{}`, 422, []string{"content:empty"}},
		{"renders over the SMS limit", "big", `{"variables":{"v":"x"}}`, 422, []string{"content:too_many_segments"}},
		{"version not an integer", "otp", `{"version":"1"}`, 422, []string{"version:invalid_type"}},
		{"unknown member", "otp", `{"name":"otp"}`, 422, []string{"name:unknown_field"}},
		{"a variable repeated", "otp", `{"variables":{"code":1,"code":2}}`, 422, []string{"variables.code:duplicate_field"}},
		{"not an object", "otp", `[]`, 400, []string{":not_an_object"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			res := preview(tt.tpl, tt.body)
			wantStatus(t, res, tt.status)
			if tt.codes != nil {
				wantCodes(t, res, tt.codes...)
			}
		})
	}
	wantStatus(t, a.do(http.MethodPost, "/v1/templates/otp/preview", `{}`, textPlain), http.StatusUnsupportedMediaType)
	if n := a.count(t); n != 0 {
		t.Errorf("a preview stored %d notifications", n)
	}
	if strings.Contains(logs.String(), canary) || strings.Contains(logs.String(), "482913") {
		t.Errorf("a log line contains a variable value:\n%s", logs)
	}
}

// Variable values and rendered content reach no log line and no error
// answer.
func TestTemplateCreate_NoValuesInLogsOrErrors(t *testing.T) {
	logs := &syncBuffer{}
	a := newTestAPIOn(t, testutil.NewPool(t, store.OpenPool), logs)
	a.mkTemplate(t, "otp", "sms", "Code {{ .code }}", []string{"code"}, nil)
	const canary = "canary-7c2e91"
	var bodies []string
	for _, use := range []string{
		`{"name":"otp","variables":{"code":"` + canary + `"}}`,                         // 202
		`{"name":"otp","variables":{"code":"` + canary + `","x":"` + canary + `"}}`,    // unknown
		`{"name":"otp","variables":{"code":"` + strings.Repeat(canary, 200) + `"}}`,    // too long
		`{"name":"otp","variables":{"code":["` + canary + `"]}}`,                       // wrong type
		`{"name":"nope","variables":{"code":"` + canary + `"}}`,                        // unknown template
		`{"name":"otp","variables":{"code":"` + canary + `","code":"` + canary + `"}}`, // repeated
	} {
		res := a.post(fromTemplate("sms", phone, use))
		if res.status >= 400 {
			bodies = append(bodies, string(res.body))
		}
	}
	if len(bodies) != 5 {
		t.Fatalf("%d error answers, want 5", len(bodies))
	}
	for _, b := range bodies {
		if strings.Contains(b, canary) {
			t.Errorf("error answer contains a value: %s", b)
		}
	}
	if strings.Contains(logs.String(), canary) {
		t.Errorf("a log line contains a value:\n%s", logs)
	}
}
