//go:build integration

package api

import (
	"net/http"
	"slices"
	"strings"
	"testing"
)

// tplBody is a createTemplate request.
func tplBody(name, channel, body string, vars ...string) string {
	return `{"name":"` + name + `","channel":"` + channel + `","body":"` + body + `","variables":` + nameList(vars) + `}`
}

// versionBody is a createTemplateVersion request.
func versionBody(body string, vars ...string) string {
	return `{"body":"` + body + `","variables":` + nameList(vars) + `}`
}

func nameList(names []string) string {
	if len(names) == 0 {
		return `[]`
	}
	return `["` + strings.Join(names, `","`) + `"]`
}

func wantStatus(t *testing.T, res response, status int) {
	t.Helper()
	if res.status != status {
		t.Fatalf("status %d, want %d: %s", res.status, status, res.body)
	}
}

func wantCodes(t *testing.T, res response, want ...string) {
	t.Helper()
	if got := res.problemCodes(t); !slices.Equal(got, want) {
		t.Fatalf("errors %v, want %v: %s", got, want, res.body)
	}
}

func TestTemplates_CreateAndRead(t *testing.T) {
	a := newTestAPI(t)

	res := a.do(http.MethodPost, "/v1/templates", tplBody("otp", "sms", "Your code is {{ .code }}", "code"), nil)
	wantStatus(t, res, http.StatusCreated)
	if loc := res.header.Get("Location"); loc != "/v1/templates/otp" {
		t.Fatalf("Location %q", loc)
	}
	obj := res.object(t)
	want := map[string]any{
		"name": "otp", "channel": "sms", "is_active": true, "latest_version": float64(1), "version": float64(1),
		"body": "Your code is {{ .code }}",
	}
	for k, v := range want {
		if obj[k] != v {
			t.Errorf("%s = %v, want %v", k, obj[k], v)
		}
	}
	if vars, ok := obj["variables"].([]any); !ok || len(vars) != 1 || vars[0] != "code" {
		t.Errorf("variables = %v", obj["variables"])
	}
	if opt, ok := obj["optional_variables"].([]any); !ok || len(opt) != 0 {
		t.Errorf("optional_variables = %#v, want an empty array", obj["optional_variables"])
	}
	for _, k := range []string{"created_at", "updated_at", "version_created_at"} {
		if s, _ := obj[k].(string); !strings.HasSuffix(s, "Z") {
			t.Errorf("%s = %v, want a UTC timestamp", k, obj[k])
		}
	}

	// The same name again.
	dup := a.do(http.MethodPost, "/v1/templates", tplBody("otp", "email", "other"), nil)
	wantStatus(t, dup, http.StatusConflict)
	wantCodes(t, dup, "name:template_exists")

	// Versions.
	v2 := a.do(http.MethodPost, "/v1/templates/otp/versions", versionBody("Code: {{ .code }}", "code"), nil)
	wantStatus(t, v2, http.StatusCreated)
	if loc := v2.header.Get("Location"); loc != "/v1/templates/otp/versions/2" {
		t.Fatalf("Location %q", loc)
	}
	if o := v2.object(t); o["version"] != float64(2) || o["latest_version"] != float64(2) || o["body"] != "Code: {{ .code }}" {
		t.Fatalf("version 2: %s", v2.body)
	}
	v3 := a.do(http.MethodPost, "/v1/templates/otp/versions",
		`{"body":"Hi {{ .first | default \"there\" }}","optional_variables":["first"]}`, nil)
	wantStatus(t, v3, http.StatusCreated)
	if o := v3.object(t); o["version"] != float64(3) || o["latest_version"] != float64(3) {
		t.Fatalf("version 3: %s", v3.body)
	}

	latest := a.get("/v1/templates/otp")
	wantStatus(t, latest, http.StatusOK)
	if o := latest.object(t); o["version"] != float64(3) || o["latest_version"] != float64(3) ||
		o["body"] != `Hi {{ .first | default "there" }}` {
		t.Fatalf("latest: %s", latest.body)
	}

	pinned := a.get("/v1/templates/otp/versions/1")
	wantStatus(t, pinned, http.StatusOK)
	o := pinned.object(t)
	if o["version"] != float64(1) || o["latest_version"] != float64(3) ||
		o["body"] != "Your code is {{ .code }}" || o["name"] != "otp" {
		t.Fatalf("pinned: %s", pinned.body)
	}
	if o["version_created_at"] == o["updated_at"] {
		t.Errorf("version_created_at should describe version 1, not the template: %s", pinned.body)
	}
}

func TestTemplates_NotFound(t *testing.T) {
	a := newTestAPI(t)
	wantStatus(t, a.do(http.MethodPost, "/v1/templates", tplBody("known", "sms", "hello"), nil), http.StatusCreated)

	for _, tc := range []struct{ name, method, path, body string }{
		{"get unknown name", http.MethodGet, "/v1/templates/nope", ""},
		{"get a name that cannot exist", http.MethodGet, "/v1/templates/Not_Valid", ""},
		{"version of an unknown name", http.MethodGet, "/v1/templates/nope/versions/1", ""},
		{"unknown version", http.MethodGet, "/v1/templates/known/versions/2", ""},
		{"largest version", http.MethodGet, "/v1/templates/known/versions/2147483647", ""},
		{"add a version to an unknown name", http.MethodPost, "/v1/templates/nope/versions", versionBody("x")},
		{"add a version to a name that cannot exist", http.MethodPost, "/v1/templates/-x/versions", versionBody("x")},
		{"deactivate unknown", http.MethodPost, "/v1/templates/nope/deactivate", ""},
		{"activate unknown", http.MethodPost, "/v1/templates/nope/activate", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wantStatus(t, a.do(tc.method, tc.path, tc.body, nil), http.StatusNotFound)
		})
	}
	// The shape of a body is checked before the template is looked up.
	wantStatus(t, a.do(http.MethodPost, "/v1/templates/nope/versions", `{}`, nil), http.StatusUnprocessableEntity)
}

func TestTemplates_VersionMustBePositiveInteger(t *testing.T) {
	a := newTestAPI(t)
	wantStatus(t, a.do(http.MethodPost, "/v1/templates", tplBody("known", "sms", "hello"), nil), http.StatusCreated)
	for _, v := range []string{"0", "-1", "abc", "1.5", "99999999999999999999"} {
		res := a.get("/v1/templates/known/versions/" + v)
		wantStatus(t, res, http.StatusBadRequest)
		wantCodes(t, res, "version:invalid_parameter")
	}
}

func TestTemplates_ActivateDeactivate(t *testing.T) {
	a := newTestAPI(t)
	wantStatus(t, a.do(http.MethodPost, "/v1/templates", tplBody("toggle", "sms", "hello"), nil), http.StatusCreated)

	active := func(res response) bool {
		t.Helper()
		wantStatus(t, res, http.StatusOK)
		return res.object(t)["is_active"].(bool)
	}
	if active(a.do(http.MethodPost, "/v1/templates/toggle/deactivate", "", nil)) {
		t.Fatal("deactivate: still active")
	}
	if active(a.do(http.MethodPost, "/v1/templates/toggle/deactivate", "", nil)) {
		t.Fatal("deactivate twice: active")
	}
	if active(a.get("/v1/templates/toggle")) {
		t.Fatal("get after deactivate: active")
	}
	// An inactive template can receive a version.
	wantStatus(t, a.do(http.MethodPost, "/v1/templates/toggle/versions", versionBody("bye"), nil), http.StatusCreated)
	if !active(a.do(http.MethodPost, "/v1/templates/toggle/activate", "", nil)) {
		t.Fatal("activate: not active")
	}

	// Neither takes a body.
	for _, op := range []string{"deactivate", "activate"} {
		res := a.do(http.MethodPost, "/v1/templates/toggle/"+op, `{}`, nil)
		wantStatus(t, res, http.StatusBadRequest)
		wantCodes(t, res, ":unexpected_body")
	}
}

func TestTemplates_Rejections(t *testing.T) {
	a := newTestAPI(t)
	for _, tc := range []struct {
		name string
		body string
		want string
	}{
		{"builtin function", tplBody("t1", "sms", `{{ printf \"%d\" 1 }}`), "body:function_not_allowed"},
		{"two templates in one source", tplBody("t2", "sms", `a{{ define \"b\" }}x{{ end }}`), "body:multiple_templates"},
		{"unused variable", tplBody("t3", "sms", "hello", "code"), "variables:unused_variable"},
		{"undeclared variable", tplBody("t4", "sms", "{{ .code }}"), "body:undeclared_variable"},
		{"e-mail ending inside a tag", tplBody("t5", "email", `<a href=\"{{ .u }}`, "u"), "body:unsafe_html"},
		{"unknown field", `{"name":"t6","channel":"sms","body":"x","subject":"y"}`, "subject:unknown_field"},
		{"repeated field", `{"name":"t7","channel":"sms","body":"x","body":"y"}`, "body:duplicate_field"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := a.do(http.MethodPost, "/v1/templates", tc.body, nil)
			wantStatus(t, res, http.StatusUnprocessableEntity)
			wantCodes(t, res, tc.want)
		})
	}
	// Nothing of a rejected request is stored.
	wantStatus(t, a.get("/v1/templates/t1"), http.StatusNotFound)

	// A version is compiled for the channel of its template.
	wantStatus(t, a.do(http.MethodPost, "/v1/templates", tplBody("mail", "email", "<p>{{ .u }}</p>", "u"), nil), http.StatusCreated)
	res := a.do(http.MethodPost, "/v1/templates/mail/versions", versionBody(`<a href=\"{{ .u }}`, "u"), nil)
	wantStatus(t, res, http.StatusUnprocessableEntity)
	wantCodes(t, res, "body:unsafe_html")
	res = a.do(http.MethodPost, "/v1/templates/mail/versions", versionBody(`{{ printf \"x\" }}`), nil)
	wantStatus(t, res, http.StatusUnprocessableEntity)
	wantCodes(t, res, "body:function_not_allowed")
}
