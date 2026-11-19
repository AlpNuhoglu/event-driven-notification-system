package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

// bodyMarker is text of a submitted body that no message may contain.
const bodyMarker = "zzqx-private-text"

func jsonObject(t *testing.T, m map[string]any) string {
	t.Helper()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func manyNames(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("v%d", i)
	}
	return out
}

// templateCase is one request body and the answer expected: status 0 is a
// valid body; codes lists field:code in order.
type templateCase struct {
	name   string
	body   string
	status int
	codes  []string
}

func checkTemplateCase(t *testing.T, tc templateCase, p *problem) {
	t.Helper()
	switch {
	case tc.status == 0 && p != nil:
		t.Fatalf("want a valid body, got %d %v", p.kind.status, codes(p))
	case tc.status != 0 && p == nil:
		t.Fatalf("want status %d, got a valid body", tc.status)
	case p == nil:
		return
	}
	if p.kind.status != tc.status {
		t.Fatalf("status %d, want %d (%v)", p.kind.status, tc.status, codes(p))
	}
	if got := codes(p); !slices.Equal(got, tc.codes) {
		t.Fatalf("errors %v, want %v", got, tc.codes)
	}
	for _, e := range p.errors {
		if strings.Contains(e.Message, bodyMarker) || strings.Contains(p.detail, bodyMarker) {
			t.Fatalf("a message contains the submitted body: %q", e.Message)
		}
	}
}

func TestValidateCreateTemplate(t *testing.T) {
	obj := func(m map[string]any) string { return jsonObject(t, m) }
	ok := func(over map[string]any) string {
		m := map[string]any{"name": "otp", "channel": "sms", "body": "Your code is {{ .code }}", "variables": []string{"code"}}
		for k, v := range over {
			if v == nil {
				delete(m, k)
				continue
			}
			m[k] = v
		}
		return obj(m)
	}
	marked := bodyMarker + " "
	tests := []templateCase{
		{"valid", ok(nil), 0, nil},
		{"valid without lists", ok(map[string]any{"body": "hello", "variables": nil}), 0, nil},
		{"valid with optional variables", ok(map[string]any{
			"body": `{{ .first | default "Customer" }}`, "variables": []string{}, "optional_variables": []string{"first"},
		}), 0, nil},
		{"name of 64 characters", ok(map[string]any{"name": strings.Repeat("a", 64)}), 0, nil},
		{"valid email body", ok(map[string]any{"channel": "email", "body": "<p>Hi {{ .code }}</p>"}), 0, nil},
		{"body of the largest size", ok(map[string]any{
			"body": strings.Repeat("a", domain.MaxTemplateBodyBytes-len("{{ .code }}")) + "{{ .code }}",
		}), 0, nil},

		{"empty body", ``, 400, []string{":empty_body"}},
		{"malformed", `{"name":`, 400, []string{":malformed_json"}},
		{"array", `[]`, 400, []string{":not_an_object"}},
		{"null", `null`, 400, []string{":not_an_object"}},

		{"missing everything", `{}`, 422, []string{"name:required", "channel:required", "body:required"}},
		{"name not a string", ok(map[string]any{"name": 5}), 422, []string{"name:invalid_type"}},
		{"name null", `{"name":null,"channel":"sms","body":"x"}`, 422, []string{"name:invalid_type"}},
		{"name empty", ok(map[string]any{"name": ""}), 422, []string{"name:invalid_value"}},
		{"name in capitals", ok(map[string]any{"name": "Otp"}), 422, []string{"name:invalid_value"}},
		{"name starts with a dash", ok(map[string]any{"name": "-otp"}), 422, []string{"name:invalid_value"}},
		{"name of 65 characters", ok(map[string]any{"name": strings.Repeat("a", 65)}), 422, []string{"name:invalid_value"}},
		{"name with a slash", ok(map[string]any{"name": "a/b"}), 422, []string{"name:invalid_value"}},
		{"channel unknown", ok(map[string]any{"channel": "fax"}), 422, []string{"channel:invalid_value"}},
		{"channel not a string", ok(map[string]any{"channel": 1}), 422, []string{"channel:invalid_type"}},
		{"body not a string", ok(map[string]any{"body": 1}), 422, []string{"body:invalid_type"}},
		{"body empty", ok(map[string]any{"body": ""}), 422, []string{"body:empty"}},
		{"body with NUL", ok(map[string]any{"body": "a\x00b"}), 422, []string{"body:invalid_character"}},
		{"body too large", ok(map[string]any{"body": strings.Repeat("a", domain.MaxTemplateBodyBytes+1)}), 422, []string{"body:too_long"}},
		{"variables not an array", ok(map[string]any{"variables": "code"}), 422, []string{"variables:invalid_type"}},
		{"variables null", `{"name":"a","channel":"sms","body":"x","variables":null}`, 422, []string{"variables:invalid_type"}},
		{"variables with a number", ok(map[string]any{"variables": []any{"code", 1}}), 422, []string{"variables:invalid_type"}},
		{"optional_variables not an array", ok(map[string]any{"optional_variables": map[string]any{}}), 422, []string{"optional_variables:invalid_type"}},
		{"optional_variables null", `{"name":"a","channel":"sms","body":"x","optional_variables":null}`, 422, []string{"optional_variables:invalid_type"}},
		{"optional_variables with null", `{"name":"a","channel":"sms","body":"x","optional_variables":[null]}`, 422, []string{"optional_variables:invalid_type"}},
		{"every shape error at once", `{"name":1,"channel":2,"body":3,"variables":4,"optional_variables":5}`, 422, []string{
			"name:invalid_type", "channel:invalid_type", "body:invalid_type", "variables:invalid_type", "optional_variables:invalid_type",
		}},

		{"unknown field", ok(map[string]any{"subject": "x"}), 422, []string{"subject:unknown_field"}},
		{"unknown field version", ok(map[string]any{"version": 2}), 422, []string{"version:unknown_field"}},
		{"duplicate name", `{"name":"a","name":"b","channel":"sms","body":"x"}`, 422, []string{"name:duplicate_field"}},
		{"duplicate body", `{"name":"a","channel":"sms","body":"x","body":"y"}`, 422, []string{"body:duplicate_field"}},
		{"duplicate variables", `{"name":"a","channel":"sms","body":"x","variables":[],"variables":[]}`, 422, []string{"variables:duplicate_field"}},
		{"shape errors stop the compile", `{"name":"a","channel":"sms","body":"{{ range .a }}","variables":5}`, 422, []string{"variables:invalid_type"}},
		{"unknown field stops the compile", `{"name":"a","channel":"sms","body":"{{ range .a }}","x":1}`, 422, []string{"x:unknown_field"}},

		// One case per TemplateProblem (template_too_large is the body size
		// above).
		{"syntax", ok(map[string]any{"body": marked + "{{ .code "}), 422, []string{"body:template_syntax"}},
		{"construct not allowed", ok(map[string]any{"body": marked + "{{ range .code }}x{{ end }}"}), 422, []string{"body:construct_not_allowed"}},
		{"function not allowed", ok(map[string]any{"body": marked + `{{ printf "x" }}`}), 422, []string{"body:function_not_allowed"}},
		{"multiple templates", ok(map[string]any{"body": marked + `{{ define "b" }}x{{ end }}{{ .code }}`}), 422, []string{"body:multiple_templates"}},
		{"invalid call", ok(map[string]any{"body": marked + `{{ .code | truncate }}`}), 422, []string{"body:invalid_call"}},
		{"undeclared variable", ok(map[string]any{"body": marked + "{{ .code }}{{ .other }}"}), 422, []string{"body:undeclared_variable"}},
		{"unsafe html", ok(map[string]any{"channel": "email", "body": marked + `<a href="{{ .code }}`}), 422, []string{"body:unsafe_html"}},
		{"unused variable", ok(map[string]any{"variables": []string{"code", "extra"}}), 422, []string{"variables:unused_variable"}},
		{"invalid variable name", ok(map[string]any{"body": "x", "variables": []string{"1a"}}), 422, []string{"variables:invalid_variable_name"}},
		{"duplicate variable", ok(map[string]any{"variables": []string{"code", "code"}}), 422, []string{"variables:duplicate_variable"}},
		{"duplicate across the lists", ok(map[string]any{"optional_variables": []string{"code"}}), 422, []string{"variables:duplicate_variable"}},
		{"too many variables", ok(map[string]any{"variables": manyNames(domain.MaxTemplateVariables + 1)}), 422, []string{"variables:too_many_variables"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req, p := validateCreateTemplate([]byte(tc.body))
			checkTemplateCase(t, tc, p)
			if p == nil && (req.Variables == nil || req.OptionalVariables == nil) {
				t.Fatal("a valid request must carry both lists")
			}
		})
	}
}

func TestValidateCreateTemplate_ParsedRequest(t *testing.T) {
	req, p := validateCreateTemplate([]byte(
		`{"name":"otp","channel":"email","body":"<p>{{ .a }}{{ .b }}</p>","variables":["a"],"optional_variables":["b"]}`))
	if p != nil {
		t.Fatalf("%v", codes(p))
	}
	if req.Name != "otp" || req.Channel != "email" || req.Body != "<p>{{ .a }}{{ .b }}</p>" ||
		!slices.Equal(*req.Variables, []string{"a"}) || !slices.Equal(*req.OptionalVariables, []string{"b"}) {
		t.Fatalf("parsed %+v", req)
	}
}

func TestValidateCreateTemplateVersion(t *testing.T) {
	obj := func(m map[string]any) string { return jsonObject(t, m) }
	ok := func(over map[string]any) string {
		m := map[string]any{"body": "Your code is {{ .code }}", "variables": []string{"code"}}
		for k, v := range over {
			m[k] = v
		}
		return obj(m)
	}
	tests := []templateCase{
		{"valid", ok(nil), 0, nil},
		{"valid without lists", ok(map[string]any{"body": "hello", "variables": []string{}}), 0, nil},

		{"empty body", ``, 400, []string{":empty_body"}},
		{"not an object", `"x"`, 400, []string{":not_an_object"}},
		{"missing body", `{}`, 422, []string{"body:required"}},
		{"body empty", ok(map[string]any{"body": ""}), 422, []string{"body:empty"}},
		{"body with NUL", ok(map[string]any{"body": "a\x00b"}), 422, []string{"body:invalid_character"}},
		{"body too large", ok(map[string]any{"body": strings.Repeat("a", domain.MaxTemplateBodyBytes+1)}), 422, []string{"body:too_long"}},
		{"variables not an array", ok(map[string]any{"variables": 1}), 422, []string{"variables:invalid_type"}},
		{"optional_variables with a number", ok(map[string]any{"optional_variables": []any{3}}), 422, []string{"optional_variables:invalid_type"}},
		{"name is not a field here", ok(map[string]any{"name": "otp"}), 422, []string{"name:unknown_field"}},
		{"channel is not a field here", ok(map[string]any{"channel": "sms"}), 422, []string{"channel:unknown_field"}},
		{"duplicate body", `{"body":"a","body":"b"}`, 422, []string{"body:duplicate_field"}},

		{"syntax", ok(map[string]any{"body": bodyMarker + "{{ .code "}), 422, []string{"body:template_syntax"}},
		{"construct not allowed", ok(map[string]any{"body": bodyMarker + "{{ with .code }}x{{ end }}"}), 422, []string{"body:construct_not_allowed"}},
		{"function not allowed", ok(map[string]any{"body": bodyMarker + `{{ len .code }}`}), 422, []string{"body:function_not_allowed"}},
		{"multiple templates", ok(map[string]any{"body": bodyMarker + `{{ define "b" }}x{{ end }}{{ .code }}`}), 422, []string{"body:multiple_templates"}},
		{"invalid call", ok(map[string]any{"body": bodyMarker + `{{ .code | truncate }}`}), 422, []string{"body:invalid_call"}},
		{"undeclared variable", ok(map[string]any{"body": bodyMarker + "{{ .code }}{{ .other }}"}), 422, []string{"body:undeclared_variable"}},
		{"unused variable", ok(map[string]any{"variables": []string{"code", "extra"}}), 422, []string{"variables:unused_variable"}},
		{"invalid variable name", ok(map[string]any{"body": "x", "variables": []string{"a-b"}}), 422, []string{"variables:invalid_variable_name"}},
		{"duplicate variable", ok(map[string]any{"variables": []string{"code", "code"}}), 422, []string{"variables:duplicate_variable"}},
		{"too many variables", ok(map[string]any{"variables": manyNames(domain.MaxTemplateVariables + 1)}), 422, []string{"variables:too_many_variables"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, p := validateCreateTemplateVersion(context.Background(), []byte(tc.body), func(context.Context) (domain.Channel, *problem) { return domain.ChannelSMS, nil })
			checkTemplateCase(t, tc, p)
		})
	}

	t.Run("the channel of the template decides the compile", func(t *testing.T) {
		body := []byte(`{"body":"<a href=\"{{ .x }}","variables":["x"]}`)
		if _, p := validateCreateTemplateVersion(context.Background(), body, func(context.Context) (domain.Channel, *problem) { return domain.ChannelSMS, nil }); p != nil {
			t.Fatalf("sms: %v", codes(p))
		}
		_, p := validateCreateTemplateVersion(context.Background(), body, func(context.Context) (domain.Channel, *problem) { return domain.ChannelEmail, nil })
		if got := codes(p); !slices.Equal(got, []string{"body:unsafe_html"}) {
			t.Fatalf("email: %v", got)
		}
	})
	t.Run("the lookup is not called for an invalid shape", func(t *testing.T) {
		called := false
		_, p := validateCreateTemplateVersion(context.Background(), []byte(`{}`), func(context.Context) (domain.Channel, *problem) { called = true; return domain.ChannelSMS, nil })
		if p == nil || called {
			t.Fatalf("problem %v, lookup called %v", p, called)
		}
	})
	t.Run("a problem of the lookup is returned as it is", func(t *testing.T) {
		_, p := validateCreateTemplateVersion(context.Background(), []byte(`{"body":"x"}`), func(context.Context) (domain.Channel, *problem) { return "", templateNotFound() })
		if p == nil || p.kind.status != http.StatusNotFound {
			t.Fatalf("problem %v", p)
		}
	})
}

// A compile error that is not a rejection (an unknown channel) is a 500 with
// the error as its cause and no detail of it in the body.
func TestCompileProblem_NotARejection(t *testing.T) {
	p := compileProblem("fax", "x", nil, nil)
	if p == nil || p.kind.status != http.StatusInternalServerError || p.cause == nil || len(p.errors) != 0 {
		t.Fatalf("problem %+v", p)
	}
	if strings.Contains(p.detail, "fax") {
		t.Fatalf("detail %q", p.detail)
	}
}

func TestTemplateFieldError(t *testing.T) {
	tests := []struct {
		te      domain.TemplateError
		field   string
		code    string
		message string
	}{
		{domain.TemplateError{Problem: domain.TemplateTooLarge}, "body", "too_long", "body must be at most 65536 bytes"},
		{domain.TemplateError{Problem: domain.TemplateTooLarge, Detail: "output"}, "body", "too_long", "body renders to more than 524288 bytes"},
		{domain.TemplateError{Problem: domain.TemplateSyntax, Detail: "invalid_utf8", Line: 2}, "body", "template_syntax", "body must be valid UTF-8 (line 2)"},
		{domain.TemplateError{Problem: domain.TemplateSyntax, Detail: "nul_character"}, "body", "template_syntax", "body must not contain the character U+0000"},
		{domain.TemplateError{Problem: domain.TemplateSyntax, Line: 3}, "body", "template_syntax", "body is not a valid template (line 3)"},
		{domain.TemplateError{Problem: domain.TemplateFunctionNotAllowed, Detail: "printf", Line: 1}, "body", "function_not_allowed", "body calls a function that is not allowed; only default and truncate are available: printf (line 1)"},
		{domain.TemplateError{Problem: domain.TemplateUnusedVariable, Detail: "extra"}, "variables", "unused_variable", "a declared variable is not used by the body: extra"},
		{domain.TemplateError{Problem: domain.TemplateTooManyVariables}, "variables", "too_many_variables", "at most 50 variables can be declared"},
		{domain.TemplateError{Problem: domain.TemplateMultiple, Detail: "define", Line: 4}, "body", "multiple_templates", "body must define exactly one template (line 4)"},
	}
	for _, tc := range tests {
		got := templateFieldError(&tc.te)
		if got.Field != tc.field || got.Code != tc.code || got.Message != tc.message {
			t.Errorf("%s: got %+v", tc.te.Problem, got)
		}
	}
}
