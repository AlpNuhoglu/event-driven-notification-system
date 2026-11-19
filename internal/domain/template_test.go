package domain

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
)

// rawVars returns the values whose names are in declared, as JSON.
func rawVars(values map[string]string, declared []string) map[string]json.RawMessage {
	out := map[string]json.RawMessage{}
	for _, name := range declared {
		if v, ok := values[name]; ok {
			out[name] = json.RawMessage(v)
		}
	}
	return out
}

func jsonString(t *testing.T, s string) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func render(t *testing.T, tpl *Template, vars map[string]json.RawMessage) string {
	t.Helper()
	out, problems, err := tpl.Render(vars)
	if err != nil || len(problems) > 0 {
		t.Fatalf("Render: %v, %v", problems, err)
	}
	return out
}

func TestValidTemplateName(t *testing.T) {
	for s, want := range map[string]bool{
		"otp": true, "0": true, "a-b_c9": true, strings.Repeat("a", 64): true,
		"": false, strings.Repeat("a", 65): false, "-a": false, "_a": false, "A": false,
		"a b": false, "ş": false, "a.b": false,
	} {
		if got := ValidTemplateName(s); got != want {
			t.Errorf("ValidTemplateName(%q) = %v, want %v", s, got, want)
		}
	}
}

func TestValidTemplateVariableName(t *testing.T) {
	for s, want := range map[string]bool{
		"a": true, "_": true, "first_name": true, "A9": true, strings.Repeat("a", 64): true,
		"": false, strings.Repeat("a", 65): false, "9a": false, "a-b": false, "ş": false,
		"a.b": false, "a b": false,
	} {
		if got := ValidTemplateVariableName(s); got != want {
			t.Errorf("ValidTemplateVariableName(%q) = %v, want %v", s, got, want)
		}
	}
}

func TestCompileTemplate_UnknownChannel(t *testing.T) {
	_, err := CompileTemplate("fax", "x", nil, nil)
	var terr *TemplateError
	if err == nil || errors.As(err, &terr) {
		t.Fatalf("err = %v, want a plain error", err)
	}
}

func TestTemplateFunctions_PipelineAndCallForm(t *testing.T) {
	tpl := mustCompile(t, ChannelSMS,
		`Hi {{ .first_name | default "Customer" }}, {{ default "x" .first_name }}: {{ .text | truncate 5 }}/{{ truncate 3 .text }}`,
		[]string{"text"}, []string{"first_name"})
	got := render(t, tpl, map[string]json.RawMessage{"text": jsonString(t, "abcdefgh")})
	if want := "Hi Customer, x: abcde/abc"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	got = render(t, tpl, map[string]json.RawMessage{"text": jsonString(t, "ab"), "first_name": jsonString(t, "Ayşe")})
	if want := "Hi Ayşe, Ayşe: ab/ab"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestTemplateTruncate_MultiByte(t *testing.T) {
	tpl := mustCompile(t, ChannelPush, `{{ .s | truncate 3 }}`, []string{"s"}, nil)
	for in, want := range map[string]string{
		"çğışöü": "çğı", "😀😀😀😀": "😀😀😀", "a😀b😀": "a😀b", "ab": "ab", "": "",
		"e\u0301\u0301x": "e\u0301\u0301", // three code points; a combining mark counts as one
	} {
		if got := render(t, tpl, map[string]json.RawMessage{"s": jsonString(t, in)}); got != want {
			t.Errorf("truncate 3 %q = %q, want %q", in, got, want)
		}
	}
	if got := templateTruncate(-1, "abc"); got != "" {
		t.Fatalf("truncate -1 = %q", got)
	}
}

func TestTemplateVariables_Declaration(t *testing.T) {
	cases := []struct {
		name          string
		body          string
		vars, opt     []string
		problem       TemplateProblem
		detail        string
		wantCompileOK bool
	}{
		{"undeclared reference", `{{ .a }}{{ .b }}`, []string{"a"}, nil, TemplateUndeclaredVariable, "b", false},
		{"declared but unused", `{{ .a }}`, []string{"a"}, []string{"b"}, TemplateUnusedVariable, "b", false},
		{"unused required", `x`, []string{"a"}, nil, TemplateUnusedVariable, "a", false},
		{"invalid name", `{{ .a }}`, []string{"a", "b-c"}, nil, TemplateInvalidVariableName, "b_c", false},
		{"invalid optional name", `{{ .a }}`, []string{"a"}, []string{""}, TemplateInvalidVariableName, "", false},
		{"duplicate in one list", `{{ .a }}`, []string{"a", "a"}, nil, TemplateDuplicateVariable, "a", false},
		{"duplicate across lists", `{{ .a }}`, []string{"a"}, []string{"a"}, TemplateDuplicateVariable, "a", false},
		{"field reference in if only", `{{ if .a }}x{{ end }}`, nil, []string{"a"}, "", "", true},
		{"field in call only", `{{ default "x" .a }}`, []string{"a"}, nil, "", "", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.wantCompileOK {
				mustCompile(t, ChannelSMS, c.body, c.vars, c.opt)
				return
			}
			wantTemplateError(t, ChannelSMS, c.body, c.vars, c.opt, c.problem, c.detail)
		})
	}
}

func manyVars(n int) ([]string, string) {
	names := make([]string, n)
	var body strings.Builder
	for i := range names {
		names[i] = fmt.Sprintf("v%d", i)
		body.WriteString("{{ ." + names[i] + " }}")
	}
	return names, body.String()
}

func TestTemplateVariables_FiftyAtTheBoundary(t *testing.T) {
	names, body := manyVars(MaxTemplateVariables)
	mustCompile(t, ChannelSMS, body, names[:20], names[20:])
	names, body = manyVars(MaxTemplateVariables + 1)
	wantTemplateError(t, ChannelSMS, body, names[:20], names[20:], TemplateTooManyVariables, "")
}

func TestTemplateRender_Variables(t *testing.T) {
	tpl := mustCompile(t, ChannelSMS, `[{{ .req }}|{{ .opt }}]`, []string{"req"}, []string{"opt"})
	cases := []struct {
		name     string
		vars     map[string]string
		want     string
		problems []VariableError
	}{
		{"string", map[string]string{"req": `"x"`}, "[x|]", nil},
		{"missing optional is empty", map[string]string{"req": `"x"`}, "[x|]", nil},
		{"optional set", map[string]string{"req": `"x"`, "opt": `"y"`}, "[x|y]", nil},
		{"number", map[string]string{"req": `482913`}, "[482913|]", nil},
		{"long numeric code", map[string]string{"req": `12345678901234567890123`}, "[12345678901234567890123|]", nil},
		{"decimal keeps its text", map[string]string{"req": `1.50`}, "[1.50|]", nil},
		{"exponent keeps its text", map[string]string{"req": `1e3`, "opt": `-0`}, "[1e3|-0]", nil},
		{"true", map[string]string{"req": `true`, "opt": `false`}, "[true|false]", nil},
		{"whitespace around value", map[string]string{"req": " \"x\" "}, "[x|]", nil},
		{"missing required", map[string]string{"opt": `"y"`}, "", []VariableError{{"req", VariableMissing}}},
		{"undeclared extra", map[string]string{"req": `"x"`, "fist_name": `"y"`}, "", []VariableError{{"fist_name", VariableUnknown}}},
		{"object", map[string]string{"req": `{"a":1}`}, "", []VariableError{{"req", VariableInvalidType}}},
		{"array", map[string]string{"req": `[1]`}, "", []VariableError{{"req", VariableInvalidType}}},
		{"null", map[string]string{"req": `null`}, "", []VariableError{{"req", VariableInvalidType}}},
		{"not JSON", map[string]string{"req": `x`}, "", []VariableError{{"req", VariableInvalidType}}},
		{"two values", map[string]string{"req": `"a" "b"`}, "", []VariableError{{"req", VariableInvalidType}}},
		{"empty raw", map[string]string{"req": ``}, "", []VariableError{{"req", VariableInvalidType}}},
		{"NUL", map[string]string{"req": `"a\u0000b"`}, "", []VariableError{{"req", VariableInvalidCharacter}}},
		{"every problem in a stable order", map[string]string{"opt": `[]`, "b": `1`, "a": `1`}, "", []VariableError{
			{"a", VariableUnknown}, {"b", VariableUnknown}, {"opt", VariableInvalidType}, {"req", VariableMissing},
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			vars := map[string]json.RawMessage{}
			for k, v := range c.vars {
				vars[k] = json.RawMessage(v)
			}
			out, problems, err := tpl.Render(vars)
			if err != nil {
				t.Fatal(err)
			}
			if out != c.want || !reflect.DeepEqual(problems, c.problems) {
				t.Fatalf("Render = %q, %v; want %q, %v", out, problems, c.want, c.problems)
			}
		})
	}
}

func TestTemplateRender_ValueLengthBoundary(t *testing.T) {
	tpl := mustCompile(t, ChannelSMS, `{{ .v }}`, []string{"v"}, nil)
	exact := strings.Repeat("ş", MaxTemplateValueChars) // 1,024 characters, 2,048 bytes
	if got := render(t, tpl, map[string]json.RawMessage{"v": jsonString(t, exact)}); got != exact {
		t.Fatalf("1,024 characters not rendered as given")
	}
	_, problems, err := tpl.Render(map[string]json.RawMessage{"v": jsonString(t, exact+"a")})
	if err != nil || !reflect.DeepEqual(problems, []VariableError{{"v", VariableTooLong}}) {
		t.Fatalf("1,025 characters: %v, %v", problems, err)
	}
	// A number is measured after conversion too.
	_, problems, _ = tpl.Render(map[string]json.RawMessage{"v": json.RawMessage(strings.Repeat("1", 1025))})
	if !reflect.DeepEqual(problems, []VariableError{{"v", VariableTooLong}}) {
		t.Fatalf("1,025-digit number: %v", problems)
	}
}

func TestTemplateBody_SizeBoundary(t *testing.T) {
	for _, ch := range Channels() {
		mustCompile(t, ch, strings.Repeat("a", MaxTemplateBodyBytes), nil, nil)
		wantTemplateError(t, ch, strings.Repeat("a", MaxTemplateBodyBytes+1), nil, nil, TemplateTooLarge, "")
	}
}

func TestTemplateBody_InvalidUTF8AndNUL(t *testing.T) {
	err := wantTemplateError(t, ChannelSMS, "ok\n\xff", nil, nil, TemplateSyntax, constructInvalidUTF8)
	if err.Line != 2 {
		t.Fatalf("line %d", err.Line)
	}
	wantTemplateError(t, ChannelSMS, "a\x00b", nil, nil, TemplateSyntax, constructNULCharacter)
}

func TestTemplateRender_OutputLimit(t *testing.T) {
	value := jsonString(t, strings.Repeat("a", MaxTemplateValueChars))
	// 512 copies of a 1,024-byte value are exactly 512 KB: accepted.
	tpl := mustCompile(t, ChannelSMS, strings.Repeat("{{ .v }}", 512), []string{"v"}, nil)
	if got := render(t, tpl, map[string]json.RawMessage{"v": value}); len(got) != MaxTemplateOutputBytes {
		t.Fatalf("len %d", len(got))
	}
	for _, ch := range Channels() {
		tpl = mustCompile(t, ch, strings.Repeat("{{ .v }}", 513), []string{"v"}, nil)
		out, problems, err := tpl.Render(map[string]json.RawMessage{"v": value})
		if err != ErrTemplateOutputTooLarge || out != "" || problems != nil { //nolint:errorlint // the contract is ==
			t.Fatalf("%s: %q, %v, %v; want ErrTemplateOutputTooLarge", ch, out[:min(len(out), 10)], problems, err)
		}
	}
}

func TestTemplateEscaping_EmailIsHTMLEscaped(t *testing.T) {
	tpl := mustCompile(t, ChannelEmail,
		`<p>{{ .v }}</p><a title="{{ .v }}" href="{{ .u }}">x</a><script>var v = {{ .v }};</script>`,
		[]string{"v", "u"}, nil)
	got := render(t, tpl, map[string]json.RawMessage{
		"v": jsonString(t, `<b>"Tom" & 'Jerry'</b>`),
		"u": jsonString(t, `javascript:alert(1)`),
	})
	want := `<p>&lt;b&gt;&#34;Tom&#34; &amp; &#39;Jerry&#39;&lt;/b&gt;</p>` +
		`<a title="&lt;b&gt;&#34;Tom&#34; &amp; &#39;Jerry&#39;&lt;/b&gt;" href="#ZgotmplZ">x</a>` +
		"<script>var v = \"\\u003cb\\u003e\\\"Tom\\\" \\u0026 'Jerry'\\u003c/b\\u003e\";</script>"
	if got != want {
		t.Fatalf("got\n%s\nwant\n%s", got, want)
	}
	// default's output is escaped too.
	tpl = mustCompile(t, ChannelEmail, `{{ .v | default "<i>" }}`, nil, []string{"v"})
	if got := render(t, tpl, nil); got != "&lt;i&gt;" {
		t.Fatalf("default output not escaped: %q", got)
	}
}

func TestTemplateEscaping_SMSAndPushAreNotEscaped(t *testing.T) {
	for _, ch := range []Channel{ChannelSMS, ChannelPush} {
		tpl := mustCompile(t, ch, `<p>{{ .v }}</p><script>{{ .v }}</script>`, []string{"v"}, nil)
		got := render(t, tpl, map[string]json.RawMessage{"v": jsonString(t, `<b>"&'</b>`)})
		if want := `<p><b>"&'</b></p><script><b>"&'</b></script>`; got != want {
			t.Fatalf("%s: got %q, want %q", ch, got, want)
		}
	}
}

func TestTemplateEscaping_UnsafeHTML(t *testing.T) {
	cases := map[string]string{
		"branches end in different JS contexts": `<script>var x = "{{ if .a }}";{{ end }}</script>`,
		"branches end in different tag states":  `<a {{ if .a }}href="{{ else }}title="{{ end }}x">`,
		"ends inside a tag":                     `<a href="{{ .a }}`,
		"ends inside script":                    `<script>{{ .a }}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := CompileTemplate(ChannelEmail, body, []string{"a"}, nil)
			var terr *TemplateError
			if !errors.As(err, &terr) || terr.Problem != TemplateUnsafeHTML {
				t.Fatalf("err = %v, want unsafe_html", err)
			}
			// The same body is fine as plain text.
			mustCompile(t, ChannelSMS, body, []string{"a"}, nil)
		})
	}
	err := wantTemplateError(t, ChannelEmail, "<p>\n<script>var x = \"{{ if .a }}\";{{ end }}</script>", []string{"a"}, nil, TemplateUnsafeHTML, "branch_end_context")
	if err.Line != 2 {
		t.Fatalf("line %d, want 2", err.Line)
	}
}

func TestTemplate_Accessors(t *testing.T) {
	vars, opt := []string{"a"}, []string{"b"}
	tpl := mustCompile(t, ChannelPush, `{{ .a }}{{ .b }}`, vars, opt)
	vars[0], opt[0] = "x", "y" // the template keeps its own copies
	if tpl.Channel() != ChannelPush || !reflect.DeepEqual(tpl.Variables(), []string{"a"}) ||
		!reflect.DeepEqual(tpl.OptionalVariables(), []string{"b"}) {
		t.Fatalf("accessors: %s %v %v", tpl.Channel(), tpl.Variables(), tpl.OptionalVariables())
	}
	tpl.Variables()[0] = "z"
	if tpl.Variables()[0] != "a" {
		t.Fatal("Variables returns the internal slice")
	}
}

func TestTemplateRender_Concurrent(t *testing.T) {
	for _, ch := range Channels() {
		tpl := mustCompile(t, ch, `<p>{{ .v | truncate 6 }}</p>`, []string{"v"}, nil)
		var wg sync.WaitGroup
		errs := make(chan error, 64)
		for i := range 64 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				v := fmt.Sprintf("value-%d", i)
				out, problems, err := tpl.Render(map[string]json.RawMessage{"v": json.RawMessage(`"` + v + `"`)})
				if err != nil || len(problems) > 0 || out != "<p>"+v[:6]+"</p>" {
					errs <- fmt.Errorf("%s: %q %v %v", ch, out, problems, err)
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatal(err)
		}
	}
}

func TestTemplateError_Error(t *testing.T) {
	e := &TemplateError{Problem: TemplateFunctionNotAllowed, Detail: "printf", Line: 3}
	if got := e.Error(); got != "template: function_not_allowed (printf) at line 3" {
		t.Fatalf("%q", got)
	}
	if got := (&TemplateError{Problem: TemplateTooLarge}).Error(); got != "template: template_too_large" {
		t.Fatalf("%q", got)
	}
}
