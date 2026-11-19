package domain

import (
	"errors"
	"strings"
	"testing"
	"text/template/parse"
)

// wantTemplateError compiles body for channel and requires a *TemplateError
// with exactly this Problem and Detail. A rejection for a different reason
// fails the test. It returns a copy of the error (a value, so that errcheck
// does not treat the result as an error to handle).
func wantTemplateError(t *testing.T, channel Channel, body string, vars, optional []string, problem TemplateProblem, detail string) TemplateError {
	t.Helper()
	tpl, err := CompileTemplate(channel, body, vars, optional)
	if err == nil {
		t.Fatalf("CompileTemplate(%q) accepted the source (%v), want %s (%s)", body, tpl != nil, problem, detail)
	}
	var terr *TemplateError
	if !errors.As(err, &terr) {
		t.Fatalf("CompileTemplate(%q) error %v is not a *TemplateError", body, err)
	}
	if terr.Problem != problem || terr.Detail != detail {
		t.Fatalf("CompileTemplate(%q) = %s (%q), want %s (%q)", body, terr.Problem, terr.Detail, problem, detail)
	}
	return *terr
}

func mustCompile(t *testing.T, channel Channel, body string, vars, optional []string) *Template {
	t.Helper()
	tpl, err := CompileTemplate(channel, body, vars, optional)
	if err != nil {
		t.Fatalf("CompileTemplate(%q): %v", body, err)
	}
	return tpl
}

var goBuiltinFunctions = []string{
	"and", "call", "html", "index", "slice", "js", "len", "not", "or", "print",
	"printf", "println", "urlquery", "eq", "ge", "gt", "le", "lt", "ne",
}

// The source is built so that, without the function-name check, it would pass
// every other rule of the walker (or fail with invalid_call), so only the name
// check can produce function_not_allowed with the name.
func TestTemplateFunctionNameCheck_RejectsEveryBuiltinInCallPosition(t *testing.T) {
	if len(goBuiltinFunctions) != 19 {
		t.Fatalf("%d builtins listed, want 19", len(goBuiltinFunctions))
	}
	for _, name := range goBuiltinFunctions {
		t.Run(name, func(t *testing.T) {
			for _, ch := range Channels() {
				wantTemplateError(t, ch, "{{ "+name+" .a .b }}", []string{"a", "b"}, nil, TemplateFunctionNotAllowed, name)
			}
		})
	}
}

func TestTemplateFunctionNameCheck_RejectsUnknownFunction(t *testing.T) {
	wantTemplateError(t, ChannelSMS, `{{ upper .a }}`, []string{"a"}, nil, TemplateFunctionNotAllowed, "upper")
	wantTemplateError(t, ChannelSMS, `{{ .a | upper }}`, []string{"a"}, nil, TemplateFunctionNotAllowed, "upper")
	wantTemplateError(t, ChannelSMS, `{{ if lower .a }}x{{ end }}`, []string{"a"}, nil, TemplateFunctionNotAllowed, "lower")
}

func TestTemplateFunctionNameCheck_RejectsPrintfAndCallElsewhere(t *testing.T) {
	cases := map[string]string{
		"printf as argument":        `{{ default printf .a }}`,
		"printf in a later stage":   `{{ .a | printf }}`,
		"printf after a call":       `{{ .a | default "x" | printf }}`,
		"printf in an if pipe":      `{{ if .a | printf }}x{{ end }}`,
		"printf as truncate's text": `{{ truncate 3 printf }}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			wantTemplateError(t, ChannelSMS, body, []string{"a"}, nil, TemplateFunctionNotAllowed, "printf")
		})
	}
	cases = map[string]string{
		"call as argument":      `{{ default call .a }}`,
		"call in a later stage": `{{ .a | call }}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			wantTemplateError(t, ChannelEmail, body, []string{"a"}, nil, TemplateFunctionNotAllowed, "call")
		})
	}
	// An allowed name in argument position would be called by the engine too.
	wantTemplateError(t, ChannelSMS, `{{ default default .a }}`, []string{"a"}, nil, TemplateFunctionNotAllowed, "default")
}

func TestTemplateOneTemplateRule_RejectsHarmlessSecondTemplate(t *testing.T) {
	// The second template is harmless and the top level is plain text: only
	// the one-template rule rejects this source.
	wantTemplateError(t, ChannelSMS, `{{define "x"}}hi{{end}}hello`, nil, nil, TemplateMultiple, constructDefine)
	wantTemplateError(t, ChannelEmail, `hello{{define "x"}}hi{{end}}`, nil, nil, TemplateMultiple, constructDefine)
}

func TestTemplateOneTemplateRule_RejectsSelfNamedDefine(t *testing.T) {
	// The definition replaces the top-level tree in the set: the tree the
	// parser returns is empty, and the set's only tree holds the printf.
	body := `{{define "` + templateParseName + `"}}{{printf "%999999d" 1}}{{end}}`
	wantTemplateError(t, ChannelSMS, body, nil, nil, TemplateMultiple, constructDefine)
	wantTemplateError(t, ChannelEmail, body, nil, nil, TemplateMultiple, constructDefine)
}

func TestTemplateOneTemplateRule_RejectsBlock(t *testing.T) {
	wantTemplateError(t, ChannelSMS, `{{block "x" .}}hi{{end}}`, nil, nil, TemplateMultiple, constructDefine)
}

func TestTemplateOneTemplateRule_EmptySelfNamedDefineIsReplaced(t *testing.T) {
	// The parser lets a non-empty top level replace an empty definition of the
	// same name; the set then holds the returned tree only, so this is one
	// template.
	tpl := mustCompile(t, ChannelSMS, `{{define "`+templateParseName+`"}}{{end}}hello`, nil, nil)
	if out, _, err := tpl.Render(nil); err != nil || out != "hello" {
		t.Fatalf("Render = %q, %v", out, err)
	}
}

func TestTemplateWalker_RejectsConstructs(t *testing.T) {
	cases := []struct {
		name, body string
		detail     string
	}{
		{"range", `{{ range .a }}x{{ end }}`, constructRange},
		{"range with break", `{{ range .a }}{{ break }}{{ end }}`, constructRange},
		{"range with continue", `{{ range .a }}{{ continue }}{{ end }}`, constructRange},
		{"range in else", `{{ if .a }}x{{ else }}{{ range .a }}{{ end }}{{ end }}`, constructRange},
		{"with", `{{ with .a }}x{{ end }}`, constructWith},
		{"template", `{{ template "x" }}`, constructTemplate},
		{"template inside if", `{{ if .a }}{{ template "x" .a }}{{ end }}`, constructTemplate},
		{"variable declaration", `{{ $x := .a }}`, constructDeclaration},
		{"variable declaration in if", `{{ if $x := .a }}y{{ end }}`, constructDeclaration},
		{"dollar", `{{ $ }}{{ .a }}`, constructVariable},
		{"dollar field", `{{ $.a }}`, constructVariable},
		{"dollar as argument", `{{ default $ .a }}`, constructVariable},
		{"chained field", `{{ .a.b }}`, constructChainedField},
		{"chained field as argument", `{{ default "x" .a.b }}`, constructChainedField},
		{"chain node", `{{ (.a).b }}`, constructChainedField},
		{"bare dot", `{{ . }}{{ .a }}`, constructDot},
		{"bare dot as argument", `{{ default . .a }}`, constructDot},
		{"bare dot in if", `{{ if . }}{{ .a }}{{ end }}`, constructDot},
		{"nil", `{{ default nil .a }}`, constructNil},
		{"parenthesised pipeline as argument", `{{ default (.a) "x" }}`, constructParenthesized},
		{"parenthesised pipeline alone", `{{ (.a) }}`, constructParenthesized},
		{"operand with arguments", `{{ .a "x" }}`, constructOperandArgs},
		{"string with arguments", `{{ "x" .a }}`, constructOperandArgs},
		{"operand after pipe", `{{ .a | .a }}`, constructOperandInPipe},
		{"parenthesised pipeline after pipe", `{{ .a | (.a) }}`, constructOperandInPipe},
		{"negative number", `{{ -1 }}{{ .a }}`, constructNumber},
		{"float", `{{ 1.5 }}{{ .a }}`, constructNumber},
		{"hex", `{{ 0x10 }}{{ .a }}`, constructNumber},
		{"rune", `{{ 'a' }}{{ .a }}`, constructNumber},
		{"too large", `{{ 1000000000 }}{{ .a }}`, constructNumber},
		{"uint overflow", `{{ 18446744073709551615 }}{{ .a }}`, constructNumber},
		{"number in if", `{{ if 1e3 }}{{ .a }}{{ end }}`, constructNumber},
		{"NUL in string literal", `{{ "\x00" }}{{ .a }}`, constructString},
		{"invalid UTF-8 in string literal", `{{ "\xff" }}{{ .a }}`, constructString},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			wantTemplateError(t, ChannelSMS, c.body, []string{"a"}, nil, TemplateConstructNotAllowed, c.detail)
		})
	}
}

func TestTemplateWalker_BreakAndContinueOutsideRangeDoNotParse(t *testing.T) {
	wantTemplateError(t, ChannelSMS, `{{ break }}`, nil, nil, TemplateSyntax, "")
	wantTemplateError(t, ChannelSMS, `{{ continue }}`, nil, nil, TemplateSyntax, "")
	wantTemplateError(t, ChannelSMS, `{{ if true }}{{ break }}{{ end }}`, nil, nil, TemplateSyntax, "")
}

// fakeNode is a node type the walker does not know: it embeds a known node
// to satisfy parse.Node, but its own type matches no case.
type fakeNode struct{ *parse.TextNode }

func TestTemplateWalker_RejectsNodesDirectly(t *testing.T) {
	w := &templateWalker{body: "x", seen: map[string]bool{}}
	nodes := map[string]parse.Node{
		constructBreak:       &parse.BreakNode{NodeType: parse.NodeBreak},
		constructContinue:    &parse.ContinueNode{NodeType: parse.NodeContinue},
		constructUnknownNode: fakeNode{&parse.TextNode{NodeType: parse.NodeText}},
	}
	for want, n := range nodes {
		err := w.node(n)
		if err == nil || err.Problem != TemplateConstructNotAllowed || err.Detail != want {
			t.Fatalf("node %T: %v, want construct_not_allowed (%s)", n, err, want)
		}
	}
	if _, err := w.operand(fakeNode{&parse.TextNode{NodeType: parse.NodeText}}); err == nil || err.Detail != constructUnknownNode {
		t.Fatalf("operand of unknown type: %v", err)
	}
	// A command with no words cannot come from the parser; it is still refused.
	empty := &parse.PipeNode{NodeType: parse.NodePipe, Cmds: []*parse.CommandNode{{NodeType: parse.NodeCommand}}}
	if err := w.pipe(empty); err == nil || err.Problem != TemplateConstructNotAllowed {
		t.Fatalf("empty command: %v", err)
	}
	if err := w.pipe(&parse.PipeNode{NodeType: parse.NodePipe}); err == nil || err.Detail != constructEmptyPipeline {
		t.Fatalf("empty pipeline: %v", err)
	}
}

func TestTemplateWalker_InvalidCalls(t *testing.T) {
	cases := []struct {
		body, fn string
	}{
		{`{{ default }}`, funcDefault},
		{`{{ default "x" }}`, funcDefault},
		{`{{ .a | default }}`, funcDefault},
		{`{{ default "x" "y" "z" }}`, funcDefault},
		{`{{ .a | default "x" "y" }}`, funcDefault},
		{`{{ default 5 .a }}`, funcDefault},
		{`{{ default true .a }}`, funcDefault},
		{`{{ default "x" 5 }}`, funcDefault},
		{`{{ 5 | default "x" }}`, funcDefault},
		{`{{ true | default "x" }}`, funcDefault},
		{`{{ truncate }}`, funcTruncate},
		{`{{ truncate 5 }}`, funcTruncate},
		{`{{ .a | truncate }}`, funcTruncate},
		{`{{ truncate 5 .a "x" }}`, funcTruncate},
		{`{{ truncate .a .a }}`, funcTruncate},
		{`{{ truncate "5" .a }}`, funcTruncate},
		{`{{ truncate -1 .a }}`, funcTruncate},
		{`{{ truncate 1.5 .a }}`, funcTruncate},
		{`{{ truncate 1000000000 .a }}`, funcTruncate},
		{`{{ truncate true .a }}`, funcTruncate},
		{`{{ truncate 5 5 }}`, funcTruncate},
		{`{{ .a | truncate .a }}`, funcTruncate},
		{`{{ 5 | truncate 3 }}`, funcTruncate},
		{`{{ truncate 5 .a | truncate .a }}`, funcTruncate},
	}
	for _, c := range cases {
		t.Run(c.body, func(t *testing.T) {
			wantTemplateError(t, ChannelSMS, c.body, nil, []string{"a"}, TemplateInvalidCall, c.fn)
		})
	}
}

func TestTemplateWalker_AcceptsTheGrammar(t *testing.T) {
	cases := []struct {
		body, want string
	}{
		{"plain text", "plain text"},
		{`{{/* a comment */}}x`, "x"},
		{`{{- /* trimmed */ -}} x`, "x"},
		{`{{ .a }}`, "A"},
		{`{{ "lit" }}`, "lit"},
		{"{{ `raw` }}", "raw"},
		{`{{ 5 }}{{ 0 }}{{ 999999999 }}`, "50999999999"},
		{`{{ true }} {{ false }}`, "true false"},
		{`{{ if .a }}yes{{ end }}`, "yes"},
		{`{{ if .missing }}yes{{ else }}no{{ end }}`, "no"},
		{`{{ if .missing }}1{{ else if .a }}2{{ else }}3{{ end }}`, "2"},
		{`{{ if true }}t{{ end }}{{ if false }}f{{ end }}{{ if 0 }}z{{ end }}`, "t"},
		{`{{ if .a | default "x" }}y{{ end }}`, "y"},
		{`{{ .missing | default "fallback" }}`, "fallback"},
		{`{{ default "fallback" .missing }}`, "fallback"},
		{`{{ default .a .missing }}`, "A"},
		{`{{ .a | truncate 0 }}|`, "|"},
		{`{{ truncate 3 "abcdef" }}`, "abc"},
		{`{{ .missing | default "abcdef" | truncate 2 }}`, "ab"},
		{`{{ truncate 4 .a | default "x" }}`, "A"},
		{"{{ if .a }}\n{{ if .missing }}a{{ else }}b{{ end }}\n{{ end }}", "\nb\n"},
	}
	for _, c := range cases {
		t.Run(c.body, func(t *testing.T) {
			var vars []string
			for _, name := range []string{"a", "missing"} {
				if strings.Contains(c.body, "."+name) {
					vars = append(vars, name)
				}
			}
			tpl := mustCompile(t, ChannelSMS, c.body, nil, vars)
			out, problems, err := tpl.Render(rawVars(map[string]string{"a": `"A"`}, vars))
			if err != nil || len(problems) > 0 || out != c.want {
				t.Fatalf("Render = %q, %v, %v; want %q", out, problems, err, c.want)
			}
		})
	}
}

func TestTemplateWalker_Lines(t *testing.T) {
	err := wantTemplateError(t, ChannelSMS, "one\ntwo\n{{ printf \"x\" }}", nil, nil, TemplateFunctionNotAllowed, "printf")
	if err.Line != 3 {
		t.Fatalf("line %d, want 3", err.Line)
	}
	err = wantTemplateError(t, ChannelSMS, "one\n{{ if .a }}\n{{ end }", []string{"a"}, nil, TemplateSyntax, "")
	if err.Line != 3 {
		t.Fatalf("syntax line %d, want 3", err.Line)
	}
	err = wantTemplateError(t, ChannelSMS, "a\n\n{{ .b }}", nil, nil, TemplateUndeclaredVariable, "b")
	if err.Line != 3 {
		t.Fatalf("undeclared line %d, want 3", err.Line)
	}
}

func TestTemplateWalker_DetailNeverCarriesSourceText(t *testing.T) {
	long := strings.Repeat("ş", 100)
	err := wantTemplateError(t, ChannelSMS, "{{ "+long+" }}", nil, nil, TemplateFunctionNotAllowed, strings.Repeat("_", 64))
	if strings.Contains(err.Error(), "ş") {
		t.Fatalf("Error() carries source text: %s", err.Error())
	}
	wantTemplateError(t, ChannelSMS, "{{ .ş }}", nil, nil, TemplateInvalidVariableName, "_")
	wantTemplateError(t, ChannelSMS, "{{ ."+strings.Repeat("a", 65)+" }}", nil, nil, TemplateInvalidVariableName, strings.Repeat("a", 64))
	// The parser's message is not passed on.
	err = wantTemplateError(t, ChannelSMS, `{{ "secret text`, nil, nil, TemplateSyntax, "")
	if strings.Contains(err.Error(), "secret") {
		t.Fatalf("Error() carries source text: %s", err.Error())
	}
}

func TestPlainNumber(t *testing.T) {
	for s, want := range map[string]bool{
		"0": true, "5": true, "999999999": true, "1000000000": false, "": false,
		"00": false, "07": false, "-1": false, "+1": false, "1.0": false, "1e3": false,
		"0x1": false, "'a'": false, "1_0": false, "1i": false,
	} {
		if got := plainNumber(s); got != want {
			t.Errorf("plainNumber(%q) = %v, want %v", s, got, want)
		}
	}
}
