package domain

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	texttemplate "text/template"
	"text/template/parse"
)

// refParseName differs from templateParseName on purpose: a source that
// defines a template named like the walker's top level then shows up in the
// reference as a second template.
const refParseName = "reference-top-level"

// refNodes collects every parse.Node reachable from v through exported
// fields, by reflection. It shares no code with the walker and does not know
// the node types, so it also finds nodes of a type added later.
func refNodes(v reflect.Value, out *[]parse.Node) {
	switch v.Kind() {
	case reflect.Interface, reflect.Pointer:
		if v.IsNil() {
			return
		}
		if n, ok := v.Interface().(parse.Node); ok {
			*out = append(*out, n)
		}
		refNodes(v.Elem(), out)
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).IsExported() {
				refNodes(v.Field(i), out)
			}
		}
	case reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			refNodes(v.Index(i), out)
		}
	}
}

// refCheck is the independent reference: it parses an accepted source the
// normal way, with text/template and a FuncMap of the two names (so any other
// function name is a parse error), and returns a reason the source should
// not have been accepted, or "".
func refCheck(body string) string {
	stub := func(string, string) string { return "" }
	tmpl, err := texttemplate.New(refParseName).Funcs(texttemplate.FuncMap{
		"default":  stub,
		"truncate": func(int, string) string { return "" },
	}).Parse(body)
	if err != nil {
		return "normal parse failed: " + err.Error()
	}
	for _, other := range tmpl.Templates() {
		// Known and harmless: the parser drops an empty (blank or comment-only)
		// definition whose name is the top level's, so the walker cannot see
		// it. Here it shows up, as an empty template under that name.
		if other.Name() == refParseName ||
			(other.Name() == templateParseName && other.Tree != nil && parse.IsEmptyTree(other.Root)) {
			continue
		}
		return "second template in set: " + other.Name()
	}
	var nodes []parse.Node
	refNodes(reflect.ValueOf(tmpl.Root), &nodes)
	allowed := map[parse.NodeType]bool{
		parse.NodeText: true, parse.NodeComment: true, parse.NodeAction: true, parse.NodeIf: true,
		parse.NodeList: true, parse.NodePipe: true, parse.NodeCommand: true, parse.NodeField: true,
		parse.NodeIdentifier: true, parse.NodeString: true, parse.NodeNumber: true, parse.NodeBool: true,
	}
	for _, n := range nodes {
		if !allowed[n.Type()] {
			return "node type not allowed: " + n.String()
		}
		switch n := n.(type) {
		case *parse.IdentifierNode:
			if n.Ident != "default" && n.Ident != "truncate" {
				return "identifier: " + n.Ident
			}
		case *parse.FieldNode:
			if len(n.Ident) != 1 {
				return "chained field: " + n.String()
			}
		case *parse.PipeNode:
			if len(n.Decl) > 0 {
				return "declaration: " + n.String()
			}
		}
	}
	return ""
}

// fuzzFields returns the single-identifier field names of body that are
// valid variable names, in order, at most MaxTemplateVariables.
func fuzzFields(body string) []string {
	tree := parse.New("fields")
	tree.Mode = parse.SkipFuncCheck
	if _, err := tree.Parse(body, "", "", map[string]*parse.Tree{}); err != nil {
		return nil
	}
	var nodes []parse.Node
	refNodes(reflect.ValueOf(tree.Root), &nodes)
	var names []string
	seen := map[string]bool{}
	for _, n := range nodes {
		f, ok := n.(*parse.FieldNode)
		if !ok || len(f.Ident) == 0 || !ValidTemplateVariableName(f.Ident[0]) || seen[f.Ident[0]] {
			continue
		}
		seen[f.Ident[0]] = true
		names = append(names, f.Ident[0])
	}
	if len(names) > MaxTemplateVariables {
		names = names[:MaxTemplateVariables]
	}
	return names
}

func validDetail(s string) bool {
	if len(s) > 64 {
		return false
	}
	for _, r := range s {
		if r != '_' && (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') {
			return false
		}
	}
	return true
}

func FuzzCompileTemplate(f *testing.F) {
	for _, s := range []string{
		"", "hello", "{{ .a }}", "{{ .a | default \"x\" | truncate 3 }}", "{{ truncate 2 .a }}",
		"{{ if .a }}x{{ else if .b }}y{{ else }}z{{ end }}", "{{ 5 }}{{ true }}", "{{/* c */}}",
		"{{ printf \"%999999d\" 1 }}", "{{ range 1000000000 }}{{ end }}", "{{ call .a }}",
		"{{define \"body\"}}{{printf \"%d\" 1}}{{end}}", "{{define \"x\"}}hi{{end}}hello",
		"{{block \"x\" .}}{{end}}", "{{ template \"x\" }}", "{{ $x := .a }}{{ $x }}", "{{ $ }}",
		"{{ .a.b }}", "{{ (.a).b }}", "{{ . }}", "{{ default nil .a }}", "{{ default (.a) .b }}",
		"{{ with .a }}{{ end }}", "{{ default printf .a }}", "{{ .a | printf }}", "{{ 18446744073709551615 }}",
		"{{ 1.5 }}", "{{ 'a' }}", "{{ 0x10 }}", "{{ 1+2i }}", "{{ truncate 99999999999 .a }}",
		"<script>var x = \"{{ if .a }}\";{{ end }}</script>", "<a href=\"{{ .u }}\">{{ .u }}</a>",
		"<script>{{ .a }}</script>", "<style>{{ .a }}</style>", "{{ \"\\x00\" }}", "{{ \"\\xff\" }}",
		"{{ if and .a .b }}x{{ end }}", "{{ .a .b }}", "{{ .a | .b }}", "{{- .a -}}", "{{ break }}",
		"{{ if .a }}{{ if .b }}{{ if .c }}x{{ end }}{{ end }}{{ end }}", "{{ .ş }}", "{{ default \"\" \"\" }}",
		"{{ truncate 0 \"\" | default \"x\" | truncate 1 }}", "{{ if true }}{{ else }}{{ end }}",
	} {
		f.Add(s, false, "value")
		f.Add(s, true, "<b>\"x\" & 'y'</b>")
	}
	f.Fuzz(func(t *testing.T, body string, email bool, value string) {
		channel := ChannelSMS
		if email {
			channel = ChannelEmail
		}
		names := fuzzFields(body)
		half := len(names) / 2
		required, optional := names[:half], names[half:]
		tpl, err := CompileTemplate(channel, body, required, optional)
		if err != nil {
			var terr *TemplateError
			if !errors.As(err, &terr) {
				t.Fatalf("CompileTemplate(%q): error %v is not a *TemplateError", body, err)
			}
			if !validDetail(terr.Detail) || terr.Line < 0 || terr.Line > strings.Count(body, "\n")+1 {
				t.Fatalf("CompileTemplate(%q): bad error fields %+v", body, terr)
			}
			return
		}
		if reason := refCheck(body); reason != "" {
			t.Fatalf("accepted %q, but the reference refuses it: %s", body, reason)
		}
		value = strings.ReplaceAll(value, "\x00", "")
		if r := []rune(value); len(r) > MaxTemplateValueChars {
			value = string(r[:MaxTemplateValueChars])
		}
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		all := map[string]json.RawMessage{}
		for _, name := range names {
			all[name] = raw
		}
		onlyRequired := map[string]json.RawMessage{}
		for _, name := range required {
			onlyRequired[name] = raw
		}
		for _, vars := range []map[string]json.RawMessage{all, onlyRequired} {
			_, problems, err := tpl.Render(vars)
			if len(problems) > 0 {
				t.Fatalf("accepted %q: render problems %v", body, problems)
			}
			if err != nil && !errors.Is(err, ErrTemplateOutputTooLarge) {
				t.Fatalf("accepted %q: render failed: %v", body, err)
			}
		}
	})
}
