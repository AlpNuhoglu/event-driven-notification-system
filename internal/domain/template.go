package domain

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	htmltemplate "html/template"
	"io"
	"slices"
	"strconv"
	"strings"
	texttemplate "text/template"
	"unicode/utf8"
)

// Template limits (ADR-0008 §2, §4).
const (
	MaxTemplateBodyBytes   = 64 * 1024  // a body larger than this is rejected
	MaxTemplateOutputBytes = 512 * 1024 // rendering fails above this (defence in depth)
	MaxTemplateVariables   = 50         // declared variables, both lists together
	MaxTemplateValueChars  = 1024       // one variable value, in characters (runes), after conversion
)

// ValidTemplateName reports whether s matches ^[a-z0-9][a-z0-9_-]{0,63}$.
func ValidTemplateName(s string) bool {
	if len(s) == 0 || len(s) > 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		ok := (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || (i > 0 && (c == '_' || c == '-'))
		if !ok {
			return false
		}
	}
	return true
}

// ValidTemplateVariableName reports whether s matches ^[A-Za-z_][A-Za-z0-9_]{0,63}$ (ASCII only).
func ValidTemplateVariableName(s string) bool {
	if len(s) == 0 || len(s) > 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		ok := c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (i > 0 && c >= '0' && c <= '9')
		if !ok {
			return false
		}
	}
	return true
}

// TemplateProblem is the stable reason a source or its declaration is rejected.
type TemplateProblem string

// The template problems; the values are API error codes.
const (
	// TemplateTooLarge means the body is over MaxTemplateBodyBytes.
	TemplateTooLarge TemplateProblem = "template_too_large"
	// TemplateSyntax means the body does not parse.
	TemplateSyntax TemplateProblem = "template_syntax"
	// TemplateConstructNotAllowed means the body uses a construct outside the allow-list.
	TemplateConstructNotAllowed TemplateProblem = "construct_not_allowed"
	// TemplateFunctionNotAllowed means the body names a function other than default and truncate.
	TemplateFunctionNotAllowed TemplateProblem = "function_not_allowed"
	// TemplateMultiple means the body defines more than one template.
	TemplateMultiple TemplateProblem = "multiple_templates"
	// TemplateInvalidCall means default or truncate is called with the wrong arguments.
	TemplateInvalidCall TemplateProblem = "invalid_call"
	// TemplateUndeclaredVariable means the body references a variable that is not declared.
	TemplateUndeclaredVariable TemplateProblem = "undeclared_variable"
	// TemplateUnusedVariable means a declared variable is not referenced by the body.
	TemplateUnusedVariable TemplateProblem = "unused_variable"
	// TemplateInvalidVariableName means a variable name does not match ^[A-Za-z_][A-Za-z0-9_]{0,63}$.
	TemplateInvalidVariableName TemplateProblem = "invalid_variable_name"
	// TemplateDuplicateVariable means a name is declared twice, in one list or across both.
	TemplateDuplicateVariable TemplateProblem = "duplicate_variable"
	// TemplateTooManyVariables means more than MaxTemplateVariables are declared.
	TemplateTooManyVariables TemplateProblem = "too_many_variables"
	// TemplateUnsafeHTML means html/template's escaper refuses the e-mail body.
	TemplateUnsafeHTML TemplateProblem = "unsafe_html"
)

// TemplateError says why a template source or its declaration is rejected.
type TemplateError struct {
	Problem TemplateProblem
	// Detail is the offending construct, function or variable: either a fixed
	// construct name of this package or an identifier of the source reduced to
	// at most 64 characters of [A-Za-z0-9_]. It never carries other text of
	// the source or a parser message. It may be empty.
	Detail string
	// Line is the 1-based line of the source, 0 when unknown.
	Line int
}

func (e *TemplateError) Error() string {
	var b strings.Builder
	b.WriteString("template: ")
	b.WriteString(string(e.Problem))
	if e.Detail != "" {
		b.WriteString(" (" + e.Detail + ")")
	}
	if e.Line > 0 {
		b.WriteString(" at line " + strconv.Itoa(e.Line))
	}
	return b.String()
}

// ErrTemplateOutputTooLarge is returned by Render when the output passes MaxTemplateOutputBytes.
var ErrTemplateOutputTooLarge = errors.New("template: output larger than 512 KB")

// ErrTemplateExecution wraps an engine error during an execution that the
// walker should have made impossible. It indicates a bug, not bad input.
var ErrTemplateExecution = errors.New("template: execution of an accepted template failed")

// VariableProblem is the stable reason a render variable is refused.
type VariableProblem string

// The variable problems.
const (
	// VariableMissing means a required variable is absent.
	VariableMissing VariableProblem = "missing"
	// VariableUnknown means the template does not declare the variable.
	VariableUnknown VariableProblem = "unknown"
	// VariableInvalidType means the value is not a JSON string, number or boolean.
	VariableInvalidType VariableProblem = "invalid_type"
	// VariableTooLong means the converted value is over MaxTemplateValueChars characters.
	VariableTooLong VariableProblem = "too_long"
	// VariableInvalidCharacter means the value contains U+0000.
	VariableInvalidCharacter VariableProblem = "invalid_character"
)

// VariableError is one problem with one render variable.
type VariableError struct {
	Name    string
	Problem VariableProblem
}

// templateExecutor is the part of *text/template.Template and
// *html/template.Template that Render uses.
type templateExecutor interface {
	Execute(w io.Writer, data any) error
}

// Template is a compiled template. It is immutable and safe for concurrent
// Render calls. CompileTemplate is the only way to obtain one.
type Template struct {
	channel   Channel
	variables []string
	optional  []string
	declared  map[string]bool // name -> required
	exec      templateExecutor
}

// Channel returns the channel the template was compiled for.
func (t *Template) Channel() Channel { return t.channel }

// Variables returns a copy of the required variables, in declaration order.
func (t *Template) Variables() []string { return slices.Clone(t.variables) }

// OptionalVariables returns a copy of the optional variables, in declaration order.
func (t *Template) OptionalVariables() []string { return slices.Clone(t.optional) }

// CompileTemplate is the only way to obtain a *Template. It parses the body,
// applies the one-template rule, walks the tree (template_walk.go), checks the
// declaration, and executes the template once with every declared variable
// set to the empty string, which surfaces html/template's escaping errors.
// A rejection is a *TemplateError; any other error is a bug or an unknown
// channel.
func CompileTemplate(channel Channel, body string, variables, optionalVariables []string) (*Template, error) {
	if channel != ChannelSMS && channel != ChannelEmail && channel != ChannelPush {
		return nil, fmt.Errorf("domain: unknown template channel %q", channel)
	}
	if len(body) > MaxTemplateBodyBytes {
		return nil, &TemplateError{Problem: TemplateTooLarge}
	}
	// The body and its output are stored as Postgres text.
	if !utf8.ValidString(body) {
		return nil, &TemplateError{Problem: TemplateSyntax, Detail: constructInvalidUTF8, Line: lineAt(body, invalidUTF8Offset(body))}
	}
	if i := strings.IndexByte(body, 0); i >= 0 {
		return nil, &TemplateError{Problem: TemplateSyntax, Detail: constructNULCharacter, Line: lineAt(body, i)}
	}
	tree, fields, terr := parseTemplate(body)
	if terr != nil {
		return nil, terr
	}
	declared, terr := checkDeclaration(variables, optionalVariables, fields)
	if terr != nil {
		return nil, terr
	}
	var exec templateExecutor
	var err error
	if channel == ChannelEmail {
		base := htmltemplate.New(templateParseName).Funcs(htmltemplate.FuncMap(templateFuncs())).Option("missingkey=error")
		exec, err = base.AddParseTree(templateParseName, tree)
	} else {
		base := texttemplate.New(templateParseName).Funcs(texttemplate.FuncMap(templateFuncs())).Option("missingkey=error")
		exec, err = base.AddParseTree(templateParseName, tree)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrTemplateExecution, err)
	}
	t := &Template{
		channel:   channel,
		variables: slices.Clone(variables),
		optional:  slices.Clone(optionalVariables),
		declared:  declared,
		exec:      exec,
	}
	data := make(map[string]string, len(declared))
	for name := range declared {
		data[name] = ""
	}
	err = exec.Execute(&limitedWriter{max: MaxTemplateOutputBytes, discard: true}, data)
	var htmlErr *htmltemplate.Error
	switch {
	case err == nil:
		return t, nil
	case errors.As(err, &htmlErr):
		line := htmlErr.Line
		if htmlErr.Node != nil { // the node, when known, overrides Line
			line = lineAt(body, int(htmlErr.Node.Position()))
		}
		return nil, &TemplateError{Problem: TemplateUnsafeHTML, Detail: htmlErrorDetail(htmlErr.ErrorCode), Line: line}
	case errors.Is(err, ErrTemplateOutputTooLarge):
		return nil, &TemplateError{Problem: TemplateTooLarge, Detail: "output"}
	default:
		return nil, fmt.Errorf("%w: %w", ErrTemplateExecution, err)
	}
}

// checkDeclaration applies ADR-0008 §4 to the declared lists and the fields
// the body references. It returns the declared names, mapped to "required".
func checkDeclaration(variables, optional []string, fields []templateField) (map[string]bool, *TemplateError) {
	if len(variables)+len(optional) > MaxTemplateVariables {
		return nil, &TemplateError{Problem: TemplateTooManyVariables}
	}
	declared := make(map[string]bool, len(variables)+len(optional))
	for i, name := range slices.Concat(variables, optional) {
		if !ValidTemplateVariableName(name) {
			return nil, &TemplateError{Problem: TemplateInvalidVariableName, Detail: templateDetail(name)}
		}
		if _, dup := declared[name]; dup {
			return nil, &TemplateError{Problem: TemplateDuplicateVariable, Detail: name}
		}
		declared[name] = i < len(variables)
	}
	referenced := make(map[string]bool, len(fields))
	for _, f := range fields {
		if _, ok := declared[f.name]; !ok {
			return nil, &TemplateError{Problem: TemplateUndeclaredVariable, Detail: f.name, Line: f.line}
		}
		referenced[f.name] = true
	}
	for _, name := range slices.Concat(variables, optional) {
		if !referenced[name] {
			return nil, &TemplateError{Problem: TemplateUnusedVariable, Detail: name}
		}
	}
	return declared, nil
}

// Render converts the request's variables and executes the template.
//   - problems non-empty: the variables are wrong (the caller answers 422); content is "".
//   - err == ErrTemplateOutputTooLarge: the output passed MaxTemplateOutputBytes.
//   - any other err: unexpected; it wraps ErrTemplateExecution.
func (t *Template) Render(variables map[string]json.RawMessage) (content string, problems []VariableError, err error) {
	data := make(map[string]string, len(t.declared))
	for name, raw := range variables {
		if _, ok := t.declared[name]; !ok {
			problems = append(problems, VariableError{Name: name, Problem: VariableUnknown})
			continue
		}
		value, problem := convertTemplateValue(raw)
		if problem != "" {
			problems = append(problems, VariableError{Name: name, Problem: problem})
			continue
		}
		data[name] = value
	}
	for name, required := range t.declared {
		if _, ok := variables[name]; ok {
			continue
		}
		if required {
			problems = append(problems, VariableError{Name: name, Problem: VariableMissing})
		} else {
			data[name] = ""
		}
	}
	if len(problems) > 0 {
		slices.SortFunc(problems, func(a, b VariableError) int {
			if c := strings.Compare(a.Name, b.Name); c != 0 {
				return c
			}
			return strings.Compare(string(a.Problem), string(b.Problem))
		})
		return "", problems, nil
	}
	w := &limitedWriter{max: MaxTemplateOutputBytes}
	if err := t.exec.Execute(w, data); err != nil {
		if errors.Is(err, ErrTemplateOutputTooLarge) {
			return "", nil, ErrTemplateOutputTooLarge
		}
		return "", nil, fmt.Errorf("%w: %w", ErrTemplateExecution, err)
	}
	return w.buf.String(), nil, nil
}

// convertTemplateValue converts one JSON value to its string form, or names
// the problem. Numbers keep their literal text (so 0 is the text "0", which
// is not empty). true is "true" and false is the empty string: a template
// tests a variable with {{ if }}, which asks whether the string is empty.
func convertTemplateValue(raw json.RawMessage) (string, VariableProblem) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return "", VariableInvalidType
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return "", VariableInvalidType
	}
	var s string
	switch v := v.(type) {
	case string:
		s = v
	case json.Number:
		s = v.String()
	case bool:
		// false is the empty string (D40 as amended): {{ if .flag }} then
		// takes the else branch for a client that sent false. s stays "".
		if v {
			s = "true"
		}
	default: // null, object, array
		return "", VariableInvalidType
	}
	if strings.ContainsRune(s, 0) {
		return "", VariableInvalidCharacter
	}
	if utf8.RuneCountInString(s) > MaxTemplateValueChars {
		return "", VariableTooLong
	}
	return s, ""
}

// templateFuncs returns the FuncMap: exactly default and truncate. Pipelines
// pass the piped value as the last argument (ADR-0008 §3).
func templateFuncs() map[string]any {
	return map[string]any{
		funcDefault:  templateDefault,
		funcTruncate: templateTruncate,
	}
}

// templateDefault returns fallback when value is empty, else value.
func templateDefault(fallback, value string) string {
	if value == "" {
		return fallback
	}
	return value
}

// templateTruncate returns the first n characters of s, never splitting a rune.
func templateTruncate(n int, s string) string {
	if n <= 0 {
		return ""
	}
	count := 0
	for i := range s {
		if count == n {
			return s[:i]
		}
		count++
	}
	return s
}

// limitedWriter collects (or, with discard, only counts) at most max bytes.
type limitedWriter struct {
	buf     bytes.Buffer
	n       int
	max     int
	discard bool
}

func (w *limitedWriter) Write(p []byte) (int, error) {
	if len(p) > w.max-w.n {
		return 0, ErrTemplateOutputTooLarge
	}
	w.n += len(p)
	if !w.discard {
		w.buf.Write(p)
	}
	return len(p), nil
}

// htmlErrorDetail names an html/template error code with a fixed construct name.
func htmlErrorDetail(code htmltemplate.ErrorCode) string {
	switch code {
	case htmltemplate.ErrAmbigContext:
		return "ambiguous_url_context"
	case htmltemplate.ErrBadHTML:
		return "bad_html"
	case htmltemplate.ErrBranchEnd:
		return "branch_end_context"
	case htmltemplate.ErrEndContext:
		return "end_context"
	case htmltemplate.ErrPartialCharset:
		return "partial_charset"
	case htmltemplate.ErrPartialEscape:
		return "partial_escape"
	case htmltemplate.ErrSlashAmbig:
		return "ambiguous_slash"
	default:
		return "html_context"
	}
}

// invalidUTF8Offset returns the byte offset of the first invalid UTF-8 sequence in s.
func invalidUTF8Offset(s string) int {
	for i, r := range s {
		if r == utf8.RuneError {
			if _, size := utf8.DecodeRuneInString(s[i:]); size == 1 {
				return i
			}
		}
	}
	return 0
}
