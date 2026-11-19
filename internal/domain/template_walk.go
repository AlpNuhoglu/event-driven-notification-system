package domain

import (
	"strings"
	"text/template/parse"
	"unicode/utf8"
)

// This file is the security boundary of the template engine (ADR-0008 §2): it
// parses a template source and decides, by an allow-list, whether the source
// may be executed at all. Everything else about templates is in template.go.

// templateParseName is the name the source is parsed under. A source that
// defines a template of this name is still rejected by the one-template rule.
const templateParseName = "body"

// Construct names used as the Detail of a TemplateError. They are a fixed set,
// so a Detail never carries text of the source.
const (
	constructRange            = "range"
	constructWith             = "with"
	constructTemplate         = "template"
	constructBreak            = "break"
	constructContinue         = "continue"
	constructDefine           = "define"
	constructVariable         = "variable"
	constructDeclaration      = "variable_declaration"
	constructChainedField     = "chained_field"
	constructDot              = "dot"
	constructNil              = "nil"
	constructParenthesized    = "parenthesized_pipeline"
	constructOperandArgs      = "operand_with_arguments"
	constructOperandInPipe    = "operand_after_pipe"
	constructEmptyPipeline    = "empty_pipeline"
	constructNumber           = "number_literal"
	constructString           = "string_literal"
	constructUnknownNode      = "unknown_node"
	constructInvalidUTF8      = "invalid_utf8"
	constructNULCharacter     = "nul_character"
	funcDefault, funcTruncate = "default", "truncate"
)

// operandType is the static type of an operand or of a call's result.
type operandType int

const (
	typeString      operandType = iota + 1 // a field, a string literal, or a call
	typeInt                                // a plain number literal (see plainNumber)
	typeOtherNumber                        // any other number literal: never accepted
	typeBool                               // a boolean literal
)

// templateField is one variable the source references, at its first reference.
type templateField struct {
	name string
	line int
}

// parseTemplate parses body, applies the one-template rule and walks the tree.
// It returns the tree that was walked, which is the tree the caller must hand
// to the engine, and the referenced variables in order of first reference.
func parseTemplate(body string) (*parse.Tree, []templateField, *TemplateError) {
	tree := parse.New(templateParseName)
	// No function maps and SkipFuncCheck: the parser accepts every identifier
	// and the walker alone decides about function names.
	tree.Mode = parse.SkipFuncCheck
	treeSet := map[string]*parse.Tree{}
	parsed, err := tree.Parse(body, "", "", treeSet)
	if err != nil {
		return nil, nil, &TemplateError{Problem: TemplateSyntax, Line: parseErrorLine(err.Error())}
	}
	// One-template rule: the set holds exactly the tree the parser returned
	// for the top level. A define or block adds a tree; a define named like
	// the top level replaces it silently, and then the set's only tree is
	// not the one returned here.
	if len(treeSet) != 1 || treeSet[templateParseName] != parsed {
		return nil, nil, &TemplateError{Problem: TemplateMultiple, Detail: constructDefine, Line: otherTreeLine(body, treeSet, parsed)}
	}
	w := &templateWalker{body: body, seen: map[string]bool{}}
	if terr := w.list(parsed.Root); terr != nil {
		return nil, nil, terr
	}
	return parsed, w.fields, nil
}

// templateWalker checks a parse tree against this grammar, and nothing else:
//
//	list     := ( text | comment | action | if )*
//	action   := pipe
//	if       := pipe list [ else-list ]
//	pipe     := command ( "|" call )*       without variable declarations
//	command  := operand | call
//	operand  := field | string | number | bool
//	field    := "." name                    one identifier, ValidTemplateVariableName
//	string   := a literal whose value is valid UTF-8 without U+0000
//	number   := 0 | [1-9][0-9]{0,8}          plainNumber; it may not feed a call's string
//	call     := ("default" | "truncate") operand*   typed as described below
//
// Why an accepted template cannot execute anything outside default and
// truncate: the only node that makes the engine call a function is an
// identifier, and an identifier is accepted only as the first word of a
// command and only with one of those two names. Every other node type that
// reaches code or data (variables, dot, chains, nil, parenthesised pipelines,
// template calls, range, with) is rejected, and each type switch rejects in
// its default case, so a node type added by a later Go version is refused.
// The one-template rule guarantees that the walked tree is the only tree in
// the set and is the very object handed to the engine, so no unwalked tree can
// be reached; without range or template there is no loop and no recursion.
//
// Why it cannot fail when executed: the data is a map[string]string holding
// every declared variable, and every field is a single declared name, so a
// field always yields a string. Every call is checked statically against its
// Go signature (two arguments, counting the piped value; default takes two
// strings, truncate an integer literal that fits in int and a string), so
// argument conversion cannot fail and neither function can fail or panic. A
// number literal is plain decimal below one billion, which the engine prints
// as an int on every platform; a boolean prints as true or false. Literals
// never feed a later stage, and a later stage is always a call. What is left
// is html/template's escaper, whose errors are static and are surfaced by the
// trial execution in CompileTemplate, and the output limit, which Render
// reports as ErrTemplateOutputTooLarge.
type templateWalker struct {
	body   string
	fields []templateField
	seen   map[string]bool
}

func (w *templateWalker) list(l *parse.ListNode) *TemplateError {
	for _, n := range l.Nodes {
		if err := w.node(n); err != nil {
			return err
		}
	}
	return nil
}

func (w *templateWalker) node(n parse.Node) *TemplateError {
	switch n := n.(type) {
	case *parse.TextNode, *parse.CommentNode:
		return nil
	case *parse.ActionNode:
		return w.pipe(n.Pipe)
	case *parse.IfNode:
		if err := w.pipe(n.Pipe); err != nil {
			return err
		}
		if err := w.list(n.List); err != nil {
			return err
		}
		if n.ElseList != nil {
			return w.list(n.ElseList)
		}
		return nil
	case *parse.RangeNode:
		return w.reject(constructRange, n)
	case *parse.WithNode:
		return w.reject(constructWith, n)
	case *parse.TemplateNode:
		return w.reject(constructTemplate, n)
	case *parse.BreakNode:
		return w.reject(constructBreak, n)
	case *parse.ContinueNode:
		return w.reject(constructContinue, n)
	default:
		return w.reject(constructUnknownNode, n)
	}
}

func (w *templateWalker) pipe(p *parse.PipeNode) *TemplateError {
	if len(p.Decl) > 0 {
		return w.reject(constructDeclaration, p)
	}
	if len(p.Cmds) == 0 {
		return w.reject(constructEmptyPipeline, p)
	}
	piped, err := w.command(p.Cmds[0])
	if err != nil {
		return err
	}
	if piped == typeOtherNumber {
		return w.reject(constructNumber, p.Cmds[0])
	}
	for _, c := range p.Cmds[1:] {
		if !isCall(c) {
			return w.reject(constructOperandInPipe, c)
		}
		if piped, err = w.call(c, []operandType{piped}); err != nil {
			return err
		}
	}
	return nil
}

// command checks the first command of a pipe: a call or a single operand.
func (w *templateWalker) command(c *parse.CommandNode) (operandType, *TemplateError) {
	if isCall(c) {
		return w.call(c, nil)
	}
	if len(c.Args) != 1 {
		return 0, w.reject(constructOperandArgs, c)
	}
	return w.operand(c.Args[0])
}

// isCall reports whether the first word of c is an identifier (a function name).
func isCall(c *parse.CommandNode) bool {
	if len(c.Args) == 0 {
		return false
	}
	_, ok := c.Args[0].(*parse.IdentifierNode)
	return ok
}

// call checks a command whose first word is an identifier. piped is the type
// of the value piped in from the previous stage, if any; Go passes it as the
// last argument.
func (w *templateWalker) call(c *parse.CommandNode, piped []operandType) (operandType, *TemplateError) {
	ident := c.Args[0].(*parse.IdentifierNode)
	if ident.Ident != funcDefault && ident.Ident != funcTruncate {
		return 0, &TemplateError{Problem: TemplateFunctionNotAllowed, Detail: templateDetail(ident.Ident), Line: w.line(ident)}
	}
	var args []operandType
	for _, a := range c.Args[1:] {
		t, err := w.operand(a)
		if err != nil {
			return 0, err
		}
		args = append(args, t)
	}
	args = append(args, piped...)
	valid := len(args) == 2 && args[1] == typeString &&
		((ident.Ident == funcDefault && args[0] == typeString) ||
			(ident.Ident == funcTruncate && args[0] == typeInt))
	if !valid {
		return 0, &TemplateError{Problem: TemplateInvalidCall, Detail: ident.Ident, Line: w.line(ident)}
	}
	return typeString, nil
}

// operand checks one operand and returns its type. An identifier here is a
// function passed as an argument, which the engine would call.
func (w *templateWalker) operand(n parse.Node) (operandType, *TemplateError) {
	switch n := n.(type) {
	case *parse.FieldNode:
		if len(n.Ident) != 1 {
			return 0, w.reject(constructChainedField, n)
		}
		if !ValidTemplateVariableName(n.Ident[0]) {
			return 0, &TemplateError{Problem: TemplateInvalidVariableName, Detail: templateDetail(n.Ident[0]), Line: w.line(n)}
		}
		if !w.seen[n.Ident[0]] {
			w.seen[n.Ident[0]] = true
			w.fields = append(w.fields, templateField{name: n.Ident[0], line: w.line(n)})
		}
		return typeString, nil
	case *parse.StringNode:
		// The output is stored as Postgres text, which takes neither.
		if !utf8.ValidString(n.Text) || strings.ContainsRune(n.Text, 0) {
			return 0, w.reject(constructString, n)
		}
		return typeString, nil
	case *parse.NumberNode:
		if !plainNumber(n.Text) {
			return typeOtherNumber, nil
		}
		return typeInt, nil
	case *parse.BoolNode:
		return typeBool, nil
	case *parse.IdentifierNode:
		return 0, &TemplateError{Problem: TemplateFunctionNotAllowed, Detail: templateDetail(n.Ident), Line: w.line(n)}
	case *parse.VariableNode:
		return 0, w.reject(constructVariable, n)
	case *parse.ChainNode:
		return 0, w.reject(constructChainedField, n)
	case *parse.DotNode:
		return 0, w.reject(constructDot, n)
	case *parse.NilNode:
		return 0, w.reject(constructNil, n)
	case *parse.PipeNode:
		return 0, w.reject(constructParenthesized, n)
	default:
		return 0, w.reject(constructUnknownNode, n)
	}
}

// plainNumber reports whether s is "0" or 1 to 9 decimal digits without a
// leading zero: no sign, base prefix, exponent, fraction or rune literal.
func plainNumber(s string) bool {
	if s == "0" {
		return true
	}
	if len(s) == 0 || len(s) > 9 || s[0] == '0' {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func (w *templateWalker) reject(construct string, n parse.Node) *TemplateError {
	return &TemplateError{Problem: TemplateConstructNotAllowed, Detail: construct, Line: w.line(n)}
}

func (w *templateWalker) line(n parse.Node) int {
	return lineAt(w.body, int(n.Position()))
}

// lineAt returns the 1-based line of byte offset pos in body, 0 if out of range.
func lineAt(body string, pos int) int {
	if pos < 0 || pos > len(body) {
		return 0
	}
	return 1 + strings.Count(body[:pos], "\n")
}

// otherTreeLine returns the first line of a tree in set other than top, or 0.
func otherTreeLine(body string, set map[string]*parse.Tree, top *parse.Tree) int {
	line := 0
	for _, t := range set {
		if t == top || t.Root == nil {
			continue
		}
		if l := lineAt(body, int(t.Root.Position())); l > 0 && (line == 0 || l < line) {
			line = l
		}
	}
	return line
}

// parseErrorLine takes the line from a parser error, whose text starts with
// "template: body:LINE:". The rest of the message is never used.
func parseErrorLine(msg string) int {
	rest, ok := strings.CutPrefix(msg, "template: "+templateParseName+":")
	if !ok {
		return 0
	}
	line := 0
	for i := 0; i < len(rest) && rest[i] >= '0' && rest[i] <= '9' && line < 1e8; i++ {
		line = line*10 + int(rest[i]-'0')
	}
	return line
}

// templateDetail makes an identifier of the source safe to return: at most 64
// characters of [A-Za-z0-9_], every other character replaced by '_'.
func templateDetail(s string) string {
	var b strings.Builder
	for _, r := range s {
		if b.Len() == 64 {
			break
		}
		if r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	return b.String()
}
