package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/api/apigen"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
)

// The codes of the template stage of a create (docs/errors.md).
const (
	codeTemplateNotFound        = "template_not_found"
	codeTemplateVersionNotFound = "template_version_not_found"
	codeTemplateInactive        = "template_inactive"
	codeTemplateChannelMismatch = "template_channel_mismatch"
	codeMissingVariable         = "missing_variable"
	codeUnknownVariable         = "unknown_variable"
)

// The members of a template use (the template member of a create, the body
// of a preview).
const (
	memberName      = "name"
	memberVersion   = "version"
	memberVariables = "variables"
)

// templateUse is a template named by a request, checked without the
// database: a valid name (empty for a preview, whose name is in the path),
// the pinned version (0: the latest one) and the variables as the raw JSON
// values the client sent. Render converts them itself, so a number keeps its
// literal text.
type templateUse struct {
	name    string
	version int
	vars    map[string]json.RawMessage // never nil
	// rawVars is the variables object as sent; nil when it was absent.
	rawVars json.RawMessage
}

// renderedTemplate is the result of the template stage of one notification:
// the content to store and its provenance.
type renderedTemplate struct {
	content string
	name    string
	version int32
	// vars is template_vars: the variables as sent, re-encoded with numbers
	// kept as their literal text; {} when none were sent.
	vars []byte
}

// checkTemplateMember checks the template member of a create request (not
// null, see validateNotificationMembers): an object whose members are name,
// version and variables. It returns nil when an error was added.
func checkTemplateMember(add addFunc, raw json.RawMessage) *templateUse {
	members, order, dups, ok := objectMembers(raw)
	if !ok {
		add(fieldTemplate, codeInvalidType, "template must be an object")
		return nil
	}
	use, ok := checkTemplateUse(add, members, order, dups, fieldTemplate+".", true)
	if !ok {
		return nil
	}
	return &use
}

// checkTemplateUse checks the members of a template use; every field of an
// error is prefix followed by the member's name. The members are checked in
// the order name (only withName), version, variables, then unknown members in
// the order they appear. ok is false when an error was added.
func checkTemplateUse(add addFunc, members map[string]json.RawMessage, order []string, dups map[string]bool,
	prefix string, withName bool,
) (use templateUse, ok bool) {
	failed := false
	addf := func(field, code, msg string) {
		failed = true
		add(field, code, msg)
	}
	known := []string{memberVersion, memberVariables}
	if withName {
		known = []string{memberName, memberVersion, memberVariables}
	}
	use.vars = map[string]json.RawMessage{}
	for _, m := range known {
		field := prefix + m
		if dups[m] {
			addf(field, codeDuplicateField, field+" must appear only once")
			continue
		}
		raw, present := members[m]
		switch m {
		case memberName:
			if !present {
				addf(field, codeRequired, field+" is required")
				continue
			}
			s, isString := stringValue(raw)
			switch {
			case !isString:
				addf(field, codeInvalidType, field+" must be a string")
			case !domain.ValidTemplateName(s):
				addf(field, codeInvalidValue, field+" must match ^[a-z0-9][a-z0-9_-]{0,63}$")
			default:
				use.name = s
			}
		case memberVersion:
			if !present {
				continue
			}
			v, code := templateVersion(raw)
			switch code {
			case codeInvalidType:
				addf(field, code, field+" must be an integer")
			case codeInvalidValue:
				addf(field, code, field+" must be an integer from 1 to 2147483647")
			default:
				use.version = v
			}
		case memberVariables:
			if !present {
				continue
			}
			vars, vorder, vdups, isObject := objectMembers(raw)
			if !isObject {
				addf(field, codeInvalidType, field+" must be an object")
				continue
			}
			reported := map[string]bool{}
			for _, k := range vorder {
				if vdups[k] && !reported[k] {
					reported[k] = true
					addf(field+"."+truncate(k, maxEchoedFieldName), codeDuplicateField, "a variable must appear only once")
				}
			}
			use.vars, use.rawVars = vars, raw
		}
	}
	reported := map[string]bool{}
	for _, m := range order {
		if isOneOf(known, m) || reported[m] {
			continue
		}
		reported[m] = true
		addf(prefix+truncate(m, maxEchoedFieldName), codeUnknownField, "this field is not defined by the API")
	}
	if failed {
		return templateUse{}, false
	}
	return use, true
}

// templateVersion reads a version: a JSON integer from 1 to the largest
// int4. code is invalid_type for any other JSON value (a string, a fraction,
// an exponent) and invalid_value for an integer out of range.
func templateVersion(raw json.RawMessage) (version int, code string) {
	s := string(raw)
	if s == "" || (s[0] != '-' && (s[0] < '0' || s[0] > '9')) || strings.ContainsAny(s, ".eE") {
		return 0, codeInvalidType
	}
	v, err := strconv.ParseInt(s, 10, 32)
	if err != nil || v < 1 {
		return 0, codeInvalidValue
	}
	return int(v), ""
}

// templateLoader is the part of the store that the template stage reads.
type templateLoader interface {
	GetTemplate(ctx context.Context, name string) (store.VersionedTemplate, error)
	GetTemplateVersion(ctx context.Context, name string, version int) (store.VersionedTemplate, error)
}

// templateCache holds the templates one request uses (ADR-0008 §8). It is
// keyed by name and pinned version (0 for the latest one) and holds what the
// load gave, an error included, and the compiled template once it is needed:
// 1,000 items naming one template cause one load and one compilation, and an
// unknown name is looked up once. Every item of the request that leaves the
// version out gets the same version. It lives for one request and is not
// safe for concurrent use; there is no process-wide cache.
type templateCache struct {
	repo    templateLoader
	timeout time.Duration
	entries map[templateKey]*templateEntry
}

type templateKey struct {
	name    string
	version int
}

// templateEntry is one load: the stored template, or the store's error.
type templateEntry struct {
	vt  store.VersionedTemplate
	err error

	compiled   bool
	tpl        *domain.Template
	compileErr error
}

func newTemplateCache(repo templateLoader, timeout time.Duration) *templateCache {
	return &templateCache{repo: repo, timeout: timeout, entries: map[templateKey]*templateEntry{}}
}

// load returns the template name at version (0: the latest one), reading it
// from the store on first use. The latest version, once loaded, also serves
// the requests that pin that version: versions are immutable.
func (c *templateCache) load(ctx context.Context, name string, version int) *templateEntry {
	key := templateKey{name, version}
	if e, ok := c.entries[key]; ok {
		return e
	}
	dbCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	e := &templateEntry{}
	if version == 0 {
		e.vt, e.err = c.repo.GetTemplate(dbCtx, name)
	} else {
		e.vt, e.err = c.repo.GetTemplateVersion(dbCtx, name, version)
	}
	c.entries[key] = e
	if version == 0 && e.err == nil {
		pinned := templateKey{name, e.vt.Version.Version}
		if _, ok := c.entries[pinned]; !ok {
			c.entries[pinned] = e
		}
	}
	return e
}

// compile compiles the stored body once (domain.CompileTemplate, which also
// runs the walker on a body read from the database).
func (e *templateEntry) compile() (*domain.Template, error) {
	if !e.compiled {
		e.compiled = true
		v := e.vt.Version
		e.tpl, e.compileErr = domain.CompileTemplate(e.vt.Channel, v.Body, v.Variables, v.OptionalVariables)
	}
	return e.tpl, e.compileErr
}

// renderFields names the fields of the errors of rendering: a create reports
// them under template, a preview at the top level of its body.
type renderFields struct {
	variables string // the prefix of a variable's field
	content   string // the field of the rendered text
}

var (
	createRenderFields  = renderFields{variables: fieldTemplate + "." + memberVariables + ".", content: fieldTemplate}
	previewRenderFields = renderFields{variables: memberVariables + ".", content: fieldContent}
)

// renderEntry runs the stages after the load on a stored template: it
// compiles the body, renders it with vars and validates the text as a
// content of the template's channel. errs are the field errors of a 422. A
// stored body that does not compile, or an unexpected error of the engine,
// is a 500 whose cause goes to the log line only.
func renderEntry(e *templateEntry, vars map[string]json.RawMessage, f renderFields) (content string, errs []apigen.FieldError, p *problem) {
	tpl, err := e.compile()
	if err != nil {
		return "", nil, internalProblem(fmt.Errorf("compile stored template: %w", err))
	}
	content, problems, err := tpl.Render(vars)
	switch {
	case len(problems) > 0:
		return "", variableErrors(problems, f.variables), nil
	case errors.Is(err, domain.ErrTemplateOutputTooLarge):
		return "", []apigen.FieldError{contentLimitError(e.vt.Channel, f.content)}, nil
	case err != nil:
		return "", nil, internalProblem(fmt.Errorf("render template: %w", err))
	}
	// The rendered text is checked as a content of the channel is.
	switch {
	case content == "":
		return "", []apigen.FieldError{fieldError(f.content, codeEmpty, "the rendered content is empty")}, nil
	case strings.ContainsRune(content, 0):
		return "", []apigen.FieldError{fieldError(f.content, codeInvalidCharacter, "the rendered content must not contain the character U+0000")}, nil
	case exceedsContentLimit(e.vt.Channel, content):
		return "", []apigen.FieldError{contentLimitError(e.vt.Channel, f.content)}, nil
	}
	return content, nil, nil
}

// variableErrors maps the variable problems of Render to field errors named
// prefix + the variable's name, cut to maxEchoedFieldName characters. A
// message names no value.
func variableErrors(problems []domain.VariableError, prefix string) []apigen.FieldError {
	out := make([]apigen.FieldError, 0, len(problems))
	for _, vp := range problems {
		field := prefix + truncate(vp.Name, maxEchoedFieldName)
		var code, msg string
		switch vp.Problem {
		case domain.VariableMissing:
			code, msg = codeMissingVariable, "the template requires this variable"
		case domain.VariableUnknown:
			code, msg = codeUnknownVariable, "the template does not declare this variable"
		case domain.VariableInvalidType:
			code, msg = codeInvalidType, "a variable must be a JSON string, number or boolean"
		case domain.VariableTooLong:
			code, msg = codeTooLong, fmt.Sprintf("a variable must be at most %d characters", domain.MaxTemplateValueChars)
		case domain.VariableInvalidCharacter:
			code, msg = codeInvalidCharacter, "a variable must not contain the character U+0000"
		default:
			code, msg = codeInvalidValue, "the variable is not valid"
		}
		out = append(out, fieldError(field, code, msg))
	}
	return out
}

// renderForCreate runs the template stage of one notification of channel
// (ADR-0008 §5 to §8): it loads the template use (through the request's
// cache), refuses an unknown name or version, an inactive template (also
// with a pinned version) and a template of another channel, then renders it
// and validates the text. errs are the notification's field errors (a 422,
// or the item's errors in a batch); p fails the whole request: a database
// failure (503 or 500) or a stored body that does not compile (500).
func (c *templateCache) renderForCreate(ctx context.Context, channel domain.Channel, use *templateUse) (r *renderedTemplate, errs []apigen.FieldError, p *problem) {
	nameField := fieldTemplate + "." + memberName
	e := c.load(ctx, use.name, use.version)
	switch {
	case errors.Is(e.err, store.ErrTemplateNotFound):
		return nil, []apigen.FieldError{fieldError(nameField, codeTemplateNotFound, "no template has this name")}, nil
	case errors.Is(e.err, store.ErrTemplateVersionNotFound):
		return nil, []apigen.FieldError{fieldError(fieldTemplate+"."+memberVersion, codeTemplateVersionNotFound, "the template has no such version")}, nil
	case e.err != nil:
		return nil, nil, storeProblem(e.err)
	case !e.vt.IsActive:
		return nil, []apigen.FieldError{fieldError(nameField, codeTemplateInactive, "the template is inactive")}, nil
	case e.vt.Channel != channel:
		return nil, []apigen.FieldError{fieldError(nameField, codeTemplateChannelMismatch, "the template is for another channel than the notification")}, nil
	}
	content, errs, p := renderEntry(e, use.vars, createRenderFields)
	if p != nil || len(errs) > 0 {
		return nil, errs, p
	}
	vars, err := encodeTemplateVars(use.rawVars)
	if err != nil {
		return nil, nil, internalProblem(err)
	}
	return &renderedTemplate{
		content: content,
		name:    e.vt.Name,
		version: int32Of(e.vt.Version.Version),
		vars:    vars,
	}, nil, nil
}

// encodeTemplateVars re-encodes the variables object as sent for
// template_vars: decoded with UseNumber, so a number keeps its literal text.
// No variables is {}.
func encodeTemplateVars(raw json.RawMessage) ([]byte, error) {
	if raw == nil {
		return []byte(`{}`), nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v map[string]any
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("decode template variables: %w", err)
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("encode template variables: %w", err)
	}
	return b, nil
}

// prepareBatch runs the template stage over the valid items of a batch that
// use a template, in request order, through one cache: an item that fails is
// rejected with its errors and the others go on. Then the dedupe keys are
// claimed among the items still valid, so an item that fails to render never
// claims its key. rejected stays in ascending order of index. A problem
// fails the whole request (a database failure, a stored body that does not
// compile).
func prepareBatch(ctx context.Context, c *templateCache, in batchInput) (batchInput, *problem) {
	in.rejected = slices.Clone(in.rejected)
	kept := make([]batchItem, 0, len(in.valid))
	for _, it := range in.valid {
		if it.template == nil {
			kept = append(kept, it)
			continue
		}
		r, errs, p := c.renderForCreate(ctx, domain.Channel(it.request.Channel), it.template)
		if p != nil {
			return batchInput{}, p
		}
		if len(errs) > 0 {
			in.reject(it.index, errs)
			continue
		}
		it.rendered = r
		kept = append(kept, it)
	}
	in.valid = kept
	claimDedupeKeys(&in)
	return in, nil
}

// internalProblem is a 500 whose cause is logged and never answered.
func internalProblem(cause error) *problem {
	return &problem{kind: kindInternal, detail: kindInternal.detail, cause: cause}
}
