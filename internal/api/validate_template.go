package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/api/apigen"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

// Field names of the template requests, in the order their errors are listed.
const (
	fieldName              = "name"
	fieldBody              = "body"
	fieldVariables         = "variables"
	fieldOptionalVariables = "optional_variables"
)

// codeTemplateExists is the code of the 409 of createTemplate.
const codeTemplateExists = "template_exists"

// The fields each template request defines. Any other field is rejected with
// unknown_field.
var (
	createTemplateFields        = []string{fieldName, fieldChannel, fieldBody, fieldVariables, fieldOptionalVariables}
	createTemplateVersionFields = []string{fieldBody, fieldVariables, fieldOptionalVariables}
)

// templateNameMessage is the message of an invalid template name.
const templateNameMessage = "name must match ^[a-z0-9][a-z0-9_-]{0,63}$"

// validateCreateTemplate checks the body of a createTemplate request
// completely, before the generated code decodes it: the shape of every field
// (400 when the body is not a JSON object, 422 listing every invalid field),
// and only when the shape is valid, the compilation of the body for the
// channel (domain.CompileTemplate), whose rejection is one more 422 error.
// No message contains a part of the submitted body.
func validateCreateTemplate(body []byte) (apigen.CreateTemplateRequest, *problem) {
	in, p := templateRequest(body, createTemplateFields)
	if p != nil {
		return apigen.CreateTemplateRequest{}, p
	}
	if p := compileProblem(in.channel, in.body, in.variables, in.optional); p != nil {
		return apigen.CreateTemplateRequest{}, p
	}
	return apigen.CreateTemplateRequest{
		Name: in.name, Channel: apigen.Channel(in.channel), Body: in.body,
		Variables: &in.variables, OptionalVariables: &in.optional,
	}, nil
}

// validateCreateTemplateVersion checks the body of a createTemplateVersion
// request like validateCreateTemplate. The channel of the template is not in
// the request: channelOf is called once the shape is valid, and a problem it
// returns (404 or 5xx) is returned as it is.
func validateCreateTemplateVersion(ctx context.Context, body []byte, channelOf func(context.Context) (domain.Channel, *problem)) (apigen.CreateTemplateVersionRequest, *problem) {
	in, p := templateRequest(body, createTemplateVersionFields)
	if p != nil {
		return apigen.CreateTemplateVersionRequest{}, p
	}
	channel, p := channelOf(ctx)
	if p != nil {
		return apigen.CreateTemplateVersionRequest{}, p
	}
	if p := compileProblem(channel, in.body, in.variables, in.optional); p != nil {
		return apigen.CreateTemplateVersionRequest{}, p
	}
	return apigen.CreateTemplateVersionRequest{
		Body: in.body, Variables: &in.variables, OptionalVariables: &in.optional,
	}, nil
}

// templateInput is the shape-valid content of a template request. The lists
// are never nil.
type templateInput struct {
	name      string
	channel   domain.Channel
	body      string
	variables []string
	optional  []string
}

// templateRequest checks the shape of a template request body whose members
// are among fields.
func templateRequest(body []byte, fields []string) (templateInput, *problem) {
	if p := checkBodyIsJSON(body); p != nil {
		return templateInput{}, p
	}
	members, order, dups, ok := objectMembers(body)
	if !ok {
		return templateInput{}, notAnObject()
	}
	var (
		in   = templateInput{variables: []string{}, optional: []string{}}
		errs []apigen.FieldError
	)
	add := func(field, code, msg string) { errs = append(errs, fieldError(field, code, msg)) }
	for _, name := range fields {
		if dups[name] {
			add(name, codeDuplicateField, name+" must appear only once")
			continue
		}
		raw, present := members[name]
		before := len(errs)
		switch name {
		case fieldName:
			checkTemplateName(add, raw, present)
			if len(errs) == before {
				in.name, _ = stringValue(raw)
			}
		case fieldChannel:
			checkEnum(add, name, raw, present, true, allowedChannels)
			if len(errs) == before {
				s, _ := stringValue(raw)
				in.channel = domain.Channel(s)
			}
		case fieldBody:
			checkText(add, name, raw, present, true, 0)
			if len(errs) == before {
				in.body, _ = stringValue(raw)
				if len(in.body) > domain.MaxTemplateBodyBytes {
					add(name, codeTooLong, fmt.Sprintf("body must be at most %d bytes", domain.MaxTemplateBodyBytes))
				}
			}
		case fieldVariables, fieldOptionalVariables:
			list, valid := stringList(add, name, raw, present)
			if valid && name == fieldVariables {
				in.variables = list
			} else if valid {
				in.optional = list
			}
		}
	}
	reported := map[string]bool{}
	for _, name := range order {
		if isOneOf(fields, name) || reported[name] {
			continue
		}
		reported[name] = true
		add(truncate(name, maxEchoedFieldName), codeUnknownField, "this field is not defined by the API")
	}
	if len(errs) > 0 {
		return templateInput{}, newProblem(kindValidation, fmt.Sprintf("%d field(s) are invalid.", len(errs)), errs...)
	}
	return in, nil
}

func isOneOf(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// checkTemplateName checks a required template name.
func checkTemplateName(add addFunc, raw json.RawMessage, present bool) {
	if !present {
		add(fieldName, codeRequired, "name is required")
		return
	}
	s, ok := stringValue(raw)
	if !ok {
		add(fieldName, codeInvalidType, "name must be a string")
		return
	}
	if !domain.ValidTemplateName(s) {
		add(fieldName, codeInvalidValue, templateNameMessage)
	}
}

// stringList checks an optional list of strings; an absent list is valid and
// empty. valid is false when an error was added.
func stringList(add addFunc, name string, raw json.RawMessage, present bool) (list []string, valid bool) {
	if !present {
		return []string{}, true
	}
	elems, isArray := arrayElements(raw, len(raw)+1)
	if !isArray {
		add(name, codeInvalidType, name+" must be an array of strings")
		return nil, false
	}
	list = make([]string, 0, len(elems))
	for _, e := range elems {
		s, isString := stringValue(e)
		if !isString {
			add(name, codeInvalidType, name+" must be an array of strings")
			return nil, false
		}
		list = append(list, s)
	}
	return list, true
}

// compileProblem compiles the template and turns the engine's rejection into
// a 422 problem with one field error. An error that is not a rejection (an
// unknown channel, a bug of the engine) is a 500 with the error as its cause;
// it never reaches a response body.
func compileProblem(channel domain.Channel, body string, variables, optional []string) *problem {
	_, err := domain.CompileTemplate(channel, body, variables, optional)
	if err == nil {
		return nil
	}
	var te *domain.TemplateError
	if !errors.As(err, &te) {
		return &problem{kind: kindInternal, detail: kindInternal.detail, cause: err}
	}
	return newProblem(kindValidation, "1 field(s) are invalid.", templateFieldError(te))
}

// templateFieldError maps a rejection of the engine to one field error: the
// code is the problem's own string, the field is variables for the problems
// of the declaration and body for every other one (template_too_large is
// body / too_long). The message is a fixed sentence that names the detail
// (already safe to echo) and the line, never the parser's text or the body.
func templateFieldError(te *domain.TemplateError) apigen.FieldError {
	field, code := fieldBody, string(te.Problem)
	var msg string
	switch te.Problem {
	case domain.TemplateTooLarge:
		code = codeTooLong
		if te.Detail == "output" {
			msg = fmt.Sprintf("body renders to more than %d bytes", domain.MaxTemplateOutputBytes)
		} else {
			msg = fmt.Sprintf("body must be at most %d bytes", domain.MaxTemplateBodyBytes)
		}
	case domain.TemplateSyntax:
		switch te.Detail {
		case "invalid_utf8":
			msg = "body must be valid UTF-8"
		case "nul_character":
			msg = "body must not contain the character U+0000"
		default:
			msg = "body is not a valid template"
		}
	case domain.TemplateConstructNotAllowed:
		msg = named("body uses a construct that is not allowed", te.Detail)
	case domain.TemplateFunctionNotAllowed:
		msg = named("body calls a function that is not allowed; only default and truncate are available", te.Detail)
	case domain.TemplateMultiple:
		msg = "body must define exactly one template"
	case domain.TemplateInvalidCall:
		msg = named("body calls a function with the wrong arguments", te.Detail)
	case domain.TemplateUndeclaredVariable:
		msg = named("body uses a variable that is not declared", te.Detail)
	case domain.TemplateUnsafeHTML:
		msg = named("body is HTML that cannot be escaped safely: a variable stands where no escaping is safe, or the body ends inside a tag, an attribute, a comment or a script", te.Detail)
	case domain.TemplateUnusedVariable:
		field = fieldVariables
		msg = named("a declared variable is not used by the body", te.Detail)
	case domain.TemplateInvalidVariableName:
		field = fieldVariables
		msg = named("a variable name must match ^[A-Za-z_][A-Za-z0-9_]{0,63}$", te.Detail)
	case domain.TemplateDuplicateVariable:
		field = fieldVariables
		msg = named("a variable is declared more than once", te.Detail)
	case domain.TemplateTooManyVariables:
		field = fieldVariables
		msg = fmt.Sprintf("at most %d variables can be declared", domain.MaxTemplateVariables)
	default:
		msg = "the template is not valid"
	}
	if te.Line > 0 {
		msg += " (line " + strconv.Itoa(te.Line) + ")"
	}
	return fieldError(field, code, msg)
}

// named appends the detail of a rejection to a fixed sentence; it is empty
// for some problems.
func named(sentence, detail string) string {
	if detail == "" {
		return sentence
	}
	return sentence + ": " + detail
}
