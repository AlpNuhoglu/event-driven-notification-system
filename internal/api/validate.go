package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/api/apigen"
)

// The stable code values of errors[] (docs/errors.md).
const (
	// 400: the body or a path parameter cannot be read.
	codeEmptyBody     = "empty_body"
	codeMalformedJSON = "malformed_json"
	codeNotAnObject   = "not_an_object"
	codeInvalidUUID   = "invalid_uuid"

	// 422: a field is invalid.
	codeRequired         = "required"
	codeInvalidType      = "invalid_type"
	codeInvalidValue     = "invalid_value"
	codeEmpty            = "empty"
	codeTooLong          = "too_long"
	codeInvalidCharacter = "invalid_character"
	codeUnknownField     = "unknown_field"
	codeDuplicateField   = "duplicate_field"
)

// maxRecipientChars is the longest recipient accepted in this phase, in
// characters (Unicode code points, as JSON Schema's maxLength counts them).
const maxRecipientChars = 512

// maxEchoedFieldName bounds the length of an unknown field's name when it is
// echoed in errors[].
const maxEchoedFieldName = 64

// Field names of the create request, in the order their errors are listed.
const (
	fieldChannel   = "channel"
	fieldRecipient = "recipient"
	fieldContent   = "content"
	fieldPriority  = "priority"
)

// createFields lists the fields the create request defines in this phase.
// Any other field is rejected with unknown_field: a client that sends a
// field of a later phase (scheduled_at, say) must not get a different
// behaviour than it asked for.
var createFields = []string{fieldChannel, fieldRecipient, fieldContent, fieldPriority}

// allowedChannels and allowedPriorities are the enum values of the request.
var (
	allowedChannels   = []string{"sms", "email", "push"}
	allowedPriorities = []string{"high", "normal", "low"}
)

// validateCreate checks a create request body completely, before the
// generated code decodes it. The generated decoder ignores unknown fields and
// does not enforce the schema, so this is the place where the request schema
// of api/openapi.yaml is enforced. It returns nil for a valid body, a 400
// problem when the body is not a JSON object, and a 422 problem that lists
// every invalid field otherwise. No message contains a submitted value.
func validateCreate(body []byte) *problem {
	if len(bytes.TrimSpace(body)) == 0 {
		return newProblem(kindMalformed, "The request body is empty.",
			fieldError("", codeEmptyBody, "the request body must be a JSON object"))
	}
	if !utf8.Valid(body) || !json.Valid(body) {
		return newProblem(kindMalformed, "The request body is not valid JSON.",
			fieldError("", codeMalformedJSON, "the request body must be valid UTF-8 JSON"))
	}
	fields, order, dups, ok := objectMembers(body)
	if !ok {
		return newProblem(kindMalformed, "The request body is not a JSON object.",
			fieldError("", codeNotAnObject, "the request body must be a JSON object"))
	}

	var errs []apigen.FieldError
	add := func(field, code, msg string) { errs = append(errs, fieldError(field, code, msg)) }

	for _, name := range createFields {
		if dups[name] {
			add(name, codeDuplicateField, name+" must appear only once")
			continue
		}
		raw, present := fields[name]
		switch name {
		case fieldChannel:
			checkEnum(add, name, raw, present, true, allowedChannels)
		case fieldPriority:
			checkEnum(add, name, raw, present, false, allowedPriorities)
		case fieldRecipient:
			checkText(add, name, raw, present, maxRecipientChars)
		case fieldContent:
			checkText(add, name, raw, present, 0)
		}
	}
	reported := map[string]bool{}
	for _, name := range order {
		if isCreateField(name) || reported[name] {
			continue
		}
		reported[name] = true
		echoed := truncate(name, maxEchoedFieldName)
		add(echoed, codeUnknownField, "this field is not defined by the API")
	}
	if len(errs) == 0 {
		return nil
	}
	return newProblem(kindValidation, fmt.Sprintf("%d field(s) are invalid.", len(errs)), errs...)
}

func isCreateField(name string) bool { return slices.Contains(createFields, name) }

// objectMembers splits a valid JSON document into the members of its top
// level object. ok is false when the document is not an object. order lists
// the member names as they appear; dups holds the names that appear more
// than once (encoding/json would silently keep the last value).
func objectMembers(body []byte) (fields map[string]json.RawMessage, order []string, dups map[string]bool, ok bool) {
	dec := json.NewDecoder(bytes.NewReader(body))
	tok, err := dec.Token()
	if err != nil {
		return nil, nil, nil, false
	}
	if d, isDelim := tok.(json.Delim); !isDelim || d != '{' {
		return nil, nil, nil, false
	}
	fields = map[string]json.RawMessage{}
	dups = map[string]bool{}
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return nil, nil, nil, false
		}
		key, isString := keyTok.(string)
		if !isString {
			return nil, nil, nil, false
		}
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return nil, nil, nil, false
		}
		if _, seen := fields[key]; seen {
			dups[key] = true
		}
		fields[key] = raw
		order = append(order, key)
	}
	return fields, order, dups, true
}

type addFunc func(field, code, message string)

// stringValue decodes a JSON string. ok is false for any other JSON type,
// null included.
func stringValue(raw json.RawMessage) (string, bool) {
	if len(raw) == 0 || raw[0] != '"' {
		return "", false
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", false
	}
	return s, true
}

func checkEnum(add addFunc, name string, raw json.RawMessage, present, required bool, allowed []string) {
	if !present {
		if required {
			add(name, codeRequired, name+" is required")
		}
		return
	}
	s, ok := stringValue(raw)
	if !ok {
		add(name, codeInvalidType, name+" must be a string")
		return
	}
	if slices.Contains(allowed, s) {
		return
	}
	add(name, codeInvalidValue, name+" must be one of: "+strings.Join(allowed, ", "))
}

// checkText checks a required, non-empty string without U+0000 (Postgres
// text cannot store it). maxChars of 0 means no length limit.
func checkText(add addFunc, name string, raw json.RawMessage, present bool, maxChars int) {
	if !present {
		add(name, codeRequired, name+" is required")
		return
	}
	s, ok := stringValue(raw)
	if !ok {
		add(name, codeInvalidType, name+" must be a string")
		return
	}
	switch {
	case s == "":
		add(name, codeEmpty, name+" must not be empty")
	case maxChars > 0 && utf8.RuneCountInString(s) > maxChars:
		add(name, codeTooLong, fmt.Sprintf("%s must be at most %d characters", name, maxChars))
	case strings.ContainsRune(s, 0):
		add(name, codeInvalidCharacter, name+" must not contain the character U+0000")
	}
}

func truncate(s string, maxRunes int) string {
	if utf8.RuneCountInString(s) <= maxRunes {
		return s
	}
	return string([]rune(s)[:maxRunes])
}
