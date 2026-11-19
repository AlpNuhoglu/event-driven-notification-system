package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/api/apigen"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
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

	// 422: a field is invalid for the notification's channel.
	codeInvalidE164     = string(domain.RecipientNotE164)
	codeInvalidEmail    = string(domain.RecipientInvalidEmail)
	codeInvalidToken    = string(domain.RecipientInvalidToken)
	codeTooManySegments = "too_many_segments"

	// 422: a batch is invalid as a whole, or one of its items repeats another.
	codeTooManyItems     = "too_many_items"
	codeDuplicateInBatch = "duplicate_in_batch"

	// 409: a dedupe_key is already held.
	codeDuplicate = "duplicate"
)

// Codes of the Idempotency-Key header and the key's reuse.
const (
	// 400: the header cannot be read.
	codeInvalidIdempotencyKey = "invalid_idempotency_key"

	// 422: the key was already used with a different request.
	codeIdempotencyKeyReused = "idempotency_key_reused"
)

// maxRecipientChars is the longest recipient of any channel, in characters
// (Unicode code points, as JSON Schema's maxLength counts them): the longest
// push token. Each channel then checks its own form (validateForChannel).
const maxRecipientChars = domain.MaxPushTokenChars

// maxDedupeKeyChars is the longest dedupe_key, in characters.
const maxDedupeKeyChars = 255

// maxBatchItems is the largest number of notifications in one batch request.
const maxBatchItems = 1000

// maxEchoedFieldName bounds the length of an unknown field's name when it is
// echoed in errors[].
const maxEchoedFieldName = 64

// Field names of the create request, in the order their errors are listed.
const (
	fieldChannel   = "channel"
	fieldRecipient = "recipient"
	fieldContent   = "content"
	fieldPriority  = "priority"
	fieldDedupeKey = "dedupe_key"
)

// fieldNotifications is the one field of the batch request.
const fieldNotifications = "notifications"

// createFields lists the fields the create request defines in this phase.
// Any other field is rejected with unknown_field: a client that sends a
// field of a later phase (scheduled_at, say) must not get a different
// behaviour than it asked for.
var createFields = []string{fieldChannel, fieldRecipient, fieldContent, fieldPriority, fieldDedupeKey}

// allowedChannels and allowedPriorities are the enum values of the request.
var (
	allowedChannels   = []string{"sms", "email", "push"}
	allowedPriorities = []string{"high", "normal", "low"}
)

// validateCreate checks a create request body completely, before the
// generated code decodes it. The generated decoder ignores unknown fields and
// does not enforce the schema, so this is the place where the request schema
// of api/openapi.yaml is enforced, followed by the rules of the channel
// (ADR-0006 section 5) with the phone check at level phone. It returns nil
// for a valid body, a 400 problem when the body is not a JSON object, and a
// 422 problem that lists every invalid field otherwise. No message contains
// a submitted value.
func validateCreate(body []byte, phone domain.PhoneValidation) *problem {
	if p := checkBodyIsJSON(body); p != nil {
		return p
	}
	fields, order, dups, ok := objectMembers(body)
	if !ok {
		return notAnObject()
	}
	errs := validateNotificationMembers(fields, order, dups, phone)
	if len(errs) == 0 {
		return nil
	}
	return newProblem(kindValidation, fmt.Sprintf("%d field(s) are invalid.", len(errs)), errs...)
}

// checkBodyIsJSON returns the 400 problem for an empty body or a body that is
// not valid UTF-8 JSON, and nil otherwise.
func checkBodyIsJSON(body []byte) *problem {
	if len(bytes.TrimSpace(body)) == 0 {
		return newProblem(kindMalformed, "The request body is empty.",
			fieldError("", codeEmptyBody, "the request body must be a JSON object"))
	}
	if !utf8.Valid(body) || !json.Valid(body) {
		return newProblem(kindMalformed, "The request body is not valid JSON.",
			fieldError("", codeMalformedJSON, "the request body must be valid UTF-8 JSON"))
	}
	return nil
}

func notAnObject() *problem {
	return newProblem(kindMalformed, "The request body is not a JSON object.",
		fieldError("", codeNotAnObject, "the request body must be a JSON object"))
}

// validateNotificationMembers checks one notification object, given as its
// members (the result of objectMembers), and returns every invalid field:
// the fields of the request in the order channel, recipient, content,
// priority, dedupe_key, then unknown fields in the order they appear.
//
// recipient and content are checked in two steps: first the rules every
// channel shares (type, empty, length, U+0000), then, only when that step
// and the channel are valid, the rules of the channel (validateForChannel).
// So a missing or invalid channel gives no channel-specific error, and each
// field has at most one error.
func validateNotificationMembers(fields map[string]json.RawMessage, order []string, dups map[string]bool, phone domain.PhoneValidation) []apigen.FieldError {
	var errs []apigen.FieldError
	add := func(field, code, msg string) { errs = append(errs, fieldError(field, code, msg)) }

	var channel domain.Channel // "" unless valid
	for _, name := range createFields {
		if dups[name] {
			add(name, codeDuplicateField, name+" must appear only once")
			continue
		}
		raw, present := fields[name]
		before := len(errs)
		switch name {
		case fieldChannel:
			checkEnum(add, name, raw, present, true, allowedChannels)
			if len(errs) == before {
				s, _ := stringValue(raw)
				channel = domain.Channel(s)
			}
		case fieldPriority:
			checkEnum(add, name, raw, present, false, allowedPriorities)
		case fieldRecipient, fieldContent:
			limit := 0
			if name == fieldRecipient {
				limit = maxRecipientChars
			}
			checkText(add, name, raw, present, true, limit)
			if len(errs) == before && channel != "" {
				s, _ := stringValue(raw)
				validateForChannel(add, channel, name, s, phone)
			}
		case fieldDedupeKey:
			checkText(add, name, raw, present, false, maxDedupeKeyChars)
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
	return errs
}

// The messages of the recipient check per channel; none contains the value.
var recipientMessages = map[domain.RecipientProblem]string{
	domain.RecipientNotE164:      "recipient must be an E.164 number such as +905551234567",
	domain.RecipientInvalidEmail: "recipient must be a single e-mail address such as user@example.com",
	domain.RecipientInvalidToken: "recipient must be a device token of 1 to 4096 characters without control characters",
}

// validateForChannel applies the rules of the channel (ADR-0006 section 5)
// to a recipient or content that passed the shared checks: the form of the
// recipient, and the size of the content (SMS segments, bytes for e-mail and
// push).
func validateForChannel(add addFunc, channel domain.Channel, name, value string, phone domain.PhoneValidation) {
	if name == fieldRecipient {
		if p := domain.ValidateRecipient(channel, value, phone); p != "" {
			add(name, string(p), recipientMessages[p])
		}
		return
	}
	switch channel {
	case domain.ChannelSMS:
		if domain.CountSMS(value).Segments > domain.MaxSMSSegments {
			add(name, codeTooManySegments, fmt.Sprintf("content needs more than %d SMS segments", domain.MaxSMSSegments))
		}
	case domain.ChannelEmail:
		if len(value) > domain.MaxEmailContentBytes {
			add(name, codeTooLong, fmt.Sprintf("content must be at most %d bytes for email", domain.MaxEmailContentBytes))
		}
	case domain.ChannelPush:
		if len(value) > domain.MaxPushContentBytes {
			add(name, codeTooLong, fmt.Sprintf("content must be at most %d bytes for push", domain.MaxPushContentBytes))
		}
	}
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

// checkText checks a non-empty string without U+0000 (Postgres text cannot
// store it); an absent optional field is valid. maxChars of 0 means no length
// limit.
func checkText(add addFunc, name string, raw json.RawMessage, present, required bool, maxChars int) {
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

// batchItem is one valid item of a batch: its position in the request's
// array and the parsed request.
type batchItem struct {
	index   int
	request apigen.CreateNotificationRequest
}

// batchInput is the result of validating a batch body whose top level is
// valid. total is the number of items in the request; valid and rejected
// partition them, and rejected is in ascending order of index.
type batchInput struct {
	total    int
	valid    []batchItem
	rejected []apigen.RejectedItem
}

// validateBatch checks a batch request body. It returns a 400 problem when
// the body cannot be read, a 422 problem when the top level is invalid (no
// item is checked then), and otherwise the items sorted into valid and
// rejected ones: a batch whose items are all rejected is still returned with
// a nil problem, and the caller decides what to answer. Among the valid
// items, the first one with a given dedupe_key stays valid and every later
// one is rejected; an invalid item never claims its key. The work is linear
// in the size of the body. No message contains a submitted value.
func validateBatch(body []byte, phone domain.PhoneValidation) (batchInput, *problem) {
	if p := checkBodyIsJSON(body); p != nil {
		return batchInput{}, p
	}
	fields, order, dups, ok := objectMembers(body)
	if !ok {
		return batchInput{}, notAnObject()
	}

	var errs []apigen.FieldError
	add := func(field, code, msg string) { errs = append(errs, fieldError(field, code, msg)) }
	var items []json.RawMessage
	switch raw, present := fields[fieldNotifications]; {
	case dups[fieldNotifications]:
		add(fieldNotifications, codeDuplicateField, fieldNotifications+" must appear only once")
	case !present:
		add(fieldNotifications, codeRequired, fieldNotifications+" is required")
	default:
		var isArray bool
		if items, isArray = arrayElements(raw, maxBatchItems+1); !isArray {
			add(fieldNotifications, codeInvalidType, fieldNotifications+" must be an array")
		} else if len(items) == 0 {
			add(fieldNotifications, codeEmpty, fieldNotifications+" must not be empty")
		} else if len(items) > maxBatchItems {
			add(fieldNotifications, codeTooManyItems, fmt.Sprintf("%s must have at most %d items", fieldNotifications, maxBatchItems))
		}
	}
	reported := map[string]bool{}
	for _, name := range order {
		if name == fieldNotifications || reported[name] {
			continue
		}
		reported[name] = true
		add(truncate(name, maxEchoedFieldName), codeUnknownField, "this field is not defined by the API")
	}
	if len(errs) > 0 {
		return batchInput{}, newProblem(kindValidation, fmt.Sprintf("%d field(s) are invalid.", len(errs)), errs...)
	}

	in := batchInput{total: len(items)}
	claimed := map[string]bool{}
	for i, raw := range items {
		itemErrs := validateBatchItem(raw, phone)
		var req apigen.CreateNotificationRequest
		if len(itemErrs) == 0 {
			// The item passed validation, so it decodes.
			if err := json.Unmarshal(raw, &req); err != nil {
				itemErrs = []apigen.FieldError{fieldError("", codeInvalidType, "each item must be a JSON object")}
			} else if req.DedupeKey != nil {
				if claimed[*req.DedupeKey] {
					itemErrs = []apigen.FieldError{fieldError(fieldDedupeKey, codeDuplicateInBatch,
						"another item of this batch has the same dedupe_key")}
				} else {
					claimed[*req.DedupeKey] = true
				}
			}
		}
		if len(itemErrs) > 0 {
			if len(itemErrs) > maxReportedErrors {
				itemErrs = itemErrs[:maxReportedErrors]
			}
			in.rejected = append(in.rejected, apigen.RejectedItem{Index: i, Errors: itemErrs})
			continue
		}
		in.valid = append(in.valid, batchItem{index: i, request: req})
	}
	return in, nil
}

// validateBatchItem checks one element of the notifications array.
func validateBatchItem(raw json.RawMessage, phone domain.PhoneValidation) []apigen.FieldError {
	fields, order, dups, ok := objectMembers(raw)
	if !ok {
		return []apigen.FieldError{fieldError("", codeInvalidType, "each item must be a JSON object")}
	}
	return validateNotificationMembers(fields, order, dups, phone)
}

// arrayElements splits a JSON array into its raw elements, stopping after
// limit elements. ok is false when the value is not an array (null included).
func arrayElements(raw json.RawMessage, limit int) (elems []json.RawMessage, ok bool) {
	if len(raw) == 0 || raw[0] != '[' {
		return nil, false
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	if _, err := dec.Token(); err != nil {
		return nil, false
	}
	for dec.More() && len(elems) < limit {
		var e json.RawMessage
		if err := dec.Decode(&e); err != nil {
			return nil, false
		}
		elems = append(elems, e)
	}
	return elems, true
}
