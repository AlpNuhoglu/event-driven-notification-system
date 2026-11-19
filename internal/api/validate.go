package api

import (
	"bytes"
	"cmp"
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

	// 422: content and template are both given.
	codeConflictsWithContent = "conflicts_with_content"

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
	fieldTemplate  = "template"
	fieldPriority  = "priority"
	fieldDedupeKey = "dedupe_key"
)

// fieldNotifications is the one field of the batch request.
const fieldNotifications = "notifications"

// createFields lists the fields the create request defines, in the order
// their errors are listed. Any other field is rejected with unknown_field: a
// client that sends a field of a later version must not get a different
// behaviour than it asked for. scheduled_at comes before expires_at, whose
// rule depends on it.
var createFields = []string{
	fieldChannel, fieldRecipient, fieldContent, fieldTemplate, fieldPriority,
	fieldScheduledAt, fieldExpiresAt, fieldDedupeKey,
}

// allowedChannels and allowedPriorities are the enum values of the request.
var (
	allowedChannels   = []string{"sms", "email", "push"}
	allowedPriorities = []string{"high", "normal", "low"}
)

// validateCreate checks a create request body completely and parses it. The
// generated decoder ignores unknown fields, does not enforce the schema and
// would decode a template variable's number as a float64, so this is the
// place where the request schema of api/openapi.yaml is enforced, followed
// by the rules of the channel (ADR-0006 section 5) with the phone check at
// level of rules.phone, and the rules of scheduled_at and expires_at
// against rules.schedule. What needs a stored template is checked later, by
// the template stage (renderForCreate). It returns the parsed request for a
// valid body, a 400 problem when the body is not a JSON object, and a 422
// problem that lists every invalid field otherwise. No message contains a
// submitted value.
func validateCreate(body []byte, rules createRules) (createRequest, *problem) {
	if p := checkBodyIsJSON(body); p != nil {
		return createRequest{}, p
	}
	fields, order, dups, ok := objectMembers(body)
	if !ok {
		return createRequest{}, notAnObject()
	}
	req, errs := validateNotificationMembers(fields, order, dups, rules)
	if len(errs) == 0 {
		return req, nil
	}
	return createRequest{}, newProblem(kindValidation, fmt.Sprintf("%d field(s) are invalid.", len(errs)), errs...)
}

// createRequest is one valid notification of a create request. request holds
// the decoded fields except the template: when the message is a template,
// request.Content is nil and template holds the template use with its
// variables as raw JSON. The template stage sets rendered.
type createRequest struct {
	request  apigen.CreateNotificationRequest
	template *templateUse
	rendered *renderedTemplate
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
// members (the result of objectMembers), and returns the parsed request or
// every invalid field: the fields of the request in the order channel,
// recipient, content, template, priority, scheduled_at, expires_at,
// dedupe_key, then unknown fields in the order they appear.
//
// recipient and content are checked in two steps: first the rules every
// channel shares (type, empty, length, U+0000), then, only when that step
// and the channel are valid, the rules of the channel (validateForChannel).
// So a missing or invalid channel gives no channel-specific error, and each
// field has at most one error.
//
// Exactly one of content and template is required; "template": null counts
// as absent. Neither is content / required, both is template /
// conflicts_with_content. The members of template are checked by
// checkTemplateMember; what needs the stored template is not checked here.
//
// scheduled_at and expires_at are checked by scheduleRules: "scheduled_at":
// null counts as absent, a scheduled_at within the tolerance is parsed as
// absent (stored as NULL), and the rule of expires_at applies only when
// scheduled_at is valid.
func validateNotificationMembers(fields map[string]json.RawMessage, order []string, dups map[string]bool, rules createRules) (createRequest, []apigen.FieldError) {
	var errs []apigen.FieldError
	add := func(field, code, msg string) { errs = append(errs, fieldError(field, code, msg)) }

	var out createRequest
	_, contentGiven := fields[fieldContent]
	templateRaw, templateGiven := fields[fieldTemplate]
	templateGiven = templateGiven && (dups[fieldTemplate] || !isJSONNull(templateRaw))

	var channel domain.Channel // "" unless valid
	phone := rules.phone
	sched := scheduleCheck{start: rules.schedule.now}
	for _, name := range createFields {
		if dups[name] {
			add(name, codeDuplicateField, name+" must appear only once")
			if name == fieldScheduledAt {
				sched.invalid = true
			}
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
				out.request.Channel = apigen.Channel(s)
			}
		case fieldPriority:
			checkEnum(add, name, raw, present, false, allowedPriorities)
			if len(errs) == before && present {
				s, _ := stringValue(raw)
				p := apigen.Priority(s)
				out.request.Priority = &p
			}
		case fieldRecipient:
			checkText(add, name, raw, present, true, maxRecipientChars)
			if len(errs) == before {
				s, _ := stringValue(raw)
				if channel != "" {
					validateForChannel(add, channel, name, s, phone)
				}
				out.request.Recipient = s
			}
		case fieldContent:
			if !present {
				if !templateGiven {
					add(name, codeRequired, "content or template is required")
				}
				continue
			}
			checkText(add, name, raw, present, true, 0)
			if len(errs) == before {
				s, _ := stringValue(raw)
				if channel != "" {
					validateForChannel(add, channel, name, s, phone)
				}
				out.request.Content = &s
			}
		case fieldTemplate:
			if !templateGiven {
				continue
			}
			if contentGiven {
				add(name, codeConflictsWithContent, "template must not be given together with content")
				continue
			}
			out.template = checkTemplateMember(add, raw)
		case fieldScheduledAt:
			out.request.ScheduledAt, sched = rules.schedule.checkScheduledAt(add, raw, present)
		case fieldExpiresAt:
			out.request.ExpiresAt = rules.schedule.checkExpiresAt(add, raw, present, sched)
		case fieldDedupeKey:
			checkText(add, name, raw, present, false, maxDedupeKeyChars)
			if len(errs) == before && present {
				s, _ := stringValue(raw)
				out.request.DedupeKey = &s
			}
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
	if len(errs) > 0 {
		return createRequest{}, errs
	}
	return out, nil
}

// isJSONNull reports whether a raw JSON value is null.
func isJSONNull(raw json.RawMessage) bool { return string(bytes.TrimSpace(raw)) == "null" }

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
	if exceedsContentLimit(channel, value) {
		e := contentLimitError(channel, name)
		add(e.Field, e.Code, e.Message)
	}
}

// exceedsContentLimit reports whether a content is over the limit of its
// channel (ADR-0006 section 5): more SMS segments than allowed, or more
// bytes than an e-mail or a push may have.
func exceedsContentLimit(channel domain.Channel, value string) bool {
	switch channel {
	case domain.ChannelSMS:
		return domain.CountSMS(value).Segments > domain.MaxSMSSegments
	case domain.ChannelEmail:
		return len(value) > domain.MaxEmailContentBytes
	case domain.ChannelPush:
		return len(value) > domain.MaxPushContentBytes
	}
	return false
}

// contentLimitError is the error of a content over the limit of its channel,
// reported on field: the content itself, or the template that rendered it.
func contentLimitError(channel domain.Channel, field string) apigen.FieldError {
	switch channel {
	case domain.ChannelSMS:
		return fieldError(field, codeTooManySegments, fmt.Sprintf("content needs more than %d SMS segments", domain.MaxSMSSegments))
	case domain.ChannelEmail:
		return fieldError(field, codeTooLong, fmt.Sprintf("content must be at most %d bytes for email", domain.MaxEmailContentBytes))
	default:
		return fieldError(field, codeTooLong, fmt.Sprintf("content must be at most %d bytes for push", domain.MaxPushContentBytes))
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
	index int
	createRequest
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
// a nil problem, and the caller decides what to answer. The dedupe keys are
// not claimed here: an item can still fail the template stage, and an
// invalid item never claims its key (prepareBatch, claimDedupeKeys). The
// work is linear in the size of the body. No message contains a submitted
// value. Every item is judged by the same rules, so at the same instant.
func validateBatch(body []byte, rules createRules) (batchInput, *problem) {
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
	for i, raw := range items {
		req, itemErrs := validateBatchItem(raw, rules)
		if len(itemErrs) > 0 {
			in.reject(i, itemErrs)
			continue
		}
		in.valid = append(in.valid, batchItem{index: i, createRequest: req})
	}
	return in, nil
}

// reject adds the item at index to the rejected ones with at most
// maxReportedErrors of its errors. The caller keeps rejected in ascending
// order of index (sortRejected).
func (in *batchInput) reject(index int, errs []apigen.FieldError) {
	if len(errs) > maxReportedErrors {
		errs = errs[:maxReportedErrors]
	}
	in.rejected = append(in.rejected, apigen.RejectedItem{Index: index, Errors: errs})
}

// usesTemplate reports whether a valid item renders a template.
func (in *batchInput) usesTemplate() bool {
	for _, it := range in.valid {
		if it.template != nil {
			return true
		}
	}
	return false
}

// claimDedupeKeys applies duplicate_in_batch to the valid items: the first
// one with a given dedupe_key stays valid and every later one is rejected.
// It runs after every other check of an item, the template stage included,
// so that an invalid item never claims its key (ADR-0006 section 6).
func claimDedupeKeys(in *batchInput) {
	claimed := map[string]bool{}
	kept := make([]batchItem, 0, len(in.valid))
	for _, it := range in.valid {
		if k := it.request.DedupeKey; k != nil {
			if claimed[*k] {
				in.reject(it.index, []apigen.FieldError{fieldError(fieldDedupeKey, codeDuplicateInBatch,
					"another item of this batch has the same dedupe_key")})
				continue
			}
			claimed[*k] = true
		}
		kept = append(kept, it)
	}
	in.valid = kept
	sortRejected(in.rejected)
}

// sortRejected puts rejected items in ascending order of index.
func sortRejected(r []apigen.RejectedItem) {
	slices.SortFunc(r, func(a, b apigen.RejectedItem) int { return cmp.Compare(a.Index, b.Index) })
}

// validateBatchItem checks one element of the notifications array.
func validateBatchItem(raw json.RawMessage, rules createRules) (createRequest, []apigen.FieldError) {
	fields, order, dups, ok := objectMembers(raw)
	if !ok {
		return createRequest{}, []apigen.FieldError{fieldError("", codeInvalidType, "each item must be a JSON object")}
	}
	return validateNotificationMembers(fields, order, dups, rules)
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
