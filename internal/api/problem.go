package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"mime"
	"net/http"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/api/apigen"
)

// problemTypeBase is the prefix of every problem type URL; the anchor names
// a section of docs/errors.md.
const problemTypeBase = "https://github.com/AlpNuhoglu/event-driven-notification-system/blob/main/docs/errors.md#"

// problemContentType is the media type of every error response (RFC 9457).
const problemContentType = "application/problem+json"

// maxReportedErrors bounds the errors[] list of a problem, so a body with a
// very large number of unknown fields cannot produce a larger response.
const maxReportedErrors = 50

// kind is one problem type: its status, the anchor of its section in
// docs/errors.md, its title and the detail used when no specific one is given.
type kind struct {
	status int
	anchor string
	title  string
	detail string
}

// The problem types. Each has a section in docs/errors.md.
var (
	kindMalformed = kind{
		http.StatusBadRequest, "malformed-request", "Malformed request",
		"The request cannot be read.",
	}
	kindNotFound = kind{
		http.StatusNotFound, "not-found", "Not found",
		"No resource exists at this path.",
	}
	kindMethodNotAllowed = kind{
		http.StatusMethodNotAllowed, "method-not-allowed", "Method not allowed",
		"This path does not support the request method; the Allow header lists the methods it supports.",
	}
	kindTooLarge = kind{
		http.StatusRequestEntityTooLarge, "payload-too-large", "Request body too large",
		"The request body is larger than the limit.",
	}
	kindUnsupportedMediaType = kind{
		http.StatusUnsupportedMediaType, "unsupported-media-type", "Unsupported media type",
		"The request body must be sent with Content-Type: application/json.",
	}
	kindValidation = kind{
		http.StatusUnprocessableEntity, "validation", "Validation failed",
		"The request has invalid fields.",
	}
	kindDuplicate = kind{
		http.StatusConflict, "duplicate", "Duplicate notification",
		"A notification with this dedupe_key already exists.",
	}
	kindIdempotencyKeyReused = kind{
		http.StatusUnprocessableEntity, "idempotency-key-reused", "Idempotency key reused",
		"This Idempotency-Key was already used with a different request.",
	}
	kindInternal = kind{
		http.StatusInternalServerError, "internal", "Internal server error",
		"An unexpected error occurred.",
	}
	kindUnavailable = kind{
		http.StatusServiceUnavailable, "unavailable", "Service unavailable",
		"The database is unavailable; the request can be retried.",
	}
)

// kindForStatus returns the problem type for a status code that a component
// outside the handlers (the router, the generated wrapper) wrote itself.
func kindForStatus(status int) kind {
	switch status {
	case http.StatusBadRequest:
		return kindMalformed
	case http.StatusNotFound:
		return kindNotFound
	case http.StatusMethodNotAllowed:
		return kindMethodNotAllowed
	case http.StatusRequestEntityTooLarge:
		return kindTooLarge
	case http.StatusUnsupportedMediaType:
		return kindUnsupportedMediaType
	case http.StatusUnprocessableEntity:
		return kindValidation
	case http.StatusServiceUnavailable:
		return kindUnavailable
	}
	return kindInternal
}

// problem is an error response. Handlers return it as an error; the strict
// server's error handler writes it. Its detail and field errors never contain
// a submitted value.
type problem struct {
	kind   kind
	detail string
	errors []apigen.FieldError
	// rejected lists the rejected items of a batch whose items were all
	// rejected: one entry per item, never truncated.
	rejected []apigen.RejectedItem
	// checks are the dependency checks of a failed readiness probe.
	checks []apigen.Check
	// cause is the underlying error of a 500 or 503. It is logged as a class
	// (see errorClass) and never written to the response.
	cause error
}

func (p *problem) Error() string {
	return fmt.Sprintf("%d %s", p.kind.status, p.kind.anchor)
}

func newProblem(k kind, detail string, errs ...apigen.FieldError) *problem {
	if detail == "" {
		detail = k.detail
	}
	return &problem{kind: k, detail: detail, errors: errs}
}

// fieldError builds one entry of errors[].
func fieldError(field, code, message string) apigen.FieldError {
	return apigen.FieldError{Field: field, Code: code, Message: message}
}

// duplicateFieldError builds the entry of errors[] for a dedupe_key that a
// notification already holds. existingID is nil when that notification
// released the key in the same instant; the member is then absent.
func duplicateFieldError(existingID *uuid.UUID) apigen.FieldError {
	e := fieldError(fieldDedupeKey, codeDuplicate, "a notification with this dedupe_key already exists")
	if existingID != nil {
		id := *existingID
		e.ExistingId = &id
	}
	return e
}

// body returns the problem document for a request with the given correlation id.
func (p *problem) body(correlationID string) apigen.Problem {
	doc := apigen.Problem{
		Type:          problemTypeBase + p.kind.anchor,
		Title:         p.kind.title,
		Status:        int32(p.kind.status), //nolint:gosec // HTTP status codes fit in int32
		Detail:        p.detail,
		CorrelationId: correlationID,
	}
	if len(p.errors) > 0 {
		errs := p.errors
		if len(errs) > maxReportedErrors {
			errs = errs[:maxReportedErrors]
		}
		doc.Errors = &errs
	}
	if len(p.rejected) > 0 {
		doc.Rejected = &p.rejected
	}
	if len(p.checks) > 0 {
		doc.Checks = &p.checks
	}
	return doc
}

// writeProblem writes p as application/problem+json. The X-Request-ID header
// is already set by the request middleware; the correlation id and the
// logged cause are taken from the request state.
func writeProblem(w http.ResponseWriter, r *http.Request, p *problem) {
	st := stateFrom(r.Context())
	if p.cause != nil {
		st.setCause(p.cause)
	}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(p.body(st.correlationID)); err != nil {
		// The document has only strings and integers; encoding cannot fail.
		buf.Reset()
	}
	h := w.Header()
	h.Del("Content-Length")
	h.Set("Content-Type", problemContentType)
	h.Set("Cache-Control", "no-store")
	w.WriteHeader(p.kind.status)
	_, _ = buf.WriteTo(w)
}

// isProblemContentType reports whether a Content-Type header value is
// application/problem+json.
func isProblemContentType(v string) bool {
	mt, _, err := mime.ParseMediaType(v)
	return err == nil && mt == problemContentType
}
