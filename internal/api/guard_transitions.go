package api

import (
	"bytes"
	"fmt"
	"io"
	"net/http"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/api/apigen"
)

// The codes of cancel and redrive (docs/errors.md).
const (
	// 400: cancel takes no body.
	codeUnexpectedBody = "unexpected_body"

	// 409: the notification's state does not allow the operation.
	codeNotCancellable      = "not_cancellable"
	codeNotFailed           = "not_failed"
	codeExpiresAtRequired   = "expires_at_required"
	codeExpiresAtNotAllowed = "expires_at_not_allowed"

	// 422: the new expires_at is closer than the minimum lead.
	codeTooSoon = "too_soon"
)

// Field names of the errors of cancel and redrive.
const (
	fieldStatus    = "status"
	fieldExpiresAt = "expires_at"
)

// CancelNotification refuses an id that is not one and then a request with
// a body (400), then hands it on to the strict handler.
func (g createGuard) CancelNotification(w http.ResponseWriter, r *http.Request, id apigen.NotificationID, params apigen.CancelNotificationParams) {
	if !requirePathID(w, r, id) || !requireNoBody(w, r) {
		return
	}
	g.ServerInterface.CancelNotification(w, r, id, params)
}

// CancelBatch refuses an id that is not one and then a request with a body
// (400), then hands it on to the strict handler.
func (g createGuard) CancelBatch(w http.ResponseWriter, r *http.Request, id apigen.BatchID, params apigen.CancelBatchParams) {
	if !requirePathID(w, r, id) || !requireNoBody(w, r) {
		return
	}
	g.ServerInterface.CancelBatch(w, r, id, params)
}

// RedriveNotification answers a redrive itself: the strict handler's decoder
// ignores unknown and repeated fields, so the optional body is validated
// here (validateRedrive) and passed on parsed. An empty body (nothing but
// white space) gives no field and needs no Content-Type; a body that is not
// empty must be application/json (415). An id that is not one is refused
// first (400).
func (g createGuard) RedriveNotification(w http.ResponseWriter, r *http.Request, id apigen.NotificationID, params apigen.RedriveNotificationParams) {
	if !requirePathID(w, r, id) {
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeProblem(w, r, bodyReadProblem(err))
		return
	}
	var req apigen.RedriveRequest
	if len(bytes.TrimSpace(body)) > 0 {
		if !isJSONContentType(r.Header.Get("Content-Type")) {
			writeProblem(w, r, newProblem(kindUnsupportedMediaType, ""))
			return
		}
		var p *problem
		if req, p = validateRedrive(body); p != nil {
			writeProblem(w, r, p)
			return
		}
	}
	resp, err := g.h.RedriveNotification(r.Context(), apigen.RedriveNotificationRequestObject{
		Id: id, Params: params, Body: &req,
	})
	if err != nil {
		responseError(w, r, err)
		return
	}
	if err := resp.VisitRedriveNotificationResponse(w); err != nil {
		responseError(w, r, err)
	}
}

// requireNoBody reads the body within the limit (413) and writes a 400 when
// it holds anything but white space. It returns false when it wrote a
// problem.
func requireNoBody(w http.ResponseWriter, r *http.Request) bool {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeProblem(w, r, bodyReadProblem(err))
		return false
	}
	if len(bytes.TrimSpace(body)) > 0 {
		writeProblem(w, r, newProblem(kindMalformed, "This request takes no body.",
			fieldError("", codeUnexpectedBody, "the request must not have a body")))
		return false
	}
	return true
}

// validateRedrive checks a redrive body that is not empty, strictly: a JSON
// object (400 otherwise) whose only field is expires_at, given once, as an
// RFC 3339 date-time with an offset (422 otherwise, every invalid field
// listed). No message contains a submitted value.
func validateRedrive(body []byte) (apigen.RedriveRequest, *problem) {
	if p := checkBodyIsJSON(body); p != nil {
		return apigen.RedriveRequest{}, p
	}
	fields, order, dups, ok := objectMembers(body)
	if !ok {
		return apigen.RedriveRequest{}, notAnObject()
	}
	var (
		errs []apigen.FieldError
		req  apigen.RedriveRequest
	)
	add := func(field, code, msg string) { errs = append(errs, fieldError(field, code, msg)) }
	if raw, present := fields[fieldExpiresAt]; dups[fieldExpiresAt] {
		add(fieldExpiresAt, codeDuplicateField, fieldExpiresAt+" must appear only once")
	} else if present {
		s, isString := stringValue(raw)
		if !isString {
			add(fieldExpiresAt, codeInvalidType, fieldExpiresAt+" must be a string")
		} else if t, ok := parseDateTime(s); !ok {
			add(fieldExpiresAt, codeInvalidValue, fieldExpiresAt+" must be an RFC 3339 date-time with an offset")
		} else {
			req.ExpiresAt = &t
		}
	}
	reported := map[string]bool{}
	for _, name := range order {
		if name == fieldExpiresAt || reported[name] {
			continue
		}
		reported[name] = true
		add(truncate(name, maxEchoedFieldName), codeUnknownField, "this field is not defined by the API")
	}
	if len(errs) > 0 {
		return apigen.RedriveRequest{}, newProblem(kindValidation, fmt.Sprintf("%d field(s) are invalid.", len(errs)), errs...)
	}
	return req, nil
}
