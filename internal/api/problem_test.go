package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/puddle/v2"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/api/apigen"
)

func TestWriteProblem(t *testing.T) {
	r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/x", http.NoBody)
	w := httptest.NewRecorder()
	r, st := withRequestHeaders(w, r)
	w.Header().Set("Content-Length", "999")
	writeProblem(w, r, newProblem(kindValidation, "1 field(s) are invalid.",
		fieldError("channel", codeRequired, "channel is required")))

	if w.Code != http.StatusUnprocessableEntity {
		t.Errorf("status = %d", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Errorf("Content-Type = %q", ct)
	}
	if w.Header().Get("Content-Length") != "" {
		t.Error("stale Content-Length kept")
	}
	var got map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"type":           "https://github.com/AlpNuhoglu/event-driven-notification-system/blob/main/docs/errors.md#validation",
		"title":          "Validation failed",
		"status":         float64(422),
		"detail":         "1 field(s) are invalid.",
		"correlation_id": st.correlationID,
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %v, want %v", k, got[k], v)
		}
	}
	errs, ok := got["errors"].([]any)
	if !ok || len(errs) != 1 {
		t.Fatalf("errors = %v", got["errors"])
	}
	if e := errs[0].(map[string]any); e["field"] != "channel" || e["code"] != "required" || e["message"] != "channel is required" {
		t.Errorf("errors[0] = %v", e)
	}
}

func TestWriteProblem_NoErrorsMember(t *testing.T) {
	w := httptest.NewRecorder()
	writeProblem(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/x", http.NoBody), newProblem(kindNotFound, ""))
	if strings.Contains(w.Body.String(), `"errors"`) {
		t.Errorf("body has errors: %s", w.Body)
	}
	if !strings.Contains(w.Body.String(), kindNotFound.detail) {
		t.Errorf("default detail missing: %s", w.Body)
	}
}

// The cause of a 5xx is recorded for the log line, never written.
func TestWriteProblem_CauseNotWritten(t *testing.T) {
	r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/x", http.NoBody)
	w := httptest.NewRecorder()
	r, st := withRequestHeaders(w, r)
	p := storeProblem(&net.OpError{Op: "dial", Net: "tcp", Err: errors.New("10.0.0.1:5432: connect: connection refused")})
	writeProblem(w, r, p)
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d", w.Code)
	}
	if strings.Contains(w.Body.String(), "dial") || strings.Contains(w.Body.String(), "10.0.0.1") {
		t.Errorf("cause leaked: %s", w.Body)
	}
	if _, cause := st.snapshot(); cause == nil {
		t.Error("cause not recorded")
	}
}

func TestKindForStatus(t *testing.T) {
	for status, want := range map[int]kind{
		400: kindMalformed, 404: kindNotFound, 405: kindMethodNotAllowed, 413: kindTooLarge,
		415: kindUnsupportedMediaType, 422: kindValidation, 503: kindUnavailable, 500: kindInternal, 418: kindInternal,
	} {
		if got := kindForStatus(status); got != want {
			t.Errorf("kindForStatus(%d) = %v, want %v", status, got.anchor, want.anchor)
		}
	}
}

// Every problem type has a section in docs/errors.md whose heading produces
// its anchor.
func TestProblemTypesDocumented(t *testing.T) {
	doc := readErrorsDoc(t)
	for _, k := range []kind{
		kindMalformed, kindNotFound, kindMethodNotAllowed, kindTooLarge,
		kindUnsupportedMediaType, kindValidation, kindInternal, kindUnavailable,
		kindDuplicate, kindIdempotencyKeyReused, kindConflict,
	} {
		if !strings.Contains(doc, "\n## `"+k.anchor+"`\n") {
			t.Errorf("docs/errors.md has no section for %s", k.anchor)
		}
	}
	for _, code := range []string{
		codeEmptyBody, codeMalformedJSON, codeNotAnObject, codeInvalidUUID, codeRequired, codeInvalidType,
		codeInvalidValue, codeEmpty, codeTooLong, codeInvalidCharacter, codeUnknownField, codeDuplicateField,
		codeTooManyItems, codeDuplicate, codeDuplicateInBatch, codeInvalidIdempotencyKey, codeIdempotencyKeyReused,
		codeInvalidE164, codeInvalidEmail, codeInvalidToken, codeTooManySegments,
		codeUnexpectedBody, codeNotCancellable, codeNotFailed, codeExpiresAtRequired, codeExpiresAtNotAllowed, codeTooSoon,
		codeUnknownParameter, codeDuplicateParameter, codeInvalidParameter, codeInvalidCursor,
		codeConflictsWithContent, codeTemplateNotFound, codeTemplateVersionNotFound, codeTemplateInactive,
		codeTemplateChannelMismatch, codeMissingVariable, codeUnknownVariable, codeInThePast, codeTooFarAhead,
	} {
		if !strings.Contains(doc, "`"+code+"`") {
			t.Errorf("docs/errors.md does not list code %s", code)
		}
	}
}

func TestDBUnavailable(t *testing.T) {
	pg := func(code string) error {
		return fmt.Errorf("create notification: %w", &pgconn.PgError{Code: code, Message: "m"})
	}
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"connection failure", pg("08006"), true},
		{"admin shutdown", pg("57P01"), true},
		{"database does not exist", pg("3D000"), true},
		{"too many connections", pg("53300"), true},
		{"auth failed", pg("28P01"), true},
		{"unique violation", pg("23505"), false},
		{"invalid text", pg("22P02"), false},
		{"closed pool", fmt.Errorf("get: %w", puddle.ErrClosedPool), true},
		{"deadline", fmt.Errorf("get: %w", context.DeadlineExceeded), true},
		{"other", errors.New("scan: bad column"), false},
		{"nil", nil, false},
	}
	for _, tt := range tests {
		if got := dbUnavailable(tt.err); got != tt.want {
			t.Errorf("%s: dbUnavailable = %v, want %v", tt.name, got, tt.want)
		}
	}
}

// A Postgres error is logged as its SQLSTATE only: its message can quote input.
func TestErrorClass(t *testing.T) {
	err := fmt.Errorf("x: %w", &pgconn.PgError{Code: "22P02", Message: `invalid input syntax: "+905551234567"`})
	if got := errorClass(err); got != "postgres error, SQLSTATE 22P02" {
		t.Errorf("errorClass = %q", got)
	}
}

// A problem writes rejected only when it has some, and an entry for a
// duplicate carries existing_id only when the id is known.
func TestWriteProblem_RejectedAndExistingID(t *testing.T) {
	id := uuid.MustParse("0193f0d2-7c1e-7b3a-9d41-5be0a1c2e3f4")
	p := newProblem(kindValidation, "", duplicateFieldError(&id))
	p.rejected = []apigen.RejectedItem{{Index: 3, Errors: []apigen.FieldError{duplicateFieldError(nil)}}}
	doc := p.body("c")
	if doc.Rejected == nil || len(*doc.Rejected) != 1 || (*doc.Rejected)[0].Index != 3 {
		t.Fatalf("rejected = %v", doc.Rejected)
	}
	e := (*doc.Errors)[0]
	if e.Code != codeDuplicate || e.Field != fieldDedupeKey || e.ExistingId == nil || *e.ExistingId != id {
		t.Errorf("errors[0] = %+v", e)
	}
	if (*doc.Rejected)[0].Errors[0].ExistingId != nil {
		t.Error("existing_id set without an id")
	}
	if doc := newProblem(kindNotFound, "").body("c"); doc.Rejected != nil {
		t.Error("rejected present on a problem without items")
	}
}
