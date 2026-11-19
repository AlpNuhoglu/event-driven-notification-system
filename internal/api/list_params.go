package api

import (
	"context"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/api/apigen"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
)

// Codes of errors[] for the query string of the listing (400).
const (
	// codeUnknownParameter: the operation does not define the parameter.
	codeUnknownParameter = "unknown_parameter"
	// codeDuplicateParameter: the parameter is given more than once.
	codeDuplicateParameter = "duplicate_parameter"
	// codeInvalidParameter: the value is empty or not valid for the parameter.
	codeInvalidParameter = "invalid_parameter"
	// codeInvalidCursor: the cursor does not decode.
	codeInvalidCursor = "invalid_cursor"
)

// The query parameters of GET /v1/notifications, in the order their errors
// are listed.
const (
	paramStatus      = "status"
	paramChannel     = "channel"
	paramPriority    = "priority"
	paramBatchID     = "batch_id"
	paramCreatedFrom = "created_from"
	paramCreatedTo   = "created_to"
	paramLimit       = "limit"
	paramCursor      = "cursor"
)

var listParams = []string{
	paramStatus, paramChannel, paramPriority, paramBatchID,
	paramCreatedFrom, paramCreatedTo, paramLimit, paramCursor,
}

// pathNotifications is the path of the listing (and of the single create).
const pathNotifications = "/v1/notifications"

// List page size: the default and the bounds of the limit parameter.
const (
	defaultListLimit = 50
	minListLimit     = store.MinListLimit
	maxListLimit     = store.MaxListLimit
)

// maxCursorChars bounds the cursor parameter (the contract's maxLength); a
// cursor is 32 characters, so a longer value cannot decode.
const maxCursorChars = 256

// listFilterKey carries the validated filter from the guard to the handler.
type listFilterKey struct{}

func withListFilter(ctx context.Context, f store.ListFilter) context.Context {
	return context.WithValue(ctx, listFilterKey{}, f)
}

func listFilterFrom(ctx context.Context) (store.ListFilter, bool) {
	f, ok := ctx.Value(listFilterKey{}).(store.ListFilter)
	return f, ok
}

// ListNotifications validates the query string before the handler runs: the
// generated wrapper has bound the typed parameters, but it ignores a
// parameter it does not know and takes the first of a repeated one. Every
// problem is a 400 with one entry of errors[] per parameter. The validated
// filter reaches the handler through the request context.
func (g createGuard) ListNotifications(w http.ResponseWriter, r *http.Request, params apigen.ListNotificationsParams) {
	f, errs := parseListQuery(r.URL.RawQuery)
	if len(errs) > 0 {
		writeProblem(w, r, listQueryProblem(errs))
		return
	}
	g.ServerInterface.ListNotifications(w, r.WithContext(withListFilter(r.Context(), f)), params)
}

// listQueryProblem is the 400 for the problems of a query string.
func listQueryProblem(errs []apigen.FieldError) *problem {
	return newProblem(kindMalformed, "The query string is invalid.", errs...)
}

// parseListQuery validates the raw query string of the listing and returns
// the filter it describes, or one error per problem: a parameter the
// operation does not define, a parameter given more than once, an invalid
// value, a cursor that does not decode. No message contains a submitted
// value; an unknown parameter's name is echoed (shortened) in field.
func parseListQuery(rawQuery string) (store.ListFilter, []apigen.FieldError) {
	f := store.ListFilter{Limit: defaultListLimit}
	var errs []apigen.FieldError
	q, err := url.ParseQuery(rawQuery)
	if err != nil {
		errs = append(errs, fieldError("", codeInvalidParameter, "the query string is not well formed"))
	}
	bad := func(name, code, message string) { errs = append(errs, fieldError(name, code, message)) }
	invalid := func(name, message string) { bad(name, codeInvalidParameter, message) }

	for _, name := range listParams {
		values, present := q[name]
		if !present {
			continue
		}
		if len(values) > 1 {
			bad(name, codeDuplicateParameter, name+" must be given at most once")
			continue
		}
		v := values[0]
		switch name {
		case paramStatus:
			if s := domain.Status(v); slices.Contains(domain.Statuses(), s) {
				f.Status = &s
			} else {
				invalid(name, "status must be one of: "+joinNames(domain.Statuses()))
			}
		case paramChannel:
			if c, err := domain.ParseChannel(v); err == nil {
				f.Channel = &c
			} else {
				invalid(name, "channel must be one of: "+joinNames(domain.Channels()))
			}
		case paramPriority:
			if p, err := domain.ParsePriority(v); err == nil {
				f.Priority = &p
			} else {
				invalid(name, "priority must be one of: high, normal, low")
			}
		case paramBatchID:
			if id, ok := parseUUID(v); ok {
				f.BatchID = &id
			} else {
				invalid(name, "batch_id must be a UUID")
			}
		case paramCreatedFrom:
			if t, ok := parseBound(v); ok {
				f.CreatedFrom = &t
			} else {
				invalid(name, "created_from must be an RFC 3339 date-time with an offset")
			}
		case paramCreatedTo:
			if t, ok := parseBound(v); ok {
				f.CreatedTo = &t
			} else {
				invalid(name, "created_to must be an RFC 3339 date-time with an offset")
			}
		case paramLimit:
			if n, ok := parseLimit(v); ok {
				f.Limit = n
			} else {
				invalid(name, "limit must be an integer from 1 to 200")
			}
		case paramCursor:
			switch {
			case v == "":
				invalid(name, "cursor must not be empty")
			case len(v) > maxCursorChars:
				bad(name, codeInvalidCursor, "cursor is not a cursor this API returned")
			default:
				if c, err := domain.DecodeCursor(v); err == nil {
					f.After = &c
				} else {
					bad(name, codeInvalidCursor, "cursor is not a cursor this API returned")
				}
			}
		}
	}

	errs = append(errs, unknownParameters(q, listParams)...)
	return f, errs
}

// joinNames lists the allowed values of an enum for a message.
func joinNames[T ~string](vs []T) string {
	names := make([]string, len(vs))
	for i, v := range vs {
		names[i] = string(v)
	}
	return strings.Join(names, ", ")
}

// parseUUID accepts the canonical 36-character form only.
func parseUUID(s string) (uuid.UUID, bool) {
	if len(s) != 36 {
		return uuid.UUID{}, false
	}
	id, err := uuid.Parse(s)
	return id, err == nil
}

// parseBound parses an RFC 3339 date-time with an offset (Z or ±hh:mm) and
// rounds it up to the next microsecond, the precision of created_at: a bound
// with finer digits then selects exactly the rows that the exact bound would
// (a row's created_at is a whole number of microseconds).
func parseBound(s string) (time.Time, bool) {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, false
	}
	t = t.UTC()
	if r := t.Truncate(time.Microsecond); r.Before(t) {
		t = r.Add(time.Microsecond)
	}
	return t, true
}

// parseLimit parses a decimal integer from minListLimit to maxListLimit.
func parseLimit(s string) (int, bool) {
	if s == "" || len(s) > 10 {
		return 0, false
	}
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
	}
	return n, n >= minListLimit && n <= maxListLimit
}

// listParameterProblem answers the generated wrapper's failure to bind a
// query parameter of the listing with the same problems the guard gives. It
// returns nil when the failure is not about the listing's query parameters.
func listParameterProblem(r *http.Request, bindErr *apigen.InvalidParamFormatError) *problem {
	if r.Method != http.MethodGet || r.URL.Path != pathNotifications || !slices.Contains(listParams, bindErr.ParamName) {
		return nil
	}
	_, errs := parseListQuery(r.URL.RawQuery)
	if len(errs) == 0 {
		// The wrapper refused a value that parseListQuery accepts.
		errs = []apigen.FieldError{fieldError(bindErr.ParamName, codeInvalidParameter, bindErr.ParamName+" is not valid")}
	}
	return listQueryProblem(errs)
}

// unknownParameters returns one unknown_parameter error for each parameter of
// q that is not in defined, sorted by name; the name is echoed, shortened.
func unknownParameters(q url.Values, defined []string) []apigen.FieldError {
	var unknown []string
	for name := range q {
		if !slices.Contains(defined, name) {
			unknown = append(unknown, name)
		}
	}
	slices.Sort(unknown)
	errs := make([]apigen.FieldError, len(unknown))
	for i, name := range unknown {
		errs[i] = fieldError(truncate(name, maxEchoedFieldName), codeUnknownParameter, "this parameter is not defined by the API")
	}
	return errs
}

// routeListNotifications is the mux pattern of the one operation that
// defines query parameters.
const routeListNotifications = http.MethodGet + " " + pathNotifications

// undefinedQueryProblem is the 400 for a request with a query string to an
// operation under /v1/ that defines no query parameter (every one but the
// two listings): a parameter the API does not define is rejected, never ignored,
// like an unknown field of a body. It returns nil when route is not such an
// operation or the query string is empty.
func undefinedQueryProblem(route, rawQuery string) *problem {
	if rawQuery == "" || route == routeListNotifications || route == routeListTemplates || !strings.Contains(route, " /v1/") {
		return nil
	}
	q, err := url.ParseQuery(rawQuery)
	var errs []apigen.FieldError
	if err != nil {
		errs = append(errs, fieldError("", codeInvalidParameter, "the query string is not well formed"))
	}
	if errs = append(errs, unknownParameters(q, nil)...); len(errs) == 0 {
		return nil
	}
	return listQueryProblem(errs)
}
