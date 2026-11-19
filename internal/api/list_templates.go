package api

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"net/url"
	"slices"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/api/apigen"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

// pathTemplates is the path of the template listing (and of createTemplate).
const pathTemplates = "/v1/templates"

// routeListTemplates is the mux pattern of the template listing.
const routeListTemplates = http.MethodGet + " " + pathTemplates

// templateListParams are the query parameters of GET /v1/templates, in the
// order their errors are listed.
var templateListParams = []string{paramLimit, paramCursor}

// templateListQuery is the validated query of the template listing.
type templateListQuery struct {
	// After is the name the page continues after; empty for the first page.
	After string
	Limit int
}

type templateListQueryKey struct{}

func templateListQueryFrom(ctx context.Context) (templateListQuery, bool) {
	q, ok := ctx.Value(templateListQueryKey{}).(templateListQuery)
	return q, ok
}

// encodeTemplateCursor is the opaque cursor of a page that ends with the
// template name: its base64url encoding without padding.
func encodeTemplateCursor(name string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(name))
}

// decodeTemplateCursor returns the name a cursor stands for. It accepts only
// what encodeTemplateCursor returns: a valid template name that encodes back
// to exactly the string that was sent.
func decodeTemplateCursor(s string) (string, bool) {
	if s == "" || len(s) > maxCursorChars {
		return "", false
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(s)
	if err != nil {
		return "", false
	}
	name := string(raw)
	if !domain.ValidTemplateName(name) || encodeTemplateCursor(name) != s {
		return "", false
	}
	return name, true
}

// parseTemplateListQuery validates the raw query string of the template
// listing with the rules and the codes of the notification listing: an
// undefined parameter, a parameter given twice, a limit that is not an
// integer from 1 to 200 and a cursor that does not decode are errors; nothing
// is clamped or ignored. No message contains a submitted value.
func parseTemplateListQuery(rawQuery string) (templateListQuery, []apigen.FieldError) {
	out := templateListQuery{Limit: defaultListLimit}
	var errs []apigen.FieldError
	q, err := url.ParseQuery(rawQuery)
	if err != nil {
		errs = append(errs, fieldError("", codeInvalidParameter, "the query string is not well formed"))
	}
	for _, name := range templateListParams {
		values, present := q[name]
		if !present {
			continue
		}
		if len(values) > 1 {
			errs = append(errs, fieldError(name, codeDuplicateParameter, name+" must be given at most once"))
			continue
		}
		v := values[0]
		switch name {
		case paramLimit:
			if n, ok := parseLimit(v); ok {
				out.Limit = n
			} else {
				errs = append(errs, fieldError(name, codeInvalidParameter, "limit must be an integer from 1 to 200"))
			}
		case paramCursor:
			if v == "" {
				errs = append(errs, fieldError(name, codeInvalidParameter, "cursor must not be empty"))
			} else if after, ok := decodeTemplateCursor(v); ok {
				out.After = after
			} else {
				errs = append(errs, fieldError(name, codeInvalidCursor, "cursor is not a cursor this API returned"))
			}
		}
	}
	errs = append(errs, unknownParameters(q, templateListParams)...)
	return out, errs
}

// ListTemplates validates the query string before the handler runs (see
// ListNotifications) and hands the result over in the context.
func (g createGuard) ListTemplates(w http.ResponseWriter, r *http.Request, params apigen.ListTemplatesParams) {
	q, errs := parseTemplateListQuery(r.URL.RawQuery)
	if len(errs) > 0 {
		writeProblem(w, r, listQueryProblem(errs))
		return
	}
	g.ServerInterface.ListTemplates(w, r.WithContext(context.WithValue(r.Context(), templateListQueryKey{}, q)), params)
}

// templateListParameterProblem answers the generated wrapper's failure to
// bind a query parameter of the template listing with the problems the guard
// gives. It returns nil when the failure is not about that listing.
func templateListParameterProblem(r *http.Request, bindErr *apigen.InvalidParamFormatError) *problem {
	if r.Method != http.MethodGet || r.URL.Path != pathTemplates || !slices.Contains(templateListParams, bindErr.ParamName) {
		return nil
	}
	_, errs := parseTemplateListQuery(r.URL.RawQuery)
	if len(errs) == 0 {
		errs = []apigen.FieldError{fieldError(bindErr.ParamName, codeInvalidParameter, bindErr.ParamName+" is not valid")}
	}
	return listQueryProblem(errs)
}

// ListTemplates returns one page of template summaries by name.
func (h *handlers) ListTemplates(ctx context.Context, _ apigen.ListTemplatesRequestObject) (apigen.ListTemplatesResponseObject, error) {
	q, ok := templateListQueryFrom(ctx)
	if !ok {
		return nil, internalProblem(errors.New("template list request without a validated query"))
	}
	dbCtx, cancel := context.WithTimeout(ctx, h.dbTimeout)
	defer cancel()
	rows, more, err := h.repo.ListTemplates(dbCtx, q.After, q.Limit)
	if err != nil {
		return nil, storeProblem(err)
	}
	body := apigen.TemplateList{Data: make([]apigen.TemplateSummary, len(rows))}
	for i, t := range rows {
		body.Data[i] = apigen.TemplateSummary{
			Name:          t.Name,
			Channel:       apigen.Channel(t.Channel),
			LatestVersion: int32Of(t.LatestVersion),
			IsActive:      t.IsActive,
		}
	}
	if more && len(rows) > 0 {
		c := encodeTemplateCursor(rows[len(rows)-1].Name)
		body.NextCursor = &c
	}
	return apigen.ListTemplates200JSONResponse{
		Body:    body,
		Headers: apigen.ListTemplates200ResponseHeaders{XRequestID: stateFrom(ctx).correlationID},
	}, nil
}
