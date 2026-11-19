package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/api/apigen"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
)

// CreateTemplate validates the body (415, 413, 400, 422; see
// validateCreateTemplate) and passes the parsed request on: the strict
// handler's decoder would ignore unknown and repeated fields.
func (g createGuard) CreateTemplate(w http.ResponseWriter, r *http.Request, params apigen.CreateTemplateParams) {
	body, ok := readJSONBody(w, r)
	if !ok {
		return
	}
	req, p := validateCreateTemplate(body)
	if p != nil {
		writeProblem(w, r, p)
		return
	}
	resp, err := g.h.CreateTemplate(r.Context(), apigen.CreateTemplateRequestObject{Params: params, Body: &req})
	if err != nil {
		responseError(w, r, err)
		return
	}
	if err := resp.VisitCreateTemplateResponse(w); err != nil {
		responseError(w, r, err)
	}
}

// CreateTemplateVersion answers 404 for a name that cannot exist without a
// database call, then validates the body like CreateTemplate; the channel to
// compile for is read from the stored template once the shape is valid.
func (g createGuard) CreateTemplateVersion(w http.ResponseWriter, r *http.Request, name apigen.TemplateName, params apigen.CreateTemplateVersionParams) {
	if !domain.ValidTemplateName(name) {
		writeProblem(w, r, templateNotFound())
		return
	}
	body, ok := readJSONBody(w, r)
	if !ok {
		return
	}
	req, p := validateCreateTemplateVersion(r.Context(), body, func(ctx context.Context) (domain.Channel, *problem) {
		return g.h.templateChannel(ctx, name)
	})
	if p != nil {
		writeProblem(w, r, p)
		return
	}
	resp, err := g.h.CreateTemplateVersion(r.Context(), apigen.CreateTemplateVersionRequestObject{Name: name, Params: params, Body: &req})
	if err != nil {
		responseError(w, r, err)
		return
	}
	if err := resp.VisitCreateTemplateVersionResponse(w); err != nil {
		responseError(w, r, err)
	}
}

// DeactivateTemplate refuses a request with a body (400), then hands it on to
// the strict handler.
func (g createGuard) DeactivateTemplate(w http.ResponseWriter, r *http.Request, name apigen.TemplateName, params apigen.DeactivateTemplateParams) {
	if !requireNoBody(w, r) {
		return
	}
	g.ServerInterface.DeactivateTemplate(w, r, name, params)
}

// ActivateTemplate refuses a request with a body (400), then hands it on to
// the strict handler.
func (g createGuard) ActivateTemplate(w http.ResponseWriter, r *http.Request, name apigen.TemplateName, params apigen.ActivateTemplateParams) {
	if !requireNoBody(w, r) {
		return
	}
	g.ServerInterface.ActivateTemplate(w, r, name, params)
}

// PreviewTemplate answers 404 for a name that cannot exist without a
// database call, then validates the optional body (validatePreview) as
// RedriveNotification does: an empty body (nothing but white space) is an
// empty preview request and needs no Content-Type; a body that is not empty
// must be application/json (415). The parsed request reaches the handler in
// the context: the strict handler's decoder would decode a variable's number
// as a float64.
func (g createGuard) PreviewTemplate(w http.ResponseWriter, r *http.Request, name apigen.TemplateName, params apigen.PreviewTemplateParams) {
	if !domain.ValidTemplateName(name) {
		writeProblem(w, r, templateNotFound())
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeProblem(w, r, bodyReadProblem(err))
		return
	}
	use := templateUse{vars: map[string]json.RawMessage{}}
	if len(bytes.TrimSpace(body)) > 0 {
		if !isJSONContentType(r.Header.Get("Content-Type")) {
			writeProblem(w, r, newProblem(kindUnsupportedMediaType, ""))
			return
		}
		var p *problem
		if use, p = validatePreview(body); p != nil {
			writeProblem(w, r, p)
			return
		}
	}
	ctx := context.WithValue(r.Context(), previewInputKey{}, use)
	resp, err := g.h.PreviewTemplate(ctx, apigen.PreviewTemplateRequestObject{Name: name, Params: params})
	if err != nil {
		responseError(w, r, err)
		return
	}
	if err := resp.VisitPreviewTemplateResponse(w); err != nil {
		responseError(w, r, err)
	}
}

type previewInputKey struct{}

// validatePreview checks a preview body that is not empty, strictly: a JSON
// object (400 otherwise) whose members are version and variables, each given
// once (422 otherwise, every invalid field listed). No message contains a
// submitted value.
func validatePreview(body []byte) (templateUse, *problem) {
	if p := checkBodyIsJSON(body); p != nil {
		return templateUse{}, p
	}
	members, order, dups, ok := objectMembers(body)
	if !ok {
		return templateUse{}, notAnObject()
	}
	var errs []apigen.FieldError
	add := func(field, code, msg string) { errs = append(errs, fieldError(field, code, msg)) }
	use, valid := checkTemplateUse(add, members, order, dups, "", false)
	if !valid {
		return templateUse{}, newProblem(kindValidation, fmt.Sprintf("%d field(s) are invalid.", len(errs)), errs...)
	}
	return use, nil
}

// templateChannel reads the channel of a stored template for the guard of
// createTemplateVersion.
func (h *handlers) templateChannel(ctx context.Context, name string) (domain.Channel, *problem) {
	dbCtx, cancel := context.WithTimeout(ctx, h.dbTimeout)
	defer cancel()
	t, err := h.repo.GetTemplate(dbCtx, name)
	if errors.Is(err, store.ErrTemplateNotFound) {
		return "", templateNotFound()
	}
	if err != nil {
		return "", storeProblem(err)
	}
	return t.Channel, nil
}
