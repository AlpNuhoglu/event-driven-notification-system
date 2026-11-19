package api

import (
	"context"
	"errors"
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
