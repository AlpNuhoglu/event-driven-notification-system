package api

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/api/apigen"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
)

// templater is the part of the store that the template endpoints use.
type templater interface {
	CreateTemplate(ctx context.Context, t store.NewTemplate) (store.VersionedTemplate, error)
	CreateTemplateVersion(ctx context.Context, v store.NewTemplateVersion) (store.VersionedTemplate, error)
	GetTemplate(ctx context.Context, name string) (store.VersionedTemplate, error)
	GetTemplateVersion(ctx context.Context, name string, version int) (store.VersionedTemplate, error)
	SetTemplateActive(ctx context.Context, name string, active bool) (store.VersionedTemplate, error)
}

const templatesPath = "/v1/templates/"

func templateNotFound() *problem { return newProblem(kindNotFound, "No template has this name.") }

func templateVersionNotFound() *problem {
	return newProblem(kindNotFound, "The template has no such version.")
}

// templateExistsProblem is the 409 of createTemplate.
func templateExistsProblem() *problem {
	return newProblem(kindTemplateExists, "",
		fieldError(fieldName, codeTemplateExists, "a template with this name already exists"))
}

// templateStoreProblem maps an error of the template store: 404 for a
// template or a version that does not exist, otherwise 500 or 503.
func templateStoreProblem(err error) *problem {
	switch {
	case errors.Is(err, store.ErrTemplateNotFound):
		return templateNotFound()
	case errors.Is(err, store.ErrTemplateVersionNotFound):
		return templateVersionNotFound()
	}
	return storeProblem(err)
}

// CreateTemplate stores the template with its version 1. createGuard has
// validated and compiled the body.
func (h *handlers) CreateTemplate(ctx context.Context, req apigen.CreateTemplateRequestObject) (apigen.CreateTemplateResponseObject, error) {
	if req.Body == nil {
		return nil, newProblem(kindMalformed, "")
	}
	b := req.Body
	dbCtx, cancel := context.WithTimeout(ctx, h.dbTimeout)
	defer cancel()
	vt, err := h.repo.CreateTemplate(dbCtx, store.NewTemplate{
		Name: b.Name, Channel: domain.Channel(b.Channel), Body: b.Body,
		Variables: derefList(b.Variables), OptionalVariables: derefList(b.OptionalVariables),
	})
	if errors.Is(err, store.ErrTemplateExists) {
		return nil, templateExistsProblem()
	}
	if err != nil {
		return nil, storeProblem(err)
	}
	return apigen.CreateTemplate201JSONResponse{
		Body: toTemplate(vt),
		Headers: apigen.CreateTemplate201ResponseHeaders{
			Location:   templatesPath + vt.Name,
			XRequestID: stateFrom(ctx).correlationID,
		},
	}, nil
}

// CreateTemplateVersion adds the next version. createGuard has validated and
// compiled the body for the template's channel.
func (h *handlers) CreateTemplateVersion(ctx context.Context, req apigen.CreateTemplateVersionRequestObject) (apigen.CreateTemplateVersionResponseObject, error) {
	if req.Body == nil {
		return nil, newProblem(kindMalformed, "")
	}
	if !domain.ValidTemplateName(req.Name) {
		return nil, templateNotFound()
	}
	b := req.Body
	dbCtx, cancel := context.WithTimeout(ctx, h.dbTimeout)
	defer cancel()
	vt, err := h.repo.CreateTemplateVersion(dbCtx, store.NewTemplateVersion{
		Name: req.Name, Body: b.Body,
		Variables: derefList(b.Variables), OptionalVariables: derefList(b.OptionalVariables),
	})
	if err != nil {
		return nil, templateStoreProblem(err)
	}
	return apigen.CreateTemplateVersion201JSONResponse{
		Body: toTemplate(vt),
		Headers: apigen.CreateTemplateVersion201ResponseHeaders{
			Location:   templatesPath + vt.Name + "/versions/" + strconv.Itoa(vt.Version.Version),
			XRequestID: stateFrom(ctx).correlationID,
		},
	}, nil
}

// GetTemplate answers the template with its latest version.
func (h *handlers) GetTemplate(ctx context.Context, req apigen.GetTemplateRequestObject) (apigen.GetTemplateResponseObject, error) {
	if !domain.ValidTemplateName(req.Name) {
		return nil, templateNotFound()
	}
	dbCtx, cancel := context.WithTimeout(ctx, h.dbTimeout)
	defer cancel()
	vt, err := h.repo.GetTemplate(dbCtx, req.Name)
	if err != nil {
		return nil, templateStoreProblem(err)
	}
	return apigen.GetTemplate200JSONResponse{
		Body:    toTemplate(vt),
		Headers: apigen.GetTemplate200ResponseHeaders{XRequestID: stateFrom(ctx).correlationID},
	}, nil
}

// GetTemplateVersion answers the template with the requested version. The
// generated code binds any integer, so 0 and negatives are refused here with
// the problem of a version that does not bind.
func (h *handlers) GetTemplateVersion(ctx context.Context, req apigen.GetTemplateVersionRequestObject) (apigen.GetTemplateVersionResponseObject, error) {
	if req.Version < 1 {
		return nil, invalidVersionProblem()
	}
	if !domain.ValidTemplateName(req.Name) {
		return nil, templateNotFound()
	}
	dbCtx, cancel := context.WithTimeout(ctx, h.dbTimeout)
	defer cancel()
	vt, err := h.repo.GetTemplateVersion(dbCtx, req.Name, int(req.Version))
	if err != nil {
		return nil, templateStoreProblem(err)
	}
	return apigen.GetTemplateVersion200JSONResponse{
		Body:    toTemplate(vt),
		Headers: apigen.GetTemplateVersion200ResponseHeaders{XRequestID: stateFrom(ctx).correlationID},
	}, nil
}

// DeactivateTemplate sets is_active to false; it is idempotent.
func (h *handlers) DeactivateTemplate(ctx context.Context, req apigen.DeactivateTemplateRequestObject) (apigen.DeactivateTemplateResponseObject, error) {
	vt, p := h.setTemplateActive(ctx, req.Name, false)
	if p != nil {
		return nil, p
	}
	return apigen.DeactivateTemplate200JSONResponse{
		Body:    toTemplate(vt),
		Headers: apigen.DeactivateTemplate200ResponseHeaders{XRequestID: stateFrom(ctx).correlationID},
	}, nil
}

// ActivateTemplate sets is_active to true; it is idempotent.
func (h *handlers) ActivateTemplate(ctx context.Context, req apigen.ActivateTemplateRequestObject) (apigen.ActivateTemplateResponseObject, error) {
	vt, p := h.setTemplateActive(ctx, req.Name, true)
	if p != nil {
		return nil, p
	}
	return apigen.ActivateTemplate200JSONResponse{
		Body:    toTemplate(vt),
		Headers: apigen.ActivateTemplate200ResponseHeaders{XRequestID: stateFrom(ctx).correlationID},
	}, nil
}

func (h *handlers) setTemplateActive(ctx context.Context, name string, active bool) (store.VersionedTemplate, *problem) {
	if !domain.ValidTemplateName(name) {
		return store.VersionedTemplate{}, templateNotFound()
	}
	dbCtx, cancel := context.WithTimeout(ctx, h.dbTimeout)
	defer cancel()
	vt, err := h.repo.SetTemplateActive(dbCtx, name, active)
	if err != nil {
		return store.VersionedTemplate{}, templateStoreProblem(err)
	}
	return vt, nil
}

// PreviewTemplate renders a template without storing anything (ADR-0008
// §9): the latest version or the pinned one, for the template's own channel,
// with the stages of a create after the load (renderEntry). It answers what
// a create would, with the field names of its body (variables.<name>,
// version) and content for the rendered text, except that an inactive
// template can be previewed; an unknown name or version is a 404. The guard
// has validated the body and hands it on in the context.
func (h *handlers) PreviewTemplate(ctx context.Context, req apigen.PreviewTemplateRequestObject) (apigen.PreviewTemplateResponseObject, error) {
	use, ok := ctx.Value(previewInputKey{}).(templateUse)
	if !ok {
		return nil, internalProblem(errors.New("preview request without validated input"))
	}
	if !domain.ValidTemplateName(req.Name) {
		return nil, templateNotFound()
	}
	e := h.newTemplateCache().load(ctx, req.Name, use.version)
	if e.err != nil {
		return nil, templateStoreProblem(e.err)
	}
	content, errs, p := renderEntry(e, use.vars, previewRenderFields)
	if p != nil {
		return nil, p
	}
	if len(errs) > 0 {
		return nil, newProblem(kindValidation, fmt.Sprintf("%d field(s) are invalid.", len(errs)), errs...)
	}
	out := apigen.TemplatePreview{Content: content}
	if e.vt.Channel == domain.ChannelSMS {
		// The content has passed validation: at most MaxSMSSegments segments.
		info := domain.CountSMS(content)
		enc := apigen.TemplatePreviewEncoding(info.Encoding)
		segments := int32Of(info.Segments)
		out.Encoding, out.Segments = &enc, &segments
	}
	return apigen.PreviewTemplate200JSONResponse{
		Body:    out,
		Headers: apigen.PreviewTemplate200ResponseHeaders{XRequestID: stateFrom(ctx).correlationID},
	}, nil
}

// invalidVersionProblem is the 400 of a version that is not a positive
// integer.
func invalidVersionProblem() *problem {
	return newProblem(kindMalformed, "A path parameter is invalid.",
		fieldError(fieldVersion, codeInvalidParameter, "version must be a positive integer"))
}

// fieldVersion is the path parameter of getTemplateVersion.
const fieldVersion = "version"

// derefList returns the list a request gave, or an empty one.
func derefList(l *[]string) []string {
	if l == nil {
		return []string{}
	}
	return slices.Clone(*l)
}

// toTemplate maps a stored template with one of its versions to the API
// object; the lists are never null and the timestamps are in UTC.
func toTemplate(vt store.VersionedTemplate) apigen.Template {
	return apigen.Template{
		Name:              vt.Name,
		Channel:           apigen.Channel(vt.Channel),
		IsActive:          vt.IsActive,
		LatestVersion:     int32Of(vt.LatestVersion),
		Version:           int32Of(vt.Version.Version),
		Body:              vt.Version.Body,
		Variables:         nonNilList(vt.Version.Variables),
		OptionalVariables: nonNilList(vt.Version.OptionalVariables),
		CreatedAt:         vt.CreatedAt.UTC(),
		UpdatedAt:         vt.UpdatedAt.UTC(),
		VersionCreatedAt:  vt.Version.CreatedAt.UTC(),
	}
}

func nonNilList(l []string) []string {
	if l == nil {
		return []string{}
	}
	return slices.Clone(l)
}
