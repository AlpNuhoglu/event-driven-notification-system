package api

import (
	"context"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
)

// The templater methods of fakeRepo: templates are tested against Postgres;
// without a configured error every name is unknown.

func (f *fakeRepo) CreateTemplate(context.Context, store.NewTemplate) (store.VersionedTemplate, error) {
	return store.VersionedTemplate{}, f.templateErr()
}

func (f *fakeRepo) CreateTemplateVersion(context.Context, store.NewTemplateVersion) (store.VersionedTemplate, error) {
	return store.VersionedTemplate{}, f.templateErr()
}

func (f *fakeRepo) GetTemplate(context.Context, string) (store.VersionedTemplate, error) {
	return store.VersionedTemplate{}, f.templateErr()
}

func (f *fakeRepo) ListTemplates(context.Context, string, int) ([]store.Template, bool, error) {
	return nil, false, f.err
}

func (f *fakeRepo) GetTemplateVersion(context.Context, string, int) (store.VersionedTemplate, error) {
	return store.VersionedTemplate{}, f.templateErr()
}

func (f *fakeRepo) SetTemplateActive(context.Context, string, bool) (store.VersionedTemplate, error) {
	return store.VersionedTemplate{}, f.templateErr()
}

func (f *fakeRepo) templateErr() error {
	if f.err != nil {
		return f.err
	}
	return store.ErrTemplateNotFound
}

// LookupIdempotencyKey finds no live key unless an error is configured.
func (f *fakeRepo) LookupIdempotencyKey(context.Context, *store.IdempotencyKey) (*store.StoredResponse, error) {
	return nil, f.err
}
