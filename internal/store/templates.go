package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store/db"
)

var (
	// ErrTemplateExists is returned by CreateTemplate when the name is taken.
	ErrTemplateExists = errors.New("store: template exists")
	// ErrTemplateNotFound is returned when no template has the given name.
	ErrTemplateNotFound = errors.New("store: template not found")
	// ErrTemplateVersionNotFound is returned when the template exists and the
	// requested version does not.
	ErrTemplateVersionNotFound = errors.New("store: template version not found")
)

// Template is one row of the templates table.
type Template struct {
	Name          string
	Channel       domain.Channel
	IsActive      bool
	LatestVersion int
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// TemplateVersion is one row of the template_versions table. Versions are
// immutable.
type TemplateVersion struct {
	Version           int
	Body              string
	Variables         []string // never nil
	OptionalVariables []string // never nil
	CreatedAt         time.Time
}

// VersionedTemplate is a template with one of its versions.
type VersionedTemplate struct {
	Template
	Version TemplateVersion
}

// NewTemplate is the input of CreateTemplate.
type NewTemplate struct {
	Name              string
	Channel           domain.Channel
	Body              string
	Variables         []string
	OptionalVariables []string
}

// NewTemplateVersion is the input of CreateTemplateVersion.
type NewTemplateVersion struct {
	Name              string
	Body              string
	Variables         []string
	OptionalVariables []string
}

// CreateTemplate inserts the template with its version 1 in one transaction.
// It returns ErrTemplateExists when the name is taken, also for concurrent
// creates of one name: exactly one wins. The store does not validate the name
// or the body; a violated CHECK is returned as the database's error.
func (s *Store) CreateTemplate(ctx context.Context, t NewTemplate) (VersionedTemplate, error) {
	var out VersionedTemplate
	err := s.inTx(ctx, func(q *db.Queries) error {
		if _, err := q.InsertTemplate(ctx, db.InsertTemplateParams{Name: t.Name, Channel: string(t.Channel)}); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrTemplateExists
			}
			return err
		}
		if err := q.InsertTemplateVersion(ctx, versionParams(t.Name, 1, t.Body, t.Variables, t.OptionalVariables)); err != nil {
			return err
		}
		row, err := q.GetTemplateLatest(ctx, t.Name)
		if err != nil {
			return err
		}
		out = fromLatestRow(row)
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrTemplateExists) {
			return VersionedTemplate{}, err
		}
		return VersionedTemplate{}, fmt.Errorf("create template: %w", err)
	}
	return out, nil
}

// CreateTemplateVersion adds the next version in one transaction: it locks the
// parent row, inserts version latest_version + 1 and moves latest_version and
// updated_at. It returns ErrTemplateNotFound when there is no such template.
// An inactive template can receive a version.
func (s *Store) CreateTemplateVersion(ctx context.Context, v NewTemplateVersion) (VersionedTemplate, error) {
	var out VersionedTemplate
	err := s.inTx(ctx, func(q *db.Queries) error {
		latest, err := q.LockTemplate(ctx, v.Name)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrTemplateNotFound
		}
		if err != nil {
			return err
		}
		next := latest + 1
		if err := q.InsertTemplateVersion(ctx, versionParams(v.Name, next, v.Body, v.Variables, v.OptionalVariables)); err != nil {
			return err
		}
		if err := q.BumpTemplateVersion(ctx, db.BumpTemplateVersionParams{Name: v.Name, LatestVersion: next}); err != nil {
			return err
		}
		row, err := q.GetTemplateLatest(ctx, v.Name)
		if err != nil {
			return err
		}
		out = fromLatestRow(row)
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrTemplateNotFound) {
			return VersionedTemplate{}, err
		}
		return VersionedTemplate{}, fmt.Errorf("create template version: %w", err)
	}
	return out, nil
}

// GetTemplate returns the template with its latest version, or
// ErrTemplateNotFound.
func (s *Store) GetTemplate(ctx context.Context, name string) (VersionedTemplate, error) {
	row, err := s.q.GetTemplateLatest(ctx, name)
	if errors.Is(err, pgx.ErrNoRows) {
		return VersionedTemplate{}, ErrTemplateNotFound
	}
	if err != nil {
		return VersionedTemplate{}, fmt.Errorf("get template: %w", err)
	}
	return fromLatestRow(row), nil
}

// GetTemplateVersion returns the template with the given version. It returns
// ErrTemplateNotFound when the template does not exist and
// ErrTemplateVersionNotFound when it exists and the version does not.
func (s *Store) GetTemplateVersion(ctx context.Context, name string, version int) (VersionedTemplate, error) {
	ver, err := toInt32(version)
	if err != nil {
		return VersionedTemplate{}, fmt.Errorf("get template version: %w", err)
	}
	row, err := s.q.GetTemplateAtVersion(ctx, db.GetTemplateAtVersionParams{Name: name, Version: ver})
	if errors.Is(err, pgx.ErrNoRows) {
		exists, err := s.q.TemplateExists(ctx, name)
		if err != nil {
			return VersionedTemplate{}, fmt.Errorf("get template version: %w", err)
		}
		if !exists {
			return VersionedTemplate{}, ErrTemplateNotFound
		}
		return VersionedTemplate{}, ErrTemplateVersionNotFound
	}
	if err != nil {
		return VersionedTemplate{}, fmt.Errorf("get template version: %w", err)
	}
	return fromLatestRow(db.GetTemplateLatestRow(row)), nil
}

// SetTemplateActive sets is_active and returns the template with its latest
// version. It is idempotent: when the value is already as asked nothing is
// written and updated_at is unchanged. ErrTemplateNotFound when there is no
// such template.
func (s *Store) SetTemplateActive(ctx context.Context, name string, active bool) (VersionedTemplate, error) {
	if _, err := s.q.SetTemplateActive(ctx, db.SetTemplateActiveParams{Name: name, IsActive: active}); err != nil {
		return VersionedTemplate{}, fmt.Errorf("set template active: %w", err)
	}
	return s.GetTemplate(ctx, name)
}

func versionParams(name string, version int32, body string, variables, optional []string) db.InsertTemplateVersionParams {
	return db.InsertTemplateVersionParams{
		TemplateName:      name,
		Version:           version,
		BodyTpl:           body,
		Variables:         nonNil(variables),
		OptionalVariables: nonNil(optional),
	}
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func fromLatestRow(r db.GetTemplateLatestRow) VersionedTemplate {
	return VersionedTemplate{
		Template: Template{
			Name:          r.Name,
			Channel:       domain.Channel(r.Channel),
			IsActive:      r.IsActive,
			LatestVersion: int(r.LatestVersion),
			CreatedAt:     r.CreatedAt,
			UpdatedAt:     r.UpdatedAt,
		},
		Version: TemplateVersion{
			Version:           int(r.Version),
			Body:              r.BodyTpl,
			Variables:         nonNil(r.Variables),
			OptionalVariables: nonNil(r.OptionalVariables),
			CreatedAt:         r.VersionCreatedAt,
		},
	}
}

// ListTemplates returns one page of templates by name in ASCII order (the
// "C" collation, whatever the database's default), starting after the name
// after (empty for the first page), and whether more templates follow. It
// reads limit + 1 rows to know. limit must be MinListLimit to MaxListLimit.
// Inactive templates are listed too. It is a keyset read: a template created
// meanwhile appears if its name sorts after the cursor and never otherwise.
func (s *Store) ListTemplates(ctx context.Context, after string, limit int) (items []Template, more bool, err error) {
	if limit < MinListLimit || limit > MaxListLimit {
		return nil, false, fmt.Errorf("store: list limit %d outside %d to %d", limit, MinListLimit, MaxListLimit)
	}
	n, err := toInt32(limit + 1)
	if err != nil {
		return nil, false, fmt.Errorf("list templates: %w", err)
	}
	rows, err := s.q.ListTemplates(ctx, db.ListTemplatesParams{After: after, N: n})
	if err != nil {
		return nil, false, fmt.Errorf("list templates: %w", err)
	}
	if len(rows) > limit {
		more = true
		rows = rows[:limit]
	}
	items = make([]Template, len(rows))
	for i, r := range rows {
		items[i] = Template{
			Name:          r.Name,
			Channel:       domain.Channel(r.Channel),
			IsActive:      r.IsActive,
			LatestVersion: int(r.LatestVersion),
			CreatedAt:     r.CreatedAt,
			UpdatedAt:     r.UpdatedAt,
		}
	}
	return items, more, nil
}
