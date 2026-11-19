//go:build integration

package store

import (
	"errors"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

const concurrentWriters = 16

func newTpl(name string) NewTemplate {
	return NewTemplate{
		Name:              name,
		Channel:           domain.ChannelEmail,
		Body:              "Hello {{.name}}",
		Variables:         []string{"name"},
		OptionalVariables: []string{"nick"},
	}
}

func mustCreateTemplate(t *testing.T, s *Store, nt NewTemplate) VersionedTemplate {
	t.Helper()
	vt, err := s.CreateTemplate(ctxT(t), nt)
	if err != nil {
		t.Fatalf("create template: %v", err)
	}
	return vt
}

func mustAddVersion(t *testing.T, s *Store, name, body string) VersionedTemplate {
	t.Helper()
	vt, err := s.CreateTemplateVersion(ctxT(t), NewTemplateVersion{Name: name, Body: body})
	if err != nil {
		t.Fatalf("create template version: %v", err)
	}
	return vt
}

func TestCreateTemplate_Version1(t *testing.T) {
	s, _ := newTestStore(t, zeroDelay)
	vt := mustCreateTemplate(t, s, newTpl("welcome"))
	if vt.Name != "welcome" || vt.Channel != domain.ChannelEmail || !vt.IsActive || vt.LatestVersion != 1 {
		t.Fatalf("template = %+v", vt.Template)
	}
	if vt.CreatedAt.IsZero() || vt.UpdatedAt.IsZero() || vt.Version.CreatedAt.IsZero() {
		t.Fatalf("timestamps not set: %+v", vt)
	}
	v := vt.Version
	if v.Version != 1 || v.Body != "Hello {{.name}}" ||
		!reflect.DeepEqual(v.Variables, []string{"name"}) || !reflect.DeepEqual(v.OptionalVariables, []string{"nick"}) {
		t.Fatalf("version = %+v", v)
	}

	bare := mustCreateTemplate(t, s, NewTemplate{Name: "bare", Channel: domain.ChannelSMS, Body: "x"})
	if bare.Version.Variables == nil || bare.Version.OptionalVariables == nil ||
		len(bare.Version.Variables) != 0 || len(bare.Version.OptionalVariables) != 0 {
		t.Fatalf("lists must be empty and not nil: %+v", bare.Version)
	}
	got, err := s.GetTemplate(ctxT(t), "bare")
	if err != nil {
		t.Fatal(err)
	}
	if got.Version.Variables == nil || got.Version.OptionalVariables == nil {
		t.Fatalf("stored lists must not be nil: %+v", got.Version)
	}
}

func TestCreateTemplate_ExistingName(t *testing.T) {
	s, _ := newTestStore(t, zeroDelay)
	first := mustCreateTemplate(t, s, newTpl("welcome"))
	other := newTpl("welcome")
	other.Channel, other.Body = domain.ChannelSMS, "changed"
	if _, err := s.CreateTemplate(ctxT(t), other); !errors.Is(err, ErrTemplateExists) {
		t.Fatalf("err = %v, want ErrTemplateExists", err)
	}
	got, err := s.GetTemplate(ctxT(t), "welcome")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, first) {
		t.Fatalf("stored template changed:\n got %+v\nwant %+v", got, first)
	}
}

func TestCreateTemplate_ConcurrentSameName(t *testing.T) {
	s, pool := newTestStore(t, zeroDelay)
	errs := make([]error, concurrentWriters)
	var wg sync.WaitGroup
	for i := range concurrentWriters {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = s.CreateTemplate(ctxT(t), newTpl("race"))
		}()
	}
	wg.Wait()
	var won, exists int
	for _, err := range errs {
		switch {
		case err == nil:
			won++
		case errors.Is(err, ErrTemplateExists):
			exists++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if won != 1 || exists != concurrentWriters-1 {
		t.Fatalf("won = %d, exists = %d", won, exists)
	}
	var templates, versions int
	if err := pool.QueryRow(ctxT(t), `SELECT (SELECT count(*) FROM templates WHERE name = 'race'),
		(SELECT count(*) FROM template_versions WHERE template_name = 'race')`).Scan(&templates, &versions); err != nil {
		t.Fatal(err)
	}
	if templates != 1 || versions != 1 {
		t.Fatalf("rows: templates = %d, versions = %d", templates, versions)
	}
}

func TestCreateTemplateVersion_ConcurrentVersionsAreConsecutive(t *testing.T) {
	s, _ := newTestStore(t, zeroDelay)
	first := mustCreateTemplate(t, s, newTpl("busy"))
	got := make([]int, concurrentWriters)
	errs := make([]error, concurrentWriters)
	var wg sync.WaitGroup
	for i := range concurrentWriters {
		wg.Add(1)
		go func() {
			defer wg.Done()
			vt, err := s.CreateTemplateVersion(ctxT(t), NewTemplateVersion{Name: "busy", Body: "body"})
			errs[i], got[i] = err, vt.Version.Version
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatalf("create version: %v", err)
		}
	}
	slices.Sort(got)
	for i, v := range got {
		if v != i+2 {
			t.Fatalf("versions = %v, want 2..%d", got, concurrentWriters+1)
		}
	}
	latest, err := s.GetTemplate(ctxT(t), "busy")
	if err != nil {
		t.Fatal(err)
	}
	if latest.LatestVersion != concurrentWriters+1 || latest.Version.Version != concurrentWriters+1 {
		t.Fatalf("latest = %d / %d", latest.LatestVersion, latest.Version.Version)
	}
	if !latest.UpdatedAt.After(first.UpdatedAt) {
		t.Fatalf("updated_at did not move: %v -> %v", first.UpdatedAt, latest.UpdatedAt)
	}
}

func TestCreateTemplateVersion_UnknownTemplate(t *testing.T) {
	s, _ := newTestStore(t, zeroDelay)
	_, err := s.CreateTemplateVersion(ctxT(t), NewTemplateVersion{Name: "missing", Body: "x"})
	if !errors.Is(err, ErrTemplateNotFound) {
		t.Fatalf("err = %v, want ErrTemplateNotFound", err)
	}
}

func TestCreateTemplateVersion_InactiveTemplate(t *testing.T) {
	s, _ := newTestStore(t, zeroDelay)
	mustCreateTemplate(t, s, newTpl("off"))
	if _, err := s.SetTemplateActive(ctxT(t), "off", false); err != nil {
		t.Fatal(err)
	}
	vt := mustAddVersion(t, s, "off", "v2")
	if vt.IsActive || vt.LatestVersion != 2 {
		t.Fatalf("template = %+v", vt.Template)
	}
}

func TestGetTemplate_LatestVersion(t *testing.T) {
	s, _ := newTestStore(t, zeroDelay)
	mustCreateTemplate(t, s, newTpl("welcome"))
	mustAddVersion(t, s, "welcome", "second")
	third := mustAddVersion(t, s, "welcome", "third")
	got, err := s.GetTemplate(ctxT(t), "welcome")
	if err != nil {
		t.Fatal(err)
	}
	if got.LatestVersion != 3 || got.Version.Version != 3 || got.Version.Body != "third" {
		t.Fatalf("got %+v", got)
	}
	if !reflect.DeepEqual(got, third) {
		t.Fatalf("read differs from the create result:\n got %+v\nwant %+v", got, third)
	}
}

func TestGetTemplate_UnknownTemplate(t *testing.T) {
	s, _ := newTestStore(t, zeroDelay)
	if _, err := s.GetTemplate(ctxT(t), "missing"); !errors.Is(err, ErrTemplateNotFound) {
		t.Fatalf("err = %v, want ErrTemplateNotFound", err)
	}
}

func TestGetTemplateVersion_Pinned(t *testing.T) {
	s, _ := newTestStore(t, zeroDelay)
	mustCreateTemplate(t, s, newTpl("welcome"))
	mustAddVersion(t, s, "welcome", "second")
	got, err := s.GetTemplateVersion(ctxT(t), "welcome", 1)
	if err != nil {
		t.Fatal(err)
	}
	if got.Version.Version != 1 || got.Version.Body != "Hello {{.name}}" || got.LatestVersion != 2 {
		t.Fatalf("got %+v", got)
	}
}

func TestGetTemplateVersion_UnknownTemplate(t *testing.T) {
	s, _ := newTestStore(t, zeroDelay)
	if _, err := s.GetTemplateVersion(ctxT(t), "missing", 1); !errors.Is(err, ErrTemplateNotFound) {
		t.Fatalf("err = %v, want ErrTemplateNotFound", err)
	}
}

func TestGetTemplateVersion_UnknownVersion(t *testing.T) {
	s, _ := newTestStore(t, zeroDelay)
	mustCreateTemplate(t, s, newTpl("welcome"))
	for _, v := range []int{2, 0, -1} {
		if _, err := s.GetTemplateVersion(ctxT(t), "welcome", v); !errors.Is(err, ErrTemplateVersionNotFound) {
			t.Fatalf("version %d: err = %v, want ErrTemplateVersionNotFound", v, err)
		}
	}
}

func TestSetTemplateActive(t *testing.T) {
	s, _ := newTestStore(t, zeroDelay)
	created := mustCreateTemplate(t, s, newTpl("welcome"))

	off, err := s.SetTemplateActive(ctxT(t), "welcome", false)
	if err != nil {
		t.Fatal(err)
	}
	if off.IsActive || !off.UpdatedAt.After(created.UpdatedAt) || off.Version.Version != 1 {
		t.Fatalf("deactivated = %+v", off.Template)
	}

	again, err := s.SetTemplateActive(ctxT(t), "welcome", false)
	if err != nil {
		t.Fatal(err)
	}
	if again.IsActive || !again.UpdatedAt.Equal(off.UpdatedAt) {
		t.Fatalf("repeat changed the row: %v -> %v", off.UpdatedAt, again.UpdatedAt)
	}

	on, err := s.SetTemplateActive(ctxT(t), "welcome", true)
	if err != nil {
		t.Fatal(err)
	}
	if !on.IsActive || !on.UpdatedAt.After(off.UpdatedAt) {
		t.Fatalf("activated = %+v", on.Template)
	}

	if _, err := s.SetTemplateActive(ctxT(t), "missing", true); !errors.Is(err, ErrTemplateNotFound) {
		t.Fatalf("err = %v, want ErrTemplateNotFound", err)
	}
}

func TestTemplates_Immutable(t *testing.T) {
	s, _ := newTestStore(t, zeroDelay)
	nt := newTpl("welcome")
	nt.Body = "Grüße, {{.name}} ☃ \U0001F600"
	mustCreateTemplate(t, s, nt)
	before, err := s.GetTemplateVersion(ctxT(t), "welcome", 1)
	if err != nil {
		t.Fatal(err)
	}
	mustAddVersion(t, s, "welcome", "something else")
	if _, err := s.SetTemplateActive(ctxT(t), "welcome", false); err != nil {
		t.Fatal(err)
	}
	after, err := s.GetTemplateVersion(ctxT(t), "welcome", 1)
	if err != nil {
		t.Fatal(err)
	}
	if after.Version.Body != nt.Body || !reflect.DeepEqual(after.Version, before.Version) {
		t.Fatalf("version 1 changed:\n got %+v\nwant %+v", after.Version, before.Version)
	}
}

func TestTemplates_BodySize(t *testing.T) {
	s, _ := newTestStore(t, zeroDelay)

	multi := strings.Repeat("é€😀", 10)
	nt := newTpl("multibyte")
	nt.Body = multi
	if got := mustCreateTemplate(t, s, nt); got.Version.Body != multi {
		t.Fatalf("multi-byte body = %q", got.Version.Body)
	}

	// 65,536 bytes exactly: 65,534 ASCII bytes and one two-byte character.
	limit := strings.Repeat("a", 65534) + "é"
	if len(limit) != 65536 {
		t.Fatalf("test body is %d bytes", len(limit))
	}
	nt = newTpl("limit")
	nt.Body = limit
	if got := mustCreateTemplate(t, s, nt); got.Version.Body != limit {
		t.Fatal("65,536-byte body did not round-trip")
	}
	if got := mustAddVersion(t, s, "limit", limit); got.Version.Body != limit {
		t.Fatal("65,536-byte version did not round-trip")
	}

	nt = newTpl("toolarge")
	nt.Body = limit + "a"
	_, err := s.CreateTemplate(ctxT(t), nt)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23514" {
		t.Fatalf("65,537-byte body: err = %v, want a check violation", err)
	}
	if _, err := s.GetTemplate(ctxT(t), "toolarge"); !errors.Is(err, ErrTemplateNotFound) {
		t.Fatalf("a refused create left a template behind: %v", err)
	}
	if _, err := s.CreateTemplateVersion(ctxT(t), NewTemplateVersion{Name: "limit", Body: limit + "a"}); !errors.As(err, &pgErr) {
		t.Fatalf("65,537-byte version: err = %v, want a check violation", err)
	}
	got, err := s.GetTemplate(ctxT(t), "limit")
	if err != nil {
		t.Fatal(err)
	}
	if got.LatestVersion != 2 {
		t.Fatalf("a refused version moved latest_version to %d", got.LatestVersion)
	}
}
