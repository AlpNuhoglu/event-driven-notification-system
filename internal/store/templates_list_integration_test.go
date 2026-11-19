//go:build integration

package store

import (
	"slices"
	"testing"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

func listNames(items []Template) []string {
	names := make([]string, len(items))
	for i, it := range items {
		names[i] = it.Name
	}
	return names
}

// ASCII order: '-' (0x2d), digits (0x30 to 0x39), '_' (0x5f), letters. A
// database whose default collation ignores punctuation would order these
// differently; the query's COLLATE "C" does not depend on it.
func TestListTemplates_AsciiOrder(t *testing.T) {
	s, _ := newTestStore(t, zeroDelay)
	created := []string{"b", "a_b", "a-b", "a1", "ab", "a", "9", "0-x", "z9_-"}
	for _, n := range created {
		mustCreateTemplate(t, s, newTpl(n))
	}
	want := []string{"0-x", "9", "a", "a-b", "a1", "a_b", "ab", "b", "z9_-"}

	items, more, err := s.ListTemplates(ctxT(t), "", 200)
	if err != nil {
		t.Fatal(err)
	}
	if got := listNames(items); !slices.Equal(got, want) || more {
		t.Errorf("names %v more %v, want %v false", got, more, want)
	}
}

func TestListTemplates_MoreAndAfter(t *testing.T) {
	s, _ := newTestStore(t, zeroDelay)
	for _, n := range []string{"a", "b", "c", "d", "e"} {
		mustCreateTemplate(t, s, newTpl(n))
	}
	ctx := ctxT(t)

	items, more, err := s.ListTemplates(ctx, "", 2)
	if err != nil || !more || !slices.Equal(listNames(items), []string{"a", "b"}) {
		t.Fatalf("first page %v more %v err %v", listNames(items), more, err)
	}
	items, more, err = s.ListTemplates(ctx, "b", 2)
	if err != nil || !more || !slices.Equal(listNames(items), []string{"c", "d"}) {
		t.Fatalf("second page %v more %v err %v", listNames(items), more, err)
	}
	// The last page: full, and nothing follows.
	items, more, err = s.ListTemplates(ctx, "c", 2)
	if err != nil || more || !slices.Equal(listNames(items), []string{"d", "e"}) {
		t.Fatalf("last page %v more %v err %v", listNames(items), more, err)
	}
	// A cursor that names no template still continues after its place.
	items, more, err = s.ListTemplates(ctx, "bb", 10)
	if err != nil || more || !slices.Equal(listNames(items), []string{"c", "d", "e"}) {
		t.Fatalf("after a missing name: %v more %v err %v", listNames(items), more, err)
	}
	items, more, err = s.ListTemplates(ctx, "e", 10)
	if err != nil || more || items == nil || len(items) != 0 {
		t.Fatalf("after the last name: %v (nil %v) more %v err %v", items, items == nil, more, err)
	}
}

func TestListTemplates_FieldsAndInactive(t *testing.T) {
	s, _ := newTestStore(t, zeroDelay)
	mustCreateTemplate(t, s, newTpl("one"))
	mustAddVersion(t, s, "one", "v2")
	mustCreateTemplate(t, s, newTpl("two"))
	if _, err := s.SetTemplateActive(ctxT(t), "two", false); err != nil {
		t.Fatal(err)
	}
	items, _, err := s.ListTemplates(ctxT(t), "", 10)
	if err != nil || len(items) != 2 {
		t.Fatalf("items %v err %v", items, err)
	}
	if one := items[0]; one.Name != "one" || one.Channel != domain.ChannelEmail || one.LatestVersion != 2 || !one.IsActive {
		t.Errorf("one: %+v", one)
	}
	if two := items[1]; two.Name != "two" || two.LatestVersion != 1 || two.IsActive {
		t.Errorf("two: %+v", two)
	}
}

func TestListTemplates_LimitOutOfRange(t *testing.T) {
	s, _ := newTestStore(t, zeroDelay)
	for _, limit := range []int{-1, 0, MaxListLimit + 1} {
		if _, _, err := s.ListTemplates(ctxT(t), "", limit); err == nil {
			t.Errorf("limit %d accepted", limit)
		}
	}
}
