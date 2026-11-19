package logging

import (
	"cmp"
	"log/slog"
	"maps"
	"slices"
	"strings"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

// droppedKeys are the attribute names whose values are never written, at
// any depth: a message's content (it can hold a one-time password) and a
// template's variables (they are the content's parts).
var droppedKeys = []string{"content", "variables", "template_vars"}

// recipientKey is the attribute name whose value is always masked.
const recipientKey = "recipient"

// redactedValue replaces a recipient attribute whose value is not a string.
const redactedValue = "***"

// Redact is the ReplaceAttr function of the central handler, the key-based
// redaction of ADR-0009 §4 and the backstop for values passed as plain
// strings:
//
//   - an attribute named content, variables or template_vars is dropped,
//     also inside a group, and so is every attribute inside a group of one
//     of these names;
//   - an attribute named recipient, or inside a group named recipient, is
//     masked whatever its type: a string as domain.Recipient masks it (a
//     domain.Recipient arrives already masked, and masking is idempotent),
//     any other value as "***";
//   - a map with string keys is written as a group, so that its keys pass
//     these rules too (encoding/json would write it without asking).
//
// Names are compared without regard to case.
func Redact(groups []string, a slog.Attr) slog.Attr {
	masked := false
	for _, g := range groups {
		if isDropped(g) {
			return slog.Attr{}
		}
		if strings.EqualFold(g, recipientKey) {
			masked = true
		}
	}
	if isDropped(a.Key) {
		return slog.Attr{}
	}
	if masked || strings.EqualFold(a.Key, recipientKey) {
		if a.Value.Kind() == slog.KindString {
			return slog.String(a.Key, domain.Recipient(a.Value.String()).Masked())
		}
		return slog.String(a.Key, redactedValue)
	}
	if a.Value.Kind() == slog.KindAny {
		if g, ok := mapGroup(a.Value.Any()); ok {
			return slog.Attr{Key: a.Key, Value: g}
		}
	}
	return a
}

// redactAttrs applies Redact to every attribute of as at any depth, under
// the group path groups, as the JSON handler would: a value is resolved
// first (a slog.LogValuer such as domain.Recipient), a group is walked, and
// a group left without attributes is removed.
func redactAttrs(groups []string, as []slog.Attr) []slog.Attr {
	out := make([]slog.Attr, 0, len(as))
	for _, a := range as {
		a.Value = a.Value.Resolve()
		if a.Value.Kind() != slog.KindGroup {
			a = Redact(groups, a)
			a.Value = a.Value.Resolve()
		}
		if a.Value.Kind() != slog.KindGroup {
			if !a.Equal(slog.Attr{}) {
				out = append(out, a)
			}
			continue
		}
		if isDropped(a.Key) {
			continue
		}
		path := groups
		if a.Key != "" {
			path = append(slices.Clip(groups), a.Key)
		}
		if members := redactAttrs(path, a.Value.Group()); len(members) > 0 {
			out = append(out, slog.Attr{Key: a.Key, Value: slog.GroupValue(members...)})
		}
	}
	return out
}

func isDropped(key string) bool {
	return slices.ContainsFunc(droppedKeys, func(k string) bool { return strings.EqualFold(k, key) })
}

// mapGroup returns the group of a map with string keys, sorted by key; the
// JSON handler applies Redact to each member.
func mapGroup(v any) (slog.Value, bool) {
	switch m := v.(type) {
	case map[string]any:
		return slog.GroupValue(groupAttrs(m)...), true
	case map[string]string:
		return slog.GroupValue(groupAttrs(m)...), true
	}
	return slog.Value{}, false
}

func groupAttrs[V any](m map[string]V) []slog.Attr {
	keys := slices.SortedFunc(maps.Keys(m), cmp.Compare[string])
	as := make([]slog.Attr, 0, len(keys))
	for _, k := range keys {
		as = append(as, slog.Any(k, m[k]))
	}
	return as
}
