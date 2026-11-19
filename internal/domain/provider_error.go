package domain

import (
	"bytes"
	"encoding/json"
	"slices"
	"strings"
	"unicode/utf8"
)

// MaxProviderErrorBytes bounds a provider error as it is stored in
// notification_attempts.error (ADR-0009 §4).
const MaxProviderErrorBytes = 1024

// ContentRedacted replaces the message's content in a stored provider
// error.
const ContentRedacted = "[content redacted]"

// providerErrorRedacted replaces the whole text in the unlikely case that
// the replacements themselves keep forming the values again.
const providerErrorRedacted = "[provider error redacted]"

// truncatedSuffix ends a provider error that was cut to
// MaxProviderErrorBytes.
const truncatedSuffix = "...(truncated)"

// RedactProviderError prepares the text of a provider error for
// notification_attempts.error, the only place it is stored (the value-based
// redaction of ADR-0009 §4). The free text of a provider can quote the
// message it was sent, which neither the Recipient type nor a key-based rule
// can catch, so:
//
//   - every occurrence of the message's own recipient is replaced by its
//     masked form (matched without regard to ASCII case, as a provider may
//     write an e-mail address in lower case; a phone number also without its
//     plus sign), and every occurrence of its
//     content by ContentRedacted; both are also matched in their JSON-escaped
//     spellings, in which a provider's JSON body quotes them;
//   - the replacements are made before the cut, so the cut can never leave a
//     part of a value behind; the result is valid UTF-8 without NUL bytes
//     (Postgres text accepts nothing else) and at most MaxProviderErrorBytes
//     long, cut on a character boundary.
//
// cut reports that text is the beginning of a longer text (a body read up to
// a limit). A value can then be broken off at its end, where no pattern
// matches it; so the end is dropped first, as many bytes as the longest
// spelling of a value minus one: an occurrence that was broken off began
// inside them.
//
// It protects only against the message's own values, not against other
// personal data a provider might return.
func RedactProviderError(text string, cut bool, recipient Recipient, content string) string {
	pats := redactionPatterns(recipient, content)
	if cut {
		longest := 0
		for _, p := range pats {
			longest = max(longest, len(p.value))
		}
		text = text[:max(0, len(text)-max(0, longest-1))]
	}
	s := strings.ToValidUTF8(strings.ReplaceAll(text, "\x00", ""), "�")
	for range 4 {
		if !containsAny(s, pats) {
			return truncateUTF8(s, MaxProviderErrorBytes)
		}
		s = replaceMatches(s, pats)
	}
	if containsAny(s, pats) {
		return providerErrorRedacted
	}
	return truncateUTF8(s, MaxProviderErrorBytes)
}

// pattern is one spelling of a value and what replaces it.
type pattern struct {
	value     string
	fold      bool // match ASCII letters without regard to case
	content   bool // a spelling of the content (else of the recipient)
	replaceBy string
}

func redactionPatterns(recipient Recipient, content string) []pattern {
	var pats []pattern
	add := func(v string, fold, isContent bool, by string) {
		if v == "" || slices.ContainsFunc(pats, func(p pattern) bool { return p.value == v && p.fold == fold }) {
			return
		}
		pats = append(pats, pattern{value: v, fold: fold, content: isContent, replaceBy: by})
	}
	for _, v := range jsonSpellings(content) {
		add(v, false, true, ContentRedacted)
	}
	masked := recipient.Masked()
	for _, v := range jsonSpellings(recipient.Reveal()) {
		add(v, true, false, masked)
	}
	// A provider may write a phone number without its plus sign.
	if digits, ok := strings.CutPrefix(recipient.Reveal(), "+"); ok && digits != "" {
		add(digits, false, false, masked)
	}
	return pats
}

// jsonSpellings returns s and the forms encoding/json writes it in inside a
// string, with and without HTML escaping.
func jsonSpellings(s string) []string {
	if s == "" {
		return nil
	}
	out := []string{s}
	for _, html := range []bool{true, false} {
		var b bytes.Buffer
		enc := json.NewEncoder(&b)
		enc.SetEscapeHTML(html)
		if enc.Encode(s) != nil {
			continue
		}
		q := strings.TrimSuffix(b.String(), "\n")
		out = append(out, q[1:len(q)-1])
	}
	return out
}

func containsAny(s string, pats []pattern) bool {
	ls := lowerASCII(s)
	for _, p := range pats {
		if (p.fold && strings.Contains(ls, lowerASCII(p.value))) || (!p.fold && strings.Contains(s, p.value)) {
			return true
		}
	}
	return false
}

// span is a byte range [from, to) of the text that a value occupies.
type span struct {
	from, to int
	content  bool
}

// replaceMatches replaces every occurrence of every pattern, overlapping
// ones included. Overlapping occurrences are merged into one range, which
// becomes ContentRedacted if it holds the content, else the masked
// recipient. Lowering ASCII letters keeps every byte offset, so the offsets
// found in the lowered text are offsets of s.
func replaceMatches(s string, pats []pattern) string {
	ls := lowerASCII(s)
	var spans []span
	for _, p := range pats {
		hay, needle := s, p.value
		if p.fold {
			hay, needle = ls, lowerASCII(p.value)
		}
		for i := 0; i <= len(hay)-len(needle); {
			j := strings.Index(hay[i:], needle)
			if j < 0 {
				break
			}
			spans = append(spans, span{from: i + j, to: i + j + len(needle), content: p.content})
			i += j + 1
		}
	}
	slices.SortFunc(spans, func(a, b span) int { return a.from - b.from })
	masked := ""
	for _, p := range pats {
		if !p.content {
			masked = p.replaceBy
			break
		}
	}
	var b strings.Builder
	last := 0
	for i := 0; i < len(spans); {
		cur := spans[i]
		for i++; i < len(spans) && spans[i].from < cur.to; i++ {
			cur.to = max(cur.to, spans[i].to)
			cur.content = cur.content || spans[i].content
		}
		b.WriteString(s[last:cur.from])
		if cur.content {
			b.WriteString(ContentRedacted)
		} else {
			b.WriteString(masked)
		}
		last = cur.to
	}
	b.WriteString(s[last:])
	return b.String()
}

// lowerASCII lowers the ASCII letters of s and leaves every other byte as
// it is, so that the result has the same length and offsets as s.
func lowerASCII(s string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'A' && r <= 'Z' {
			return r + ('a' - 'A')
		}
		return r
	}, s)
}

// truncateUTF8 cuts s to at most limit bytes on a character boundary and
// marks the cut.
func truncateUTF8(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	cut := limit - len(truncatedSuffix)
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + truncatedSuffix
}
