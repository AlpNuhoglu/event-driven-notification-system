package domain

import (
	"log/slog"
	"strings"
)

// Recipient is a phone number, an e-mail address or a device token. Every
// formatting and encoding path (fmt, slog, encoding/json) yields the masked
// form; only Reveal returns the real value. Code that must hand the real
// value on (the database, the provider request, the API response) calls
// Reveal explicitly: marshalling a Recipient directly sends the mask.
type Recipient string

const maskTail = "***"

// Masked returns the masked form: a phone number +905551234567 becomes
// +90555***4567, an e-mail alice@example.com becomes a***@example.com and a
// device token keeps only its first six characters.
func (r Recipient) Masked() string {
	s := string(r)
	switch {
	case strings.HasPrefix(s, "+"):
		rs := []rune(s)
		if len(rs) < 11 {
			return maskTail
		}
		return string(rs[:6]) + maskTail + string(rs[len(rs)-4:])
	case strings.Contains(s, "@"):
		at := strings.LastIndex(s, "@")
		local, domain := []rune(s[:at]), s[at+1:]
		if len(local) == 0 {
			return maskTail + "@" + domain
		}
		return string(local[:1]) + maskTail + "@" + domain
	default:
		rs := []rune(s)
		if len(rs) < 7 {
			return maskTail
		}
		return string(rs[:6]) + maskTail
	}
}

// String returns the masked form, so fmt verbs cannot leak the value.
func (r Recipient) String() string { return r.Masked() }

// GoString returns the masked form for the %#v verb.
func (r Recipient) GoString() string { return `"` + r.Masked() + `"` }

// LogValue implements slog.LogValuer and always logs the masked form.
func (r Recipient) LogValue() slog.Value { return slog.StringValue(r.Masked()) }

// Reveal returns the real value. It is the only way to get it.
func (r Recipient) Reveal() string { return string(r) }

// MarshalText returns the masked form, so encoding/json (and slog's JSON
// handler, for a Recipient nested in a struct or slice) cannot leak the value.
func (r Recipient) MarshalText() ([]byte, error) { return []byte(r.Masked()), nil }
