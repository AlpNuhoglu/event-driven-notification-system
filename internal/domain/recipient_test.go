package domain

import (
	"bytes"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

func TestMasked(t *testing.T) {
	tests := []struct{ in, want string }{
		{"+905551234567", "+90555***4567"},
		{"+90555123", "***"},
		{"alice@example.com", "a***@example.com"},
		{"@example.com", "***@example.com"},
		{"abcdef0123456789", "abcdef***"},
		{"abcdef", "***"},
		{"", "***"},
	}
	for _, tt := range tests {
		if got := Recipient(tt.in).Masked(); got != tt.want {
			t.Errorf("Masked(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
	if Recipient("alice@example.com").Reveal() != "alice@example.com" {
		t.Error("Reveal must return the real value")
	}
}

type holder struct {
	To   Recipient
	Name string
}

func TestRecipientNeverLeaks(t *testing.T) {
	for _, raw := range []string{"+905551234567", "alice@example.com", "abcdef0123456789"} {
		r := Recipient(raw)
		var buf bytes.Buffer
		l := slog.New(slog.NewJSONHandler(&buf, nil))
		l.Info("x", "to", r)
		l.Info("y", slog.Any("h", holder{To: r, Name: "n"}))
		l.Info("z", slog.Group("g", slog.Any("to", r)))
		l.Info("w", slog.Any("rs", []Recipient{r}))
		out := buf.String() + fmt.Sprintf("%s %v %+v %q %#v %v %+v", r, r, r, r, r, holder{To: r}, holder{To: r})
		if strings.Contains(out, raw) {
			t.Errorf("raw value %q leaked: %s", raw, out)
		}
		if !strings.Contains(buf.String(), r.Masked()) {
			t.Errorf("masked form missing from log: %s", buf.String())
		}
	}
}
