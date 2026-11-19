package logging

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

// Reserved fake values (D71).
const (
	canaryContent  = "Your code is 917253"
	canaryVariable = "variable-value-48213"
	realPhone      = "+905551234567"
	maskedPhone    = "+90555***4567"
	realEmail      = "alice@example.com"
	maskedEmail    = "a***@example.com"
)

func debugLogger(buf *bytes.Buffer) *slog.Logger {
	return New(buf, Options{Service: "test", Level: slog.LevelDebug})
}

// content, variables and template_vars are dropped wherever they appear,
// whatever their type, and the line keeps its other attributes.
func TestRedact_DropsContentAndVariables(t *testing.T) {
	vars := map[string]any{"code": canaryVariable}
	cases := map[string]func(l *slog.Logger){
		"top level":      func(l *slog.Logger) { l.Info("m", "content", canaryContent, "kept", 1) },
		"variables":      func(l *slog.Logger) { l.Info("m", "variables", vars, "kept", 1) },
		"template_vars":  func(l *slog.Logger) { l.Info("m", slog.Any("template_vars", vars), "kept", 1) },
		"bytes":          func(l *slog.Logger) { l.Info("m", "content", []byte(canaryContent), "kept", 1) },
		"inside a group": func(l *slog.Logger) { l.Info("m", slog.Group("n", "content", canaryContent, "kept", 1)) },
		"nested groups":  func(l *slog.Logger) { l.Info("m", slog.Group("a", slog.Group("b", "variables", vars)), "kept", 1) },
		"group named content": func(l *slog.Logger) {
			l.Info("m", slog.Group("content", "body", canaryContent), "kept", 1)
		},
		"through With":      func(l *slog.Logger) { l.With("content", canaryContent).Info("m", "kept", 1) },
		"With in a group":   func(l *slog.Logger) { l.WithGroup("g").With("template_vars", vars).Info("m", "kept", 1) },
		"WithGroup content": func(l *slog.Logger) { l.With("kept", 1).WithGroup("content").Info("m", "body", canaryContent) },
		"other case":        func(l *slog.Logger) { l.Info("m", "Content", canaryContent, "VARIABLES", vars, "kept", 1) },
		"map member":        func(l *slog.Logger) { l.Info("m", "req", map[string]any{"content": canaryContent, "kept": 1}) },
		"string map member": func(l *slog.Logger) { l.Info("m", "req", map[string]string{"variables": canaryVariable, "kept": "1"}) },
		"nested map": func(l *slog.Logger) {
			l.Info("m", "req", map[string]any{"inner": map[string]any{"template_vars": vars}}, "kept", 1)
		},
		"debug level": func(l *slog.Logger) { l.Debug("m", "content", canaryContent, "kept", 1) },
		"error level": func(l *slog.Logger) { l.Error("m", "content", canaryContent, "kept", 1) },
	}
	for name, logIt := range cases {
		var buf bytes.Buffer
		logIt(debugLogger(&buf))
		out := buf.String()
		// A group that redaction empties leaves valid JSON behind (see
		// handler.Handle on the JSON handler's own handling of that case).
		if !json.Valid(buf.Bytes()) {
			t.Errorf("%s: not valid JSON: %s", name, out)
		}
		if strings.Contains(out, canaryContent) || strings.Contains(out, canaryVariable) {
			t.Errorf("%s: a dropped value was written: %s", name, out)
		}
		for _, k := range []string{`"content"`, `"variables"`, `"template_vars"`} {
			if strings.Contains(strings.ToLower(out), k) {
				t.Errorf("%s: key %s was written: %s", name, k, out)
			}
		}
		if !strings.Contains(out, `"kept"`) {
			t.Errorf("%s: the other attributes are missing: %s", name, out)
		}
	}
}

// recipient is masked whatever its type and wherever it appears.
func TestRedact_MasksRecipient(t *testing.T) {
	cases := []struct {
		name   string
		logIt  func(l *slog.Logger)
		secret string
		want   string
	}{
		{"plain string", func(l *slog.Logger) { l.Info("m", "recipient", realPhone) }, realPhone, `"recipient":"` + maskedPhone + `"`},
		{"e-mail string", func(l *slog.Logger) { l.Info("m", "recipient", realEmail) }, realEmail, `"recipient":"` + maskedEmail + `"`},
		{"Recipient", func(l *slog.Logger) { l.Info("m", "recipient", domain.Recipient(realPhone)) }, realPhone, `"recipient":"` + maskedPhone + `"`},
		{"inside a group", func(l *slog.Logger) { l.Info("m", slog.Group("n", "recipient", realPhone)) }, realPhone, `"n":{"recipient":"` + maskedPhone + `"}`},
		{"through With", func(l *slog.Logger) { l.With("recipient", realEmail).Info("m") }, realEmail, `"recipient":"` + maskedEmail + `"`},
		{"other case", func(l *slog.Logger) { l.Info("m", "Recipient", realPhone) }, realPhone, `"Recipient":"` + maskedPhone + `"`},
		{"not a string", func(l *slog.Logger) { l.Info("m", "recipient", []string{realPhone}) }, realPhone, `"recipient":"***"`},
		{"a number", func(l *slog.Logger) { l.Info("m", "recipient", 905551234567) }, "905551234567", `"recipient":"***"`},
		{"group named recipient", func(l *slog.Logger) { l.Info("m", slog.Group("recipient", "to", realPhone)) }, realPhone, `"recipient":{"to":"` + maskedPhone + `"}`},
		{"map member", func(l *slog.Logger) { l.Info("m", "req", map[string]string{"recipient": realEmail}) }, realEmail, `"req":{"recipient":"` + maskedEmail + `"}`},
	}
	for _, tt := range cases {
		var buf bytes.Buffer
		tt.logIt(debugLogger(&buf))
		out := buf.String()
		if strings.Contains(out, tt.secret) {
			t.Errorf("%s: the real value was written: %s", tt.name, out)
		}
		if !strings.Contains(out, tt.want) {
			t.Errorf("%s: want %s in %s", tt.name, tt.want, out)
		}
	}
}

type envelope struct {
	To   domain.Recipient
	Note string
}

// A domain.Recipient is masked however it reaches the central handler or a
// formatting path (ADR-0009 §4, typed redaction).
func TestRecipient_MaskedInEveryPath(t *testing.T) {
	for _, raw := range []string{realPhone, realEmail, "device-token-0123456789abcdef"} {
		r := domain.Recipient(raw)
		var buf bytes.Buffer
		l := debugLogger(&buf)
		l.Info("attribute", "to", r)
		l.Info("struct", "env", envelope{To: r, Note: "n"})
		l.Info("pointer to struct", "env", &envelope{To: r})
		l.Info("slice", "to", []domain.Recipient{r, r})
		l.Info("map value", "to", map[string]domain.Recipient{"a": r})
		l.Info("map value through any", "to", map[string]any{"a": r})
		l.Info("group", slog.Group("g", slog.Any("to", r)))
		l.With("to", r).WithGroup("g").Info("with", "to", r)
		out := buf.String()
		if strings.Count(out, "\n") != 8 {
			t.Fatalf("%s: want 8 lines: %s", r, out)
		}
		if strings.Count(out, r.Masked()) < 8 {
			t.Errorf("the masked form is missing from some lines: %s", out)
		}
		formatted := fmt.Sprintf("%v|%s|%+v|%#v|%q|%v|%+v|%#v|%v",
			r, r, r, r, r, envelope{To: r}, envelope{To: r}, envelope{To: r}, []domain.Recipient{r})
		direct, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		nested, err := json.Marshal(map[string]any{"env": envelope{To: r}, "list": []domain.Recipient{r}, "map": map[string]domain.Recipient{"k": r}})
		if err != nil {
			t.Fatal(err)
		}
		for name, s := range map[string]string{"log": out, "fmt": formatted, "json": string(direct), "nested json": string(nested)} {
			if strings.Contains(s, raw) {
				t.Errorf("%s: the real value %q leaked: %s", name, raw, s)
			}
		}
		if string(direct) != `"`+r.Masked()+`"` {
			t.Errorf("json.Marshal = %s, want the masked form", direct)
		}
	}
}
