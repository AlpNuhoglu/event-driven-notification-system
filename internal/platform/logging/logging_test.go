package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

// lines decodes the JSON lines written to buf.
func lines(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for raw := range strings.Lines(buf.String()) {
		var m map[string]any
		if err := json.Unmarshal([]byte(raw), &m); err != nil {
			t.Fatalf("not a JSON line %q: %v", raw, err)
		}
		out = append(out, m)
	}
	return out
}

func only(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	ls := lines(t, buf)
	if len(ls) != 1 {
		t.Fatalf("want 1 line, got %d: %s", len(ls), buf)
	}
	return ls[0]
}

func spanContext(t *testing.T) trace.SpanContext {
	t.Helper()
	tid, err := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	if err != nil {
		t.Fatal(err)
	}
	sid, err := trace.SpanIDFromHex("00f067aa0ba902b7")
	if err != nil {
		t.Fatal(err)
	}
	return trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled})
}

func TestHandler_StandardFieldsOnEveryRecord(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, Options{Service: "relay"})
	l.Info("one")
	l.Warn("two", "k", "v")
	l.With("a", 1).WithGroup("g").Error("three", "b", 2)
	for _, m := range lines(t, &buf) {
		if m["service"] != "relay" || m["version"] != "dev" {
			t.Errorf("service %v, version %v; want relay, dev: %v", m["service"], m["version"], m)
		}
		if _, ok := m["role"]; ok {
			t.Errorf("the role key is still written: %v", m)
		}
	}

	buf.Reset()
	New(&buf, Options{Service: "api", Version: "0123abc"}).Info("x")
	if m := only(t, &buf); m["version"] != "0123abc" {
		t.Errorf("version %v, want 0123abc", m["version"])
	}
}

func TestHandler_FieldsFromContext(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, Options{Service: "worker"})
	ctx := trace.ContextWithSpanContext(WithCorrelationID(context.Background(), "req-1"), spanContext(t))

	l.InfoContext(ctx, "with")
	m := only(t, &buf)
	if m["correlation_id"] != "req-1" || m["trace_id"] != "4bf92f3577b34da6a3ce929d0e0e4736" || m["span_id"] != "00f067aa0ba902b7" {
		t.Errorf("context fields %v", m)
	}

	// Without them in the context, the fields are absent, not empty.
	buf.Reset()
	l.InfoContext(context.Background(), "without")
	l.Info("no context")
	for _, m := range lines(t, &buf) {
		for _, k := range []string{"correlation_id", "trace_id", "span_id"} {
			if _, ok := m[k]; ok {
				t.Errorf("%s present without a value in the context: %v", k, m)
			}
		}
	}

	// An empty id leaves the context as it was.
	if WithCorrelationID(ctx, "") != ctx || CorrelationID(context.Background()) != "" {
		t.Error("an empty correlation id changed the context")
	}
}

// The fields of the context are top-level, also under an open group, and
// replace an attribute of the same key.
func TestHandler_ContextFieldsStayTopLevel(t *testing.T) {
	var buf bytes.Buffer
	ctx := WithCorrelationID(context.Background(), "req-2")
	New(&buf, Options{Service: "api"}).WithGroup("g").InfoContext(ctx, "m", "a", 1)
	m := only(t, &buf)
	g, _ := m["g"].(map[string]any)
	if m["correlation_id"] != "req-2" || g["a"] != float64(1) || g["correlation_id"] != nil {
		t.Errorf("line %v", m)
	}

	buf.Reset()
	New(&buf, Options{Service: "api"}).InfoContext(ctx, "m", "correlation_id", "spoofed")
	if raw := buf.String(); strings.Contains(raw, "spoofed") || strings.Count(raw, "correlation_id") != 1 {
		t.Errorf("an attribute replaced the context's correlation id: %s", raw)
	}
}

// WithAttrs and WithGroup nest as slog's own handlers nest them.
func TestHandler_WithAttrsAndWithGroup(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, Options{Service: "api"})
	l.With("a", 1).WithGroup("g").With("b", 2).WithGroup("h").Info("m", "c", 3)
	m := only(t, &buf)
	g, _ := m["g"].(map[string]any)
	h, _ := g["h"].(map[string]any)
	if m["a"] != float64(1) || g["b"] != float64(2) || h["c"] != float64(3) {
		t.Errorf("nesting: %s", buf.String())
	}

	// A group without attributes is left out.
	buf.Reset()
	l.WithGroup("empty").Info("m")
	if strings.Contains(buf.String(), "empty") {
		t.Errorf("an empty group was written: %s", buf.String())
	}

	// A record's attribute replaces a With attribute of the same key.
	buf.Reset()
	l.With("channel", "sms").Info("m", "channel", "email")
	if raw := buf.String(); strings.Count(raw, `"channel"`) != 1 || !strings.Contains(raw, `"channel":"email"`) {
		t.Errorf("duplicate or wrong key: %s", raw)
	}

	// The loggers derived from one another do not share their attributes.
	buf.Reset()
	base := l.With("x", 1)
	base.With("y", 2).Info("first")
	base.With("z", 3).Info("second")
	ls := lines(t, &buf)
	if _, ok := ls[1]["y"]; ok || ls[1]["z"] != float64(3) || ls[0]["z"] != nil {
		t.Errorf("attributes leaked between derived loggers: %s", buf.String())
	}
}

func TestHandler_LevelFromOptions(t *testing.T) {
	for _, tt := range []struct {
		level      slog.Leveler
		wantDebug  bool
		wantInfo   bool
		wantErrors bool
	}{
		{slog.LevelDebug, true, true, true},
		{nil, false, true, true},
		{slog.LevelError, false, false, true},
	} {
		var buf bytes.Buffer
		l := New(&buf, Options{Service: "api", Level: tt.level})
		l.Debug("debug")
		l.Info("info")
		l.Error("error")
		out := buf.String()
		if strings.Contains(out, `"msg":"debug"`) != tt.wantDebug ||
			strings.Contains(out, `"msg":"info"`) != tt.wantInfo ||
			strings.Contains(out, `"msg":"error"`) != tt.wantErrors {
			t.Errorf("level %v: %s", tt.level, out)
		}
	}
}

func TestParseLevel(t *testing.T) {
	for in, want := range map[string]slog.Level{
		"": slog.LevelInfo, "debug": slog.LevelDebug, "info": slog.LevelInfo,
		"warn": slog.LevelWarn, "error": slog.LevelError, "DEBUG": slog.LevelDebug,
	} {
		if got, err := ParseLevel(in); err != nil || got != want {
			t.Errorf("ParseLevel(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	if _, err := ParseLevel("verbose"); err == nil {
		t.Error("ParseLevel accepted an unknown level")
	}
}
