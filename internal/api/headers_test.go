package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestValidRequestID(t *testing.T) {
	tests := []struct {
		id   string
		want bool
	}{
		{"abc", true},
		{"A-Z_a.z-0_9", true},
		{"0193f0d2-7c1e-7b3a-9d41-5be0a1c2e3f4", true},
		{strings.Repeat("a", 128), true},
		{strings.Repeat("a", 129), false},
		{"", false},
		{"a b", false},
		{"a/b", false},
		{"a\nb", false},
		{"ş", false},
		{"<script>", false},
	}
	for _, tt := range tests {
		if got := validRequestID(tt.id); got != tt.want {
			t.Errorf("validRequestID(%q) = %v, want %v", tt.id, got, tt.want)
		}
	}
}

func TestCorrelationID(t *testing.T) {
	if got := correlationID([]string{"req-1"}); got != "req-1" {
		t.Errorf("valid id replaced: %q", got)
	}
	for _, in := range [][]string{nil, {""}, {"bad id"}, {strings.Repeat("a", 129)}, {"a", "b"}} {
		got := correlationID(in)
		id, err := uuid.Parse(got)
		if err != nil || id.Version() != 7 {
			t.Errorf("correlationID(%q) = %q, want a new UUIDv7", in, got)
		}
	}
}

func TestTraceParent(t *testing.T) {
	const valid = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	tests := []struct {
		name string
		in   []string
		want string
	}{
		{"valid", []string{valid}, valid},
		{"absent", nil, ""},
		{"two values", []string{valid, valid}, ""},
		{"upper case hex", []string{strings.ToUpper(valid)}, ""},
		{"version ff", []string{"ff" + valid[2:]}, ""},
		{"zero trace id", []string{"00-00000000000000000000000000000000-00f067aa0ba902b7-01"}, ""},
		{"zero parent id", []string{"00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01"}, ""},
		{"short", []string{valid[:54]}, ""},
		{"version 00 with suffix", []string{valid + "-extra"}, ""},
		{"wrong separator", []string{strings.Replace(valid, "-", "_", 1)}, ""},
		{"non-hex", []string{"00-4bf92f3577b34da6a3ce929d0e0e473g-00f067aa0ba902b7-01"}, ""},
		{"later version with suffix", []string{"01" + valid[2:] + "-future"}, "01" + valid[2:]},
		{"later version without dash", []string{"01" + valid[2:] + "x"}, ""},
	}
	for _, tt := range tests {
		if got := traceParent(tt.in); got != tt.want {
			t.Errorf("%s: traceParent(%q) = %q, want %q", tt.name, tt.in, got, tt.want)
		}
	}
}

// The middleware normalises both headers on the request and echoes the id.
func TestWithRequestHeaders(t *testing.T) {
	r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/healthz", http.NoBody)
	r.Header.Add("X-Request-ID", "a")
	r.Header.Add("X-Request-ID", "b")
	r.Header.Set("traceparent", "garbage")
	w := httptest.NewRecorder()
	r2, st := withRequestHeaders(w, r)
	if got := w.Header().Get("X-Request-ID"); got != st.correlationID || got == "a" || got == "b" {
		t.Errorf("echoed %q, state %q", got, st.correlationID)
	}
	if vs := r2.Header.Values("X-Request-ID"); len(vs) != 1 || vs[0] != st.correlationID {
		t.Errorf("request header = %q", vs)
	}
	if _, ok := r2.Header["Traceparent"]; ok || st.traceParent != "" {
		t.Errorf("invalid traceparent kept: %q", st.traceParent)
	}
	if stateFrom(r2.Context()) != st {
		t.Error("state not in the context")
	}
}
