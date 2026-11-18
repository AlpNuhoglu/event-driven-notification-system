// Checks oapi-codegen strict server on net/http ServeMux, errors as problem+json.
// Run: go test -count=1 -v ./...
package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/AlpNuhoglu/event-driven-notification-system/experiments/e2/api"
)

func do(t *testing.T, method, path, body string) (*http.Response, []byte) {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	newHandler().ServeHTTP(rec, req)
	res := rec.Result()
	t.Cleanup(func() { _ = res.Body.Close() })
	return res, rec.Body.Bytes()
}

func TestStrictServer(t *testing.T) {
	problem := func(t *testing.T, status int, res *http.Response, b []byte) {
		t.Helper()
		if res.StatusCode != status {
			t.Fatalf("status %d, want %d (%s)", res.StatusCode, status, b)
		}
		if ct := res.Header.Get("Content-Type"); ct != "application/problem+json" {
			t.Fatalf("content-type %q", ct)
		}
		var p api.Problem
		if err := json.Unmarshal(b, &p); err != nil || p.Status != status {
			t.Fatalf("bad problem body %s (%v)", b, err)
		}
	}
	thing := func(t *testing.T, status int, res *http.Response, b []byte) {
		t.Helper()
		if res.StatusCode != status || res.Header.Get("Content-Type") != "application/json" {
			t.Fatalf("got %d %q %s", res.StatusCode, res.Header.Get("Content-Type"), b)
		}
		var th api.Thing
		if err := json.Unmarshal(b, &th); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("a post valid", func(t *testing.T) {
		res, b := do(t, "POST", "/things", `{"name":"x"}`)
		thing(t, 201, res, b)
	})
	t.Run("b get known", func(t *testing.T) {
		res, b := do(t, "GET", "/things/"+knownID, "")
		thing(t, 200, res, b)
	})
	t.Run("c get unknown typed 404", func(t *testing.T) {
		res, b := do(t, "GET", "/things/"+unknownID, "")
		problem(t, 404, res, b)
	})
	t.Run("d not a uuid", func(t *testing.T) {
		res, b := do(t, "GET", "/things/not-a-uuid", "")
		problem(t, 400, res, b)
	})
	t.Run("e malformed json", func(t *testing.T) {
		res, b := do(t, "POST", "/things", `{"name":`)
		problem(t, 400, res, b)
	})
	t.Run("f handler error", func(t *testing.T) {
		res, b := do(t, "POST", "/things", `{"name":"boom"}`)
		problem(t, 500, res, b)
	})
	t.Run("g delete not in spec", func(t *testing.T) {
		res, b := do(t, "DELETE", "/things", "")
		t.Logf("status=%d content-type=%q allow=%q body=%q", res.StatusCode, res.Header.Get("Content-Type"), res.Header.Get("Allow"), b)
		if res.StatusCode != 405 || res.Header.Get("Content-Type") != "text/plain; charset=utf-8" {
			t.Fatalf("unexpected: %d %q", res.StatusCode, res.Header.Get("Content-Type"))
		}
	})
	t.Run("h empty object body (no schema validation)", func(t *testing.T) {
		res, b := do(t, "POST", "/things", `{}`)
		t.Logf("status=%d content-type=%q body=%s", res.StatusCode, res.Header.Get("Content-Type"), b)
	})
}
