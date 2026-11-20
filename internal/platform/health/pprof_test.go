package health

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// The admin handler serves the runtime's profiles (D33, ADR-0009 §8); the
// handler of a public port, which has the health endpoints only, does not.
func TestAdminHandler_ServesPprofAndThePublicHandlerDoesNot(t *testing.T) {
	admin := httptest.NewServer(AdminHandler(http.NotFoundHandler(), NewReadiness(0)))
	defer admin.Close()
	public := httptest.NewServer(Handler())
	defer public.Close()

	get := func(url string) (int, string) {
		t.Helper()
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, http.NoBody)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, string(b)
	}

	if code, body := get(admin.URL + "/debug/pprof/"); code != http.StatusOK || !strings.Contains(body, "goroutine") {
		t.Errorf("admin GET /debug/pprof/: status %d, want 200 and the list of profiles; body:\n%s", code, body)
	}
	if code, body := get(admin.URL + "/debug/pprof/goroutine?debug=1"); code != http.StatusOK || !strings.Contains(body, "goroutine profile:") {
		t.Errorf("admin GET /debug/pprof/goroutine?debug=1: status %d, want 200 and a goroutine profile; body:\n%s", code, body)
	}
	if code, _ := get(admin.URL + "/debug/pprof/cmdline"); code != http.StatusOK {
		t.Errorf("admin GET /debug/pprof/cmdline: status %d, want 200", code)
	}
	for _, path := range []string{"/debug/pprof/", "/debug/pprof/goroutine", "/debug/pprof/profile", "/metrics"} {
		if code, _ := get(public.URL + path); code != http.StatusNotFound {
			t.Errorf("public GET %s: status %d, want 404", path, code)
		}
	}
}
