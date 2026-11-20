package api

import (
	"net/http"
	"testing"
)

// /metrics and pprof stay on the internal admin port (D33): the api's
// public handler does not serve them, while it does serve the health
// endpoints (TestHealth).
func TestRouter_MetricsAndPprofAreNotPublic(t *testing.T) {
	c, _ := newFakeAPI(t, &fakeRepo{})
	for _, path := range []string{"/metrics", "/debug/pprof/", "/debug/pprof/heap", "/debug/pprof/goroutine"} {
		if res := c.do(http.MethodGet, path, "", nil); res.status != http.StatusNotFound {
			t.Errorf("GET %s on the public port: status %d, want 404", path, res.status)
		}
	}
}
