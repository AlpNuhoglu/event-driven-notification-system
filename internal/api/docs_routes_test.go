package api

import (
	"context"
	"net/http"
	"strings"
	"testing"
)

func TestDocsRoutes(t *testing.T) {
	c, _ := newFakeAPI(t, &fakeRepo{})
	tests := []struct {
		method, path string
		status       int
		contentType  string
		allow        string
	}{
		{http.MethodGet, "/docs", http.StatusOK, "text/html", ""},
		{http.MethodGet, "/docs/swagger-ui-bundle.js", http.StatusOK, "text/javascript", ""},
		{http.MethodGet, "/docs/nope", http.StatusNotFound, "application/problem+json", ""},
		{http.MethodGet, "/docs/LICENSE", http.StatusNotFound, "application/problem+json", ""},
		{http.MethodPost, "/docs", http.StatusMethodNotAllowed, "application/problem+json", "GET"},
		{http.MethodPost, "/docs/swagger-ui.css", http.StatusMethodNotAllowed, "application/problem+json", "GET"},
		{http.MethodGet, "/openapi.yaml", http.StatusOK, "", ""},
	}
	for _, tc := range tests {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			req, err := http.NewRequestWithContext(context.Background(), tc.method, c.srv.URL+tc.path, nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := c.srv.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != tc.status {
				t.Fatalf("status %d, want %d", resp.StatusCode, tc.status)
			}
			if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, tc.contentType) {
				t.Errorf("Content-Type %q, want prefix %q", ct, tc.contentType)
			}
			if tc.allow != "" && !strings.Contains(resp.Header.Get("Allow"), tc.allow) {
				t.Errorf("Allow %q, want it to contain %q", resp.Header.Get("Allow"), tc.allow)
			}
		})
	}
}
