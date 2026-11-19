package swaggerui

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"testing"
)

// vendoredSHA256 are the checksums of the files copied unchanged from
// swagger-ui-dist; README.md lists the same values.
var vendoredSHA256 = map[string]string{
	"LICENSE":              "cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30",
	"NOTICE":               "0d20d1adef18aee3f40dd258172155521ce702ac445cb5f7b7d60ed32dad2fb2",
	"favicon-32x32.png":    "3ed612f41e050ca5e7000cad6f1cbe7e7da39f65fca99c02e99e6591056e5837",
	"swagger-ui-bundle.js": "e6a898692f2d768e807c3ba4626068a69a2c5dd960dd6c76f3e4d3a3de180af2",
	"swagger-ui.css":       "5ed08757e53da3e026d6cbecc61fe5d43585c9560b302d534ad56692519a273d",
}

func get(t *testing.T, path string, hdr ...string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil)
	for i := 0; i+1 < len(hdr); i += 2 {
		r.Header.Set(hdr[i], hdr[i+1])
	}
	w := httptest.NewRecorder()
	Handler().ServeHTTP(w, r)
	return w
}

func TestServesPageAndAssets(t *testing.T) {
	tests := []struct{ path, ct string }{
		{"/docs", "text/html; charset=utf-8"},
		{"/docs/swagger-ui.css", "text/css; charset=utf-8"},
		{"/docs/swagger-ui-bundle.js", "text/javascript; charset=utf-8"},
		{"/docs/swagger-initializer.js", "text/javascript; charset=utf-8"},
		{"/docs/favicon-32x32.png", "image/png"},
	}
	for _, tc := range tests {
		t.Run(tc.path, func(t *testing.T) {
			w := get(t, tc.path)
			if w.Code != http.StatusOK {
				t.Fatalf("status %d", w.Code)
			}
			h := w.Header()
			if h.Get("Content-Type") != tc.ct {
				t.Errorf("Content-Type %q", h.Get("Content-Type"))
			}
			if h.Get("X-Content-Type-Options") != "nosniff" {
				t.Errorf("X-Content-Type-Options %q", h.Get("X-Content-Type-Options"))
			}
			if h.Get("Cache-Control") != "no-cache" {
				t.Errorf("Cache-Control %q", h.Get("Cache-Control"))
			}
			if h.Get("ETag") == "" {
				t.Error("no ETag")
			}
			csp := h.Get("Content-Security-Policy")
			if tc.path == "/docs" {
				if csp != contentSecurityPolicy || !strings.Contains(csp, "default-src 'none'; script-src 'self'") {
					t.Errorf("Content-Security-Policy %q", csp)
				}
			} else if csp != "" {
				t.Errorf("unexpected Content-Security-Policy %q", csp)
			}
			if w.Body.Len() == 0 {
				t.Error("empty body")
			}
		})
	}
}

func TestETag(t *testing.T) {
	for _, p := range []string{"/docs", "/docs/swagger-ui-bundle.js"} {
		etag := get(t, p).Header().Get("ETag")
		for _, inm := range []string{etag, "W/" + etag, `"x", ` + etag, "*"} {
			w := get(t, p, "If-None-Match", inm)
			if w.Code != http.StatusNotModified || w.Body.Len() != 0 {
				t.Errorf("%s If-None-Match %q: status %d, %d body bytes", p, inm, w.Code, w.Body.Len())
			}
			if w.Header().Get("ETag") != etag || w.Header().Get("Cache-Control") != "no-cache" {
				t.Errorf("%s: 304 lacks ETag or Cache-Control", p)
			}
		}
		if w := get(t, p, "If-None-Match", `"other"`); w.Code != http.StatusOK {
			t.Errorf("%s: non-matching If-None-Match gave %d", p, w.Code)
		}
	}
}

func TestNotServed(t *testing.T) {
	for _, p := range []string{
		"/docs/nope", "/docs/", "/docs/index.html", "/docs/LICENSE", "/docs/NOTICE",
		"/docs/..%2Fserver.go", "/docs/../server.go", "/docs/dist/index.html",
		"/docs/swagger-ui.css/", `/docs/..\server.go`, "/other",
	} {
		r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil)
		r.URL.Path = strings.ReplaceAll(p, "%2F", "/")
		w := httptest.NewRecorder()
		Handler().ServeHTTP(w, r)
		if w.Code != http.StatusNotFound {
			t.Errorf("%s: status %d, want 404", p, w.Code)
		}
	}
}

func TestMethods(t *testing.T) {
	r := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/docs", nil)
	w := httptest.NewRecorder()
	Handler().ServeHTTP(w, r)
	if w.Code != http.StatusMethodNotAllowed || w.Header().Get("Allow") == "" {
		t.Errorf("POST: status %d, Allow %q", w.Code, w.Header().Get("Allow"))
	}
	r = httptest.NewRequestWithContext(context.Background(), http.MethodHead, "/docs", nil)
	w = httptest.NewRecorder()
	Handler().ServeHTTP(w, r)
	if w.Code != http.StatusOK || w.Body.Len() != 0 {
		t.Errorf("HEAD: status %d, %d body bytes", w.Code, w.Body.Len())
	}
}

func TestVendoredFilesUnchanged(t *testing.T) {
	for name, want := range vendoredSHA256 {
		b, err := dist.ReadFile("dist/" + name)
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(b)
		if got := hex.EncodeToString(sum[:]); got != want {
			t.Errorf("%s: sha256 %s, want %s", name, got, want)
		}
	}
}

func TestReadmeListsChecksums(t *testing.T) {
	b, err := readFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	for name, sum := range vendoredSHA256 {
		if !strings.Contains(b, sum+"  dist/"+name) {
			t.Errorf("README.md does not list %s %s", name, sum)
		}
	}
}

func readFile(name string) (string, error) {
	b, err := os.ReadFile(name) //nolint:gosec // fixed file name of this package
	return string(b), err
}

func embedded(t *testing.T, name string) string {
	t.Helper()
	b, err := dist.ReadFile("dist/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestOwnFilesAreSelfContained(t *testing.T) {
	remote := regexp.MustCompile(`(?i)(https?:)?//[a-z0-9]`)
	inlineScript := regexp.MustCompile(`(?is)<script(?:\s[^>]*)?>\s*[^<\s]`)
	for _, name := range []string{"index.html", "swagger-initializer.js"} {
		s := embedded(t, name)
		if m := remote.FindString(s); m != "" {
			t.Errorf("%s references another host: %q", name, m)
		}
		if inlineScript.MatchString(s) && name == "index.html" {
			t.Errorf("%s has an inline script", name)
		}
		if strings.Contains(strings.ToLower(s), "style=") || strings.Contains(strings.ToLower(s), "<style") {
			t.Errorf("%s has inline style", name)
		}
	}
}

func TestPageReferencesAreServed(t *testing.T) {
	page := embedded(t, "index.html")
	refs := regexp.MustCompile(`(?:src|href)="([^"]*)"`).FindAllStringSubmatch(page, -1)
	if len(refs) != 4 {
		t.Fatalf("found %d references, want 4", len(refs))
	}
	for _, m := range refs {
		if !strings.HasPrefix(m[1], "/docs/") {
			t.Errorf("reference %q is not under /docs/", m[1])
		}
		if w := get(t, m[1]); w.Code != http.StatusOK {
			t.Errorf("reference %q: status %d", m[1], w.Code)
		}
	}
}

func TestInitializer(t *testing.T) {
	s := embedded(t, "swagger-initializer.js")
	for _, want := range []string{`url: "/openapi.yaml"`, "validatorUrl: null"} {
		if !strings.Contains(s, want) {
			t.Errorf("initializer lacks %s", want)
		}
	}
}
