// Package swaggerui serves Swagger UI at /docs for the specification at
// /openapi.yaml. Every asset is embedded, so the page works with the network
// disconnected; the Content-Security-Policy of the page forbids loading
// anything from another origin.
package swaggerui

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"net/http"
	"strings"
)

//go:embed dist
var dist embed.FS

// contentSecurityPolicy is sent with the page.
const contentSecurityPolicy = "default-src 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'; " +
	"img-src 'self' data:; font-src 'self' data:; connect-src 'self'; base-uri 'none'; " +
	"form-action 'none'; frame-ancestors 'none'"

type asset struct {
	body        []byte
	contentType string
	etag        string
}

// served maps a file name of dist to its content type; LICENSE and NOTICE
// are embedded but not listed here.
var served = map[string]string{
	"index.html":             "text/html; charset=utf-8",
	"swagger-ui.css":         "text/css; charset=utf-8",
	"swagger-ui-bundle.js":   "text/javascript; charset=utf-8",
	"swagger-initializer.js": "text/javascript; charset=utf-8",
	"favicon-32x32.png":      "image/png",
}

// Handler serves GET /docs (the page) and GET /docs/{asset}. It is meant to
// be mounted at those two patterns; any other path or method is not found.
func Handler() http.Handler {
	assets := make(map[string]asset, len(served))
	for name, ct := range served {
		b, err := dist.ReadFile("dist/" + name)
		if err != nil {
			panic("swaggerui: missing embedded asset " + name + ": " + err.Error())
		}
		sum := sha256.Sum256(b)
		assets[name] = asset{body: b, contentType: ct, etag: `"` + hex.EncodeToString(sum[:16]) + `"`}
	}
	return handler{assets: assets}
}

type handler struct {
	assets map[string]asset
}

func (h handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	name, ok := assetName(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	a, ok := h.assets[name]
	if !ok {
		http.NotFound(w, r)
		return
	}
	hd := w.Header()
	hd.Set("Content-Type", a.contentType)
	hd.Set("X-Content-Type-Options", "nosniff")
	hd.Set("Cache-Control", "no-cache")
	hd.Set("ETag", a.etag)
	if name == "index.html" {
		hd.Set("Content-Security-Policy", contentSecurityPolicy)
	}
	if etagMatches(r.Header.Get("If-None-Match"), a.etag) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodGet {
		_, _ = w.Write(a.body)
	}
}

// assetName maps a request path to the name of an embedded file: /docs is the
// page, /docs/{name} an asset. A name with a slash or a dot-dot is refused.
func assetName(p string) (string, bool) {
	if p == "/docs" {
		return "index.html", true
	}
	name, ok := strings.CutPrefix(p, "/docs/")
	if !ok || name == "" || name == "index.html" || strings.ContainsAny(name, `/\`) {
		return "", false
	}
	if _, ok := served[name]; !ok {
		return "", false
	}
	return name, true
}

func etagMatches(header, etag string) bool {
	for _, v := range strings.Split(header, ",") {
		v = strings.TrimSpace(v)
		if v == "*" || strings.TrimPrefix(v, "W/") == etag {
			return true
		}
	}
	return false
}
