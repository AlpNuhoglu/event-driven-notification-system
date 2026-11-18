// Package api embeds the OpenAPI specification, api/openapi.yaml: the single
// source of truth of the HTTP contract. The server code in
// internal/api/apigen is generated from it, and the API serves it at
// /openapi.yaml.
package api

import _ "embed"

//go:embed openapi.yaml
var openAPI []byte

// OpenAPI returns a copy of the specification document.
func OpenAPI() []byte {
	out := make([]byte, len(openAPI))
	copy(out, openAPI)
	return out
}
