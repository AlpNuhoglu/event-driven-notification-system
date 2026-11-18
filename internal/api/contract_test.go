package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers"
	"github.com/getkin/kin-openapi/routers/legacy"

	spec "github.com/AlpNuhoglu/event-driven-notification-system/api"
)

// contract validates responses against api/openapi.yaml with kin-openapi.
type contract struct {
	doc     *openapi3.T
	router  routers.Router
	problem *openapi3.Schema
}

var (
	contractOnce sync.Once
	contractVal  *contract
	contractErr  error
)

func loadContract(t testing.TB) *contract {
	t.Helper()
	contractOnce.Do(func() {
		loader := openapi3.NewLoader()
		doc, err := loader.LoadFromData(spec.OpenAPI())
		if err != nil {
			contractErr = err
			return
		}
		if err := doc.Validate(loader.Context); err != nil {
			contractErr = err
			return
		}
		router, err := legacy.NewRouter(doc)
		if err != nil {
			contractErr = err
			return
		}
		contractVal = &contract{doc: doc, router: router, problem: doc.Components.Schemas["Problem"].Value}
	})
	if contractErr != nil {
		t.Fatalf("load contract: %v", contractErr)
	}
	return contractVal
}

// response is a response read completely.
type response struct {
	status int
	header http.Header
	body   []byte
}

// object decodes the body into a generic map.
func (r response) object(t testing.TB) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(r.body, &m); err != nil {
		t.Fatalf("decode body %q: %v", r.body, err)
	}
	return m
}

// problemCodes returns "field:code" for each entry of errors[].
func (r response) problemCodes(t testing.TB) []string {
	t.Helper()
	var p struct {
		Errors []struct{ Field, Code string } `json:"errors"`
	}
	if err := json.Unmarshal(r.body, &p); err != nil {
		t.Fatalf("decode problem %q: %v", r.body, err)
	}
	out := make([]string, 0, len(p.Errors))
	for _, e := range p.Errors {
		out = append(out, e.Field+":"+e.Code)
	}
	return out
}

// check fails the test when res is not described by the specification:
// status, content type, headers and body schema of the operation, or, for a
// path or method the specification does not have, a 404 or 405 problem. It
// also checks the rules every response follows: X-Request-ID is set, and an
// error is application/problem+json with correlation_id equal to it.
func (c *contract) check(t testing.TB, req *http.Request, res response) {
	t.Helper()
	reqID := res.header.Get("X-Request-ID")
	if reqID == "" {
		t.Errorf("%s %s: response has no X-Request-ID", req.Method, req.URL.Path)
	}
	if res.status >= 400 {
		if ct := res.header.Get("Content-Type"); ct != problemContentType {
			t.Errorf("%s %s: status %d with Content-Type %q, want %s", req.Method, req.URL.Path, res.status, ct, problemContentType)
		}
		var p struct {
			CorrelationID string `json:"correlation_id"`
			Status        int    `json:"status"`
		}
		if err := json.Unmarshal(res.body, &p); err != nil {
			t.Errorf("%s %s: problem body %q: %v", req.Method, req.URL.Path, res.body, err)
		}
		if p.CorrelationID != reqID {
			t.Errorf("%s %s: correlation_id %q != X-Request-ID %q", req.Method, req.URL.Path, p.CorrelationID, reqID)
		}
		if p.Status != res.status {
			t.Errorf("%s %s: problem status %d != response status %d", req.Method, req.URL.Path, p.Status, res.status)
		}
	}

	route, params, err := c.router.FindRoute(req)
	if err == nil && !routeMatches(route.Path, params, req.URL.Path) {
		// The legacy router lets a path parameter match an empty segment
		// (GET /v1/notifications finds /v1/notifications/{id}).
		err = routers.ErrPathNotFound
	}
	if err != nil {
		// Not an operation of the specification: only a 404 or a 405 problem
		// is allowed (documented in info.description).
		if res.status != http.StatusNotFound && res.status != http.StatusMethodNotAllowed {
			t.Errorf("%s %s: no operation in the spec, status %d", req.Method, req.URL.Path, res.status)
			return
		}
		if res.status == http.StatusMethodNotAllowed && res.header.Get("Allow") == "" {
			t.Errorf("%s %s: 405 without Allow", req.Method, req.URL.Path)
		}
		var v any
		if err := json.Unmarshal(res.body, &v); err != nil {
			t.Errorf("%s %s: body: %v", req.Method, req.URL.Path, err)
			return
		}
		if err := c.problem.VisitJSON(v, uuidFormat); err != nil {
			t.Errorf("%s %s: body is not a Problem: %v", req.Method, req.URL.Path, err)
		}
		return
	}
	in := &openapi3filter.ResponseValidationInput{
		RequestValidationInput: &openapi3filter.RequestValidationInput{
			Request: req, PathParams: params, Route: route,
		},
		Status: res.status,
		Header: res.header,
		Body:   io.NopCloser(bytes.NewReader(res.body)),
		Options: &openapi3filter.Options{
			IncludeResponseStatus:   true,
			MultiError:              true,
			SchemaValidationOptions: []openapi3.SchemaValidationOption{uuidFormat},
		},
	}
	if err := openapi3filter.ValidateResponse(context.Background(), in); err != nil {
		t.Errorf("%s %s: response %d does not match the spec: %v\nbody: %s", req.Method, req.URL.Path, res.status, err, res.body)
	}
}

// uuidFormat makes the check enforce format: uuid, which kin-openapi does not
// check by default. RFC 9562 includes version 7, the ids of this API.
var uuidFormat = openapi3.WithStringFormatValidator("uuid",
	openapi3.NewRegexpFormatValidator(openapi3.FormatOfStringForUUIDOfRFC9562))

// routeMatches reports whether the template with its parameters filled in
// is the request path.
func routeMatches(template string, params map[string]string, path string) bool {
	for k, v := range params {
		if v == "" {
			return false
		}
		template = strings.ReplaceAll(template, "{"+k+"}", v)
	}
	return template == path
}

// apiClient sends requests to a test server and checks every response
// against the contract.
type apiClient struct {
	t        testing.TB
	srv      *httptest.Server
	contract *contract
}

func newAPIClient(t testing.TB, h http.Handler) *apiClient {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return &apiClient{t: t, srv: srv, contract: loadContract(t)}
}

// do sends a request. A non-empty body is sent with Content-Type
// application/json unless headers set another one ("" removes it).
func (c *apiClient) do(method, path, body string, headers map[string][]string) response {
	c.t.Helper()
	var rd io.Reader = http.NoBody
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, c.srv.URL+path, rd)
	if err != nil {
		c.t.Fatalf("new request: %v", err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, vs := range headers {
		req.Header.Del(k)
		for _, v := range vs {
			switch v {
			case "":
			case emptyHeaderValue:
				req.Header.Add(k, "")
			default:
				req.Header.Add(k, v)
			}
		}
	}
	return c.send(req)
}

// emptyHeaderValue in the headers of do sends the header with an empty
// value ("" removes the header).
const emptyHeaderValue = "\x00empty"

// send sends a request and checks the response against the contract.
func (c *apiClient) send(req *http.Request) response {
	c.t.Helper()
	resp, err := c.srv.Client().Do(req)
	if err != nil {
		c.t.Fatalf("%s %s: %v", req.Method, req.URL.Path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		c.t.Fatalf("read body: %v", err)
	}
	res := response{status: resp.StatusCode, header: resp.Header, body: b}
	c.contract.check(c.t, req, res)
	return res
}

func (c *apiClient) get(path string) response { return c.do(http.MethodGet, path, "", nil) }

func (c *apiClient) post(body string) response {
	return c.do(http.MethodPost, "/v1/notifications", body, nil)
}

// recordingTB records the failures of a check instead of failing the test.
type recordingTB struct {
	testing.TB
	failures []string
}

func (r *recordingTB) Helper() {}

func (r *recordingTB) Errorf(format string, args ...any) {
	r.failures = append(r.failures, fmt.Sprintf(format, args...))
}

// The contract check itself rejects responses the specification does not
// describe.
func TestContract_RejectsUndescribedResponses(t *testing.T) {
	c := loadContract(t)
	hdr := func(ct string) http.Header {
		return http.Header{"Content-Type": {ct}, "X-Request-Id": {"r1"}}
	}
	problem := `{"type":"https://example.com/x","title":"t","status":%d,"detail":"d","correlation_id":"r1"}`
	tests := []struct {
		name, method, path string
		res                response
	}{
		{"undeclared status", http.MethodGet, "/healthz", response{418, hdr(problemContentType), fmt.Appendf(nil, problem, 418)}},
		{"body not matching the schema", http.MethodGet, "/healthz", response{200, hdr("application/json"), []byte(`{"status":"fine"}`)}},
		{"undeclared content type", http.MethodGet, "/healthz", response{200, hdr("text/plain"), []byte(`ok`)}},
		{"plain-text 404", http.MethodGet, "/nope", response{404, hdr("text/plain"), []byte("404 page not found")}},
		{"unknown path answered 200", http.MethodGet, "/nope", response{200, hdr("application/json"), []byte(`{}`)}},
		{"missing X-Request-ID", http.MethodGet, "/healthz", response{200, http.Header{"Content-Type": {"application/json"}}, []byte(`{"status":"ok"}`)}},
		{"problem without correlation id", http.MethodGet, "/v1/notifications/" + "0193f0d2-7c1e-7b3a-9d41-5be0a1c2e3f4", response{404, hdr(problemContentType), []byte(`{"type":"x","title":"t","status":404,"detail":"d"}`)}},
		{"Idempotent-Replayed not a boolean", http.MethodPost, "/v1/notifications/batch", response{202, withHeaders(hdr("application/json"), "Location", "/v1/batches/"+contractID, "Idempotent-Replayed", "yes"), []byte(contractBatchAccepted)}},
		{"batch accepted without Location", http.MethodPost, "/v1/notifications/batch", response{202, hdr("application/json"), []byte(contractBatchAccepted)}},
		{"batch accepted with null lists", http.MethodPost, "/v1/notifications/batch", response{202, withHeaders(hdr("application/json"), "Location", "/v1/batches/"+contractID), []byte(`{"batch_id":"` + contractID + `","accepted":null,"rejected":null}`)}},
		{"undeclared 409 on the batch endpoint", http.MethodPost, "/v1/notifications/batch", response{409, hdr(problemContentType), fmt.Appendf(nil, contractProblem, 409, "")}},
		{"rejected item without errors", http.MethodPost, "/v1/notifications/batch", response{422, hdr(problemContentType), fmt.Appendf(nil, contractProblem, 422, `,"rejected":[{"index":0}]`)}},
		{"existing_id not a UUID", http.MethodPost, "/v1/notifications", response{409, hdr(problemContentType), fmt.Appendf(nil, contractProblem, 409, `,"errors":[{"field":"dedupe_key","code":"duplicate","message":"m","existing_id":"x"}]`)}},
		{"batch without a status", http.MethodGet, "/v1/batches/" + contractID, response{200, hdr("application/json"), []byte(`{"id":"` + contractID + `","total":1,"accepted":1,"rejected":0,"created_at":"2026-01-01T00:00:00Z","status_counts":{"pending":1,"queued":0,"processing":0,"retrying":0,"sent":0,"failed":0}}`)}},
	}
	for _, tt := range tests {
		req, err := http.NewRequestWithContext(context.Background(), tt.method, "http://127.0.0.1"+tt.path, http.NoBody)
		if err != nil {
			t.Fatal(err)
		}
		rec := &recordingTB{TB: t}
		c.check(rec, req, tt.res)
		if len(rec.failures) == 0 {
			t.Errorf("%s: accepted", tt.name)
		}
	}
	// And accepts conforming ones.
	for _, tt := range []struct {
		method, path string
		res          response
	}{
		{http.MethodGet, "/healthz", response{200, hdr("application/json"), []byte(`{"status":"ok"}`)}},
		{http.MethodPost, "/v1/notifications/batch", response{202, withHeaders(hdr("application/json"), "Location", "/v1/batches/"+contractID, "Idempotent-Replayed", "true"), []byte(contractBatchAccepted)}},
		{http.MethodPost, "/v1/notifications/batch", response{422, hdr(problemContentType), fmt.Appendf(nil, contractProblem, 422, `,"rejected":[{"index":0,"errors":[{"field":"dedupe_key","code":"duplicate","message":"m","existing_id":"`+contractID+`"}]}]`)}},
	} {
		req, _ := http.NewRequestWithContext(context.Background(), tt.method, "http://127.0.0.1"+tt.path, http.NoBody)
		rec := &recordingTB{TB: t}
		c.check(rec, req, tt.res)
		if len(rec.failures) != 0 {
			t.Errorf("%s %s: conforming response rejected: %v", tt.method, tt.path, rec.failures)
		}
	}
}

const (
	contractID            = "0193f0d2-7c1e-7b3a-9d41-5be0a1c2e3f4"
	contractBatchAccepted = `{"batch_id":"` + contractID + `","accepted":[{"index":0,"id":"` + contractID + `"}],"rejected":[]}`
	// contractProblem takes the status and extra members.
	contractProblem = `{"type":"https://example.com/x","title":"t","status":%d,"detail":"d","correlation_id":"r1"%s}`
)

// withHeaders returns h with the name/value pairs added.
func withHeaders(h http.Header, kv ...string) http.Header {
	for i := 0; i+1 < len(kv); i += 2 {
		h.Set(kv[i], kv[i+1])
	}
	return h
}
