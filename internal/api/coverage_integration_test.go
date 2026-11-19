//go:build integration

package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// coverageEnv holds one API per way of answering: the real handler on a
// real database, the real handler on a database that does not exist (503),
// a repository that fails with an unexpected error (500), and a readiness
// check that panics (the 500 of the recovery).
type coverageEnv struct {
	real   *cancelAPI
	dead   *apiClient
	fail   *apiClient
	panics *apiClient
	// last is the method and path of the last request sent.
	lastMethod, lastPath string
}

// panicPinger is a readiness dependency that panics.
type panicPinger struct{}

func (panicPinger) Ping(context.Context) error { panic("ping panicked") }

func newCoverageEnv(t *testing.T) *coverageEnv {
	t.Helper()
	fail, _ := newFakeAPI(t, &fakeRepo{err: errors.New("unexpected store failure")})
	panics := newAPIClient(t, NewHandler(Options{
		Repository: &fakeRepo{}, DB: panicPinger{}, Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Retry: testRetry(), MaxBodyBytes: itMaxBody,
	}))
	return &coverageEnv{
		real:   newCancelAPI(t),
		dead:   newTestAPIOn(t, deadPool(t), &syncBuffer{}).apiClient,
		fail:   fail,
		panics: panics,
	}
}

func (e *coverageEnv) send(c *apiClient, method, path, body string, headers map[string][]string) response {
	e.lastMethod, e.lastPath = method, path
	return c.do(method, path, body, headers)
}

// pair is one documented response: an operation and a status.
type pair struct {
	op     string
	status int
}

func (p pair) String() string { return p.op + " " + strconv.Itoa(p.status) }

// coverageCase produces the response p.status of operation p.op.
type coverageCase struct {
	pair
	run func(t *testing.T, e *coverageEnv) response
}

// notProducible lists the documented responses that no test can produce,
// and why.
var notProducible = map[pair]string{
	{"getHealthz", 500}: "the handler calls no dependency; only a panic in it would answer 500, " +
		"and the recovery is covered on other operations",
	{"getOpenAPISpec", 500}: "the handler serves the embedded specification and calls no dependency; " +
		"only a panic in it would answer 500, and the recovery is covered on other operations",
}

var (
	textPlain = map[string][]string{"Content-Type": {"text/plain"}}
	// overLimit is a body larger than itMaxBody, the limit of the real API.
	overLimit = `{"content":"` + strings.Repeat("a", itMaxBody) + `"}`
	someID    = "01a1f168-ddba-7269-a5b1-2376f9c0048f"
)

func pathf(format string, id any) string { return fmt.Sprintf(format, id) }

// coverageCases has one case per documented operation and status. Each
// response is validated against the contract by the client.
func coverageCases() []coverageCase {
	c := func(op string, status int, run func(t *testing.T, e *coverageEnv) response) coverageCase {
		return coverageCase{pair{op, status}, run}
	}
	post := func(client func(*coverageEnv) *apiClient, p, body string, h map[string][]string) func(t *testing.T, e *coverageEnv) response {
		return func(_ *testing.T, e *coverageEnv) response { return e.send(client(e), http.MethodPost, p, body, h) }
	}
	get := func(client func(*coverageEnv) *apiClient, p string) func(t *testing.T, e *coverageEnv) response {
		return func(_ *testing.T, e *coverageEnv) response { return e.send(client(e), http.MethodGet, p, "", nil) }
	}
	live := func(e *coverageEnv) *apiClient { return e.real.apiClient }
	dead := func(e *coverageEnv) *apiClient { return e.dead }
	fail := func(e *coverageEnv) *apiClient { return e.fail }
	created := func(t *testing.T, e *coverageEnv) uuid.UUID { return e.real.create(t, smsBody) }
	batch := func(t *testing.T, e *coverageEnv) string {
		r := e.real.do(http.MethodPost, pathBatch, batchBody(item("")), nil)
		if r.status != http.StatusAccepted {
			t.Fatalf("batch: %d", r.status)
		}
		return decodeBatch(t, r).BatchID
	}
	const batchOK = `{"notifications":[{"channel":"sms","recipient":"+905551234567","content":"hello"}]}`
	return []coverageCase{
		c("createNotification", 202, post(live, pathCreate, smsBody, nil)),
		c("createNotification", 400, post(live, pathCreate, `{`, nil)),
		c("createNotification", 409, func(t *testing.T, e *coverageEnv) response {
			e.real.create(t, smsWithKey("coverage-dk"))
			return e.send(e.real.apiClient, http.MethodPost, pathCreate, smsWithKey("coverage-dk"), nil)
		}),
		c("createNotification", 413, post(live, pathCreate, overLimit, nil)),
		c("createNotification", 415, post(live, pathCreate, smsBody, textPlain)),
		c("createNotification", 422, post(live, pathCreate, `{}`, nil)),
		c("createNotification", 500, post(fail, pathCreate, smsBody, nil)),
		c("createNotification", 503, post(dead, pathCreate, smsBody, nil)),

		c("createNotificationBatch", 202, post(live, pathBatch, batchOK, nil)),
		c("createNotificationBatch", 400, post(live, pathBatch, `{`, nil)),
		c("createNotificationBatch", 413, post(live, pathBatch, overLimit, nil)),
		c("createNotificationBatch", 415, post(live, pathBatch, batchOK, textPlain)),
		c("createNotificationBatch", 422, post(live, pathBatch, `{"notifications":[{}]}`, nil)),
		c("createNotificationBatch", 500, post(fail, pathBatch, batchOK, nil)),
		c("createNotificationBatch", 503, post(dead, pathBatch, batchOK, nil)),

		c("listNotifications", 200, get(live, "/v1/notifications?limit=5")),
		c("listNotifications", 400, get(live, "/v1/notifications?limit=0")),
		c("listNotifications", 500, get(fail, "/v1/notifications")),
		c("listNotifications", 503, get(dead, "/v1/notifications")),

		c("getNotification", 200, func(t *testing.T, e *coverageEnv) response {
			return e.send(e.real.apiClient, http.MethodGet, pathf("/v1/notifications/%s", created(t, e)), "", nil)
		}),
		c("getNotification", 400, get(live, "/v1/notifications/not-a-uuid")),
		c("getNotification", 404, get(live, pathf("/v1/notifications/%s", someID))),
		c("getNotification", 500, get(fail, pathf("/v1/notifications/%s", someID))),
		c("getNotification", 503, get(dead, pathf("/v1/notifications/%s", someID))),

		c("listNotificationAttempts", 200, func(t *testing.T, e *coverageEnv) response {
			return e.send(e.real.apiClient, http.MethodGet, pathf("/v1/notifications/%s/attempts", created(t, e)), "", nil)
		}),
		c("listNotificationAttempts", 400, get(live, "/v1/notifications/not-a-uuid/attempts")),
		c("listNotificationAttempts", 404, get(live, pathf("/v1/notifications/%s/attempts", someID))),
		c("listNotificationAttempts", 500, get(fail, pathf("/v1/notifications/%s/attempts", someID))),
		c("listNotificationAttempts", 503, get(dead, pathf("/v1/notifications/%s/attempts", someID))),

		c("cancelNotification", 200, func(t *testing.T, e *coverageEnv) response {
			return e.send(e.real.apiClient, http.MethodPost, pathf("/v1/notifications/%s/cancel", created(t, e)), "", nil)
		}),
		c("cancelNotification", 400, post(live, pathf("/v1/notifications/%s/cancel", someID), `{}`, nil)),
		c("cancelNotification", 404, post(live, pathf("/v1/notifications/%s/cancel", someID), "", nil)),
		c("cancelNotification", 409, func(t *testing.T, e *coverageEnv) response {
			id := created(t, e)
			e.real.claim(t, id, itLease)
			return e.send(e.real.apiClient, http.MethodPost, pathf("/v1/notifications/%s/cancel", id), "", nil)
		}),
		c("cancelNotification", 413, post(live, pathf("/v1/notifications/%s/cancel", someID), overLimit, nil)),
		c("cancelNotification", 500, post(fail, pathf("/v1/notifications/%s/cancel", someID), "", nil)),
		c("cancelNotification", 503, post(dead, pathf("/v1/notifications/%s/cancel", someID), "", nil)),

		c("redriveNotification", 200, func(t *testing.T, e *coverageEnv) response {
			id := created(t, e)
			e.real.toFailed(t, id)
			return e.send(e.real.apiClient, http.MethodPost, pathf("/v1/notifications/%s/redrive", id), "", nil)
		}),
		c("redriveNotification", 400, post(live, pathf("/v1/notifications/%s/redrive", someID), `{`, nil)),
		c("redriveNotification", 404, post(live, pathf("/v1/notifications/%s/redrive", someID), "", nil)),
		c("redriveNotification", 409, func(t *testing.T, e *coverageEnv) response {
			return e.send(e.real.apiClient, http.MethodPost, pathf("/v1/notifications/%s/redrive", created(t, e)), "", nil)
		}),
		c("redriveNotification", 413, post(live, pathf("/v1/notifications/%s/redrive", someID), overLimit, nil)),
		c("redriveNotification", 415, post(live, pathf("/v1/notifications/%s/redrive", someID), `{}`, textPlain)),
		c("redriveNotification", 422, post(live, pathf("/v1/notifications/%s/redrive", someID), `{"expires_at":1}`, nil)),
		c("redriveNotification", 500, post(fail, pathf("/v1/notifications/%s/redrive", someID), "", nil)),
		c("redriveNotification", 503, post(dead, pathf("/v1/notifications/%s/redrive", someID), "", nil)),

		c("getBatch", 200, func(t *testing.T, e *coverageEnv) response {
			return e.send(e.real.apiClient, http.MethodGet, "/v1/batches/"+batch(t, e), "", nil)
		}),
		c("getBatch", 400, get(live, "/v1/batches/not-a-uuid")),
		c("getBatch", 404, get(live, pathf("/v1/batches/%s", someID))),
		c("getBatch", 500, get(fail, pathf("/v1/batches/%s", someID))),
		c("getBatch", 503, get(dead, pathf("/v1/batches/%s", someID))),

		c("cancelBatch", 200, func(t *testing.T, e *coverageEnv) response {
			return e.send(e.real.apiClient, http.MethodPost, "/v1/batches/"+batch(t, e)+"/cancel", "", nil)
		}),
		c("cancelBatch", 400, post(live, pathf("/v1/batches/%s/cancel", someID), `{}`, nil)),
		c("cancelBatch", 404, post(live, pathf("/v1/batches/%s/cancel", someID), "", nil)),
		c("cancelBatch", 413, post(live, pathf("/v1/batches/%s/cancel", someID), overLimit, nil)),
		c("cancelBatch", 500, post(fail, pathf("/v1/batches/%s/cancel", someID), "", nil)),
		c("cancelBatch", 503, post(dead, pathf("/v1/batches/%s/cancel", someID), "", nil)),

		c("createTemplate", 201, post(live, "/v1/templates", tplBody("cov-create", "sms", "hello"), nil)),
		c("createTemplate", 400, post(live, "/v1/templates", `{`, nil)),
		c("createTemplate", 409, func(_ *testing.T, e *coverageEnv) response {
			e.send(e.real.apiClient, http.MethodPost, "/v1/templates", tplBody("cov-dup", "sms", "hello"), nil)
			return e.send(e.real.apiClient, http.MethodPost, "/v1/templates", tplBody("cov-dup", "sms", "hello"), nil)
		}),
		c("createTemplate", 413, post(live, "/v1/templates", overLimit, nil)),
		c("createTemplate", 415, post(live, "/v1/templates", tplBody("cov-415", "sms", "hello"), textPlain)),
		c("createTemplate", 422, post(live, "/v1/templates", `{}`, nil)),
		c("createTemplate", 500, post(fail, "/v1/templates", tplBody("cov-500", "sms", "hello"), nil)),
		c("createTemplate", 503, post(dead, "/v1/templates", tplBody("cov-503", "sms", "hello"), nil)),

		c("createTemplateVersion", 201, func(_ *testing.T, e *coverageEnv) response {
			e.send(e.real.apiClient, http.MethodPost, "/v1/templates", tplBody("cov-ver", "sms", "hello"), nil)
			return e.send(e.real.apiClient, http.MethodPost, "/v1/templates/cov-ver/versions", versionBody("bye"), nil)
		}),
		c("createTemplateVersion", 400, post(live, "/v1/templates/cov/versions", `{`, nil)),
		c("createTemplateVersion", 404, post(live, "/v1/templates/cov-unknown/versions", versionBody("x"), nil)),
		c("createTemplateVersion", 413, post(live, "/v1/templates/cov/versions", overLimit, nil)),
		c("createTemplateVersion", 415, post(live, "/v1/templates/cov/versions", versionBody("x"), textPlain)),
		c("createTemplateVersion", 422, post(live, "/v1/templates/cov/versions", `{}`, nil)),
		c("createTemplateVersion", 500, post(fail, "/v1/templates/cov/versions", versionBody("x"), nil)),
		c("createTemplateVersion", 503, post(dead, "/v1/templates/cov/versions", versionBody("x"), nil)),

		c("listTemplates", 200, get(live, "/v1/templates?limit=5")),
		c("listTemplates", 400, get(live, "/v1/templates?limit=0")),
		c("listTemplates", 500, get(fail, "/v1/templates")),
		c("listTemplates", 503, get(dead, "/v1/templates")),

		c("getTemplate", 200, func(_ *testing.T, e *coverageEnv) response {
			e.send(e.real.apiClient, http.MethodPost, "/v1/templates", tplBody("cov-get", "sms", "hello"), nil)
			return e.send(e.real.apiClient, http.MethodGet, "/v1/templates/cov-get", "", nil)
		}),
		c("getTemplate", 404, get(live, "/v1/templates/cov-unknown")),
		c("getTemplate", 500, get(fail, "/v1/templates/cov")),
		c("getTemplate", 503, get(dead, "/v1/templates/cov")),

		c("getTemplateVersion", 200, func(_ *testing.T, e *coverageEnv) response {
			e.send(e.real.apiClient, http.MethodPost, "/v1/templates", tplBody("cov-getv", "sms", "hello"), nil)
			return e.send(e.real.apiClient, http.MethodGet, "/v1/templates/cov-getv/versions/1", "", nil)
		}),
		c("getTemplateVersion", 400, get(live, "/v1/templates/cov/versions/abc")),
		c("getTemplateVersion", 404, get(live, "/v1/templates/cov-unknown/versions/1")),
		c("getTemplateVersion", 500, get(fail, "/v1/templates/cov/versions/1")),
		c("getTemplateVersion", 503, get(dead, "/v1/templates/cov/versions/1")),

		c("previewTemplate", 200, func(_ *testing.T, e *coverageEnv) response {
			e.send(e.real.apiClient, http.MethodPost, "/v1/templates", tplBody("cov-prev", "sms", "hello {{ .n }}", "n"), nil)
			return e.send(e.real.apiClient, http.MethodPost, "/v1/templates/cov-prev/preview", `{"variables":{"n":1}}`, nil)
		}),
		c("previewTemplate", 400, post(live, "/v1/templates/cov/preview", `{`, nil)),
		c("previewTemplate", 404, post(live, "/v1/templates/cov-unknown/preview", "", nil)),
		c("previewTemplate", 413, post(live, "/v1/templates/cov/preview", overLimit, nil)),
		c("previewTemplate", 415, post(live, "/v1/templates/cov/preview", `{}`, textPlain)),
		c("previewTemplate", 422, post(live, "/v1/templates/cov/preview", `{"version":0}`, nil)),
		c("previewTemplate", 500, post(fail, "/v1/templates/cov/preview", "", nil)),
		c("previewTemplate", 503, post(dead, "/v1/templates/cov/preview", "", nil)),

		c("deactivateTemplate", 200, func(_ *testing.T, e *coverageEnv) response {
			e.send(e.real.apiClient, http.MethodPost, "/v1/templates", tplBody("cov-off", "sms", "hello"), nil)
			return e.send(e.real.apiClient, http.MethodPost, "/v1/templates/cov-off/deactivate", "", nil)
		}),
		c("deactivateTemplate", 400, post(live, "/v1/templates/cov/deactivate", `{}`, nil)),
		c("deactivateTemplate", 404, post(live, "/v1/templates/cov-unknown/deactivate", "", nil)),
		c("deactivateTemplate", 413, post(live, "/v1/templates/cov/deactivate", overLimit, nil)),
		c("deactivateTemplate", 500, post(fail, "/v1/templates/cov/deactivate", "", nil)),
		c("deactivateTemplate", 503, post(dead, "/v1/templates/cov/deactivate", "", nil)),

		c("activateTemplate", 200, func(_ *testing.T, e *coverageEnv) response {
			e.send(e.real.apiClient, http.MethodPost, "/v1/templates", tplBody("cov-on", "sms", "hello"), nil)
			return e.send(e.real.apiClient, http.MethodPost, "/v1/templates/cov-on/activate", "", nil)
		}),
		c("activateTemplate", 400, post(live, "/v1/templates/cov/activate", `{}`, nil)),
		c("activateTemplate", 404, post(live, "/v1/templates/cov-unknown/activate", "", nil)),
		c("activateTemplate", 413, post(live, "/v1/templates/cov/activate", overLimit, nil)),
		c("activateTemplate", 500, post(fail, "/v1/templates/cov/activate", "", nil)),
		c("activateTemplate", 503, post(dead, "/v1/templates/cov/activate", "", nil)),

		c("getMetricsSummary", 200, get(live, "/v1/metrics/summary")),
		c("getMetricsSummary", 500, get(fail, "/v1/metrics/summary")),
		c("getMetricsSummary", 503, get(dead, "/v1/metrics/summary")),

		c("getHealthz", 200, get(live, "/healthz")),

		c("getReadyz", 200, get(live, "/readyz")),
		c("getReadyz", 500, get(func(e *coverageEnv) *apiClient { return e.panics }, "/readyz")),
		c("getReadyz", 503, get(dead, "/readyz")),

		c("getOpenAPISpec", 200, get(live, "/openapi.yaml")),
	}
}

// specPairs returns every documented operation and status of the contract.
func specPairs(t *testing.T) map[pair]bool {
	t.Helper()
	doc := loadContract(t).doc
	out := map[pair]bool{}
	for _, p := range doc.Paths.InMatchingOrder() {
		for _, op := range doc.Paths.Value(p).Operations() {
			for code := range op.Responses.Map() {
				status, err := strconv.Atoi(code)
				if err != nil {
					t.Fatalf("%s: response %q is not a status code", op.OperationID, code)
				}
				out[pair{op.OperationID, status}] = true
			}
		}
	}
	return out
}

// The table of cases and the contract cannot drift apart: every documented
// response has a case or a reason in notProducible, and nothing else is in
// either. A response added to api/openapi.yaml fails this test until a case
// produces it.
func TestContract_CoverageTableMatchesSpec(t *testing.T) {
	spec := specPairs(t)
	covered := map[pair]bool{}
	for _, cc := range coverageCases() {
		if covered[cc.pair] {
			t.Errorf("%s has two cases", cc.pair)
		}
		covered[cc.pair] = true
		if !spec[cc.pair] {
			t.Errorf("%s has a case but is not in the contract", cc.pair)
		}
		if _, ok := notProducible[cc.pair]; ok {
			t.Errorf("%s has a case and is listed as not producible", cc.pair)
		}
	}
	for p := range notProducible {
		if !spec[p] {
			t.Errorf("%s is listed as not producible but is not in the contract", p)
		}
	}
	var missing []string
	for p := range spec {
		if _, ok := notProducible[p]; !covered[p] && !ok {
			missing = append(missing, p.String())
		}
	}
	slices.Sort(missing)
	if len(missing) > 0 {
		t.Errorf("documented responses without a case: %v", missing)
	}
}

// Every case produces its status on its operation, and the client validates
// each answer against the contract.
func TestContract_EveryDocumentedResponse(t *testing.T) {
	e := newCoverageEnv(t)
	router := loadContract(t).router
	for _, cc := range coverageCases() {
		t.Run(cc.String(), func(t *testing.T) {
			res := cc.run(t, e)
			if res.status != cc.status {
				t.Fatalf("status %d, want %d: %s", res.status, cc.status, res.body)
			}
			req, err := http.NewRequestWithContext(context.Background(), e.lastMethod, "http://localhost"+e.lastPath, http.NoBody)
			if err != nil {
				t.Fatal(err)
			}
			route, _, err := router.FindRoute(req)
			if err != nil || route.Operation.OperationID != cc.op {
				t.Fatalf("%s %s is not operation %s (%v)", e.lastMethod, e.lastPath, cc.op, err)
			}
		})
	}
}

// bodyEndpoints are the operations that read a request body, with a path
// that reaches them.
var bodyEndpoints = []struct{ op, path string }{
	{"createNotification", pathCreate},
	{"createNotificationBatch", pathBatch},
	{"redriveNotification", "/v1/notifications/" + someID + "/redrive"},
	{"cancelNotification", "/v1/notifications/" + someID + "/cancel"},
	{"cancelBatch", "/v1/batches/" + someID + "/cancel"},
	{"createTemplate", "/v1/templates"},
	{"createTemplateVersion", "/v1/templates/cov/versions"},
	{"previewTemplate", "/v1/templates/cov/preview"},
	{"deactivateTemplate", "/v1/templates/cov/deactivate"},
	{"activateTemplate", "/v1/templates/cov/activate"},
}

// MAX_BODY_BYTES answers 413 on every endpoint that reads a body: a body of
// exactly the limit is read (whatever it then answers), one byte more is
// 413, with and without a Content-Length.
func TestBodyLimit_EveryBodyEndpoint(t *testing.T) {
	a := newCancelAPI(t)
	padded := func(n int) string { return `{"x":1}` + strings.Repeat(" ", n-len(`{"x":1}`)) }
	for _, ep := range bodyEndpoints {
		for _, chunked := range []bool{false, true} {
			for _, size := range []int{itMaxBody, itMaxBody + 1} {
				req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, a.srv.URL+ep.path,
					strings.NewReader(padded(size)))
				if err != nil {
					t.Fatal(err)
				}
				req.Header.Set("Content-Type", "application/json")
				if chunked {
					req.ContentLength = -1 // sent with Transfer-Encoding: chunked
					req.Body = io.NopCloser(strings.NewReader(padded(size)))
				}
				res := a.send(req)
				if tooLarge := res.status == http.StatusRequestEntityTooLarge; tooLarge != (size > itMaxBody) {
					t.Errorf("%s, %d bytes, chunked %v: status %d", ep.op, size, chunked, res.status)
				}
			}
		}
	}
	// The table covers the operations of the contract that have a 413.
	var ops []string
	for p := range specPairs(t) {
		if p.status == http.StatusRequestEntityTooLarge {
			ops = append(ops, p.op)
		}
	}
	var listed []string
	for _, ep := range bodyEndpoints {
		listed = append(listed, ep.op)
	}
	slices.Sort(ops)
	slices.Sort(listed)
	if !slices.Equal(ops, listed) {
		t.Errorf("operations with a 413 in the contract %v, tested %v", ops, listed)
	}
}

// X-Request-ID on every operation: 1 to 128 characters of [A-Za-z0-9._-]
// are echoed; anything else (129 characters, another character, empty,
// sent twice) is ignored and replaced by a new id, never an error.
func TestRequestID_EveryOperation(t *testing.T) {
	a := newCancelAPI(t)
	id := a.create(t, smsBody)
	failed := a.create(t, smsBody)
	a.toFailed(t, failed)
	r := a.do(http.MethodPost, pathBatch, batchBody(item("")), nil)
	batchID := decodeBatch(t, r).BatchID
	ops := []struct{ op, method, path, body string }{
		{"createNotification", http.MethodPost, pathCreate, smsBody},
		{"createNotificationBatch", http.MethodPost, pathBatch, batchBody(item(""))},
		{"listNotifications", http.MethodGet, "/v1/notifications", ""},
		{"getNotification", http.MethodGet, "/v1/notifications/" + id.String(), ""},
		{"listNotificationAttempts", http.MethodGet, "/v1/notifications/" + id.String() + "/attempts", ""},
		{"cancelNotification", http.MethodPost, "/v1/notifications/" + id.String() + "/cancel", ""},
		{"redriveNotification", http.MethodPost, "/v1/notifications/" + failed.String() + "/redrive", ""},
		{"getBatch", http.MethodGet, "/v1/batches/" + batchID, ""},
		{"cancelBatch", http.MethodPost, "/v1/batches/" + batchID + "/cancel", ""},
		{"createTemplate", http.MethodPost, "/v1/templates", tplBody("rid-tpl", "sms", "hello")},
		{"createTemplateVersion", http.MethodPost, "/v1/templates/rid-tpl/versions", versionBody("bye")},
		{"listTemplates", http.MethodGet, "/v1/templates", ""},
		{"getTemplate", http.MethodGet, "/v1/templates/rid-tpl", ""},
		{"getTemplateVersion", http.MethodGet, "/v1/templates/rid-tpl/versions/1", ""},
		{"previewTemplate", http.MethodPost, "/v1/templates/rid-tpl/preview", ""},
		{"deactivateTemplate", http.MethodPost, "/v1/templates/rid-tpl/deactivate", ""},
		{"activateTemplate", http.MethodPost, "/v1/templates/rid-tpl/activate", ""},
		{"getMetricsSummary", http.MethodGet, "/v1/metrics/summary", ""},
		{"getHealthz", http.MethodGet, "/healthz", ""},
		{"getReadyz", http.MethodGet, "/readyz", ""},
		{"getOpenAPISpec", http.MethodGet, "/openapi.yaml", ""},
	}
	longValid := strings.Repeat("a", maxRequestIDLen-4) + "._-9"
	for _, o := range ops {
		for _, tc := range []struct {
			name   string
			values []string
			echo   string // "" means a new UUIDv7
		}{
			{"valid", []string{"req-1_A.b"}, "req-1_A.b"},
			{"128 characters", []string{longValid}, longValid},
			{"129 characters", []string{longValid + "a"}, ""},
			{"another character", []string{"bad id"}, ""},
			{"non-ASCII", []string{"çğ"}, ""},
			{"empty", []string{emptyHeaderValue}, ""},
			{"sent twice", []string{"a", "b"}, ""},
			{"sent twice, same value", []string{"same", "same"}, ""},
		} {
			res := a.do(o.method, o.path, o.body, map[string][]string{"X-Request-ID": tc.values})
			if res.status == http.StatusBadRequest {
				t.Errorf("%s, %s: 400 %s", o.op, tc.name, res.body)
			}
			got := res.header.Get("X-Request-ID")
			if tc.echo != "" {
				if got != tc.echo {
					t.Errorf("%s, %s: X-Request-ID %q, want it echoed", o.op, tc.name, got)
				}
				continue
			}
			if u, err := uuid.Parse(got); err != nil || u.Version() != 7 {
				t.Errorf("%s, %s: X-Request-ID %q, want a new UUIDv7", o.op, tc.name, got)
			}
		}
	}
	// Every operation of the contract is in the list.
	var specOps, listed []string
	seen := map[string]bool{}
	for p := range specPairs(t) {
		if !seen[p.op] {
			seen[p.op] = true
			specOps = append(specOps, p.op)
		}
	}
	for _, o := range ops {
		listed = append(listed, o.op)
	}
	slices.Sort(specOps)
	slices.Sort(listed)
	if !slices.Equal(specOps, listed) {
		t.Errorf("operations in the contract %v, tested %v", specOps, listed)
	}
}
