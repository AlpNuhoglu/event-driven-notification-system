// Package api is the REST API role: the HTTP handler generated from
// api/openapi.yaml (internal/api/apigen) with its handlers, the middleware
// that every request passes (body limit, correlation id, access log, RFC 9457
// problems for every error) and the role's Service.
package api

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"path"
	"time"

	"github.com/google/uuid"

	spec "github.com/AlpNuhoglu/event-driven-notification-system/api"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/api/apigen"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/config"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
)

// Repository is the part of the store the API uses.
type Repository interface {
	CreateSingle(ctx context.Context, key *store.IdempotencyKey, n store.NewNotification,
		respond func(store.Notification) (store.StoredResponse, error)) (store.SingleResult, error)
	CreateBatch(ctx context.Context, key *store.IdempotencyKey, b store.NewBatch,
		respond func(store.BatchResult) (store.StoredResponse, error)) (store.BatchResult, error)
	GetNotification(ctx context.Context, id uuid.UUID) (store.Notification, error)
	GetBatch(ctx context.Context, id uuid.UUID) (store.Batch, error)
}

// Pinger checks that the database answers (readiness).
type Pinger interface {
	Ping(ctx context.Context) error
}

// DefaultDBTimeout bounds each database call of a request when
// Options.DBTimeout is zero.
const DefaultDBTimeout = 5 * time.Second

// DefaultIdempotencyKeyTTL is the retention of an idempotency key when
// Options.IdempotencyKeyTTL is zero (the default of IDEMPOTENCY_KEY_TTL).
const DefaultIdempotencyKeyTTL = 24 * time.Hour

// Options configures NewHandler.
type Options struct {
	Repository Repository
	DB         Pinger
	// Logger receives one line per request. It must not be nil.
	Logger *slog.Logger
	// Retry supplies each priority's TTL and failure budget.
	Retry config.RetryConfig
	// MaxBodyBytes limits every request body; a larger one is answered 413.
	MaxBodyBytes int64
	// DBTimeout bounds each database call; DefaultDBTimeout when zero.
	DBTimeout time.Duration
	// IdempotencyKeyTTL is how long an Idempotency-Key stays live
	// (IDEMPOTENCY_KEY_TTL); DefaultIdempotencyKeyTTL when zero.
	IdempotencyKeyTTL time.Duration
}

// NewHandler returns the API's http.Handler.
func NewHandler(o Options) http.Handler {
	if o.DBTimeout <= 0 {
		o.DBTimeout = DefaultDBTimeout
	}
	if o.IdempotencyKeyTTL <= 0 {
		o.IdempotencyKeyTTL = DefaultIdempotencyKeyTTL
	}
	h := &handlers{
		repo:      o.Repository,
		db:        o.DB,
		limits:    limitsFrom(o.Retry),
		dbTimeout: o.DBTimeout,
		spec:      spec.OpenAPI(),
	}
	strict := apigen.NewStrictHandlerWithOptions(h, nil, apigen.StrictHTTPServerOptions{
		RequestErrorHandlerFunc:  requestError,
		ResponseErrorHandlerFunc: responseError,
	})
	mux := http.NewServeMux()
	guard := createGuard{ServerInterface: strict, h: h, keyTTL: o.IdempotencyKeyTTL}
	apigen.HandlerWithOptions(guard, apigen.StdHTTPServerOptions{
		BaseRouter:       mux,
		ErrorHandlerFunc: parameterError,
	})
	return &frame{next: &router{mux: mux}, logger: o.Logger, maxBodyBytes: o.MaxBodyBytes}
}

// createGuard runs before the generated code decodes a create request: it
// checks the media type (415) and the size (413), validates the body (400,
// 422; see validateCreate and validateBatch), then reads the Idempotency-Key
// header (400) and hashes the request. Only a request that passes all of
// this reaches the store, so a request that fails validation never touches
// idempotency_keys. What it learned reaches the handler through the request
// context (createInput).
type createGuard struct {
	apigen.ServerInterface
	h      *handlers
	keyTTL time.Duration
}

// CreateNotification hands a valid body on to the strict handler, which
// decodes it again.
func (g createGuard) CreateNotification(w http.ResponseWriter, r *http.Request, params apigen.CreateNotificationParams) {
	body, ok := readJSONBody(w, r)
	if !ok {
		return
	}
	if p := validateCreate(body); p != nil {
		writeProblem(w, r, p)
		return
	}
	key, p := g.requestKey(r, body)
	if p != nil {
		writeProblem(w, r, p)
		return
	}
	r = r.WithContext(withCreateInput(r.Context(), createInput{key: key}))
	r.Body = io.NopCloser(bytes.NewReader(body))
	g.ServerInterface.CreateNotification(w, r, params)
}

// CreateNotificationBatch answers a batch request itself: the strict
// handler's decoder would refuse a body with an invalid item, and the items
// have already been parsed by validateBatch. It calls the strict handler's
// method with the validated input in the context and writes its typed
// response, as the generated strict wrapper does.
func (g createGuard) CreateNotificationBatch(w http.ResponseWriter, r *http.Request, params apigen.CreateNotificationBatchParams) {
	body, ok := readJSONBody(w, r)
	if !ok {
		return
	}
	in, p := validateBatch(body)
	if p != nil {
		writeProblem(w, r, p)
		return
	}
	key, p := g.requestKey(r, body)
	if p != nil {
		writeProblem(w, r, p)
		return
	}
	ctx := withCreateInput(r.Context(), createInput{key: key, batch: in})
	resp, err := g.h.CreateNotificationBatch(ctx, apigen.CreateNotificationBatchRequestObject{Params: params})
	if err != nil {
		responseError(w, r, err)
		return
	}
	if err := resp.VisitCreateNotificationBatchResponse(w); err != nil {
		responseError(w, r, err)
	}
}

// readJSONBody checks Content-Type (415) and reads the body within the limit
// (413). It writes the problem and returns false when either fails.
func readJSONBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	if !isJSONContentType(r.Header.Get("Content-Type")) {
		writeProblem(w, r, newProblem(kindUnsupportedMediaType, ""))
		return nil, false
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeProblem(w, r, bodyReadProblem(err))
		return nil, false
	}
	return body, true
}

// requestKey reads the Idempotency-Key header (nil when absent) and binds it
// to the hash of the request as received.
func (g createGuard) requestKey(r *http.Request, body []byte) (*store.IdempotencyKey, *problem) {
	key, present, p := idempotencyKey(r.Header)
	if p != nil || !present {
		return nil, p
	}
	hash, err := requestHash(r.Method, r.URL.Path, body)
	if err != nil {
		return nil, &problem{kind: kindInternal, detail: kindInternal.detail, cause: err}
	}
	return &store.IdempotencyKey{Key: key, RequestHash: hash, Retention: g.keyTTL}, nil
}

// createInput is what createGuard hands to the create handlers.
type createInput struct {
	// key is nil when the request has no Idempotency-Key.
	key *store.IdempotencyKey
	// batch is the validated batch (batch requests only).
	batch batchInput
}

type createInputKey struct{}

func withCreateInput(ctx context.Context, in createInput) context.Context {
	return context.WithValue(ctx, createInputKey{}, in)
}

func createInputFrom(ctx context.Context) (createInput, bool) {
	in, ok := ctx.Value(createInputKey{}).(createInput)
	return in, ok
}

func isJSONContentType(v string) bool {
	mt, _, err := mime.ParseMediaType(v)
	return err == nil && mt == "application/json"
}

// bodyReadProblem maps an error from reading the request body.
func bodyReadProblem(err error) *problem {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		return newProblem(kindTooLarge, "")
	}
	return newProblem(kindMalformed, "The request body could not be read.")
}

// requestError is the strict server's handler for a body it cannot decode.
// validateCreate has accepted the body before, so this is a fallback.
func requestError(w http.ResponseWriter, r *http.Request, err error) {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writeProblem(w, r, newProblem(kindTooLarge, ""))
		return
	}
	writeProblem(w, r, newProblem(kindMalformed, "The request body is not valid JSON.",
		fieldError("", codeMalformedJSON, "the request body must be valid UTF-8 JSON")))
}

// responseError is the strict server's handler for an error a handler
// returned or a response that could not be written.
func responseError(w http.ResponseWriter, r *http.Request, err error) {
	var p *problem
	if !errors.As(err, &p) {
		p = &problem{kind: kindInternal, detail: kindInternal.detail, cause: err}
	}
	writeProblem(w, r, p)
}

// parameterError is the generated wrapper's handler for a parameter it
// cannot bind: the id path parameter, or the Idempotency-Key header when it
// is sent more than once (the wrapper refuses that before createGuard runs).
// X-Request-ID and traceparent never fail: the middleware normalises them
// first.
func parameterError(w http.ResponseWriter, r *http.Request, err error) {
	var tooMany *apigen.TooManyValuesForParamError
	var invalid *apigen.InvalidParamFormatError
	if (errors.As(err, &tooMany) && tooMany.ParamName == headerIdempotencyKey) ||
		(errors.As(err, &invalid) && invalid.ParamName == headerIdempotencyKey) {
		writeProblem(w, r, invalidIdempotencyKeyProblem())
		return
	}
	if errors.As(err, &invalid) {
		writeProblem(w, r, newProblem(kindMalformed, "A path parameter is invalid.",
			fieldError(invalid.ParamName, codeInvalidUUID, invalid.ParamName+" must be a UUID")))
		return
	}
	writeProblem(w, r, newProblem(kindMalformed, "A request parameter is invalid."))
}

// router serves the mux. A path that is not in canonical form is answered
// 404 here (the mux would redirect it), and a 4xx or 5xx that the mux or the
// generated wrapper writes as text/plain is rewritten as a problem.
type router struct {
	mux *http.ServeMux
}

func (rt *router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	st := stateFrom(r.Context())
	if r.Method == http.MethodConnect || cleanPath(r.URL.Path) != r.URL.Path {
		writeProblem(w, r, newProblem(kindNotFound, ""))
		return
	}
	_, pattern := rt.mux.Handler(r)
	st.setRoute(pattern)
	rt.mux.ServeHTTP(&problemGuard{ResponseWriter: w, r: r}, r)
}

// cleanPath is the canonical form of a path as http.ServeMux computes it.
func cleanPath(p string) string {
	if p == "" {
		return "/"
	}
	if p[0] != '/' {
		p = "/" + p
	}
	np := path.Clean(p)
	if p[len(p)-1] == '/' && np != "/" {
		np += "/"
	}
	return np
}

// problemGuard rewrites an error status that was not written as a problem
// (the mux's own 404 and 405, a default error handler of generated code) into
// a problem of the matching type; the original body is discarded. Headers
// such as Allow are kept.
type problemGuard struct {
	http.ResponseWriter
	r           *http.Request
	wroteHeader bool
	discard     bool
}

func (g *problemGuard) WriteHeader(status int) {
	if g.wroteHeader {
		return
	}
	g.wroteHeader = true
	if status >= http.StatusBadRequest && !isProblemContentType(g.Header().Get("Content-Type")) {
		g.discard = true
		g.Header().Del("X-Content-Type-Options")
		writeProblem(g.ResponseWriter, g.r, newProblem(kindForStatus(status), ""))
		return
	}
	g.ResponseWriter.WriteHeader(status)
}

func (g *problemGuard) Write(b []byte) (int, error) {
	if !g.wroteHeader {
		g.WriteHeader(http.StatusOK)
	}
	if g.discard {
		return len(b), nil
	}
	return g.ResponseWriter.Write(b)
}

// Unwrap gives http.ResponseController access to the underlying writer.
func (g *problemGuard) Unwrap() http.ResponseWriter { return g.ResponseWriter }

// frame is the outermost layer: it limits the body, establishes the
// correlation id and trace parent, turns a panic into a 500 problem and logs
// one line per request. The line has the method, the route template (never
// the raw path), the status, the duration and the correlation id; never a
// header value, a recipient, a content or a body.
type frame struct {
	next         http.Handler
	logger       *slog.Logger
	maxBodyBytes int64
}

func (f *frame) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if f.maxBodyBytes > 0 {
		r.Body = http.MaxBytesReader(w, r.Body, f.maxBodyBytes)
	}
	r, st := withRequestHeaders(w, r)
	rec := &statusRecorder{ResponseWriter: w}
	defer func() {
		if v := recover(); v != nil {
			if v == http.ErrAbortHandler { //nolint:errorlint // the sentinel is compared as net/http does
				panic(v)
			}
			st.setCause(errors.New("panic in handler"))
			if !rec.wroteHeader {
				writeProblem(rec, r, newProblem(kindInternal, ""))
			}
		}
		f.log(r, st, rec.status(), time.Since(start))
	}()
	f.next.ServeHTTP(rec, r)
}

func (f *frame) log(r *http.Request, st *requestState, status int, d time.Duration) {
	route, cause := st.snapshot()
	if route == "" {
		route = "unmatched"
	}
	attrs := []slog.Attr{
		slog.String("method", r.Method),
		slog.String("route", route),
		slog.Int("status", status),
		slog.Float64("duration_ms", float64(d.Microseconds())/1000),
		slog.String("correlation_id", st.correlationID),
	}
	level := slog.LevelInfo
	if status >= http.StatusInternalServerError {
		level = slog.LevelError
		if cause != nil {
			attrs = append(attrs, slog.String("error", errorClass(cause)))
		}
	}
	f.logger.LogAttrs(r.Context(), level, "http request", attrs...)
}

// statusRecorder remembers the status code written.
type statusRecorder struct {
	http.ResponseWriter
	code        int
	wroteHeader bool
}

func (s *statusRecorder) WriteHeader(code int) {
	if !s.wroteHeader {
		s.code, s.wroteHeader = code, true
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	if !s.wroteHeader {
		s.code, s.wroteHeader = http.StatusOK, true
	}
	return s.ResponseWriter.Write(b)
}

// Unwrap gives http.ResponseController access to the underlying writer.
func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }

func (s *statusRecorder) status() int {
	if !s.wroteHeader {
		return http.StatusOK
	}
	return s.code
}
