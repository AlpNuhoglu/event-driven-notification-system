// Package api is the REST API role: the HTTP handler generated from
// api/openapi.yaml (internal/api/apigen) with its handlers, the middleware
// that every request passes (body limit, correlation id, access log, RFC 9457
// problems for every error) and the role's Service.
package api

import (
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
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/api/swaggerui"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/events"
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
	LookupIdempotencyKey(ctx context.Context, key *store.IdempotencyKey) (*store.StoredResponse, error)
	canceller
	lister
	templater
	summarizer
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

// DefaultExpiresAtMinLead is the smallest distance of a new expires_at from
// now when Options.ExpiresAtMinLead is zero (the default of
// EXPIRES_AT_MIN_LEAD).
const DefaultExpiresAtMinLead = 30 * time.Second

// DefaultScheduledAtTolerance is how far in the past a scheduled_at may be
// and still mean "now" when Options.ScheduledAtTolerance is zero (the
// default of SCHEDULE_PAST_TOLERANCE).
const DefaultScheduledAtTolerance = 30 * time.Second

// DefaultScheduledAtMaxAhead is how far ahead a notification may be
// scheduled when Options.ScheduledAtMaxAhead is zero (the default of
// SCHEDULE_MAX_HORIZON: 30 days).
const DefaultScheduledAtMaxAhead = 720 * time.Hour

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
	// Events receives the status event of every transition the API makes
	// (cancel, redrive), after its transaction has committed; events.Nop when
	// nil.
	Events events.Publisher
	// ExpiresAtMinLead is how far ahead a new expires_at must be
	// (EXPIRES_AT_MIN_LEAD): from now for a redrive, from the due time for a
	// create. DefaultExpiresAtMinLead when zero.
	ExpiresAtMinLead time.Duration
	// ScheduledAtTolerance is how far in the past a scheduled_at may be and
	// still mean "now" (SCHEDULE_PAST_TOLERANCE); DefaultScheduledAtTolerance
	// when zero.
	ScheduledAtTolerance time.Duration
	// ScheduledAtMaxAhead is how far ahead a notification may be scheduled
	// (SCHEDULE_MAX_HORIZON); DefaultScheduledAtMaxAhead when zero.
	ScheduledAtMaxAhead time.Duration
	// Now is the clock that judges scheduled_at and expires_at of a create,
	// read once per request; time.Now when nil.
	Now func() time.Time
	// IdempotencyKeyTTL is how long an Idempotency-Key stays live
	// (IDEMPOTENCY_KEY_TTL); DefaultIdempotencyKeyTTL when zero.
	IdempotencyKeyTTL time.Duration
	// PhoneValidation is the level of the check of an SMS recipient
	// (PHONE_VALIDATION); domain.PhonePossible when empty.
	PhoneValidation domain.PhoneValidation
	// SummaryWindow, SummaryCacheTTL and SummaryBacklogTTL are the window of
	// the metrics summary, the lifetime of its cached answer and the lifetime
	// of its cached due backlog (SUMMARY_WINDOW, SUMMARY_CACHE_TTL,
	// SUMMARY_BACKLOG_CACHE_TTL); the Default values when zero.
	SummaryWindow     time.Duration
	SummaryCacheTTL   time.Duration
	SummaryBacklogTTL time.Duration
	// LaneHWM is the high-water mark each priority's lanes report in the
	// summary (LANE_HWM_*); DefaultLaneHWM when nil.
	LaneHWM map[domain.Priority]int
	// Metrics receives the api's metrics; nil means metrics that are not
	// registered anywhere.
	Metrics *Metrics
}

// NewHandler returns the API's http.Handler.
func NewHandler(o Options) http.Handler {
	if o.DBTimeout <= 0 {
		o.DBTimeout = DefaultDBTimeout
	}
	if o.IdempotencyKeyTTL <= 0 {
		o.IdempotencyKeyTTL = DefaultIdempotencyKeyTTL
	}
	if o.ExpiresAtMinLead <= 0 {
		o.ExpiresAtMinLead = DefaultExpiresAtMinLead
	}
	if o.ScheduledAtTolerance <= 0 {
		o.ScheduledAtTolerance = DefaultScheduledAtTolerance
	}
	if o.ScheduledAtMaxAhead <= 0 {
		o.ScheduledAtMaxAhead = DefaultScheduledAtMaxAhead
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.SummaryWindow <= 0 {
		o.SummaryWindow = DefaultSummaryWindow
	}
	if o.SummaryCacheTTL <= 0 {
		o.SummaryCacheTTL = DefaultSummaryCacheTTL
	}
	if o.SummaryBacklogTTL <= 0 {
		o.SummaryBacklogTTL = DefaultSummaryBacklogTTL
	}
	if o.LaneHWM == nil {
		o.LaneHWM = DefaultLaneHWM
	}
	if o.Events == nil {
		o.Events = events.Nop{}
	}
	if o.PhoneValidation == "" {
		o.PhoneValidation = domain.PhonePossible
	}
	if o.Metrics == nil {
		o.Metrics, _ = NewMetrics(nil) // cannot fail without a registry
	}
	// The final transitions the API makes (cancel) are counted on their way
	// to the publisher.
	o.Events = o.Metrics.finished.Publisher(o.Events)
	h := &handlers{
		repo:      o.Repository,
		db:        o.DB,
		limits:    limitsFrom(o.Retry),
		dbTimeout: o.DBTimeout,
		events:    o.Events,
		minLead:   o.ExpiresAtMinLead,
		phone:     o.PhoneValidation,
		schedule: scheduleRules{
			tolerance: o.ScheduledAtTolerance,
			maxAhead:  o.ScheduledAtMaxAhead,
			minLead:   o.ExpiresAtMinLead,
		},
		now:  o.Now,
		spec: spec.OpenAPI(),
		summary: &summaryService{
			src: o.Repository, now: o.Now, window: o.SummaryWindow, cacheTTL: o.SummaryCacheTTL,
			backlogTTL: o.SummaryBacklogTTL, dbTimeout: o.DBTimeout, hwm: o.LaneHWM,
		},
		metrics: o.Metrics,
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
	docs := swaggerui.Handler()
	mux.Handle("GET /docs", docs)
	mux.Handle("GET /docs/{asset}", docs)
	return &frame{next: &router{mux: mux}, logger: o.Logger, maxBodyBytes: o.MaxBodyBytes, metrics: o.Metrics}
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

// CreateNotification answers a create request itself, as
// CreateNotificationBatch does: the strict handler's decoder would decode a
// template variable's number as a float64 (and refuse one out of its range),
// so the request parsed by validateCreate is handed on in the context and
// the typed response is written here.
func (g createGuard) CreateNotification(w http.ResponseWriter, r *http.Request, params apigen.CreateNotificationParams) {
	answer := &createAnswer{}
	w, counted := g.h.metrics.trackCreate(w, r, answer)
	defer counted()
	body, ok := readJSONBody(w, r)
	if !ok {
		return
	}
	answer.body = body
	req, p := validateCreate(body, g.h.createRules())
	if p != nil {
		if rep, rp := g.replayForClockErrors(r, body, p); rp != nil {
			p = rp
		} else if rep != nil {
			if err := rep.VisitCreateNotificationResponse(w); err != nil {
				responseError(w, r, err)
			}
			return
		}
		writeProblem(w, r, p)
		return
	}
	key, p := g.requestKey(r, body)
	if p != nil {
		writeProblem(w, r, p)
		return
	}
	ctx := withCreateInput(r.Context(), createInput{key: key, single: &req})
	resp, err := g.h.CreateNotification(ctx, apigen.CreateNotificationRequestObject{Params: params, Body: &req.request})
	if err != nil {
		responseError(w, r, err)
		return
	}
	if err := resp.VisitCreateNotificationResponse(w); err != nil {
		responseError(w, r, err)
	}
}

// replayForClockErrors handles a single create that failed validation only
// by the rules judged against the clock (clockDependentOnly) and carries an
// Idempotency-Key: it may be the retry of a request that was accepted when
// it was first sent, and a retry gets the stored answer (ADR-0006 section
// 11), not a 422 that depends on when it arrived. It returns the response to
// replay when the key is live for the same request. Otherwise the validation
// problem stands and both results are nil, also for a key that is live for
// another request; only a database failure is returned as a problem, because
// the answer is then not known.
func (g createGuard) replayForClockErrors(r *http.Request, body []byte, validation *problem) (*replayedResponse, *problem) {
	if validation.kind != kindValidation || !clockDependentOnly(validation.errors) {
		return nil, nil
	}
	key, p := g.requestKey(r, body)
	if p != nil || key == nil {
		return nil, nil
	}
	rep, p := g.h.earlyReplay(r.Context(), key, "id", "/v1/notifications/", stateFrom(r.Context()).correlationID)
	if p != nil && p.kind == kindIdempotencyKeyReused {
		return nil, nil
	}
	return rep, p
}

// CreateNotificationBatch answers a batch request itself: the strict
// handler's decoder would refuse a body with an invalid item, and the items
// have already been parsed by validateBatch. It calls the strict handler's
// method with the validated input in the context and writes its typed
// response, as the generated strict wrapper does.
func (g createGuard) CreateNotificationBatch(w http.ResponseWriter, r *http.Request, params apigen.CreateNotificationBatchParams) {
	answer := &createAnswer{batch: true}
	w, counted := g.h.metrics.trackCreate(w, r, answer)
	defer counted()
	body, ok := readJSONBody(w, r)
	if !ok {
		return
	}
	answer.body = body
	in, p := validateBatch(body, g.h.createRules())
	if p != nil {
		writeProblem(w, r, p)
		return
	}
	key, p := g.requestKey(r, body)
	if p != nil {
		writeProblem(w, r, p)
		return
	}
	ctx := withCreateInput(r.Context(), createInput{key: key, batch: in, answer: answer})
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
	// single is the validated request (single create only).
	single *createRequest
	// batch is the validated batch (batch requests only).
	batch batchInput
	// answer receives the items a stored batch still rejected, for the
	// metrics (batch requests only; nil otherwise).
	answer *createAnswer
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
// cannot bind: the version of a template (not an integer), a query
// parameter of the listing (it
// gives the problems the guard gives, see parseListQuery), or the
// Idempotency-Key header when it is sent more than once (the wrapper refuses
// that before createGuard runs).
// X-Request-ID and traceparent never fail: the middleware normalises them
// first. The id path parameters never fail either: they are bound as
// strings and parsed by createGuard (see ids.go); the last answer below is
// the one an id gets there.
func parameterError(w http.ResponseWriter, r *http.Request, err error) {
	var tooMany *apigen.TooManyValuesForParamError
	var invalid *apigen.InvalidParamFormatError
	if (errors.As(err, &tooMany) && tooMany.ParamName == headerIdempotencyKey) ||
		(errors.As(err, &invalid) && invalid.ParamName == headerIdempotencyKey) {
		writeProblem(w, r, invalidIdempotencyKeyProblem())
		return
	}
	if errors.As(err, &invalid) {
		if invalid.ParamName == fieldVersion {
			writeProblem(w, r, invalidVersionProblem())
			return
		}
		if p := listParameterProblem(r, invalid); p != nil {
			writeProblem(w, r, p)
			return
		}
		if p := templateListParameterProblem(r, invalid); p != nil {
			writeProblem(w, r, p)
			return
		}
		writeProblem(w, r, invalidPathIDProblem(invalid.ParamName))
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
	if p := undefinedQueryProblem(pattern, r.URL.RawQuery); p != nil {
		writeProblem(w, r, p)
		return
	}
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
// correlation id and trace parent, turns a panic into a 500 problem, logs
// one line per request and counts it (http_requests_total and
// http_request_duration_seconds, by route template). The line has the
// method, the route template (never the raw path), the status, the duration
// and the correlation id; never a header value, a recipient, a content or a
// body.
type frame struct {
	next         http.Handler
	logger       *slog.Logger
	maxBodyBytes int64
	metrics      *Metrics
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
		d := time.Since(start)
		f.log(r, st, rec.status(), d)
		pattern, _ := st.snapshot()
		f.metrics.observeRequest(pattern, r.Method, rec.status(), d)
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
