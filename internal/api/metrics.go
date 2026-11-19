package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/prometheus/client_golang/prometheus"

	spec "github.com/AlpNuhoglu/event-driven-notification-system/api"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/api/apigen"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/metrics"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/queue"
)

// The names of the api's metrics (ADR-0009 §1).
const (
	RequestsMetric        = "http_requests_total"
	RequestDurationMetric = "http_request_duration_seconds"
	CreatedMetric         = "notifications_created_total"
	RejectedMetric        = "notifications_rejected_total"
	ReplaysMetric         = "idempotent_replays_total"
)

// The label values of the api's metrics that are not a channel, a priority
// or a status.
const (
	// routeUnmatched is the route of a request that matches no route; the
	// raw path is never a label value.
	routeUnmatched = "unmatched"
	// methodOther is the method of a request whose method is not a standard
	// HTTP method.
	methodOther = "OTHER"

	sourceSingle = "single"
	sourceBatch  = "batch"

	// The reasons of notifications_rejected_total: the request could not be
	// read (body, media type, size); a field is invalid (schema, channel
	// rules, schedule, template); the dedupe_key is held (by a stored
	// notification or an earlier item of the batch); the Idempotency-Key
	// header is invalid or live for another request.
	reasonMalformed      = "malformed"
	reasonInvalid        = "invalid"
	reasonDuplicate      = "duplicate"
	reasonIdempotencyKey = "idempotency_key"
)

var (
	rejectReasons = []string{reasonMalformed, reasonInvalid, reasonDuplicate, reasonIdempotencyKey}
	// httpBuckets cover one API request, up to the server's read and write
	// timeouts (30 s).
	httpBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}
)

// Metrics are the api's metrics.
type Metrics struct {
	requests *prometheus.CounterVec   // route, method, code
	duration *prometheus.HistogramVec // route, method
	created  *prometheus.CounterVec   // channel, priority, source
	rejected *prometheus.CounterVec   // channel, reason
	replays  prometheus.Counter
	finished *metrics.Finished
}

// NewMetrics returns the api's metrics registered on reg (the role
// registers them on the default registry, which its admin port serves); a
// metric that is already registered there is reused (metrics.Register).
// Every series of a fixed label set is created at zero: the route and
// method of every operation of api/openapi.yaml with each status code the
// specification documents for it, every channel, priority and source of a
// creation, every channel (and "unknown") with every reason of a rejection,
// and the cancellation of every lane. A nil reg leaves the metrics
// unregistered and creates no series.
func NewMetrics(reg prometheus.Registerer) (*Metrics, error) {
	m := &Metrics{}
	var err error
	register := func(name string, f func() error) {
		if err == nil {
			if ferr := f(); ferr != nil {
				err = fmt.Errorf("api: register %s: %w", name, ferr)
			}
		}
	}
	register(RequestsMetric, func() (e error) {
		m.requests, e = metrics.Register(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: RequestsMetric,
			Help: "API requests by route template, method and status code.",
		}, []string{"route", "method", "code"}))
		return e
	})
	register(RequestDurationMetric, func() (e error) {
		m.duration, e = metrics.Register(reg, prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    RequestDurationMetric,
			Help:    "Duration of an API request, by route template and method.",
			Buckets: httpBuckets,
		}, []string{"route", "method"}))
		return e
	})
	register(CreatedMetric, func() (e error) {
		m.created, e = metrics.Register(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: CreatedMetric,
			Help: "Notifications stored by a create request; source is single or batch. A replay stores nothing.",
		}, []string{"channel", "priority", "source"}))
		return e
	})
	register(RejectedMetric, func() (e error) {
		m.rejected, e = metrics.Register(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: RejectedMetric,
			Help: "Notifications of create requests that were refused, by reason: malformed, invalid, duplicate, idempotency_key.",
		}, []string{"channel", "reason"}))
		return e
	})
	register(ReplaysMetric, func() (e error) {
		m.replays, e = metrics.Register(reg, prometheus.NewCounter(prometheus.CounterOpts{
			Name: ReplaysMetric,
			Help: "Create requests answered with the stored response of their Idempotency-Key.",
		}))
		return e
	})
	register(metrics.FinishedMetric, func() (e error) {
		m.finished, e = metrics.NewFinished(reg, domain.Channels(), domain.StatusCancelled)
		return e
	})
	if err != nil {
		return nil, err
	}
	if reg == nil {
		return m, nil
	}
	for _, c := range domain.Channels() {
		for _, p := range domain.Priorities() {
			m.created.WithLabelValues(string(c), p.String(), sourceSingle)
			m.created.WithLabelValues(string(c), p.String(), sourceBatch)
		}
	}
	for _, c := range append(channelNames(), metrics.Unknown) {
		for _, r := range rejectReasons {
			m.rejected.WithLabelValues(c, r)
		}
	}
	ops, err := documentedOperations(spec.OpenAPI())
	if err != nil {
		return nil, fmt.Errorf("api metrics: %w", err)
	}
	for _, op := range ops {
		m.duration.WithLabelValues(op.route, op.method)
		for _, code := range op.codes {
			m.requests.WithLabelValues(op.route, op.method, code)
		}
	}
	return m, nil
}

// registerMetrics registers every metric the api serves on reg: its own
// (NewMetrics) and the counters of its event publisher.
func registerMetrics(reg prometheus.Registerer, ev *queue.EventPublisher) (*Metrics, error) {
	m, err := NewMetrics(reg)
	if err != nil {
		return nil, err
	}
	if err := queue.RegisterEventMetrics(reg, ev); err != nil {
		return nil, fmt.Errorf("api metrics: %w", err)
	}
	return m, nil
}

func channelNames() []string {
	var out []string
	for _, c := range domain.Channels() {
		out = append(out, string(c))
	}
	return out
}

// operation is one operation of the specification: its route template, its
// method and the status codes documented for it.
type operation struct {
	route, method string
	codes         []string
}

// documentedOperations lists the operations of an OpenAPI document.
func documentedOperations(doc []byte) ([]operation, error) {
	t, err := openapi3.NewLoader().LoadFromData(doc)
	if err != nil {
		return nil, fmt.Errorf("load specification: %w", err)
	}
	var out []operation
	for path, item := range t.Paths.Map() {
		for method, op := range item.Operations() {
			o := operation{route: path, method: strings.ToUpper(method)}
			if op.Responses != nil {
				for code := range op.Responses.Map() {
					if n, err := strconv.Atoi(code); err == nil && n >= 100 && n <= 599 {
						o.codes = append(o.codes, code)
					}
				}
			}
			out = append(out, o)
		}
	}
	return out, nil
}

// observeRequest counts one answered request. route is the pattern the mux
// matched ("GET /v1/notifications/{id}"), empty when none did; the label is
// its path, the route template, never the raw path.
func (m *Metrics) observeRequest(pattern, method string, status int, d time.Duration) {
	route := routeLabel(pattern)
	method = methodLabel(method)
	m.requests.WithLabelValues(route, method, strconv.Itoa(status)).Inc()
	m.duration.WithLabelValues(route, method).Observe(d.Seconds())
}

// routeLabel returns the path of a mux pattern ("METHOD /path" or
// "/path"), or routeUnmatched for an empty one.
func routeLabel(pattern string) string {
	if pattern == "" {
		return routeUnmatched
	}
	if _, path, ok := strings.Cut(pattern, " "); ok {
		return path
	}
	return pattern
}

// methodLabel returns m when it is a standard HTTP method, and methodOther
// otherwise: the method is chosen by the client.
func methodLabel(m string) string {
	switch m {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch,
		http.MethodDelete, http.MethodConnect, http.MethodOptions, http.MethodTrace:
		return m
	}
	return methodOther
}

// createdOne counts a notification a create request stored.
func (m *Metrics) createdOne(c domain.Channel, p domain.Priority, source string) {
	m.created.WithLabelValues(metrics.ChannelLabel(c), metrics.PriorityLabel(p), source).Inc()
}

// createAnswer is what a create request's answer says about its
// notifications, gathered by createGuard while the request is served.
type createAnswer struct {
	batch bool
	// status is the status code written, replayed whether the answer was
	// a replay (Idempotent-Replayed: true).
	status   int
	replayed bool
	// body is the request body, when it could be read: the channels of the
	// rejected notifications are read from it.
	body []byte
	// problem is the problem written, if any.
	problem *problem
	// rejected are the items a batch that was stored still rejected.
	rejected []apigen.RejectedItem
}

// trackCreate returns the writer a create request is answered on and the
// function that counts the answer, a, once the request has been served.
func (m *Metrics) trackCreate(w http.ResponseWriter, r *http.Request, a *createAnswer) (http.ResponseWriter, func()) {
	rec := &statusRecorder{ResponseWriter: w}
	st := stateFrom(r.Context())
	return rec, func() {
		a.status = rec.status()
		a.replayed = rec.Header().Get(headerIdempotentReplayed) == "true"
		a.problem = st.lastProblem()
		m.answeredCreate(*a)
	}
}

// answeredCreate counts what the answer of a create request says: a replay;
// or each notification the request had rejected, with its channel when the
// body names a valid one ("unknown" otherwise) and the reason. A 5xx
// rejects nothing: the request can be retried. The stored notifications are
// counted where they are stored (createdOne).
func (m *Metrics) answeredCreate(a createAnswer) {
	switch {
	case a.replayed:
		m.replays.Inc()
	case a.status >= 500:
	case a.status >= 400 && a.problem != nil && len(a.problem.rejected) > 0:
		m.rejectItems(a.body, a.problem.rejected)
	case a.status >= 400:
		channel := metrics.Unknown
		if !a.batch {
			channel = channelOf(a.body)
		}
		m.rejected.WithLabelValues(channel, problemReason(a.problem)).Inc()
	default:
		m.rejectItems(a.body, a.rejected)
	}
}

// rejectItems counts the rejected items of a batch body.
func (m *Metrics) rejectItems(body []byte, items []apigen.RejectedItem) {
	if len(items) == 0 {
		return
	}
	channels := itemChannels(body)
	for _, it := range items {
		channel := metrics.Unknown
		if it.Index >= 0 && it.Index < len(channels) {
			channel = channels[it.Index]
		}
		m.rejected.WithLabelValues(channel, itemReason(it.Errors)).Inc()
	}
}

// problemReason maps the problem of a refused create request to its reason.
func problemReason(p *problem) string {
	if p == nil {
		return reasonInvalid
	}
	switch p.kind {
	case kindMalformed:
		for _, e := range p.errors {
			if e.Code == codeInvalidIdempotencyKey {
				return reasonIdempotencyKey
			}
		}
		return reasonMalformed
	case kindTooLarge, kindUnsupportedMediaType:
		return reasonMalformed
	case kindDuplicate:
		return reasonDuplicate
	case kindIdempotencyKeyReused:
		return reasonIdempotencyKey
	}
	return reasonInvalid
}

// itemReason maps the errors of a rejected batch item to its reason.
func itemReason(errs []apigen.FieldError) string {
	for _, e := range errs {
		if e.Code == codeDuplicate || e.Code == codeDuplicateInBatch {
			return reasonDuplicate
		}
	}
	return reasonInvalid
}

// channelOf returns the label of the channel member of a JSON object, or
// "unknown" when the body is not an object or has no valid channel.
func channelOf(body []byte) string {
	var v struct {
		Channel string `json:"channel"`
	}
	if json.Unmarshal(body, &v) != nil {
		return metrics.Unknown
	}
	return metrics.ChannelLabel(domain.Channel(v.Channel))
}

// itemChannels returns the channel label of every item of a batch body, in
// the order of its notifications array.
func itemChannels(body []byte) []string {
	var v struct {
		Notifications []json.RawMessage `json:"notifications"`
	}
	if json.Unmarshal(body, &v) != nil {
		return nil
	}
	out := make([]string, len(v.Notifications))
	for i, raw := range v.Notifications {
		out[i] = channelOf(raw)
	}
	return out
}
