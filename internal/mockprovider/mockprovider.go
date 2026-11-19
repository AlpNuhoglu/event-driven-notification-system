// Package mockprovider is a fake notification provider for automated tests
// and load tests. It never stores a recipient or message content: of a
// request body it keeps the top-level field names and the channel name only.
package mockprovider

import (
	"context"
	"encoding/json"
	"maps"
	"math/rand/v2"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/ratewindow"
)

const (
	maxBodyBytes  = 1 << 20
	arrivalLayout = "2006-01-02T15:04:05.000000Z"
	unknownChan   = "unknown"
)

// ChannelReport is the evidence for one channel.
type ChannelReport struct {
	Count        int      `json:"count"`
	MaxPerSecond int      `json:"max_per_second"`
	Arrivals     []string `json:"arrivals"`
	// Statuses maps an answer status to its count; "closed" and "hung" count
	// the requests that got no answer.
	Statuses map[string]int `json:"statuses"`
}

// Report is the body of GET /admin/report.
type Report struct {
	Total           int                      `json:"total"`
	Channels        map[string]ChannelReport `json:"channels"`
	IdempotencyKeys map[string]int           `json:"idempotency_keys"`
	DuplicateKeys   int                      `json:"duplicate_keys"`
	FieldNameSets   [][]string               `json:"field_name_sets"`
}

// Server is the mock provider. Create it with New.
type Server struct {
	now  func() time.Time
	rand func() float64
	wait func(ctx context.Context, d time.Duration)

	mu       sync.Mutex
	cfg      Config
	statuses map[string]map[string]int
	total    int
	arrivals map[string][]time.Time
	keys     map[string]int
	fieldSet map[string][]string
}

// Option customizes a Server.
type Option func(*Server)

// WithRand sets the random source: a function returning a value in [0, 1).
// It is called with the server lock held, so it need not be safe for
// concurrent use.
func WithRand(f func() float64) Option { return func(s *Server) { s.rand = f } }

// WithWait sets how the latency is waited out. The function must return when
// d has passed or ctx has ended.
func WithWait(f func(ctx context.Context, d time.Duration)) Option {
	return func(s *Server) { s.wait = f }
}

// New returns a Server that reads the time from now.
func New(now func() time.Time, opts ...Option) *Server {
	s := &Server{now: now, rand: rand.Float64, wait: sleepContext}
	for _, o := range opts {
		o(s)
	}
	s.reset()
	return s
}

func sleepContext(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}

func (s *Server) reset() {
	s.cfg = defaultConfig()
	s.statuses = map[string]map[string]int{}
	s.total = 0
	s.arrivals = map[string][]time.Time{}
	s.keys = map[string]int{}
	s.fieldSet = map[string][]string{}
}

// Handler serves the provider endpoint (every path outside /admin/) and the
// admin API.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /admin/config", s.handleConfig)
	mux.HandleFunc("GET /admin/report", s.handleReport)
	mux.HandleFunc("POST /admin/reset", s.handleReset)
	mux.HandleFunc("GET /healthz", handleHealth)
	mux.HandleFunc("GET /readyz", handleHealth)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || strings.HasPrefix(r.URL.Path, "/admin/") {
			http.NotFound(w, r)
			return
		}
		s.handleProvider(w, r)
	})
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleProvider(w http.ResponseWriter, r *http.Request) {
	// The arrival time is taken first, before the body is read and before the
	// lock: a request must not look later than it was because another request
	// or a report held the lock. The rate-limit evidence rests on these times.
	arrived := s.now().UTC()
	var fields map[string]json.RawMessage
	names := []string{}
	channel := unknownChan
	body := http.MaxBytesReader(w, r.Body, maxBodyBytes)
	if err := json.NewDecoder(body).Decode(&fields); err == nil {
		for k := range fields {
			names = append(names, k)
		}
		slices.Sort(names)
		var c string
		if raw, ok := fields["channel"]; ok && json.Unmarshal(raw, &c) == nil {
			switch c {
			case "sms", "email", "push":
				channel = c
			}
		}
	}
	fields = nil

	key := r.Header.Get("Idempotency-Key")
	s.mu.Lock()
	s.total++
	s.arrivals[channel] = append(s.arrivals[channel], arrived)
	seen := s.keys[key]
	s.keys[key]++
	s.fieldSet[strings.Join(names, "\x00")] = names
	o := s.decideLocked(s.cfg.rule(channel), seen)
	label := strconv.Itoa(o.status)
	switch {
	case o.rule.CloseConnection:
		label = "closed"
	case o.rule.Hang:
		label = "hung"
	}
	if s.statuses[channel] == nil {
		s.statuses[channel] = map[string]int{}
	}
	s.statuses[channel][label]++
	s.mu.Unlock()

	if o.latency > 0 {
		s.wait(r.Context(), o.latency)
	}
	switch label {
	case "closed":
		closeConnection(w)
		return
	case "hung":
		<-r.Context().Done()
		return
	}
	s.answer(w, o)
}

// closeConnection closes the TCP connection without writing a response.
func closeConnection(w http.ResponseWriter) {
	conn, _, err := http.NewResponseController(w).Hijack()
	if err != nil {
		// No hijack (for example HTTP/2): abort the handler instead.
		panic(http.ErrAbortHandler)
	}
	_ = conn.Close()
}

func (s *Server) answer(w http.ResponseWriter, o outcome) {
	status := o.status
	if o.rule.RetryAfter != "" && (status == http.StatusTooManyRequests || status == http.StatusServiceUnavailable) {
		w.Header().Set("Retry-After", o.rule.RetryAfter)
	}
	if o.rule.Location != "" && status >= 300 && status <= 399 {
		w.Header().Set("Location", o.rule.Location)
	}
	switch {
	case o.rule.MalformedBody && status >= 200 && status <= 299:
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(status)
		_, _ = w.Write([]byte("not json"))
	case status == http.StatusAccepted:
		writeJSON(w, status, map[string]string{
			"messageId": uuid.NewString(),
			"status":    "accepted",
			"timestamp": s.now().UTC().Format(time.RFC3339),
		})
	default:
		writeJSON(w, status, struct{}{})
	}
}

func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	cfg, err := parseConfig(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	s.mu.Lock()
	s.cfg = cfg
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, cfg)
}

func (s *Server) handleReset(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	s.reset()
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReport(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.report())
}

func (s *Server) report() Report {
	// Only the copies are made under the lock; sorting and the window
	// computation run outside it, so a report never delays the handler that
	// records arrivals.
	s.mu.Lock()
	rep := Report{
		Total:           s.total,
		Channels:        map[string]ChannelReport{},
		IdempotencyKeys: maps.Clone(s.keys),
		FieldNameSets:   [][]string{},
	}
	statuses := make(map[string]map[string]int, len(s.statuses))
	for ch, m := range s.statuses {
		statuses[ch] = maps.Clone(m)
	}
	arrivals := make(map[string][]time.Time, len(s.arrivals))
	for ch, ts := range s.arrivals {
		arrivals[ch] = slices.Clone(ts)
	}
	for _, names := range s.fieldSet {
		rep.FieldNameSets = append(rep.FieldNameSets, slices.Clone(names))
	}
	s.mu.Unlock()

	for ch, sorted := range arrivals {
		slices.SortFunc(sorted, time.Time.Compare)
		cr := ChannelReport{
			Count:        len(sorted),
			MaxPerSecond: ratewindow.MaxInWindow(sorted, time.Second),
			Arrivals:     make([]string, len(sorted)),
			Statuses:     statuses[ch],
		}
		for i, t := range sorted {
			cr.Arrivals[i] = t.Format(arrivalLayout)
		}
		rep.Channels[ch] = cr
	}
	for _, n := range rep.IdempotencyKeys {
		if n > 1 {
			rep.DuplicateKeys++
		}
	}
	slices.SortFunc(rep.FieldNameSets, func(a, b []string) int {
		return strings.Compare(strings.Join(a, "\x00"), strings.Join(b, "\x00"))
	})
	return rep
}
