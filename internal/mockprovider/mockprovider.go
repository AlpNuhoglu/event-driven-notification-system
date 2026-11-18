// Package mockprovider is a fake notification provider for automated tests
// and load tests. It never stores a recipient or message content: of a
// request body it keeps the top-level field names and the channel name only.
package mockprovider

import (
	"encoding/json"
	"maps"
	"net/http"
	"slices"
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
	now func() time.Time

	mu       sync.Mutex
	status   int
	total    int
	arrivals map[string][]time.Time
	keys     map[string]int
	fieldSet map[string][]string
}

// New returns a Server that reads the time from now.
func New(now func() time.Time) *Server {
	s := &Server{now: now}
	s.reset()
	return s
}

func (s *Server) reset() {
	s.status = http.StatusAccepted
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

	s.mu.Lock()
	s.total++
	s.arrivals[channel] = append(s.arrivals[channel], arrived)
	s.keys[r.Header.Get("Idempotency-Key")]++
	s.fieldSet[strings.Join(names, "\x00")] = names
	status := s.status
	s.mu.Unlock()

	if status != http.StatusAccepted {
		writeJSON(w, status, struct{}{})
		return
	}
	writeJSON(w, status, map[string]string{
		"messageId": uuid.NewString(),
		"status":    "accepted",
		"timestamp": s.now().UTC().Format(time.RFC3339),
	})
}

func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Status int `json:"status"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes)).Decode(&req); err != nil ||
		req.Status < 100 || req.Status > 599 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "status must be 100-599"})
		return
	}
	s.mu.Lock()
	s.status = req.Status
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]int{"status": req.Status})
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
