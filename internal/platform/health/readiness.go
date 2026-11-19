package health

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// Check reports whether one dependency of a role is ready: nil when it is.
// It must return when its context ends.
type Check func(ctx context.Context) error

// The status of a check and of the whole answer.
const (
	StatusOK   = "ok"
	StatusFail = "fail"
)

// Result is the outcome of one check, as /readyz reports it (ADR-0009 §7).
// The check's error is not part of it: an error text can name hosts and
// users, and /readyz is read by whoever can reach the port.
type Result struct {
	Name       string  `json:"name"`
	Status     string  `json:"status"`
	DurationMS float64 `json:"duration_ms"`
}

// Report is the body of /readyz.
type Report struct {
	Status string   `json:"status"`
	Checks []Result `json:"checks"`
}

type namedCheck struct {
	name  string
	check Check
}

// Readiness holds the dependency checks of a role. The role registers them
// when it starts (ReadinessFrom gives it the registry of its admin port),
// and /readyz runs them on every request. A role that registers none is
// ready while its process serves.
type Readiness struct {
	timeout time.Duration

	mu     sync.RWMutex
	checks []namedCheck
}

// NewReadiness returns an empty registry. Each check gets at most timeout
// (READINESS_CHECK_TIMEOUT); zero or less leaves a check bounded by the
// request alone.
func NewReadiness(timeout time.Duration) *Readiness {
	return &Readiness{timeout: timeout}
}

// Add registers a check under name. The checks are reported in the order
// they were added. Add on a nil registry does nothing.
func (r *Readiness) Add(name string, check Check) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.checks = append(r.checks, namedCheck{name: name, check: check})
}

// Run runs every check, all at the same time, and reports whether all
// passed. A check that has not returned when its time is up has failed.
func (r *Readiness) Run(ctx context.Context) Report {
	r.mu.RLock()
	checks := append([]namedCheck(nil), r.checks...)
	r.mu.RUnlock()

	rep := Report{Status: StatusOK, Checks: make([]Result, len(checks))}
	var wg sync.WaitGroup
	for i, c := range checks {
		wg.Go(func() { rep.Checks[i] = r.run(ctx, c) })
	}
	wg.Wait()
	for _, res := range rep.Checks {
		if res.Status != StatusOK {
			rep.Status = StatusFail
		}
	}
	return rep
}

// run runs one check within the timeout. The check runs on a goroutine of
// its own, so that one that ignores its context cannot hold the answer; it
// then ends on its own time.
func (r *Readiness) run(ctx context.Context, c namedCheck) Result {
	if r.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.timeout)
		defer cancel()
	}
	start := time.Now()
	done := make(chan error, 1)
	go func() { done <- c.check(ctx) }()
	var err error
	select {
	case err = <-done:
	case <-ctx.Done():
		err = ctx.Err()
	}
	res := Result{Name: c.name, Status: StatusOK, DurationMS: float64(time.Since(start).Microseconds()) / 1000}
	if err != nil {
		res.Status = StatusFail
	}
	return res
}

// ServeHTTP answers /readyz: 200 when every check passed, 503 otherwise,
// with the status and duration of each check in both.
func (r *Readiness) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	rep := r.Run(req.Context())
	w.Header().Set("Content-Type", "application/json")
	if rep.Status != StatusOK {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	_ = json.NewEncoder(w).Encode(rep)
}

type readinessKey struct{}

// WithReadiness returns a context that carries the registry of the admin
// port, for the role's service.
func WithReadiness(ctx context.Context, r *Readiness) context.Context {
	return context.WithValue(ctx, readinessKey{}, r)
}

// ReadinessFrom returns the registry carried by ctx, or nil when there is
// none (a service started by a test without an admin port); Add on nil does
// nothing.
func ReadinessFrom(ctx context.Context) *Readiness {
	r, _ := ctx.Value(readinessKey{}).(*Readiness)
	return r
}
