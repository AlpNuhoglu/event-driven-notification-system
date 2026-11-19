//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

const (
	defaultPrometheusURL = "http://localhost:9090"

	// scrapeWaitTimeout bounds the wait for the first scrapes (interval 5 s)
	// and for the rate() windows to hold two samples.
	scrapeWaitTimeout = 90 * time.Second
	// prometheusPollInterval is how often Prometheus is asked again.
	prometheusPollInterval = time.Second

	// dashboardRateInterval replaces Grafana's $__rate_interval, which only
	// Grafana can resolve.
	dashboardRateInterval = "1m"
)

// scrapedJobs are the application roles Prometheus must scrape (ADR-0009 §8).
var scrapedJobs = []string{"api", "relay", "worker", "gateway"}

// expectedAlerts are the six rules of alerts.yml (ADR-0009 §3).
var expectedAlerts = []string{
	"RelayDown", "HighLaneStalled", "PoisonMessages",
	"ExpirySpike", "CircuitNotClosed", "LimiterFallbackActive",
}

// TestDashboardSmoke checks the monitoring set-up of the running stack:
// Prometheus scrapes the four application roles, loaded the alert rules, and
// every query of the "Overview" dashboard returns at least one series.
func TestDashboardSmoke(t *testing.T) {
	s := newStack(t)
	promURL := strings.TrimRight(envOr("E2E_PROMETHEUS_URL", defaultPrometheusURL), "/")

	t.Run("scrape targets are up", func(t *testing.T) {
		var last string
		ok := poll(scrapeWaitTimeout, prometheusPollInterval, func() bool {
			last = missingTargets(s, promURL)
			return last == ""
		})
		if !ok {
			t.Fatalf("scrape targets not up within %s: %s", scrapeWaitTimeout, last)
		}
	})

	t.Run("alert rules are loaded", func(t *testing.T) {
		var got []string
		var lastErr error
		ok := poll(scrapeWaitTimeout, prometheusPollInterval, func() bool {
			got, lastErr = loadedAlerts(s, promURL)
			return lastErr == nil && equalStrings(got, expectedAlerts)
		})
		if !ok {
			t.Fatalf("loaded alert rules %v (error %v), want %v", got, lastErr, expectedAlerts)
		}
	})

	t.Run("every dashboard query returns data", func(t *testing.T) {
		queries := dashboardQueries(t)
		if len(queries) == 0 {
			t.Fatal("the dashboard has no queries")
		}
		pending := queries
		lastErr := map[string]error{}
		poll(scrapeWaitTimeout, prometheusPollInterval, func() bool {
			var still []dashboardQuery
			for _, q := range pending {
				n, err := seriesCount(s, promURL, q.expr)
				lastErr[q.key()] = err
				if err != nil || n == 0 {
					still = append(still, q)
				}
			}
			pending = still
			return len(pending) == 0
		})
		for _, q := range pending {
			if err := lastErr[q.key()]; err != nil {
				t.Errorf("panel %q: query failed: %v\n  expr: %s", q.panel, err, q.expr)
				continue
			}
			t.Errorf("panel %q: no series within %s\n  expr: %s", q.panel, scrapeWaitTimeout, q.expr)
		}
	})
}

type dashboardQuery struct {
	panel string
	expr  string
}

func (q dashboardQuery) key() string { return q.panel + "\x00" + q.expr }

// dashboardQueries reads the dashboard file and returns the expression of
// every target of every panel, nested panels (rows) included, with Grafana's
// rate interval replaced. A remaining Grafana variable fails the test: the
// expressions must not use any.
func dashboardQueries(t *testing.T) []dashboardQuery {
	t.Helper()
	path := filepath.Join("..", "..", "deploy", "grafana", "dashboards", "overview.json")
	raw, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		t.Fatalf("read the dashboard: %v", err)
	}
	var dash struct {
		Panels []dashboardPanel `json:"panels"`
	}
	if err := json.Unmarshal(raw, &dash); err != nil {
		t.Fatalf("parse the dashboard: %v", err)
	}
	var out []dashboardQuery
	var walk func(ps []dashboardPanel)
	walk = func(ps []dashboardPanel) {
		for _, p := range ps {
			for _, tg := range p.Targets {
				if tg.Expr == "" {
					continue
				}
				expr := strings.ReplaceAll(tg.Expr, "$__rate_interval", dashboardRateInterval)
				if strings.Contains(expr, "$") {
					t.Errorf("panel %q: expression uses a Grafana variable: %s", p.Title, tg.Expr)
				}
				out = append(out, dashboardQuery{panel: p.Title, expr: expr})
			}
			walk(p.Panels)
		}
	}
	walk(dash.Panels)
	return out
}

type dashboardPanel struct {
	Title   string `json:"title"`
	Targets []struct {
		Expr string `json:"expr"`
	} `json:"targets"`
	Panels []dashboardPanel `json:"panels"`
}

// prometheusGet calls the Prometheus HTTP API and decodes the "data" member.
func prometheusGet(s *stack, promURL, path string, params url.Values, data any) error {
	u := promURL + path
	if len(params) > 0 {
		u += "?" + params.Encode()
	}
	code, _, body, err := s.do(http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	if code != http.StatusOK {
		return fmt.Errorf("status %d: %s", code, body)
	}
	var env struct {
		Status string          `json:"status"`
		Data   json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		return err
	}
	if env.Status != "success" {
		return fmt.Errorf("status %q: %s", env.Status, body)
	}
	return json.Unmarshal(env.Data, data)
}

// seriesCount runs an instant query and returns the number of series.
func seriesCount(s *stack, promURL, expr string) (int, error) {
	var data struct {
		Result []json.RawMessage `json:"result"`
	}
	if err := prometheusGet(s, promURL, "/api/v1/query", url.Values{"query": {expr}}, &data); err != nil {
		return 0, err
	}
	return len(data.Result), nil
}

// missingTargets describes the targets of the four application roles that
// are not up, or "" when every one of them is.
func missingTargets(s *stack, promURL string) string {
	var data struct {
		ActiveTargets []struct {
			Labels    map[string]string `json:"labels"`
			Health    string            `json:"health"`
			LastError string            `json:"lastError"`
		} `json:"activeTargets"`
	}
	if err := prometheusGet(s, promURL, "/api/v1/targets", url.Values{"state": {"active"}}, &data); err != nil {
		return err.Error()
	}
	health := map[string]string{}
	for _, tg := range data.ActiveTargets {
		job := tg.Labels["job"]
		if tg.Health != "up" {
			health[job] = fmt.Sprintf("%s %s", tg.Health, tg.LastError)
		} else if _, bad := health[job]; !bad {
			health[job] = "up"
		}
	}
	var problems []string
	for _, job := range scrapedJobs {
		if h, ok := health[job]; !ok {
			problems = append(problems, job+": no target")
		} else if h != "up" {
			problems = append(problems, job+": "+h)
		}
	}
	return strings.Join(problems, "; ")
}

// loadedAlerts returns the sorted names of the alerting rules Prometheus
// has loaded.
func loadedAlerts(s *stack, promURL string) ([]string, error) {
	var data struct {
		Groups []struct {
			Rules []struct {
				Type string `json:"type"`
				Name string `json:"name"`
			} `json:"rules"`
		} `json:"groups"`
	}
	if err := prometheusGet(s, promURL, "/api/v1/rules", nil, &data); err != nil {
		return nil, err
	}
	var names []string
	for _, g := range data.Groups {
		for _, r := range g.Rules {
			if r.Type == "alerting" {
				names = append(names, r.Name)
			}
		}
	}
	sort.Strings(names)
	return names, nil
}

// equalStrings reports whether got, sorted, equals want, in any order.
func equalStrings(got, want []string) bool {
	w := append([]string(nil), want...)
	sort.Strings(w)
	if len(got) != len(w) {
		return false
	}
	for i := range got {
		if got[i] != w[i] {
			return false
		}
	}
	return true
}
