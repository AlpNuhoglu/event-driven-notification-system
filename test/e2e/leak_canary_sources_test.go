//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
)

// The sources of the canary scan: where the stack's output is read, how
// each source proves that it was read, and the search itself.

// stackServices are the services of the stack under test (the two Compose
// files of the repository and the mock override). Each one's log is a
// source; a service missing from the stack fails the scan.
var stackServices = []string{
	"api", "relay", "worker", "gateway", "migrate", "mockprovider",
	"postgres", "rabbitmq", "redis", "prometheus", "grafana", "jaeger",
}

// metricsControls names, for each /metrics read from inside the stack, a
// metric that must be in it.
var metricsControls = map[string]string{
	"http://api:8081/metrics":       "http_requests_total",
	"http://relay:8081/metrics":     "relay_published_total",
	"http://worker:8081/metrics":    "delivery_attempts_total",
	"http://gateway:8081/metrics":   "ws_clients_disconnected_total",
	"http://rabbitmq:15692/metrics": "rabbitmq_identity_info",
}

// tracedRoles are the services whose spans the scan requires in Jaeger.
var tracedRoles = []string{"api", "relay", "worker"}

// maxTraces bounds the traces read per service; a full answer fails the
// scan, as it may have left traces out.
const maxTraces = 5000

// maxFindingsShown bounds the findings printed per source and canary.
const maxFindingsShown = 3

// documentedExample is the one text of the product that holds a canary by
// definition: the message of invalid_e164 names the reserved fake number as
// its example (ADR-0006 §9), whatever the request held. It is cut from the
// API's answers before they are searched; the number anywhere else, also
// in another message, is a match. The requests that get this message carry
// a recipient that is not the number (a token), so it cannot be an echo.
const documentedExample = "recipient must be an E.164 number such as " + fakePhone

// document is one searched unit of a source: a log line, a span, a
// /metrics line, a series, an answer.
type document struct {
	where string
	text  string
	// recipientAllowed: a successful answer that documents a notification
	// (exchange.recipientAllowed); not searched for recipients.
	recipientAllowed bool
}

// source is the output of one place of the stack.
type source struct {
	name string
	docs []document
}

// collect waits until the output of the run has arrived and reads every
// source. cancelled and sent are the ids of the control notifications.
func (r *canaryRun) collect(start time.Time, cancelled, sent string) []source {
	r.t.Helper()
	promURL := strings.TrimRight(envOr("E2E_PROMETHEUS_URL", defaultPrometheusURL), "/")
	jaegerURL := strings.TrimRight(envOr("E2E_JAEGER_URL", defaultJaegerURL), "/")
	promNow, err := r.promTime(promURL)
	if err != nil {
		r.t.Fatal(err)
	}

	// The output has arrived when the control lines are in the api's and
	// the worker's logs, the control trace in Jaeger holds the spans of the
	// api, the relay and the worker, and Prometheus has scraped every target
	// after the traffic.
	var missing []string
	if !poll(canaryOutputTimeout, outputPollInterval, func() bool {
		missing = missing[:0]
		for svc, id := range map[string]string{"api": cancelled, "worker": sent} {
			lines, err := serviceLogsSince(start.Add(-time.Minute), svc)
			if err != nil || !slices.ContainsFunc(lines, func(l string) bool { return strings.Contains(l, id) }) {
				missing = append(missing, fmt.Sprintf("the id %s in the %s's log (error %v)", id, svc, err))
			}
		}
		if roles, err := r.controlTraceRoles(jaegerURL, start, sent); err != nil || !containsAll(roles, tracedRoles) {
			missing = append(missing, fmt.Sprintf("the control trace of %s with spans of %v in Jaeger (have %v, error %v)", sent, tracedRoles, roles, err))
		}
		if stale, err := r.staleTargets(promURL, promNow); err != nil || len(stale) > 0 {
			missing = append(missing, fmt.Sprintf("a scrape of every Prometheus target after the traffic (stale %v, error %v)", stale, err))
		}
		return len(missing) == 0
	}) {
		r.t.Fatalf("the output of the run did not arrive within %s; missing: %s", canaryOutputTimeout, strings.Join(missing, "; "))
	}

	var all []source
	all = append(all, r.logSources(cancelled, sent)...)
	all = append(all, r.spanSource(jaegerURL, start, sent))
	all = append(all, r.metricsSources()...)
	all = append(all, r.seriesSource(promURL, start))
	all = append(all, source{name: "mockprovider report", docs: []document{{where: "GET /admin/report", text: r.report}}})
	ex := source{name: "API answers"}
	for _, e := range r.ex {
		ex.docs = append(ex.docs, document{
			where:            fmt.Sprintf("answer %d to %q", e.status, e.label),
			text:             strings.ReplaceAll(headerText(e.header)+"\n"+string(e.body), documentedExample, "<documented example>"),
			recipientAllowed: e.recipientAllowed && e.status/100 == 2,
		})
	}
	all = append(all, ex, source{name: "gateway messages", docs: r.ws})
	for _, s := range all {
		if len(s.docs) == 0 {
			r.t.Errorf("source %s: empty; the scan of it would have searched nothing", s.name)
		}
	}
	return all
}

// containsAll reports whether have holds every element of want.
func containsAll(have, want []string) bool {
	for _, w := range want {
		if !slices.Contains(have, w) {
			return false
		}
	}
	return true
}

// logSources reads the log of every service of the stack. The api's log
// must hold the id of the notification it cancelled, the worker's the id of
// the one it sent.
func (r *canaryRun) logSources(cancelled, sent string) []source {
	r.t.Helper()
	out, err := compose("config", "--services")
	if err != nil {
		r.t.Fatal(err)
	}
	services := strings.Fields(out)
	for _, want := range stackServices {
		if !slices.Contains(services, want) {
			r.t.Errorf("source logs: the stack has no service %s (it has %v)", want, services)
		}
	}
	controls := map[string]string{"api": cancelled, "worker": sent}
	var all []source
	for _, svc := range services {
		text, err := compose("logs", "--no-color", "--no-log-prefix", svc)
		if err != nil {
			r.t.Fatalf("logs of %s: %v", svc, err)
		}
		src := source{name: "log of " + svc}
		for i, line := range strings.Split(text, "\n") {
			if strings.TrimSpace(line) != "" {
				src.docs = append(src.docs, document{where: "line " + strconv.Itoa(i+1), text: line})
			}
		}
		if id, ok := controls[svc]; ok && !strings.Contains(text, id) {
			r.t.Errorf("source %s: control missing: the id %s of a notification of the run is not in it", src.name, id)
		}
		all = append(all, src)
	}
	return all
}

// otlpTraces is an answer of Jaeger's API v3 (GET /api/v3/traces): the
// spans in OTLP JSON, grouped by resource (the service) and scope.
type otlpTraces struct {
	Result struct {
		ResourceSpans []struct {
			Resource   json.RawMessage `json:"resource"`
			ScopeSpans []struct {
				Scope json.RawMessage   `json:"scope"`
				Spans []json.RawMessage `json:"spans"`
			} `json:"scopeSpans"`
		} `json:"resourceSpans"`
	} `json:"result"`
}

// otlpAttrs are the attributes of a resource or a span.
type otlpAttrs struct {
	Attributes []struct {
		Key   string `json:"key"`
		Value struct {
			StringValue string `json:"stringValue"`
		} `json:"value"`
	} `json:"attributes"`
}

// get returns the string value of the attribute key.
func (a otlpAttrs) get(key string) string {
	for _, kv := range a.Attributes {
		if kv.Key == key {
			return kv.Value.StringValue
		}
	}
	return ""
}

// otlpSpan holds the members of a span the scan reads.
type otlpSpan struct {
	otlpAttrs
	TraceID string `json:"traceId"`
	SpanID  string `json:"spanId"`
	Name    string `json:"name"`
}

// jaegerSpans returns one document per span of the traces of a service
// that started in [from, to), optionally only the traces with a span that
// has the attribute filter, and the number of traces. A query without a
// match is Jaeger's 404 "No traces found": no span.
func (r *canaryRun) jaegerSpans(jaegerURL, service string, from, to time.Time, filter map[string]string) ([]spanDoc, int, error) {
	q := url.Values{}
	q.Set("query.service_name", service)
	q.Set("query.start_time_min", from.UTC().Format(time.RFC3339Nano))
	q.Set("query.start_time_max", to.UTC().Format(time.RFC3339Nano))
	q.Set("query.num_traces", strconv.Itoa(maxTraces))
	if filter != nil {
		b, err := json.Marshal(filter)
		if err != nil {
			return nil, 0, err
		}
		q.Set("query.attributes", string(b))
	}
	code, _, body, err := r.s.do(http.MethodGet, jaegerURL+"/api/v3/traces?"+q.Encode(), nil)
	if err != nil {
		return nil, 0, err
	}
	if code == http.StatusNotFound && strings.Contains(string(body), "No traces found") {
		return nil, 0, nil
	}
	if code != http.StatusOK {
		return nil, 0, fmt.Errorf("jaeger traces of %s: status %d", service, code)
	}
	var ans otlpTraces
	if err := json.Unmarshal(body, &ans); err != nil {
		return nil, 0, fmt.Errorf("jaeger traces of %s: decode: %w", service, err)
	}
	var docs []spanDoc
	traces := map[string]bool{}
	for _, rs := range ans.Result.ResourceSpans {
		var res otlpAttrs
		if err := json.Unmarshal(rs.Resource, &res); err != nil {
			return nil, 0, fmt.Errorf("jaeger traces of %s: decode a resource: %w", service, err)
		}
		for _, ss := range rs.ScopeSpans {
			for _, raw := range ss.Spans {
				var sp otlpSpan
				if err := json.Unmarshal(raw, &sp); err != nil {
					return nil, 0, fmt.Errorf("jaeger traces of %s: decode a span: %w", service, err)
				}
				traces[sp.TraceID] = true
				docs = append(docs, spanDoc{
					span: sp, service: res.get("service.name"),
					text: string(raw) + "\n" + string(rs.Resource) + "\n" + string(ss.Scope),
				})
			}
		}
	}
	return docs, len(traces), nil
}

// spanDoc is one span as the scan searches it: the span, its resource and
// its scope.
type spanDoc struct {
	span    otlpSpan
	service string
	text    string
}

// controlTraceRoles returns the services of the trace that holds a span
// of the worker with notification.id = id.
func (r *canaryRun) controlTraceRoles(jaegerURL string, start time.Time, id string) ([]string, error) {
	docs, _, err := r.jaegerSpans(jaegerURL, "worker", start.Add(-5*time.Minute), time.Now().Add(5*time.Minute),
		map[string]string{"notification.id": id})
	if err != nil {
		return nil, err
	}
	var roles []string
	for _, d := range docs {
		if !slices.Contains(roles, d.service) {
			roles = append(roles, d.service)
		}
	}
	slices.Sort(roles)
	return roles, nil
}

// spanSource reads every span Jaeger holds for every service, of the
// traces that started since the run did (no earlier span can hold a canary
// of the run). Its control: a span of the worker with notification.id =
// sent.
func (r *canaryRun) spanSource(jaegerURL string, start time.Time, sent string) source {
	r.t.Helper()
	src := source{name: "Jaeger spans"}
	code, _, body, err := r.s.do(http.MethodGet, jaegerURL+"/api/v3/services", nil)
	var services struct {
		Services []string `json:"services"`
	}
	if err != nil || code != http.StatusOK || json.Unmarshal(body, &services) != nil {
		r.t.Fatalf("source %s: services: status %d, error %v", src.name, code, err)
	}
	for _, want := range tracedRoles {
		if !slices.Contains(services.Services, want) {
			r.t.Errorf("source %s: no span of the service %s (services %v)", src.name, want, services.Services)
		}
	}
	seen := map[string]bool{}
	control := false
	for _, svc := range services.Services {
		docs, traces, err := r.jaegerSpans(jaegerURL, svc, start.Add(-5*time.Minute), time.Now().Add(5*time.Minute), nil)
		if err != nil {
			r.t.Fatalf("source %s: %v", src.name, err)
		}
		if traces >= maxTraces {
			r.t.Errorf("source %s: %d traces of %s, the limit of the query: some may have been left out", src.name, traces, svc)
		}
		for _, d := range docs {
			key := d.span.TraceID + "/" + d.span.SpanID
			if seen[key] {
				continue
			}
			seen[key] = true
			if d.service == "worker" && d.span.get("notification.id") == sent {
				control = true
			}
			src.docs = append(src.docs, document{
				where: fmt.Sprintf("trace %s span %s (%s, %q)", d.span.TraceID, d.span.SpanID, d.service, d.span.Name),
				text:  d.text,
			})
		}
	}
	if !control {
		r.t.Errorf("source %s: control missing: no span of the worker with notification.id %s", src.name, sent)
	}
	return src
}

// metricsSources reads the /metrics of every role and of RabbitMQ from
// inside the stack's network (the admin port is not published), each with
// its control metric. mockprovider has no /metrics: that is checked, so
// that one added later is not left out.
func (r *canaryRun) metricsSources() []source {
	r.t.Helper()
	targets := make([]string, 0, len(metricsControls))
	for u := range metricsControls {
		targets = append(targets, u)
	}
	slices.Sort(targets)
	var all []source
	for _, u := range targets {
		out, err := compose("exec", "-T", "prometheus", "wget", "-q", "-O", "-", u)
		if err != nil {
			r.t.Fatalf("read %s: %v", u, err)
		}
		src := source{name: "/metrics " + u}
		found := false
		for i, line := range strings.Split(out, "\n") {
			if line == "" {
				continue
			}
			src.docs = append(src.docs, document{where: "line " + strconv.Itoa(i+1), text: line})
			if strings.HasPrefix(line, metricsControls[u]) {
				found = true
			}
		}
		if !found {
			r.t.Errorf("source %s: control missing: no sample of %s", src.name, metricsControls[u])
		}
		all = append(all, src)
	}
	code, _, _, err := r.s.do(http.MethodGet, r.s.mockURL+"/metrics", nil)
	if err != nil || code != http.StatusNotFound {
		r.t.Errorf("mockprovider /metrics: status %d, error %v; want 404 (it has none), or add it to the scan", code, err)
	}
	return all
}

// seriesSource reads every series Prometheus stored since the run started,
// with all its label values. Its control: a series of the worker's
// delivery_attempts_total.
func (r *canaryRun) seriesSource(promURL string, start time.Time) source {
	r.t.Helper()
	src := source{name: "Prometheus series"}
	q := url.Values{}
	q.Set("match[]", `{__name__=~".+"}`)
	q.Set("start", strconv.FormatInt(start.Add(-time.Minute).Unix(), 10))
	q.Set("end", strconv.FormatInt(time.Now().Add(time.Minute).Unix(), 10))
	code, _, body, err := r.s.do(http.MethodGet, promURL+"/api/v1/series?"+q.Encode(), nil)
	if err != nil || code != http.StatusOK {
		r.t.Fatalf("source %s: status %d, error %v", src.name, code, err)
	}
	var ans struct {
		Data []map[string]string `json:"data"`
	}
	if err := json.Unmarshal(body, &ans); err != nil {
		r.t.Fatalf("source %s: decode: %v", src.name, err)
	}
	control := false
	for _, labels := range ans.Data {
		b, _ := json.Marshal(labels) //nolint:errchkjson // a map of strings
		src.docs = append(src.docs, document{where: "series " + labels["__name__"], text: string(b)})
		if labels["__name__"] == "delivery_attempts_total" && labels["job"] == "worker" {
			control = true
		}
	}
	if !control {
		r.t.Errorf("source %s: control missing: no series of delivery_attempts_total of the job worker", src.name)
	}
	return src
}

// promTime returns Prometheus' own clock, so that the scrapes are compared
// with a time of the same clock.
func (r *canaryRun) promTime(promURL string) (float64, error) {
	code, _, body, err := r.s.do(http.MethodGet, promURL+"/api/v1/query?query=time()", nil)
	if err != nil {
		return 0, err
	}
	if code != http.StatusOK {
		return 0, fmt.Errorf("prometheus time(): status %d", code)
	}
	var ans struct {
		Data struct {
			Result []json.RawMessage `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &ans); err != nil || len(ans.Data.Result) != 2 {
		return 0, fmt.Errorf("prometheus time(): %s", body)
	}
	var v string
	if err := json.Unmarshal(ans.Data.Result[1], &v); err != nil {
		return 0, err
	}
	return strconv.ParseFloat(v, 64)
}

// staleTargets returns the Prometheus targets whose last scrape is not
// after since (seconds of Prometheus' clock).
func (r *canaryRun) staleTargets(promURL string, since float64) ([]string, error) {
	code, _, body, err := r.s.do(http.MethodGet, promURL+"/api/v1/targets?state=active", nil)
	if err != nil {
		return nil, err
	}
	if code != http.StatusOK {
		return nil, fmt.Errorf("prometheus targets: status %d", code)
	}
	var ans struct {
		Data struct {
			ActiveTargets []struct {
				ScrapeURL  string    `json:"scrapeUrl"`
				LastScrape time.Time `json:"lastScrape"`
			} `json:"activeTargets"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &ans); err != nil {
		return nil, err
	}
	if len(ans.Data.ActiveTargets) == 0 {
		return nil, fmt.Errorf("prometheus has no active target")
	}
	var stale []string
	for _, tg := range ans.Data.ActiveTargets {
		if float64(tg.LastScrape.UnixNano())/1e9 <= since {
			stale = append(stale, tg.ScrapeURL)
		}
	}
	return stale, nil
}

// finding is one match of a canary.
type finding struct {
	source, where, canary, part string
	context                     string
}

// scanAll searches every source for every canary and fails the test with
// each match: the source, where in it, the canary's name, the beginning of
// the searched value and its length, and the text around the match with
// every canary cut out.
func (r *canaryRun) scanAll(all []source) {
	r.t.Helper()
	total := 0
	for _, src := range all {
		bytes := 0
		counts := map[string]int{}
		var shown []finding
		for _, d := range src.docs {
			bytes += len(d.text)
			lower := lowerASCII(d.text)
			var every [][2]int // the matches of every canary, so that no window cuts one
			for _, k := range r.c.list {
				every = append(every, k.matches(lower)...)
			}
			for _, k := range r.c.list {
				if d.recipientAllowed && k.recipient {
					continue
				}
				for _, m := range k.matches(lower) {
					counts[k.name]++
					if counts[k.name] > maxFindingsShown {
						continue
					}
					from, to := max(0, m[0]-160), min(len(d.text), m[1]+160)
					for _, o := range every {
						if o[0] < from && from < o[1] {
							from = o[0]
						}
						if o[0] < to && to < o[1] {
							to = o[1]
						}
					}
					part := ""
					if n := m[1] - m[0]; n < len(k.needle) {
						part = fmt.Sprintf(", a part of %d characters", n)
					}
					shown = append(shown, finding{
						source: src.name, where: d.where, canary: k.name, part: part,
						context: strings.ToValidUTF8(r.c.cut(d.text[from:to]), "?"),
					})
				}
			}
		}
		r.t.Logf("source %s: %d documents, %d bytes searched", src.name, len(src.docs), bytes)
		for _, f := range shown {
			k := r.c.byName(f.canary)
			r.t.Errorf("LEAK in %s, %s: canary %s (%.6s..., %d characters%s) found: %s",
				f.source, f.where, f.canary, k.needle, len(k.needle), f.part, f.context)
		}
		names := make([]string, 0, len(counts))
		for n := range counts {
			names = append(names, n)
		}
		slices.Sort(names)
		for _, n := range names {
			total += counts[n]
			if counts[n] > maxFindingsShown {
				r.t.Errorf("LEAK in %s: canary %s found %d times (%d shown)", src.name, n, counts[n], maxFindingsShown)
			}
		}
	}
	if total > 0 {
		r.t.Errorf("canary scan: %d matches", total)
	} else {
		r.t.Logf("canary scan: %d canaries, %d sources, no match", len(r.c.list), len(all))
	}
}

// byName returns the canary of that name.
func (c *canaries) byName(name string) canary {
	for _, k := range c.list {
		if k.name == name {
			return k
		}
	}
	panic("no canary " + name)
}
