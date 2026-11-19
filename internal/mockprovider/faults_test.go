package mockprovider

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	emailBody = `{"to":"a@example.com","channel":"email","content":"y"}`
	pushBody  = `{"to":"b@example.com","channel":"push","content":"z"}`
)

// seq is a deterministic random source that cycles through its values.
type seq struct {
	vals []float64
	i    int
}

func (q *seq) next() float64 {
	v := q.vals[q.i%len(q.vals)]
	q.i++
	return v
}

// waits records the latency waits instead of sleeping.
type waits struct {
	mu sync.Mutex
	ds []time.Duration
}

func (w *waits) wait(_ context.Context, d time.Duration) {
	w.mu.Lock()
	w.ds = append(w.ds, d)
	w.mu.Unlock()
}

func (w *waits) got() []time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.ds)
}

func newServer(t *testing.T, opts ...Option) *httptest.Server {
	t.Helper()
	c := &clock{t: time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC)}
	ts := httptest.NewServer(New(c.now, opts...).Handler())
	t.Cleanup(ts.Close)
	return ts
}

// answer is the part of a response the tests look at.
type answer struct {
	StatusCode int
	Header     http.Header
}

func doResp(t *testing.T, method, url, body string, hdr map[string]string) (answer, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return answer{StatusCode: resp.StatusCode, Header: resp.Header}, string(b)
}

func putConfig(t *testing.T, ts *httptest.Server, body string) int {
	t.Helper()
	code, _ := do(t, http.MethodPut, ts.URL+"/admin/config", body, nil)
	return code
}

func post(t *testing.T, ts *httptest.Server, body string, hdr map[string]string) int {
	t.Helper()
	resp, _ := doResp(t, http.MethodPost, ts.URL+"/", body, hdr)
	return resp.StatusCode
}

func TestConfigShorthandAndFullForm(t *testing.T) {
	ts := newServer(t)
	code, b := do(t, http.MethodPut, ts.URL+"/admin/config", `{"status":201}`, nil)
	if code != http.StatusOK {
		t.Fatalf("shorthand: %d", code)
	}
	var cfg Config
	if err := json.Unmarshal(b, &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Status != 201 || cfg.Default.Status != 201 || cfg.Default.FailStatus != 500 {
		t.Errorf("effective config: %+v", cfg)
	}
	if got := post(t, ts, smsBody, nil); got != 201 {
		t.Errorf("answer %d", got)
	}
	// {} is a valid full form: it restores the defaults.
	if putConfig(t, ts, `{}`) != http.StatusOK || post(t, ts, smsBody, nil) != 202 {
		t.Error("empty config must restore the defaults")
	}
	full := `{"default":{"status":200,"fail_status":503,"retry_after":"7"},"channels":{"sms":{"status":204}}}`
	code, b = do(t, http.MethodPut, ts.URL+"/admin/config", full, nil)
	if code != http.StatusOK {
		t.Fatalf("full form: %d %s", code, b)
	}
	cfg = Config{}
	if err := json.Unmarshal(b, &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Status != 200 || cfg.Channels["sms"].Status != 204 || cfg.Channels["sms"].FailStatus != 500 {
		t.Errorf("effective config: %+v", cfg)
	}
}

func TestPerChannelRulesFallBackToDefault(t *testing.T) {
	ts := newServer(t)
	cfg := `{"default":{"status":200},"channels":{"sms":{"status":201},"push":{"status":429}}}`
	if putConfig(t, ts, cfg) != http.StatusOK {
		t.Fatal("config rejected")
	}
	for body, want := range map[string]int{smsBody: 201, pushBody: 429, emailBody: 200, `{"channel":"fax"}`: 200, `garbage`: 200} {
		if got := post(t, ts, body, nil); got != want {
			t.Errorf("%s: got %d want %d", body, got, want)
		}
	}
}

func TestConfigStrictness(t *testing.T) {
	ts := newServer(t)
	if putConfig(t, ts, `{"status":418}`) != http.StatusOK {
		t.Fatal("setup")
	}
	bad := []string{
		`{"statuss":200}`,
		`{"status":200,"default":{"status":200}}`,
		`{"default":{"bogus":1}}`,
		`{"channels":{"fax":{"status":200}}}`,
		`{"status":99}`, `{"status":600}`,
		`{"default":{"status":99}}`, `{"default":{"fail_status":600}}`,
		`{"default":{"fail_rate":1.5}}`, `{"default":{"fail_rate":-0.1}}`,
		`{"default":{"fail_first":-1}}`,
		`{"default":{"latency_ms":-1}}`, `{"default":{"latency_jitter_ms":-1}}`,
		`{"default":{"latency_ms":9999999999999}}`,
		`{"status":"200"}`, `{"default":{"hang":"yes"}}`, `{"default":{"fail_first":1.5}}`,
		`{"default":{"retry_after":5}}`, `{"channels":[]}`,
		`{"status":200} {"status":201}`, `nope`, ``,
	}
	for _, b := range bad {
		code, body := do(t, http.MethodPut, ts.URL+"/admin/config", b, nil)
		if code != http.StatusBadRequest {
			t.Errorf("%s: %d", b, code)
			continue
		}
		var e map[string]string
		if err := json.Unmarshal([]byte(body), &e); err != nil || e["error"] == "" {
			t.Errorf("%s: error body %q", b, body)
		}
	}
	if got := post(t, ts, smsBody, nil); got != 418 {
		t.Errorf("a rejected config changed the state: %d", got)
	}
}

func TestFailFirstPerKey(t *testing.T) {
	ts := newServer(t)
	putConfig(t, ts, `{"default":{"fail_first":2,"fail_status":503}}`)
	h := map[string]string{"Idempotency-Key": "k1"}
	var got []int
	for range 4 {
		got = append(got, post(t, ts, smsBody, h))
	}
	if want := []int{503, 503, 202, 202}; !slices.Equal(got, want) {
		t.Errorf("k1: %v want %v", got, want)
	}
	if c := post(t, ts, smsBody, map[string]string{"Idempotency-Key": "k2"}); c != 503 {
		t.Errorf("second key starts at zero: %d", c)
	}
	do(t, http.MethodPost, ts.URL+"/admin/reset", "", nil)
	putConfig(t, ts, `{"default":{"fail_first":1}}`)
	if c := post(t, ts, smsBody, h); c != 500 {
		t.Errorf("counted since the last reset: %d", c)
	}
}

func TestFailRateWithFixedRandom(t *testing.T) {
	q := &seq{vals: []float64{0.1, 0.9, 0.5, 0.49}}
	ts := newServer(t, WithRand(q.next))
	putConfig(t, ts, `{"default":{"fail_rate":0.5,"fail_status":502}}`)
	var got []int
	for range 4 {
		got = append(got, post(t, ts, smsBody, nil))
	}
	if want := []int{502, 202, 202, 502}; !slices.Equal(got, want) {
		t.Errorf("got %v want %v", got, want)
	}
	putConfig(t, ts, `{"default":{"fail_rate":1}}`)
	if c := post(t, ts, smsBody, nil); c != 500 {
		t.Errorf("rate 1: %d", c)
	}
}

func TestRetryAfterOnlyOn429And503(t *testing.T) {
	ts := newServer(t)
	for status, want := range map[int]string{429: "30", 503: "30", 500: "", 202: "", 302: ""} {
		putConfig(t, ts, `{"default":{"status":`+strconv.Itoa(status)+`,"retry_after":"30"}}`)
		resp, _ := doResp(t, http.MethodPost, ts.URL+"/", smsBody, nil)
		if got := resp.Header.Get("Retry-After"); got != want {
			t.Errorf("status %d: Retry-After %q want %q", status, got, want)
		}
	}
	// An injected failure with fail_status 503 carries it, verbatim.
	date := "Wed, 21 Oct 2026 07:28:00 GMT"
	putConfig(t, ts, `{"default":{"fail_first":1,"fail_status":503,"retry_after":"`+date+`"}}`)
	resp, _ := doResp(t, http.MethodPost, ts.URL+"/", smsBody, map[string]string{"Idempotency-Key": "x"})
	if resp.Header.Get("Retry-After") != date {
		t.Errorf("verbatim header: %q", resp.Header.Get("Retry-After"))
	}
}

func TestLocationOnlyOn3xx(t *testing.T) {
	ts := newServer(t)
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	for status, want := range map[int]string{301: "/elsewhere", 307: "/elsewhere", 200: "", 429: ""} {
		putConfig(t, ts, `{"default":{"status":`+strconv.Itoa(status)+`,"location":"/elsewhere"}}`)
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/", strings.NewReader(smsBody))
		if err != nil {
			t.Fatal(err)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if got := resp.Header.Get("Location"); got != want {
			t.Errorf("status %d: Location %q want %q", status, got, want)
		}
	}
}

func TestCloseConnection(t *testing.T) {
	ts := newServer(t)
	putConfig(t, ts, `{"channels":{"sms":{"close_connection":true}}}`)
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/", strings.NewReader(smsBody))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(req)
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("expected a transport error")
	}
	r, _ := report(t, ts)
	if r.Total != 1 || r.Channels["sms"].Statuses["closed"] != 1 {
		t.Errorf("report: %+v", r)
	}
	if c := post(t, ts, emailBody, nil); c != 202 {
		t.Errorf("email: %d", c)
	}
}

func TestHang(t *testing.T) {
	c := &clock{t: time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC)}
	h := New(c.now).Handler()
	done := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.ServeHTTP(w, r)
		if r.Method == http.MethodPost {
			close(done)
		}
	}))
	t.Cleanup(ts.Close)
	putConfig(t, ts, `{"default":{"hang":true}}`)
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ts.URL+"/", strings.NewReader(smsBody))
	if err != nil {
		t.Fatal(err)
	}
	if resp, err := http.DefaultClient.Do(req); err == nil {
		_ = resp.Body.Close()
		t.Fatal("expected the client to give up")
	}
	<-done // the handler returned
	r, _ := report(t, ts)
	if r.Total != 1 || r.Channels["sms"].Statuses["hung"] != 1 {
		t.Errorf("report: %+v", r)
	}
}

func TestMalformedBody(t *testing.T) {
	ts := newServer(t)
	putConfig(t, ts, `{"default":{"malformed_body":true}}`)
	resp, body := doResp(t, http.MethodPost, ts.URL+"/", smsBody, nil)
	if resp.StatusCode != 202 || body != "not json" {
		t.Errorf("got %d %q", resp.StatusCode, body)
	}
	putConfig(t, ts, `{"default":{"malformed_body":true,"fail_first":1}}`)
	resp, body = doResp(t, http.MethodPost, ts.URL+"/", smsBody, map[string]string{"Idempotency-Key": "m"})
	if resp.StatusCode != 500 || strings.TrimSpace(body) != "{}" {
		t.Errorf("non-2xx keeps {}: %d %q", resp.StatusCode, body)
	}
}

func TestLatencyAndJitter(t *testing.T) {
	q := &seq{vals: []float64{0, 0.5, 0.999999}}
	w := &waits{}
	ts := newServer(t, WithRand(q.next), WithWait(w.wait))
	putConfig(t, ts, `{"default":{"latency_ms":100,"latency_jitter_ms":50}}`)
	for range 3 {
		post(t, ts, smsBody, nil)
	}
	got := w.got()
	if len(got) != 3 || got[0] != 100*time.Millisecond || got[1] != 125*time.Millisecond {
		t.Fatalf("waits: %v", got)
	}
	if got[2] < 100*time.Millisecond || got[2] > 150*time.Millisecond {
		t.Errorf("upper bound: %v", got[2])
	}
	putConfig(t, ts, `{"default":{"latency_ms":40}}`)
	post(t, ts, smsBody, nil)
	if got := w.got(); got[len(got)-1] != 40*time.Millisecond {
		t.Errorf("fixed latency: %v", got)
	}
	putConfig(t, ts, `{}`)
	n := len(w.got())
	post(t, ts, smsBody, nil)
	if len(w.got()) != n {
		t.Error("no latency configured, yet the server waited")
	}
}

func TestSleepContextEndsWithContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	sleepContext(ctx, time.Hour) // returns at once
}

func TestResetRestoresConfiguration(t *testing.T) {
	ts := newServer(t)
	putConfig(t, ts, `{"default":{"status":500,"hang":true}}`)
	do(t, http.MethodPost, ts.URL+"/admin/reset", "", nil)
	if c := post(t, ts, smsBody, nil); c != 202 {
		t.Errorf("after reset: %d", c)
	}
	r, _ := report(t, ts)
	if r.Channels["sms"].Statuses["202"] != 1 || len(r.Channels["sms"].Statuses) != 1 {
		t.Errorf("statuses: %v", r.Channels["sms"].Statuses)
	}
}

func TestReportStatuses(t *testing.T) {
	ts := newServer(t)
	putConfig(t, ts, `{"default":{"fail_first":2},"channels":{"push":{"status":200,"fail_first":1}}}`)
	h := map[string]string{"Idempotency-Key": "k"}
	for range 5 {
		post(t, ts, smsBody, h)
	}
	post(t, ts, pushBody, map[string]string{"Idempotency-Key": "p"})
	r, raw := report(t, ts)
	if s := r.Channels["sms"].Statuses; s["500"] != 2 || s["202"] != 3 || len(s) != 2 {
		t.Errorf("sms statuses: %v", s)
	}
	if s := r.Channels["push"].Statuses; s["500"] != 1 || len(s) != 1 {
		t.Errorf("push statuses: %v", s)
	}
	if !strings.Contains(raw, `"statuses":{"202":3,"500":2}`) {
		t.Errorf("raw report: %s", raw)
	}
}

func TestConcurrentRequestsDuringConfigChanges(t *testing.T) {
	ts := newServer(t)
	var wg sync.WaitGroup
	cfgs := []string{
		`{"status":202}`,
		`{"default":{"fail_rate":0.5},"channels":{"sms":{"fail_first":1}}}`,
		`{"default":{"status":429,"retry_after":"1"}}`,
	}
	for i := range 4 {
		wg.Go(func() {
			for j := range 15 {
				putConfig(t, ts, cfgs[(i+j)%len(cfgs)])
			}
		})
	}
	for i := range 8 {
		wg.Go(func() {
			for range 15 {
				post(t, ts, smsBody, map[string]string{"Idempotency-Key": strconv.Itoa(i)})
				report(t, ts)
			}
		})
	}
	wg.Wait()
	if r, _ := report(t, ts); r.Total != 120 {
		t.Errorf("total %d", r.Total)
	}
}
