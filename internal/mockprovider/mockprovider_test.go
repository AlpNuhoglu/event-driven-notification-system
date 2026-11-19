package mockprovider

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func newTest(t *testing.T) (*httptest.Server, *clock) {
	t.Helper()
	c := &clock{t: time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC)}
	ts := httptest.NewServer(New(c.now).Handler())
	t.Cleanup(ts.Close)
	return ts, c
}

func do(t *testing.T, method, url, body string, hdr map[string]string) (int, []byte) {
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
	return resp.StatusCode, b
}

func report(t *testing.T, ts *httptest.Server) (Report, string) {
	t.Helper()
	code, b := do(t, http.MethodGet, ts.URL+"/admin/report", "", nil)
	if code != http.StatusOK {
		t.Fatalf("report status %d", code)
	}
	var r Report
	if err := json.Unmarshal(b, &r); err != nil {
		t.Fatal(err)
	}
	return r, string(b)
}

const smsBody = `{"to":"+905551234567","channel":"sms","content":"hello 482913"}`

func TestAcceptedAnswer(t *testing.T) {
	ts, c := newTest(t)
	code, b := do(t, http.MethodPost, ts.URL+"/some/path", smsBody, nil)
	if code != http.StatusAccepted {
		t.Fatalf("status %d", code)
	}
	var m map[string]string
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	if len(m) != 3 {
		t.Fatalf("fields: %v", m)
	}
	if _, err := uuid.Parse(m["messageId"]); err != nil {
		t.Errorf("messageId: %v", err)
	}
	if m["status"] != "accepted" {
		t.Errorf("status field %q", m["status"])
	}
	if ts, err := time.Parse(time.RFC3339, m["timestamp"]); err != nil || !ts.Equal(c.now()) {
		t.Errorf("timestamp %q: %v", m["timestamp"], err)
	}
}

func TestOnlyFieldNamesKept(t *testing.T) {
	ts, _ := newTest(t)
	do(t, http.MethodPost, ts.URL+"/", smsBody, nil)
	r, raw := report(t, ts)
	if len(r.FieldNameSets) != 1 || strings.Join(r.FieldNameSets[0], ",") != "channel,content,to" {
		t.Fatalf("sets: %v", r.FieldNameSets)
	}
	for _, s := range []string{"+905551234567", "482913", "hello"} {
		if strings.Contains(raw, s) {
			t.Errorf("report contains %q", s)
		}
	}
	do(t, http.MethodPost, ts.URL+"/", `{"to":"a@example.com","channel":"email","subject":"x","content":"y"}`, nil)
	r, _ = report(t, ts)
	if len(r.FieldNameSets) != 2 {
		t.Fatalf("sets: %v", r.FieldNameSets)
	}
}

func TestChannelsArrivalsAndRate(t *testing.T) {
	ts, c := newTest(t)
	for range 150 {
		do(t, http.MethodPost, ts.URL+"/", smsBody, nil)
		c.advance(10 * time.Millisecond)
	}
	do(t, http.MethodPost, ts.URL+"/", `{"channel":"push"}`, nil)
	do(t, http.MethodPost, ts.URL+"/", `{"channel":"fax"}`, nil)
	r, _ := report(t, ts)
	if r.Total != 152 {
		t.Errorf("total %d", r.Total)
	}
	sms := r.Channels["sms"]
	if sms.Count != 150 || sms.MaxPerSecond != 100 || len(sms.Arrivals) != 150 {
		t.Errorf("sms: count %d max %d", sms.Count, sms.MaxPerSecond)
	}
	if sms.Arrivals[0] != "2026-11-18T10:00:00.000000Z" || sms.Arrivals[1] != "2026-11-18T10:00:00.010000Z" {
		t.Errorf("arrivals: %v", sms.Arrivals[:2])
	}
	if r.Channels["push"].Count != 1 || r.Channels["unknown"].Count != 1 {
		t.Errorf("channels: %v", r.Channels)
	}
}

func TestDuplicateKeys(t *testing.T) {
	ts, _ := newTest(t)
	h := map[string]string{"Idempotency-Key": "k1"}
	do(t, http.MethodPost, ts.URL+"/", smsBody, h)
	do(t, http.MethodPost, ts.URL+"/", smsBody, h)
	do(t, http.MethodPost, ts.URL+"/", smsBody, map[string]string{"Idempotency-Key": "k2"})
	r, _ := report(t, ts)
	if r.IdempotencyKeys["k1"] != 2 || r.IdempotencyKeys["k2"] != 1 || r.DuplicateKeys != 1 {
		t.Errorf("keys: %v dup %d", r.IdempotencyKeys, r.DuplicateKeys)
	}
}

func TestConfigurableStatusAndReset(t *testing.T) {
	ts, _ := newTest(t)
	if code, _ := do(t, http.MethodPut, ts.URL+"/admin/config", `{"status":500}`, nil); code != http.StatusOK {
		t.Fatalf("config %d", code)
	}
	code, b := do(t, http.MethodPost, ts.URL+"/", smsBody, nil)
	if code != http.StatusInternalServerError || strings.TrimSpace(string(b)) != "{}" {
		t.Errorf("got %d %q", code, b)
	}
	if r, _ := report(t, ts); r.Total != 1 {
		t.Errorf("total %d", r.Total)
	}
	for _, bad := range []string{`{"status":99}`, `{"status":600}`, `nope`} {
		if code, _ := do(t, http.MethodPut, ts.URL+"/admin/config", bad, nil); code != http.StatusBadRequest {
			t.Errorf("%s: %d", bad, code)
		}
	}
	do(t, http.MethodPost, ts.URL+"/admin/reset", "", nil)
	r, _ := report(t, ts)
	if r.Total != 0 || len(r.Channels) != 0 || len(r.IdempotencyKeys) != 0 || len(r.FieldNameSets) != 0 {
		t.Errorf("not cleared: %+v", r)
	}
	if code, _ := do(t, http.MethodPost, ts.URL+"/", smsBody, nil); code != http.StatusAccepted {
		t.Errorf("status after reset %d", code)
	}
}

func TestNonJSONBody(t *testing.T) {
	ts, _ := newTest(t)
	do(t, http.MethodPost, ts.URL+"/", "not json at all", nil)
	do(t, http.MethodPost, ts.URL+"/", `[1,2]`, nil)
	r, _ := report(t, ts)
	if r.Channels["unknown"].Count != 2 || len(r.FieldNameSets) != 1 || len(r.FieldNameSets[0]) != 0 {
		t.Errorf("report: %+v", r)
	}
	if code, _ := do(t, http.MethodPost, ts.URL+"/", smsBody, nil); code != http.StatusAccepted {
		t.Errorf("server broken: %d", code)
	}
}

func TestHealth(t *testing.T) {
	ts, _ := newTest(t)
	for _, p := range []string{"/healthz", "/readyz"} {
		if code, _ := do(t, http.MethodGet, ts.URL+p, "", nil); code != http.StatusOK {
			t.Errorf("%s: %d", p, code)
		}
	}
}

func TestConcurrentRequests(t *testing.T) {
	ts, _ := newTest(t)
	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			do(t, http.MethodPost, ts.URL+"/", smsBody, map[string]string{"Idempotency-Key": "same"})
		})
	}
	wg.Wait()
	r, _ := report(t, ts)
	if r.Total != 50 || r.Channels["sms"].Count != 50 || r.IdempotencyKeys["same"] != 50 {
		t.Errorf("report: %+v", r)
	}
}
