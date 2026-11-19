//go:build integration

package api

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/metrics"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

// newMeasuredTestAPI is newTestAPI with the api's metrics on a registry of
// the test's own.
func newMeasuredTestAPI(t *testing.T) (*testAPI, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	m, err := NewMetrics(reg)
	if err != nil {
		t.Fatal(err)
	}
	a := newTestAPIOn(t, testutil.NewPool(t, store.OpenPool), &syncBuffer{}, func(o *Options) { o.Metrics = m })
	return a, reg
}

// A batch counts each stored item with its channel and priority and source
// batch, and each item it rejected (in a 202 and in a 422 that rejects them
// all) with the channel the item names and its reason; a dedupe_key held by
// a stored notification rejects a single create as a duplicate; a replay is
// counted as a replay and stores nothing; a cancel is a final transition of
// the api.
func TestMetrics_CreateBatchReplayAndCancel(t *testing.T) {
	a, reg := newMeasuredTestAPI(t)
	created := func(channel, priority, source string) float64 {
		return testutil.MetricValue(t, reg, CreatedMetric, testutil.Labels("channel", channel, "priority", priority, "source", source))
	}
	rejected := func(channel, reason string) float64 {
		return testutil.MetricValue(t, reg, RejectedMetric, rejectedLabels(channel, reason))
	}

	// A batch with a stored item, an invalid one and a duplicate within the
	// batch.
	r := a.postKey(pathBatch, batchBody(
		item(`,"dedupe_key":"k1","priority":"low"`),
		`{"channel":"email","recipient":"not-an-address","content":"x"}`,
		`{"channel":"push","recipient":"device-token","content":"x","dedupe_key":"k1"}`,
	), "", "")
	if r.status != http.StatusAccepted {
		t.Fatalf("batch: %d %s", r.status, r.body)
	}
	if v := created("sms", "low", sourceBatch); v != 1 {
		t.Errorf("created sms/low/batch = %v, want 1", v)
	}
	if v := rejected("email", reasonInvalid); v != 1 {
		t.Errorf("rejected email/invalid = %v, want 1", v)
	}
	if v := rejected("push", reasonDuplicate); v != 1 {
		t.Errorf("rejected push/duplicate = %v, want 1", v)
	}

	// A batch whose items are all rejected: one per item.
	if r := a.postKey(pathBatch, batchBody(faxItem, `{"channel":"email","recipient":"x","content":""}`), "", ""); r.status != http.StatusUnprocessableEntity {
		t.Fatalf("all rejected: %d %s", r.status, r.body)
	}
	if v := rejected(metrics.Unknown, reasonInvalid); v != 1 {
		t.Errorf("rejected unknown/invalid = %v, want 1 (the fax item)", v)
	}
	if v := rejected("email", reasonInvalid); v != 2 {
		t.Errorf("rejected email/invalid = %v, want 2", v)
	}

	// A single create whose dedupe_key a stored notification holds.
	if r := a.post(item(`,"dedupe_key":"k1"`)); r.status != http.StatusConflict {
		t.Fatalf("duplicate: %d %s", r.status, r.body)
	}
	if v := rejected("sms", reasonDuplicate); v != 1 {
		t.Errorf("rejected sms/duplicate = %v, want 1", v)
	}

	// A create and its retry with the same Idempotency-Key.
	for range 2 {
		if r := a.postKey(pathCreate, item(`,"priority":"high"`), "metrics-key", ""); r.status != http.StatusAccepted {
			t.Fatalf("create with a key: %d %s", r.status, r.body)
		}
	}
	if v := created("sms", "high", sourceSingle); v != 1 {
		t.Errorf("created sms/high/single = %v, want 1 (the retry is a replay)", v)
	}
	if v := testutil.MetricValue(t, reg, ReplaysMetric, nil); v != 1 {
		t.Errorf("replays = %v, want 1", v)
	}

	// Cancelling the stored notification is a final transition.
	r = a.post(item(`,"priority":"normal"`))
	var n struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(r.body, &n); err != nil || r.status != http.StatusAccepted {
		t.Fatalf("create: %d %s", r.status, r.body)
	}
	if r := a.do(http.MethodPost, "/v1/notifications/"+n.ID+"/cancel", "", nil); r.status != http.StatusOK {
		t.Fatalf("cancel: %d %s", r.status, r.body)
	}
	if v := testutil.MetricValue(t, reg, metrics.FinishedMetric, testutil.Labels("channel", "sms", "priority", "normal", "status", "cancelled")); v != 1 {
		t.Errorf("finished sms/normal/cancelled = %v, want 1", v)
	}
	if v := testutil.MetricValue(t, reg, RequestsMetric, requestLabels("/v1/notifications/{id}/cancel", http.MethodPost, "200")); v != 1 {
		t.Errorf("cancel requests = %v, want 1", v)
	}
}
