//go:build integration

package api

import (
	"context"
	"encoding/json"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/api/apigen"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

const summaryPath = "/v1/metrics/summary"

// countingStore counts the statements of the summary that reach Postgres.
type countingStore struct {
	*store.Store
	queued, backlog, window atomic.Int32
}

func (c *countingStore) SummaryQueued(ctx context.Context) ([]store.LaneCount, error) {
	c.queued.Add(1)
	return c.Store.SummaryQueued(ctx)
}

func (c *countingStore) SummaryBacklogDue(ctx context.Context, now time.Time) ([]store.LaneCount, error) {
	c.backlog.Add(1)
	return c.Store.SummaryBacklogDue(ctx, now)
}

func (c *countingStore) SummaryWindow(ctx context.Context, since time.Time) (map[domain.Channel]store.ChannelWindow, error) {
	c.window.Add(1)
	return c.Store.SummaryWindow(ctx, since)
}

type summaryEnv struct {
	*testAPI
	counts *countingStore
	clock  *testClock
}

func newSummaryEnv(t *testing.T) *summaryEnv {
	t.Helper()
	pool := testutil.NewPool(t, store.OpenPool)
	counts := &countingStore{Store: store.New(pool, store.ProfilesFrom(testRetry()))}
	clock := newTestClock(clockStart)
	api := newTestAPIOn(t, pool, &syncBuffer{}, func(o *Options) {
		o.Repository = counts
		o.Now = clock.Now
		o.SummaryWindow = 5 * time.Minute
		o.SummaryCacheTTL = 2 * time.Second
		o.SummaryBacklogTTL = 10 * time.Second
		o.LaneHWM = map[domain.Priority]int{domain.PriorityHigh: 11, domain.PriorityNormal: 22, domain.PriorityLow: 33}
	})
	return &summaryEnv{testAPI: api, counts: counts, clock: clock}
}

func (e *summaryEnv) summary(t *testing.T) apigen.MetricsSummary {
	t.Helper()
	res := e.get(summaryPath)
	if res.status != 200 {
		t.Fatalf("GET %s: %d %s", summaryPath, res.status, res.body)
	}
	var s apigen.MetricsSummary
	if err := json.Unmarshal(res.body, &s); err != nil {
		t.Fatalf("decode %s: %v", res.body, err)
	}
	return s
}

// note describes a notification row to insert.
type note struct {
	channel   domain.Channel
	priority  domain.Priority
	status    string
	due       time.Time
	created   time.Time
	scheduled *time.Time
	sent      *time.Time
}

func insertNote(t *testing.T, pool *pgxpool.Pool, n note) uuid.UUID {
	t.Helper()
	id := uuid.Must(uuid.NewV7())
	if n.due.IsZero() {
		n.due = n.created
	}
	reason := (*string)(nil)
	if n.status == "failed" {
		r := "max_failures"
		reason = &r
	}
	_, err := pool.Exec(context.Background(), `
INSERT INTO notifications (id, channel, priority, recipient, content, status, failure_reason, due_at, scheduled_at,
                           expires_at, max_failures, correlation_id, created_at, updated_at, sent_at)
VALUES ($1, $2, $3, 'user@example.com', 'hello', $4, $5, $6, $7, $8, 5, 'corr', $9, $9, $10)`,
		id, string(n.channel), int16(n.priority), n.status, reason, n.due, n.scheduled,
		clockStart.Add(24*time.Hour), n.created, n.sent)
	if err != nil {
		t.Fatalf("insert notification: %v", err)
	}
	return id
}

// attemptRow describes an attempt row to insert; a nil finished is an
// in-flight attempt.
type attemptRow struct {
	id       uuid.UUID
	no       int
	channel  domain.Channel
	outcome  domain.Outcome
	latency  *int32
	finished *time.Time
}

func insertAttempt(t *testing.T, pool *pgxpool.Pool, a attemptRow) {
	t.Helper()
	started := clockStart
	if a.finished != nil {
		started = a.finished.Add(-time.Second)
	}
	_, err := pool.Exec(context.Background(), `
INSERT INTO notification_attempts (notification_id, attempt_no, channel, provider, outcome, latency_ms, started_at, finished_at)
VALUES ($1, $2, $3, 'primary', $4, $5, $6, $7)`,
		a.id, a.no, string(a.channel), string(a.outcome), a.latency, started, a.finished)
	if err != nil {
		t.Fatalf("insert attempt: %v", err)
	}
}

func i32(v int32) *int32 { return &v }

func at(offset time.Duration) *time.Time {
	v := clockStart.Add(offset)
	return &v
}

func lane(t *testing.T, s apigen.MetricsSummary, c domain.Channel, p domain.Priority) apigen.LaneSummary {
	t.Helper()
	for _, l := range s.Lanes {
		if string(l.Channel) == string(c) && string(l.Priority) == p.String() {
			return l
		}
	}
	t.Fatalf("no lane %s/%s", c, p)
	return apigen.LaneSummary{}
}

func channel(t *testing.T, s apigen.MetricsSummary, c domain.Channel) apigen.ChannelSummary {
	t.Helper()
	for _, ch := range s.Channels {
		if string(ch.Channel) == string(c) {
			return ch
		}
	}
	t.Fatalf("no channel %s", c)
	return apigen.ChannelSummary{}
}

func wantPercentiles(t *testing.T, what string, got apigen.Percentiles, p50, p95 float64) {
	t.Helper()
	if got.P50 == nil || got.P95 == nil || math.Abs(*got.P50-p50) > 1e-6 || math.Abs(*got.P95-p95) > 1e-6 {
		t.Errorf("%s = %s, want p50 %v p95 %v", what, fmtPercentiles(got), p50, p95)
	}
}

func fmtPercentiles(p apigen.Percentiles) string {
	b, _ := json.Marshal(p)
	return string(b)
}

func TestSummary_ValuesOnAKnownDataset(t *testing.T) {
	e := newSummaryEnv(t)
	pool := e.pool
	old := clockStart.Add(-time.Hour)

	// Lanes. sms/high: 3 queued; due backlog 2 pending + 1 retrying (a
	// pending row due in the future and a processing row do not count).
	for range 3 {
		insertNote(t, pool, note{channel: domain.ChannelSMS, priority: domain.PriorityHigh, status: "queued", created: old})
	}
	for range 2 {
		insertNote(t, pool, note{channel: domain.ChannelSMS, priority: domain.PriorityHigh, status: "pending", due: clockStart.Add(-time.Minute), created: old})
	}
	insertNote(t, pool, note{channel: domain.ChannelSMS, priority: domain.PriorityHigh, status: "retrying", due: clockStart, created: old})
	for range 4 {
		insertNote(t, pool, note{channel: domain.ChannelSMS, priority: domain.PriorityHigh, status: "pending", due: clockStart.Add(time.Minute), created: old})
	}
	insertNote(t, pool, note{channel: domain.ChannelSMS, priority: domain.PriorityHigh, status: "processing", created: old})
	insertNote(t, pool, note{channel: domain.ChannelSMS, priority: domain.PriorityLow, status: "queued", created: old})
	for range 5 {
		insertNote(t, pool, note{channel: domain.ChannelEmail, priority: domain.PriorityNormal, status: "pending", due: clockStart.Add(-time.Hour), created: old})
	}

	// sms attempts finished in the window: every outcome with its own count.
	finished := at(-time.Minute)
	other := insertNote(t, pool, note{channel: domain.ChannelSMS, priority: domain.PriorityNormal, status: "failed", created: old})
	counts := []struct {
		outcome domain.Outcome
		n       int
	}{
		{domain.OutcomeRetryable, 2},
		{domain.OutcomePermanent, 3},
		{domain.OutcomeProviderFatal, 4},
		{domain.OutcomeThrottled, 5},
		{domain.OutcomeUnknown, 6},
		{domain.OutcomeUnreachable, 8},
		{domain.OutcomeAborted, 9},
		{domain.OutcomeInternalError, 10},
	}
	no := 0
	for _, c := range counts {
		for range c.n {
			no++
			a := attemptRow{id: other, no: no, channel: domain.ChannelSMS, outcome: c.outcome, finished: finished}
			if no == 1 {
				a.latency = i32(1000) // a failed attempt's latency counts too
			}
			insertAttempt(t, pool, a)
		}
	}
	// Seven successes, each on its own notification: provider latencies
	// 100..700 ms; end-to-end 1000..6000 ms from created_at, and 500 ms
	// from scheduled_at for the seventh, which was created an hour earlier.
	for i := 1; i <= 7; i++ {
		sent := clockStart.Add(-time.Minute)
		n := note{channel: domain.ChannelSMS, priority: domain.PriorityNormal, status: "sent", created: sent.Add(-time.Duration(i) * time.Second), sent: &sent}
		if i == 7 {
			sched := sent.Add(-500 * time.Millisecond)
			n.created, n.scheduled = sent.Add(-time.Hour), &sched
		}
		id := insertNote(t, pool, n)
		insertAttempt(t, pool, attemptRow{
			id: id, no: 1, channel: domain.ChannelSMS, outcome: domain.OutcomeSuccess,
			latency: i32(int32(i * 100)), finished: finished,
		})
	}
	// Outside the window (finished before it starts) or not finished: not counted.
	sentOld := clockStart.Add(-6 * time.Minute)
	oldID := insertNote(t, pool, note{channel: domain.ChannelSMS, priority: domain.PriorityNormal, status: "sent", created: sentOld.Add(-90 * time.Second), sent: &sentOld})
	insertAttempt(t, pool, attemptRow{
		id: oldID, no: 1, channel: domain.ChannelSMS, outcome: domain.OutcomeSuccess,
		latency: i32(99999), finished: &sentOld,
	})
	justBefore := clockStart.Add(-5*time.Minute - time.Second)
	insertAttempt(t, pool, attemptRow{id: other, no: 100, channel: domain.ChannelSMS, outcome: domain.OutcomePermanent, finished: &justBefore})
	insertAttempt(t, pool, attemptRow{id: other, no: 101, channel: domain.ChannelSMS, outcome: domain.OutcomeInFlight})

	s := e.summary(t)

	if s.Window != "5m" {
		t.Errorf("window = %q, want 5m", s.Window)
	}
	if !s.GeneratedAt.Equal(clockStart) {
		t.Errorf("generated_at = %s, want %s", s.GeneratedAt, clockStart)
	}
	if len(s.Lanes) != 9 || len(s.Channels) != 3 {
		t.Fatalf("lanes %d channels %d, want 9 and 3", len(s.Lanes), len(s.Channels))
	}
	i := 0
	for _, c := range domain.Channels() {
		for _, p := range domain.Priorities() {
			l := s.Lanes[i]
			if string(l.Channel) != string(c) || string(l.Priority) != p.String() {
				t.Errorf("lane %d is %s/%s, want %s/%s", i, l.Channel, l.Priority, c, p)
			}
			i++
		}
	}
	for i, c := range domain.Channels() {
		if string(s.Channels[i].Channel) != string(c) {
			t.Errorf("channel %d is %s, want %s", i, s.Channels[i].Channel, c)
		}
	}
	wantLane := func(c domain.Channel, p domain.Priority, queued, hwm, due int64) {
		t.Helper()
		l := lane(t, s, c, p)
		if l.Queued != queued || int64(l.Hwm) != hwm || l.BacklogDue != due {
			t.Errorf("lane %s/%s = queued %d hwm %d backlog_due %d, want %d %d %d",
				c, p, l.Queued, l.Hwm, l.BacklogDue, queued, hwm, due)
		}
	}
	wantLane(domain.ChannelSMS, domain.PriorityHigh, 3, 11, 3)
	wantLane(domain.ChannelSMS, domain.PriorityNormal, 0, 22, 0)
	wantLane(domain.ChannelSMS, domain.PriorityLow, 1, 33, 0)
	wantLane(domain.ChannelEmail, domain.PriorityNormal, 0, 22, 5)
	wantLane(domain.ChannelPush, domain.PriorityHigh, 0, 11, 0)

	sms := channel(t, s, domain.ChannelSMS)
	if sms.Attempts != (apigen.AttemptCounts{
		Success: 7, Retryable: 2, Permanent: 3, ProviderFatal: 4, Throttled: 5, Unknown: 6,
		Unreachable: 8, Aborted: 9, InternalError: 10,
	}) {
		t.Errorf("sms attempts = %+v", sms.Attempts)
	}
	// success / (all finished except aborted) = 7 / (7+2+3+4+5+6+8+10).
	if sms.SuccessRate == nil || *sms.SuccessRate != 7.0/45.0 {
		t.Errorf("success_rate = %v, want %v", sms.SuccessRate, 7.0/45.0)
	}
	// Latencies 100..700 and 1000: percentile_cont(0.5) is at position 3.5
	// of 0..7 (400 and 500 -> 450), percentile_cont(0.95) at 6.65
	// (700 + 0.65 * (1000 - 700) = 895).
	wantPercentiles(t, "provider_latency_ms", sms.ProviderLatencyMs, 450, 895)
	// End-to-end 500, 1000, 2000, 3000, 4000, 5000, 6000 ms: position 3
	// (3000) and position 5.7 (5000 + 0.7 * 1000 = 5700).
	wantPercentiles(t, "end_to_end_latency_ms", sms.EndToEndLatencyMs, 3000, 5700)
}

func TestSummary_NullsWhenThereIsNothingToMeasure(t *testing.T) {
	e := newSummaryEnv(t)
	other := insertNote(t, e.pool, note{channel: domain.ChannelEmail, priority: domain.PriorityNormal, status: "failed", created: clockStart.Add(-time.Hour)})
	for no := 1; no <= 3; no++ {
		insertAttempt(t, e.pool, attemptRow{id: other, no: no, channel: domain.ChannelEmail, outcome: domain.OutcomeAborted, finished: at(-time.Second)})
	}

	s := e.summary(t)
	for _, c := range domain.Channels() {
		ch := channel(t, s, c)
		if ch.SuccessRate != nil {
			t.Errorf("%s: success_rate = %v, want null", c, *ch.SuccessRate)
		}
		if ch.ProviderLatencyMs.P50 != nil || ch.ProviderLatencyMs.P95 != nil ||
			ch.EndToEndLatencyMs.P50 != nil || ch.EndToEndLatencyMs.P95 != nil {
			t.Errorf("%s: latencies are not null: %+v %+v", c, ch.ProviderLatencyMs, ch.EndToEndLatencyMs)
		}
	}
	if got := channel(t, s, domain.ChannelEmail).Attempts.Aborted; got != 3 {
		t.Errorf("email aborted = %d, want 3", got)
	}
	// Every outcome is present on the wire, also when zero.
	var raw struct {
		Channels []struct {
			Attempts map[string]int64 `json:"attempts"`
		} `json:"channels"`
	}
	if err := json.Unmarshal(e.get(summaryPath).body, &raw); err != nil {
		t.Fatal(err)
	}
	for _, o := range []string{"success", "retryable", "permanent", "provider_fatal", "throttled", "unknown", "unreachable", "aborted", "internal_error"} {
		if _, ok := raw.Channels[1].Attempts[o]; !ok {
			t.Errorf("outcome %s is missing from the attempts", o)
		}
	}
}

func TestSummary_BacklogDueIsCachedSeparatelyFromTheAnswer(t *testing.T) {
	e := newSummaryEnv(t)
	pool := e.pool
	old := clockStart.Add(-time.Hour)
	due := func() {
		insertNote(t, pool, note{channel: domain.ChannelSMS, priority: domain.PriorityHigh, status: "pending", due: clockStart.Add(-time.Minute), created: old})
	}
	queued := func() {
		insertNote(t, pool, note{channel: domain.ChannelSMS, priority: domain.PriorityHigh, status: "queued", created: old})
	}
	want := func(step string, gotQueued, gotDue int64, wantQueued, wantDue int64) {
		t.Helper()
		if gotQueued != wantQueued || gotDue != wantDue {
			t.Errorf("%s: queued %d backlog_due %d, want %d and %d", step, gotQueued, gotDue, wantQueued, wantDue)
		}
	}
	calls := func(queued, backlog int32) {
		t.Helper()
		if q, b := e.counts.queued.Load(), e.counts.backlog.Load(); q != queued || b != backlog {
			t.Errorf("statements run: queued %d backlog %d, want %d and %d", q, b, queued, backlog)
		}
	}

	due()
	s := e.summary(t)
	l := lane(t, s, domain.ChannelSMS, domain.PriorityHigh)
	want("first", l.Queued, l.BacklogDue, 0, 1)
	calls(1, 1)

	// Within the answer's lifetime nothing runs, whatever changed.
	due()
	queued()
	e.clock.Advance(time.Second)
	again := e.summary(t)
	if !again.GeneratedAt.Equal(s.GeneratedAt) {
		t.Errorf("a cached answer changed generated_at: %s -> %s", s.GeneratedAt, again.GeneratedAt)
	}
	calls(1, 1)

	// The answer has expired (3 s) but the backlog (10 s) has not: the new
	// queued row is seen, the new due row is not.
	e.clock.Advance(2 * time.Second)
	l = lane(t, e.summary(t), domain.ChannelSMS, domain.PriorityHigh)
	want("after 3s", l.Queued, l.BacklogDue, 1, 1)
	calls(2, 1)

	// 11 s after the backlog was counted it is counted again.
	e.clock.Advance(8 * time.Second)
	l = lane(t, e.summary(t), domain.ChannelSMS, domain.PriorityHigh)
	want("after 11s", l.Queued, l.BacklogDue, 1, 2)
	calls(3, 2)
}
