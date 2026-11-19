package api

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
)

// testClock is a clock the tests move by hand.
type testClock struct {
	mu  sync.Mutex
	t   time.Time
	obs atomic.Int32
}

func newTestClock(t time.Time) *testClock { return &testClock{t: t} }

// Now reads the clock and counts the reading.
func (c *testClock) Now() time.Time {
	c.obs.Add(1)
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// stubSummarizer is a summarizer that counts its calls, can fail and can
// block its first statement until release is closed.
type stubSummarizer struct {
	queued, backlog, window atomic.Int32
	err                     atomic.Pointer[error]
	release                 chan struct{}
}

func (s *stubSummarizer) fail() error {
	if p := s.err.Load(); p != nil {
		return *p
	}
	return nil
}

func (s *stubSummarizer) SummaryQueued(context.Context) ([]store.LaneCount, error) {
	s.queued.Add(1)
	if s.release != nil {
		<-s.release
	}
	return nil, s.fail()
}

func (s *stubSummarizer) SummaryBacklogDue(context.Context, time.Time) ([]store.LaneCount, error) {
	s.backlog.Add(1)
	return nil, s.fail()
}

func (s *stubSummarizer) SummaryWindow(context.Context, time.Time) (map[domain.Channel]store.ChannelWindow, error) {
	s.window.Add(1)
	return nil, s.fail()
}

func newStubService(src summarizer, clock *testClock) *summaryService {
	return &summaryService{
		src: src, now: clock.Now, window: 5 * time.Minute, cacheTTL: 2 * time.Second,
		backlogTTL: 10 * time.Second, dbTimeout: 5 * time.Second, hwm: DefaultLaneHWM,
	}
}

var clockStart = time.Date(2026, 11, 18, 10, 5, 2, 0, time.UTC)

func TestSummaryService_ConcurrentRequestsOnColdCacheComputeOnce(t *testing.T) {
	const n = 8
	src := &stubSummarizer{release: make(chan struct{})}
	clock := newTestClock(clockStart)
	svc := newStubService(src, clock)

	var wg sync.WaitGroup
	errs := make(chan error, n)
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := svc.get(context.Background())
			errs <- err
		}()
	}
	// The computing request reads the clock three times (before the flight,
	// inside it, and at the start of the computation), every other request once: n+2
	// readings mean that all n requests have started while the computation
	// is blocked.
	testutil.Eventually(t, func() bool { return clock.obs.Load() >= n+2 }, 10*time.Second)
	close(src.release)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("get: %v", err)
		}
	}
	if got := src.queued.Load(); got != 1 {
		t.Errorf("the computation ran %d times for %d concurrent requests, want 1", got, n)
	}
	if got := src.window.Load(); got != 1 {
		t.Errorf("the window statements ran %d times, want 1", got)
	}
}

func TestSummaryService_ErrorIsNotCached(t *testing.T) {
	src := &stubSummarizer{}
	boom := errors.New("boom")
	src.err.Store(&boom)
	clock := newTestClock(clockStart)
	svc := newStubService(src, clock)

	if _, err := svc.get(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("first get: %v, want boom", err)
	}
	src.err.Store(nil)
	if _, err := svc.get(context.Background()); err != nil {
		t.Fatalf("second get: %v", err)
	}
	if got := src.queued.Load(); got != 2 {
		t.Errorf("computations = %d, want 2 (the failure must not be cached)", got)
	}
}

func TestSummaryService_AnswerIsReusedUntilItsTTL(t *testing.T) {
	src := &stubSummarizer{}
	clock := newTestClock(clockStart)
	svc := newStubService(src, clock)

	first, err := svc.get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	again, err := svc.get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if src.queued.Load() != 1 || !again.GeneratedAt.Equal(first.GeneratedAt) {
		t.Errorf("a request within the TTL recomputed (queued=%d, generated_at %s -> %s)",
			src.queued.Load(), first.GeneratedAt, again.GeneratedAt)
	}
	clock.Advance(2 * time.Second)
	later, err := svc.get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if src.queued.Load() != 2 || !later.GeneratedAt.After(first.GeneratedAt) {
		t.Errorf("a request after the TTL did not recompute (queued=%d, generated_at %s)",
			src.queued.Load(), later.GeneratedAt)
	}
}

func TestFormatWindow(t *testing.T) {
	for in, want := range map[time.Duration]string{
		5 * time.Minute:              "5m",
		90 * time.Second:             "1m30s",
		45 * time.Second:             "45s",
		time.Hour:                    "1h",
		2*time.Hour + 30*time.Minute: "2h30m",
		time.Hour + 5*time.Minute + 3*time.Second: "1h5m3s",
	} {
		if got := formatWindow(in); got != want {
			t.Errorf("formatWindow(%s) = %q, want %q", in, got, want)
		}
	}
}

func TestChannelSummary_SuccessRateIgnoresAborted(t *testing.T) {
	w := store.ChannelWindow{Outcomes: map[domain.Outcome]int64{
		domain.OutcomeSuccess: 3, domain.OutcomePermanent: 1, domain.OutcomeAborted: 96,
	}}
	got := channelSummary(domain.ChannelSMS, w)
	if got.SuccessRate == nil || *got.SuccessRate != 0.75 {
		t.Errorf("success_rate = %v, want 0.75", got.SuccessRate)
	}
	only := channelSummary(domain.ChannelSMS, store.ChannelWindow{Outcomes: map[domain.Outcome]int64{domain.OutcomeAborted: 5}})
	if only.SuccessRate != nil {
		t.Errorf("success_rate with only aborted attempts = %v, want nil", *only.SuccessRate)
	}
}
