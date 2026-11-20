//go:build e2e

package e2e

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/ratewindow"
)

const (
	// defaultLoadSmokeCount is about 67 seconds of sending at 90 msg/s.
	defaultLoadSmokeCount = 6000
	// pacingRate is the default RATE_LIMIT_PER_SECOND of the stack; it only
	// sizes the drain deadline.
	pacingRate = 90
	// rateLimit is the limit no one-second window may exceed (D24).
	rateLimit = 100
	// reportPollInterval is how often mockprovider's report is read during
	// the drain. The report is built under the mock's lock, so it is not
	// read more often than needed.
	reportPollInterval = time.Second
	// progressInterval is how often the drain logs its progress.
	progressInterval = 10 * time.Second
	// arrivalLayout is the layout of mockprovider's arrival timestamps.
	arrivalLayout = "2006-01-02T15:04:05.000000Z"
)

// TestLoadSmoke (D51) sends about a minute of SMS at the full rate through
// the stack and checks at the provider that no half-open one-second window
// holds more than 100 arrivals. It logs one line with the measured margin:
//
//	load-smoke: 6000 sms in 66.7 s (90.0/s); largest one-second window: 91 (limit 100)
//
// and appends it to $GITHUB_STEP_SUMMARY when that is set.
func TestLoadSmoke(t *testing.T) {
	count := defaultLoadSmokeCount
	if v := os.Getenv("LOAD_SMOKE_COUNT"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 2 {
			t.Fatalf("LOAD_SMOKE_COUNT=%q: want an integer of at least 2", v)
		}
		count = n
	}
	s := newStack(t)
	s.resetMock(t)

	ids := createSMS(t, s, count)

	// The drain takes count/rate seconds; the deadline allows twice that
	// plus a minute for the start of the stack's pipeline.
	drainTimeout := 2*time.Duration(count)*time.Second/pacingRate + time.Minute
	waitStart := time.Now()
	lastLog := waitStart
	received := 0
	var lastErr error
	ok := poll(drainTimeout, reportPollInterval, func() bool {
		rep, err := s.report()
		if err != nil {
			lastErr = err
			return false
		}
		received = rep.Channels["sms"].Count
		if time.Since(lastLog) >= progressInterval {
			lastLog = time.Now()
			t.Logf("load-smoke: %d of %d received after %.0f s", received, count, time.Since(waitStart).Seconds())
		}
		return received >= count
	})
	if !ok {
		t.Fatalf("mockprovider received %d of %d sms requests within %s (last report error: %v)",
			received, count, drainTimeout, lastErr)
	}

	rep, err := s.report()
	if err != nil {
		t.Fatal(err)
	}
	sms := rep.Channels["sms"]
	arrivals := make([]time.Time, len(sms.Arrivals))
	for i, a := range sms.Arrivals {
		ts, err := time.Parse(arrivalLayout, a)
		if err != nil {
			t.Fatalf("arrival %d: %v", i, err)
		}
		arrivals[i] = ts
	}
	// The report lists the arrivals sorted.
	span := arrivals[len(arrivals)-1].Sub(arrivals[0])
	rate := 0.0
	if span > 0 {
		rate = float64(len(arrivals)-1) / span.Seconds()
	}
	line := fmt.Sprintf("load-smoke: %d sms in %.1f s (%.1f/s); largest one-second window: %d (limit %d)",
		sms.Count, span.Seconds(), rate, sms.MaxPerSecond, rateLimit)
	t.Log(line)
	appendStepSummary(t, line)

	// The pass criterion (D24, D51).
	if sms.MaxPerSecond > rateLimit {
		t.Errorf("a one-second window at the provider holds %d sms arrivals, more than %d", sms.MaxPerSecond, rateLimit)
	}

	// Cross-check the mock's own figure. The report carries the arrivals
	// truncated to microseconds, so a pair the mock saw just under one second
	// apart can appear exactly one second apart, and the reverse. The true
	// maximum therefore lies between the maxima over windows one microsecond
	// shorter and one microsecond longer; it equals the one-second figure
	// computed here except in that rare boundary case.
	computed := ratewindow.MaxInWindow(arrivals, time.Second)
	lo := ratewindow.MaxInWindow(arrivals, time.Second-time.Microsecond)
	hi := ratewindow.MaxInWindow(arrivals, time.Second+time.Microsecond)
	if sms.MaxPerSecond < lo || sms.MaxPerSecond > hi {
		t.Errorf("mockprovider's max_per_second %d disagrees with the window computed from its arrivals: %d (bounds %d..%d)",
			sms.MaxPerSecond, computed, lo, hi)
	}
	if computed != sms.MaxPerSecond {
		t.Logf("load-smoke: window computed from the microsecond arrivals %d, mockprovider %d (boundary case, within %d..%d)",
			computed, sms.MaxPerSecond, lo, hi)
	}
	if computed > rateLimit {
		t.Errorf("the window computed from the arrivals holds %d sms, more than %d", computed, rateLimit)
	}

	// Nothing lost, nothing duplicated, nothing else sent.
	if sms.Count != count || rep.Total != count {
		t.Errorf("mockprovider received %d sms of %d requests in total, want %d", sms.Count, rep.Total, count)
	}
	if rep.DuplicateKeys != 0 {
		t.Errorf("duplicate_keys %d, want 0", rep.DuplicateKeys)
	}
	missing, repeated := 0, 0
	for _, id := range ids {
		switch n := rep.IdempotencyKeys[id]; {
		case n == 0:
			missing++
		case n > 1:
			repeated++
		}
	}
	if missing != 0 || repeated != 0 {
		t.Errorf("of %d notifications, %d never reached the provider and %d reached it more than once", count, missing, repeated)
	}

	// Every notification is sent in the API: a sample, not 6000 reads. The
	// final write follows the provider's answer, so each is polled briefly.
	for _, i := range sampleIndexes(count, 10) {
		got := s.waitSent(t, ids[i], sendTimeout)
		if got.AttemptCount != 1 {
			t.Errorf("notification %s: attempt_count %d, want 1", ids[i], got.AttemptCount)
		}
	}
}

// createSMS creates count SMS notifications of priority normal with a few
// concurrent clients and returns their ids in creation order.
func createSMS(t *testing.T, s *stack, count int) []string {
	t.Helper()
	return createN(t, s, createRequest{Channel: "sms", Recipient: fakePhone, Content: "load-smoke", Priority: "normal"}, count, "load-smoke")
}

// sampleIndexes returns up to n indexes spread evenly over [0, count),
// including the first and the last.
func sampleIndexes(count, n int) []int {
	if count <= n {
		out := make([]int, count)
		for i := range out {
			out[i] = i
		}
		return out
	}
	out := make([]int, n)
	for i := range out {
		out[i] = i * (count - 1) / (n - 1)
	}
	return out
}

// appendStepSummary appends line to the GitHub Actions step summary when the
// test runs in a workflow.
func appendStepSummary(t *testing.T, line string) {
	t.Helper()
	path := os.Getenv("GITHUB_STEP_SUMMARY")
	if path == "" {
		return
	}
	//nolint:gosec // the path is the step summary file named by the Actions runner
	f, err := os.OpenFile(filepath.Clean(path), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		t.Errorf("open step summary: %v", err)
		return
	}
	if _, err := fmt.Fprintf(f, "%s\n", line); err != nil {
		t.Errorf("write step summary: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Errorf("close step summary: %v", err)
	}
}
