//go:build e2e

package e2e

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

const (
	// perChannelLayoutFile is the Compose override of the per-channel worker
	// layout, at the repository root.
	perChannelLayoutFile = "docker-compose.override.worker-per-channel.yml"
	// round1Count is the number of notifications per channel of round 1:
	// about three seconds of sending at 98 per second, so that the largest
	// one-second window at the mock shows the pacing of each channel.
	round1Count = 300
	// round2Count is the number of notifications per channel of round 2.
	round2Count = 30
	// attemptsMetric is the worker's counter of delivery attempts by
	// channel, provider and outcome.
	attemptsMetric = "delivery_attempts_total"
)

// channelWorkers are the services of the per-channel layout, by channel.
var channelWorkers = map[domain.Channel]string{
	domain.ChannelSMS:   "worker-sms",
	domain.ChannelEmail: "worker-email",
	domain.ChannelPush:  "worker-push",
}

// recipientOf returns a reserved fake recipient for a channel.
func recipientOf(c domain.Channel) string {
	switch c {
	case domain.ChannelEmail:
		return fakeEmail
	case domain.ChannelPush:
		return fakeDevice
	default:
		return fakePhone
	}
}

// The chaos scenario "Per-channel worker deployment" (ADR-0010 §1, ADR-0005):
// it runs on the layout of docker-compose.override.worker-per-channel.yml,
// in the second pass of make test-e2e, and is skipped on the default layout.
// The layout is checked first: no single worker, one worker per channel that
// serves only its channel with WORKER_REPLICAS 1 (the fallback limiter's
// divisor counts the workers of one channel), and Prometheus scrapes the
// three as the job "worker". Round 1: 300 notifications on each channel end
// sent, each channel's requests are made by its own worker (its
// delivery_attempts_total) and no channel has more than 100 requests in a
// one-second window at the mock: the rate limit holds per channel, as the
// limiter's key is per channel. Round 2 shows the failure the layout is
// for: the email worker is killed (a crash of one process); sms and push
// keep sending and end sent while every email waits, unsent; once the email
// worker is started again, the emails are sent by it.
func TestComposeE2E_WorkerPerChannel(t *testing.T) {
	if filepath.Base(os.Getenv(layoutFileVar)) != perChannelLayoutFile {
		t.Skipf("runs on the per-channel worker layout only (%s=%s): the second pass of make test-e2e", layoutFileVar, perChannelLayoutFile)
	}
	const label = "worker per channel"
	s := newStack(t)
	s.resetMock(t)
	t.Cleanup(func() { s.leaveClean(t) })
	var ids []string
	s.reportChaosAtEnd(t, label, &ids)

	checkPerChannelLayout(t, s)

	// Round 1: every channel sends, each from its own worker.
	before := attemptsByWorker(t)
	round1 := map[domain.Channel][]string{}
	for _, c := range channels {
		round1[c] = createN(t, s, createRequest{Channel: string(c), Recipient: recipientOf(c), Content: label, Priority: "high"}, round1Count, label)
		ids = append(ids, round1[c]...)
	}
	waitAllSent(t, ids, drainTimeout)
	checkOwnAttempts(t, label+": round 1", before, attemptsByWorker(t), round1Count)
	rep, err := s.report()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range channels {
		cr := rep.Channels[string(c)]
		t.Logf("%s: round 1: %s: %d requests at the mock, largest one-second window %d (limit %d)", label, c, cr.Count, cr.MaxPerSecond, rateLimit)
		if cr.Count != round1Count {
			t.Errorf("%s: round 1: the mock received %d %s requests, want %d", label, cr.Count, c, round1Count)
		}
		if cr.MaxPerSecond > rateLimit {
			t.Errorf("%s: round 1: a one-second window holds %d %s requests, more than %d", label, cr.MaxPerSecond, c, rateLimit)
		}
	}

	// Round 2: the email worker crashes; sms and push keep sending.
	const crashed = domain.ChannelEmail
	crashedWorker := channelWorkers[crashed]
	killed := false
	t.Cleanup(func() {
		if killed {
			if _, err := compose("start", crashedWorker); err != nil {
				t.Errorf("cleanup: start %s: %v", crashedWorker, err)
			}
		}
	})
	crashedID, err := compose("ps", "-q", crashedWorker)
	if err != nil {
		t.Fatal(err)
	}
	killed = true
	if _, err := compose("kill", crashedWorker); err != nil { // SIGKILL by default
		t.Fatal(err)
	}
	killedAt := time.Now()
	if out, err := docker("inspect", "--format", "{{.State.Status}} {{.State.ExitCode}}", strings.TrimSpace(crashedID)); err != nil {
		t.Fatal(err)
	} else if got := strings.TrimSpace(out); got != fmt.Sprintf("exited %d", sigkillExitCode) {
		t.Fatalf("%s: after docker-compose kill, %s is %q, want exited %d", label, crashedWorker, got, sigkillExitCode)
	}
	before = attemptsByWorker(t, crashedWorker)
	round2 := map[domain.Channel][]string{}
	var others []string
	for _, c := range channels {
		round2[c] = createN(t, s, createRequest{Channel: string(c), Recipient: recipientOf(c), Content: label, Priority: "high"}, round2Count, label)
		ids = append(ids, round2[c]...)
		if c != crashed {
			others = append(others, round2[c]...)
		}
	}
	waitAllSent(t, others, sendTimeout)
	t.Logf("%s: round 2: with %s killed, every sms and push sent %.1f s after the kill", label, crashedWorker, time.Since(killedAt).Seconds())
	c, err := statusCounts(round2[crashed])
	if err != nil {
		t.Fatal(err)
	}
	if c["sent"] != 0 || c["failed"] != 0 {
		t.Errorf("%s: round 2: with %s killed, its notifications are %s; want none ended", label, crashedWorker, formatCounts(c))
	}
	if rep, err = s.report(); err != nil {
		t.Fatal(err)
	}
	if got := rep.Channels[string(crashed)].Count; got != round1Count {
		t.Errorf("%s: round 2: the mock received %d %s requests with %s killed, want the %d of round 1 only", label, got, crashed, crashedWorker, round1Count)
	}
	t.Logf("%s: round 2: %s while %s was down", label, formatCounts(c), crashedWorker)

	if _, err := compose("start", crashedWorker); err != nil {
		t.Fatal(err)
	}
	killed = false
	waitAllSent(t, round2[crashed], drainTimeout)
	t.Logf("%s: round 2: every %s sent %.1f s after the kill", label, crashed, time.Since(killedAt).Seconds())
	if err := waitHealthy(healthyTimeout); err != nil {
		t.Error(err)
	}
	// The restarted process counts from zero, so for it the whole count is
	// that of round 2.
	checkOwnAttempts(t, label+": round 2", before, attemptsByWorker(t), round2Count)

	if rep, err = s.report(); err != nil {
		t.Fatal(err)
	}
	repeated, missing := keyCounts(rep, ids)
	t.Logf("%s: provider received %d requests for %d notifications; %d reached it more than once (reported), %d never",
		label, rep.Total, len(ids), repeated, missing)
	if missing > 0 {
		t.Errorf("%s: %d sent notifications never reached the provider", label, missing)
	}
}

// checkPerChannelLayout checks the layout of the stack under test: no single
// worker, one running worker per channel that serves only its channel, with
// WORKER_REPLICAS unset or 1, and the three scraped by Prometheus as the
// job "worker".
func checkPerChannelLayout(t *testing.T, s *stack) {
	t.Helper()
	out, err := compose("config", "--services")
	if err != nil {
		t.Fatal(err)
	}
	services := strings.Fields(out)
	if slices.Contains(services, "worker") {
		t.Errorf("the per-channel layout still has the service worker: %v", services)
	}
	var want []string
	for _, c := range channels {
		w := channelWorkers[c]
		want = append(want, w+":8081")
		if !slices.Contains(services, w) {
			t.Fatalf("the per-channel layout has no service %s: %v", w, services)
		}
		if out, err := compose("ps", "-q", "--status", "running", w); err != nil {
			t.Fatal(err)
		} else if strings.TrimSpace(out) == "" {
			t.Fatalf("%s is not running: the per-channel layout needs one running worker per channel, or the %s notifications are never sent", w, c)
		}
		got, ok, err := serviceSetting(w, "WORKER_CHANNELS")
		if err != nil {
			t.Fatal(err)
		}
		if !ok || got != string(c) {
			t.Errorf("%s: WORKER_CHANNELS=%q (set %v), want %s", w, got, ok, c)
		}
		replicas, ok, err := serviceSetting(w, "WORKER_REPLICAS")
		if err != nil {
			t.Fatal(err)
		}
		if ok && replicas != "1" {
			t.Errorf("%s: WORKER_REPLICAS=%s, want 1: the fallback limiter divides the rate by the workers of one channel", w, replicas)
		}
	}
	slices.Sort(want)

	promURL := strings.TrimRight(envOr("E2E_PROMETHEUS_URL", defaultPrometheusURL), "/")
	var got []string
	var lastErr error
	if !poll(scrapeWaitTimeout, prometheusPollInterval, func() bool {
		var data struct {
			ActiveTargets []struct {
				Labels map[string]string `json:"labels"`
				Health string            `json:"health"`
			} `json:"activeTargets"`
		}
		if lastErr = prometheusGet(s, promURL, "/api/v1/targets", url.Values{"state": {"active"}}, &data); lastErr != nil {
			return false
		}
		got = got[:0]
		for _, tg := range data.ActiveTargets {
			if tg.Labels["job"] == "worker" {
				got = append(got, fmt.Sprintf("%s(%s)", tg.Labels["instance"], tg.Health))
			}
		}
		slices.Sort(got)
		up := make([]string, len(want))
		for i, w := range want {
			up[i] = w + "(up)"
		}
		return slices.Equal(got, up)
	}) {
		t.Errorf("Prometheus job worker: targets %v (error %v), want %v, each up", got, lastErr, want)
	}
}

// checkOwnAttempts compares two readings of attemptsByWorker: between them
// each worker made n attempts on its own channel and none on another.
func checkOwnAttempts(t *testing.T, what string, before, after map[string]map[domain.Channel]float64, n int) {
	t.Helper()
	for _, c := range channels {
		w := channelWorkers[c]
		for _, other := range channels {
			got := after[w][other] - before[w][other]
			want := 0.0
			if other == c {
				want = float64(n)
			}
			if got != want {
				t.Errorf("%s: %s made %v %s attempts, want %v: each channel's requests come from its own worker", what, w, got, other, want)
			}
		}
	}
}

// attemptsByWorker reads delivery_attempts_total of the per-channel workers
// (all of them, or the ones named), by worker and channel, over every
// outcome. A worker that is not running is skipped.
func attemptsByWorker(t *testing.T, skip ...string) map[string]map[domain.Channel]float64 {
	t.Helper()
	got := map[string]map[domain.Channel]float64{}
	for _, c := range channels {
		w := channelWorkers[c]
		got[w] = map[domain.Channel]float64{}
		if slices.Contains(skip, w) {
			continue
		}
		for _, ch := range channels {
			v, err := metricSum(w, attemptsMetric, map[string]string{"channel": string(ch)})
			if err != nil {
				t.Fatal(err)
			}
			got[w][ch] = v
		}
	}
	return got
}
