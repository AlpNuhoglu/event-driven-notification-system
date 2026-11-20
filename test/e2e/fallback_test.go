//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	// composeTimeout bounds one docker-compose command.
	composeTimeout = 2 * time.Minute
	// fallbackTimeout bounds the switch of limiter_fallback_active.
	fallbackTimeout = time.Minute
	// fallbackMetric is the worker's gauge of the Redis-down fallback (D25).
	fallbackMetric = "limiter_fallback_active"
	// fallbackAlert is the alert rule on that gauge (deploy/prometheus/alerts.yml).
	fallbackAlert = "LimiterFallbackActive"
	// alertTimeout bounds the wait for an alert rule's state to follow the
	// gauge: a scrape and a rule evaluation (5 s each) and a margin.
	alertTimeout = time.Minute
	// layoutFileVar names the layout override of the stack under test, a
	// Compose file of the repository root (see compose).
	layoutFileVar = "E2E_LAYOUT_FILE"
)

// compose runs docker-compose on the stack under test: the two Compose files
// of the repository root, the e2e override of this directory (as make
// test-e2e uses them), the layout override E2E_LAYOUT_FILE when it is set (a
// file of the repository root; the second pass of make test-e2e sets it to
// the per-channel worker override) and the project E2E_PROJECT (make
// test-e2e sets it; without it, Compose's default project, as for make
// up-mock).
func compose(args ...string) (string, error) {
	root := filepath.Join("..", "..")
	all := []string{
		"-f", filepath.Join(root, "docker-compose.yml"),
		"-f", filepath.Join(root, "docker-compose.mock.yml"),
		"-f", "docker-compose.e2e.yml",
	}
	if f := os.Getenv(layoutFileVar); f != "" {
		all = append(all, "-f", filepath.Join(root, filepath.Base(f)))
	}
	if p := os.Getenv("E2E_PROJECT"); p != "" {
		all = append(all, "-p", p)
	}
	all = append(all, args...)
	ctx, cancel := context.WithTimeout(context.Background(), composeTimeout)
	defer cancel()
	var out bytes.Buffer
	cmd := exec.CommandContext(ctx, "docker-compose", all...) //nolint:gosec // fixed command, arguments from the test
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		return out.String(), fmt.Errorf("docker-compose %s: %w: %s", strings.Join(args, " "), err, out.String())
	}
	return out.String(), nil
}

// fallbackActive reads limiter_fallback_active from the worker's /metrics.
func fallbackActive() (float64, error) {
	return metric("worker", fallbackMetric)
}

// sendOne creates one SMS, waits until it is sent and returns its id.
func (s *stack) sendOne(t *testing.T, content string) string {
	t.Helper()
	n, _, err := s.create(createRequest{Channel: "sms", Recipient: fakePhone, Content: content, Priority: "high"})
	if err != nil {
		t.Fatal(err)
	}
	s.waitSent(t, n.ID, sendTimeout)
	return n.ID
}

// untilFallback sends one notification after another until the gauge reads
// want, and returns the ids it sent. The gauge changes only on a
// reservation, so the worker needs something to send.
func (s *stack) untilFallback(t *testing.T, want float64) []string {
	t.Helper()
	var ids []string
	var got float64
	var lastErr error
	ok := poll(fallbackTimeout, statusPollInterval, func() bool {
		ids = append(ids, s.sendOne(t, "fallback"))
		got, lastErr = fallbackActive()
		return lastErr == nil && got == want
	})
	if !ok {
		t.Fatalf("%s did not become %v within %s (last value %v, error %v)", fallbackMetric, want, fallbackTimeout, got, lastErr)
	}
	return ids
}

// alertState returns the state of an alerting rule as the stack's
// Prometheus reports it: inactive, pending or firing.
func (s *stack) alertState(name string) (string, error) {
	var data struct {
		Groups []struct {
			Rules []struct {
				Name  string `json:"name"`
				State string `json:"state"`
			} `json:"rules"`
		} `json:"groups"`
	}
	promURL := strings.TrimRight(envOr("E2E_PROMETHEUS_URL", defaultPrometheusURL), "/")
	if err := prometheusGet(s, promURL, "/api/v1/rules", url.Values{"type": {"alert"}}, &data); err != nil {
		return "", err
	}
	for _, g := range data.Groups {
		for _, r := range g.Rules {
			if r.Name == name {
				return r.State, nil
			}
		}
	}
	return "", fmt.Errorf("prometheus has no alerting rule %s", name)
}

// waitAlertState waits until the alerting rule name is in state want and
// returns how long that took; while describes the moment for the message.
func (s *stack) waitAlertState(t *testing.T, name, want, while string) time.Duration {
	t.Helper()
	start := time.Now()
	var got string
	var err error
	if !poll(alertTimeout, prometheusPollInterval, func() bool {
		got, err = s.alertState(name)
		return err == nil && got == want
	}) {
		t.Fatalf("%s: the alert %s is %q (error %v) after %s, want %s", while, name, got, err, alertTimeout, want)
	}
	return time.Since(start)
}

// The exit criterion of D25 on the Compose stack, and the chaos scenario
// "Redis stopped" (ADR-0010 §1): with the redis service stopped,
// notifications are still sent, limiter_fallback_active reads 1 on the
// worker's /metrics and the stack's Prometheus shows the alert
// LimiterFallbackActive firing; after Redis is started again the gauge
// returns to 0, the alert is no longer firing and sending continues. The
// stack is left with Redis up and the fallback inactive, as load-smoke (the
// last test) needs it.
func TestComposeE2E_RedisDownSendingContinues(t *testing.T) {
	const label = "redis stopped"
	s := newStack(t)
	s.resetMock(t)
	if v, err := fallbackActive(); err != nil || v != 0 {
		t.Fatalf("before the test: %s %v, error %v; want 0", fallbackMetric, v, err)
	}
	if st, err := s.alertState(fallbackAlert); err != nil || st != "inactive" {
		t.Fatalf("before the test: the alert %s is %q (error %v), want inactive", fallbackAlert, st, err)
	}

	var ids []string
	redisUp := false
	t.Cleanup(func() {
		if !redisUp {
			if _, err := compose("start", "redis"); err != nil {
				t.Errorf("cleanup: start redis: %v", err)
				return
			}
			s.untilFallback(t, 0)
		}
		s.resetMock(t)
	})
	s.reportChaosAtEnd(t, label, &ids)

	if _, err := compose("stop", "redis"); err != nil {
		t.Fatal(err)
	}
	stopped := time.Now()
	const whileDown = 5
	for i := range whileDown {
		ids = append(ids, s.sendOne(t, fmt.Sprintf("redis down %d", i)))
	}
	if v, err := fallbackActive(); err != nil || v != 1 {
		t.Fatalf("with redis stopped: %s %v, error %v; want 1", fallbackMetric, v, err)
	}
	firing := s.waitAlertState(t, fallbackAlert, "firing", "with redis stopped")
	t.Logf("%s: the alert %s firing %.1f s after the gauge read 1 (%.1f s after the stop)",
		label, fallbackAlert, firing.Seconds(), time.Since(stopped).Seconds())

	if _, err := compose("start", "redis"); err != nil {
		t.Fatal(err)
	}
	redisUp = true
	probes := s.untilFallback(t, 0)
	ids = append(ids, probes...)
	resolved := s.waitAlertState(t, fallbackAlert, "inactive", "after redis was started again")
	ids = append(ids, s.sendOne(t, "redis back"))

	rep, err := s.report()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%s: %d sent while redis was stopped, %d until the gauge returned to 0; the alert inactive %.1f s after that",
		label, whileDown, len(probes), resolved.Seconds())
	if rep.Total != len(ids) || rep.DuplicateKeys != 0 {
		t.Errorf("mockprovider received %d requests (%d duplicate keys), want %d and none", rep.Total, rep.DuplicateKeys, len(ids))
	}
}
