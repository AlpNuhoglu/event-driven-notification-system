//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
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
)

// compose runs docker-compose on the stack under test: the two Compose files
// of the repository root and the project E2E_PROJECT (make test-e2e sets it;
// without it, Compose's default project, as for make up-mock).
func compose(args ...string) (string, error) {
	root := filepath.Join("..", "..")
	all := []string{"-f", filepath.Join(root, "docker-compose.yml"), "-f", filepath.Join(root, "docker-compose.mock.yml")}
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
// The admin port is not published on the host (ADR-0009 §8), so the request
// is made from inside the stack's network: by the prometheus container,
// whose image has wget (its healthcheck uses it).
func fallbackActive() (float64, error) {
	out, err := compose("exec", "-T", "prometheus", "wget", "-q", "-O", "-", "http://worker:8081/metrics")
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(out, "\n") {
		if v, ok := strings.CutPrefix(line, fallbackMetric+" "); ok {
			return strconv.ParseFloat(strings.TrimSpace(v), 64)
		}
	}
	return 0, fmt.Errorf("%s not found in the worker's /metrics", fallbackMetric)
}

// sendOne creates one SMS and waits until it is sent.
func (s *stack) sendOne(t *testing.T, content string) {
	t.Helper()
	n, _, err := s.create(createRequest{Channel: "sms", Recipient: fakePhone, Content: content, Priority: "high"})
	if err != nil {
		t.Fatal(err)
	}
	s.waitSent(t, n.ID, sendTimeout)
}

// untilFallback sends one notification after another until the gauge reads
// want. The gauge changes only on a reservation, so the worker needs
// something to send.
func (s *stack) untilFallback(t *testing.T, want float64) int {
	t.Helper()
	sent := 0
	var got float64
	var lastErr error
	ok := poll(fallbackTimeout, statusPollInterval, func() bool {
		s.sendOne(t, "fallback")
		sent++
		got, lastErr = fallbackActive()
		return lastErr == nil && got == want
	})
	if !ok {
		t.Fatalf("%s did not become %v within %s (last value %v, error %v)", fallbackMetric, want, fallbackTimeout, got, lastErr)
	}
	return sent
}

// The exit criterion of D25 on the Compose stack: with the redis service
// stopped, notifications are still sent and limiter_fallback_active reads 1
// on the worker's /metrics; after Redis is started again it returns to 0 and
// sending continues. The stack is left with Redis up and the fallback
// inactive, as load-smoke (the last test) needs it.
func TestComposeE2E_RedisDownSendingContinues(t *testing.T) {
	s := newStack(t)
	s.resetMock(t)
	if v, err := fallbackActive(); err != nil || v != 0 {
		t.Fatalf("before the test: %s %v, error %v; want 0", fallbackMetric, v, err)
	}

	redisUp := false
	t.Cleanup(func() {
		if redisUp {
			return
		}
		if _, err := compose("start", "redis"); err != nil {
			t.Errorf("start redis: %v", err)
			return
		}
		s.untilFallback(t, 0)
		s.resetMock(t)
	})

	if _, err := compose("stop", "redis"); err != nil {
		t.Fatal(err)
	}
	const whileDown = 5
	for i := range whileDown {
		s.sendOne(t, fmt.Sprintf("redis down %d", i))
	}
	if v, err := fallbackActive(); err != nil || v != 1 {
		t.Fatalf("with redis stopped: %s %v, error %v; want 1", fallbackMetric, v, err)
	}

	if _, err := compose("start", "redis"); err != nil {
		t.Fatal(err)
	}
	redisUp = true
	probes := s.untilFallback(t, 0)
	s.sendOne(t, "redis back")

	rep, err := s.report()
	if err != nil {
		t.Fatal(err)
	}
	total := whileDown + probes + 1
	t.Logf("redis fallback: %d sent while redis was stopped, %d until the gauge returned to 0", whileDown, probes)
	if rep.Total != total || rep.DuplicateKeys != 0 {
		t.Errorf("mockprovider received %d requests (%d duplicate keys), want %d and none", rep.Total, rep.DuplicateKeys, total)
	}
	s.resetMock(t)
}
