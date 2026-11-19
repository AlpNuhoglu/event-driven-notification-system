//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/mockprovider"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/queue"
)

// Operations on the containers of the stack under test, used by the chaos
// scenarios: metrics, SQL, queue depths, container states, health and logs.

const (
	// healthyTimeout bounds the wait until every service is up and healthy.
	healthyTimeout = 2 * time.Minute
	// drainPollInterval is how often the statuses of a set of notifications
	// or the queue depths are read while waiting for them.
	drainPollInterval = 500 * time.Millisecond
	// createClients is the number of concurrent clients of createN.
	createClients = 8
	// statsSettle is how long a queue depth must read zero before it is
	// believed. rabbitmqctl reports the depth of a quorum queue from
	// statistics that the broker refreshes about every five seconds (seen on
	// RabbitMQ 4.4.1: the figure stayed unchanged for seconds while the
	// queue was consumed), so a single reading can be stale.
	statsSettle = 6 * time.Second
)

// channels are the notification channels, each with a dispatch and a
// poison queue.
var channels = []domain.Channel{domain.ChannelSMS, domain.ChannelEmail, domain.ChannelPush}

// docker runs a docker command and returns its combined output.
func docker(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), composeTimeout)
	defer cancel()
	var out bytes.Buffer
	cmd := exec.CommandContext(ctx, "docker", args...) //nolint:gosec // fixed command, arguments from the test
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		return out.String(), fmt.Errorf("docker %s: %w: %s", strings.Join(args, " "), err, out.String())
	}
	return out.String(), nil
}

// metric reads an unlabelled sample from a service's /metrics. The admin
// port is not published on the host (ADR-0009 §8), so the request is made
// from inside the stack's network: by the prometheus container, whose image
// has wget (its healthcheck uses it).
func metric(service, name string) (float64, error) {
	out, err := compose("exec", "-T", "prometheus", "wget", "-q", "-O", "-", "http://"+service+":8081/metrics")
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(out, "\n") {
		if v, ok := strings.CutPrefix(line, name+" "); ok {
			return strconv.ParseFloat(strings.TrimSpace(v), 64)
		}
	}
	return 0, fmt.Errorf("%s not found in the %s's /metrics", name, service)
}

// psql runs one SQL statement in the postgres container and returns its
// unaligned, tuples-only output.
func psql(query string) (string, error) {
	return compose("exec", "-T", "postgres", "psql", "-U", "notifications", "-d", "notifications",
		"-X", "-q", "-At", "-v", "ON_ERROR_STOP=1", "-c", query)
}

// uuidArray renders ids as an SQL uuid[] literal. Each id is parsed first,
// so nothing but UUIDs reaches the statement.
func uuidArray(ids []string) (string, error) {
	parts := make([]string, len(ids))
	for i, id := range ids {
		u, err := uuid.Parse(id)
		if err != nil {
			return "", fmt.Errorf("id %q: %w", id, err)
		}
		parts[i] = u.String()
	}
	return "'{" + strings.Join(parts, ",") + "}'::uuid[]", nil
}

// statuses returns the status of each of the notifications ids.
func statuses(ids []string) (map[string]string, error) {
	arr, err := uuidArray(ids)
	if err != nil {
		return nil, err
	}
	out, err := psql("SELECT id, status FROM notifications WHERE id = ANY(" + arr + ")")
	if err != nil {
		return nil, err
	}
	got := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		id, st, ok := strings.Cut(line, "|")
		if !ok {
			continue
		}
		got[id] = st
	}
	return got, nil
}

// statusCounts returns how many of the notifications ids are in each status.
func statusCounts(ids []string) (map[string]int, error) {
	st, err := statuses(ids)
	if err != nil {
		return nil, err
	}
	counts := map[string]int{}
	for _, s := range st {
		counts[s]++
	}
	if missing := len(ids) - len(st); missing > 0 {
		counts["missing"] = missing
	}
	return counts, nil
}

// formatCounts prints status counts in a stable order.
func formatCounts(c map[string]int) string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s=%d", k, c[k])
	}
	return strings.Join(parts, " ")
}

// waitAllSent waits until every notification of ids is sent, reading the
// statuses in Postgres. It fails the test with the last counts when one of
// them ends failed or cancelled, or when not all are sent within timeout.
// Errors while reading (Postgres restarting) are retried.
func waitAllSent(t *testing.T, ids []string, timeout time.Duration) {
	t.Helper()
	var last map[string]int
	var lastErr error
	terminal := false
	ok := poll(timeout, drainPollInterval, func() bool {
		c, err := statusCounts(ids)
		if err != nil {
			lastErr = err
			return false
		}
		last, lastErr = c, nil
		if c["failed"] > 0 || c["cancelled"] > 0 {
			terminal = true
			return true
		}
		return c["sent"] == len(ids)
	})
	if ok && !terminal {
		return
	}
	if terminal {
		t.Fatalf("of %d notifications, some ended other than sent: %s", len(ids), formatCounts(last))
	}
	t.Fatalf("of %d notifications, not all sent within %s: %s (last read error %v)", len(ids), timeout, formatCounts(last), lastErr)
}

// waitCounts waits until the statuses of ids satisfy cond and returns them.
func waitCounts(t *testing.T, ids []string, timeout time.Duration, what string, cond func(map[string]int) bool) map[string]int {
	t.Helper()
	var last map[string]int
	var lastErr error
	if !poll(timeout, statusPollInterval, func() bool {
		last, lastErr = statusCounts(ids)
		return lastErr == nil && cond(last)
	}) {
		t.Fatalf("%s: not within %s; last counts %s, error %v", what, timeout, formatCounts(last), lastErr)
	}
	return last
}

// queueDepths returns the number of messages (ready and unacknowledged) of
// every queue of the broker, read with rabbitmqctl in its container.
func queueDepths() (map[string]int, error) {
	out, err := compose("exec", "-T", "rabbitmq", "rabbitmqctl", "list_queues", "--quiet", "--no-table-headers", "name", "messages")
	if err != nil {
		return nil, err
	}
	depths := map[string]int{}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		f := strings.Fields(line)
		if len(f) != 2 {
			continue
		}
		n, err := strconv.Atoi(f[1])
		if err != nil {
			return nil, fmt.Errorf("list_queues line %q: %w", line, err)
		}
		depths[f[0]] = n
	}
	return depths, nil
}

// checkPoisonEmpty fails the test unless every channel's poison queue
// exists and is empty, and returns the depths it read. The reading can be
// a few seconds old (see statsSettle): call it after waitDispatchEmpty.
func checkPoisonEmpty(t *testing.T) map[string]int {
	t.Helper()
	d, err := queueDepths()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range channels {
		q := queue.PoisonQueue(c)
		n, ok := d[q]
		switch {
		case !ok:
			t.Errorf("poison queue %s does not exist", q)
		case n != 0:
			t.Errorf("poison queue %s holds %d messages, want 0", q, n)
		}
	}
	return d
}

// waitDispatchEmpty waits until every dispatch queue exists and has read
// empty for statsSettle without interruption: no backlog is left in the
// broker. The poison queues are not waited for (a message there is never
// consumed); checkPoisonEmpty reads them afterwards.
func waitDispatchEmpty(timeout time.Duration) error {
	var last map[string]int
	var lastErr error
	var emptySince time.Time
	if poll(timeout, drainPollInterval, func() bool {
		last, lastErr = queueDepths()
		if lastErr != nil {
			return false
		}
		for _, c := range channels {
			if n, ok := last[queue.DispatchQueue(c)]; !ok || n != 0 {
				emptySince = time.Time{}
				return false
			}
		}
		if emptySince.IsZero() {
			emptySince = time.Now()
		}
		return time.Since(emptySince) >= statsSettle
	}) {
		return nil
	}
	return fmt.Errorf("dispatch queues not empty for %s within %s: %v (last error %v)", statsSettle, timeout, last, lastErr)
}

// containerState is what tells a restarted or exited container apart.
type containerState struct {
	ID           string
	RestartCount int
	StartedAt    string
	Status       string
}

func (c containerState) String() string {
	return fmt.Sprintf("%.12s restarts=%d started=%s %s", c.ID, c.RestartCount, c.StartedAt, c.Status)
}

// containerStates returns the state of the one container of each service.
func containerStates(services ...string) (map[string]containerState, error) {
	got := map[string]containerState{}
	for _, svc := range services {
		out, err := compose("ps", "-q", svc)
		if err != nil {
			return nil, err
		}
		id := strings.TrimSpace(out)
		if id == "" || strings.Contains(id, "\n") {
			return nil, fmt.Errorf("want one running %s container, got %q", svc, id)
		}
		out, err = docker("inspect", "--format", "{{.Id}} {{.RestartCount}} {{.State.StartedAt}} {{.State.Status}}", id)
		if err != nil {
			return nil, err
		}
		f := strings.Fields(out)
		if len(f) != 4 {
			return nil, fmt.Errorf("docker inspect %s: %q", svc, out)
		}
		n, err := strconv.Atoi(f[1])
		if err != nil {
			return nil, fmt.Errorf("restart count of %s: %w", svc, err)
		}
		got[svc] = containerState{ID: f[0], RestartCount: n, StartedAt: f[2], Status: f[3]}
	}
	return got, nil
}

// checkNoServiceExited compares the containers with their states before a
// fault: the same container, not restarted by the restart policy and not
// started again (an exit followed by a restart changes StartedAt).
func checkNoServiceExited(t *testing.T, before map[string]containerState) {
	t.Helper()
	services := make([]string, 0, len(before))
	for svc := range before {
		services = append(services, svc)
	}
	sort.Strings(services)
	after, err := containerStates(services...)
	if err != nil {
		t.Fatal(err)
	}
	for _, svc := range services {
		b, a := before[svc], after[svc]
		if a.ID != b.ID || a.RestartCount != b.RestartCount || a.StartedAt != b.StartedAt || a.Status != "running" {
			t.Errorf("service %s exited or restarted during the fault: before %s, after %s", svc, b, a)
		}
	}
}

// psEntry is one line of docker-compose ps --format json.
type psEntry struct {
	Service string `json:"Service"`
	State   string `json:"State"`
	Health  string `json:"Health"`
}

// waitHealthy waits until every long-running service of the stack (every
// service but migrate) has a running and healthy container.
func waitHealthy(timeout time.Duration) error {
	out, err := compose("config", "--services")
	if err != nil {
		return err
	}
	var want []string
	for _, svc := range strings.Fields(out) {
		if svc != "migrate" {
			want = append(want, svc)
		}
	}
	var unhealthy []string
	var lastErr error
	if poll(timeout, drainPollInterval, func() bool {
		out, err := compose("ps", "--format", "json")
		if err != nil {
			lastErr = err
			return false
		}
		got := map[string]psEntry{}
		for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
			var e psEntry
			if json.Unmarshal([]byte(line), &e) == nil && e.Service != "" {
				got[e.Service] = e
			}
		}
		unhealthy = unhealthy[:0]
		for _, svc := range want {
			if e := got[svc]; e.State != "running" || e.Health != "healthy" {
				unhealthy = append(unhealthy, fmt.Sprintf("%s(%s/%s)", svc, e.State, e.Health))
			}
		}
		lastErr = nil
		return len(unhealthy) == 0
	}) {
		return nil
	}
	return fmt.Errorf("services not running and healthy within %s: %v (last error %v)", timeout, unhealthy, lastErr)
}

// serviceLogsSince returns the log lines of the services since the given time.
func serviceLogsSince(since time.Time, services ...string) ([]string, error) {
	args := append([]string{"logs", "--no-color", "--no-log-prefix", "--since", since.UTC().Format(time.RFC3339Nano)}, services...)
	out, err := compose(args...)
	if err != nil {
		return nil, err
	}
	return strings.Split(out, "\n"), nil
}

// logLine holds the fields of a service's JSON log line the tests read.
type logLine struct {
	Level string `json:"level"`
	Msg   string `json:"msg"`
	Role  string `json:"role"`
	Error string `json:"error"`
}

// countMessages counts the JSON log lines by their msg.
func countMessages(lines []string) map[string]int {
	got := map[string]int{}
	for _, line := range lines {
		var l logLine
		if json.Unmarshal([]byte(line), &l) == nil && l.Msg != "" {
			got[l.Msg]++
		}
	}
	return got
}

// channelErrors returns the log lines of a channel closed by the broker with
// an error other than the connection's closure (320 CONNECTION_FORCED, which
// every broker shutdown produces): for example 404 NOT_FOUND, a publish to
// an exchange that does not exist.
func channelErrors(lines []string) []string {
	var bad []string
	for _, line := range lines {
		var l logLine
		if json.Unmarshal([]byte(line), &l) != nil {
			continue
		}
		if strings.HasPrefix(l.Msg, "rabbitmq channel closed with an error") && !strings.Contains(l.Error, "CONNECTION_FORCED") {
			bad = append(bad, line)
		}
		if strings.Contains(l.Error, "NOT_FOUND") {
			bad = append(bad, line)
		}
	}
	return slices.Compact(bad)
}

// keyCounts compares the mock's report with the notifications ids: how many
// of them reached the provider more than once, and how many never did.
func keyCounts(rep mockprovider.Report, ids []string) (repeated, missing int) {
	for _, id := range ids {
		switch n := rep.IdempotencyKeys[id]; {
		case n == 0:
			missing++
		case n > 1:
			repeated++
		}
	}
	return repeated, missing
}

// createN creates n notifications like req with a few concurrent clients
// and returns their ids in creation order. Every create must succeed.
func createN(t *testing.T, s *stack, req createRequest, n int, label string) []string {
	t.Helper()
	ids := make([]string, n)
	next := make(chan int)
	var (
		mu       sync.Mutex
		failures int
		firstErr error
		wg       sync.WaitGroup
	)
	start := time.Now()
	for range createClients {
		wg.Go(func() {
			for i := range next {
				created, _, err := s.create(req)
				if err != nil {
					mu.Lock()
					failures++
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
					continue
				}
				ids[i] = created.ID
			}
		})
	}
	for i := range n {
		next <- i
	}
	close(next)
	wg.Wait()
	elapsed := time.Since(start)
	if failures > 0 {
		t.Fatalf("%d of %d creates failed; first error: %v", failures, n, firstErr)
	}
	t.Logf("%s: created %d %s in %.1f s (%.0f/s) with %d clients",
		label, n, req.Channel, elapsed.Seconds(), float64(n)/elapsed.Seconds(), createClients)
	return ids
}

// workerConcurrency returns the worker's WORKER_CONCURRENCY (25 when it is
// not set): the bound of sends in flight per channel.
func workerConcurrency(t *testing.T) int {
	t.Helper()
	v, ok, err := workerSetting("WORKER_CONCURRENCY")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		return 25
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		t.Fatalf("WORKER_CONCURRENCY=%q: %v", v, err)
	}
	return n
}

// leaveClean is the cleanup of a chaos scenario: it restores the mock,
// waits until every service is up and healthy and the broker holds no
// backlog, and clears the mock's records, so that the next test starts on
// the stack it expects.
func (s *stack) leaveClean(t *testing.T) {
	t.Helper()
	if err := waitHealthy(healthyTimeout); err != nil {
		t.Errorf("cleanup: %v", err)
	}
	s.resetMock(t)
	if err := waitDispatchEmpty(healthyTimeout); err != nil {
		t.Errorf("cleanup: %v", err)
	}
	checkPoisonEmpty(t)
	s.resetMock(t)
}
