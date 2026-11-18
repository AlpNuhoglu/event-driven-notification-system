//go:build integration

// Shared helpers for the E4 recovery tests: a RabbitMQ container bound to a
// FIXED host port (so the client can redial the same address after a restart
// or a replacement), a timestamped event recorder, and listeners that record
// every NotifyStateChange / NotifyClose event of connections and channels.
//
// Run: go test -tags=integration -count=1 -v -timeout 20m ./...
package e4

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/wait"
)

const rabbitImage = "rabbitmq:4.4.1-alpine"

// envDuration reads a duration such as "60s" from the environment.
func envDuration(t *testing.T, name string, def time.Duration) time.Duration {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		t.Fatalf("%s=%q: %v", name, v, err)
	}
	return d
}

// fixedHostPort picks one free host port (never 5672/5673) for the whole test.
// E4_HOST_PORT overrides it.
func fixedHostPort(t *testing.T) int {
	t.Helper()
	if v := os.Getenv("E4_HOST_PORT"); v != "" {
		p, err := strconv.Atoi(v)
		if err != nil {
			t.Fatalf("E4_HOST_PORT: %v", err)
		}
		return p
	}
	for i := 0; i < 20; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		p := l.Addr().(*net.TCPAddr).Port
		_ = l.Close()
		if p != 5672 && p != 5673 {
			return p
		}
	}
	t.Fatal("no free port")
	return 0
}

// startBroker starts a fresh rabbitmq container whose 5672 is published on
// 127.0.0.1:<port>. The container is terminated (with its volumes) at test end.
func startBroker(ctx context.Context, t *testing.T, port int) *testcontainers.DockerContainer {
	t.Helper()
	c, err := testcontainers.Run(ctx, rabbitImage,
		testcontainers.WithExposedPorts("5672/tcp"),
		testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
			hc.PortBindings = network.PortMap{
				network.MustParsePort("5672/tcp"): {{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: strconv.Itoa(port)}},
			}
		}),
		testcontainers.WithWaitStrategy(wait.ForLog("Server startup complete").WithStartupTimeout(3*time.Minute)),
	)
	if err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })
	return c
}

func amqpURL(port int) string { return fmt.Sprintf("amqp://guest:guest@127.0.0.1:%d/", port) }

// recoveryConfig returns a Config with automatic recovery enabled.
// maxRetries == 0 and interval == 0 keep the library defaults (ReconnectionConfig nil).
func recoveryConfig(maxRetries int, interval time.Duration) amqp.Config {
	r := &amqp.Recovery{}
	if maxRetries > 0 {
		r.ReconnectionConfig = &amqp.ReconnectionConfig{MaxRetryCount: maxRetries, RetryInterval: interval}
	}
	return amqp.Config{Recovery: r}
}

// rabbitctl runs rabbitmqctl inside the broker container and returns its output.
func rabbitctl(ctx context.Context, t *testing.T, c *testcontainers.DockerContainer, args ...string) string {
	t.Helper()
	code, r, err := c.Exec(ctx, append([]string{"rabbitmqctl", "-q"}, args...), tcexec.Multiplexed())
	if err != nil {
		t.Fatalf("rabbitmqctl %v: %v", args, err)
	}
	b, _ := io.ReadAll(r)
	out := string(b)
	if code != 0 {
		t.Fatalf("rabbitmqctl %v exit %d: %s", args, code, out)
	}
	return strings.TrimSpace(out)
}

// ---- event recorder --------------------------------------------------------

type event struct {
	at   time.Time
	src  string
	what string
}

type recorder struct {
	mu     sync.Mutex
	t0     time.Time
	events []event
}

func newRecorder() *recorder { return &recorder{t0: time.Now()} }

func (r *recorder) stamp(at time.Time) string {
	return fmt.Sprintf("%s (+%7.3fs)", at.Format("15:04:05.000"), at.Sub(r.t0).Seconds())
}

// add records and prints one event. It prints to stdout, not t.Logf, because
// listener goroutines may outlive the test function.
func (r *recorder) add(src, format string, args ...any) {
	e := event{at: time.Now(), src: src, what: fmt.Sprintf(format, args...)}
	r.mu.Lock()
	r.events = append(r.events, e)
	r.mu.Unlock()
	fmt.Printf("%s %-22s %s\n", r.stamp(e.at), src, e.what)
}

// all returns the events of src (prefix match) whose text contains substr.
func (r *recorder) all(src, substr string) []event {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []event
	for _, e := range r.events {
		if strings.HasPrefix(e.src, src) && strings.Contains(e.what, substr) {
			out = append(out, e)
		}
	}
	return out
}

// waitFor polls until an event of src containing substr exists.
func (r *recorder) waitFor(t *testing.T, src, substr string, timeout time.Duration) event {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if es := r.all(src, substr); len(es) > 0 {
			return es[0]
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout after %s waiting for %s %q", timeout, src, substr)
		}
		time.Sleep(50 * time.Millisecond) // polling interval, not a guess
	}
}

// libLogger forwards the library's internal log lines to the recorder.
type libLogger struct{ r *recorder }

func (l libLogger) Printf(format string, v ...any) { l.r.add("lib", format, v...) }

// watchConn records connection state changes and NotifyClose events.
func watchConn(r *recorder, name string, c *amqp.Connection) {
	sc := make(chan *amqp.StateChanged, 64)
	c.NotifyStateChange(sc)
	go func() {
		for s := range sc {
			r.add(name+".state", "%s -> %s err=%v skipped=%d", s.From, s.To, s.Err, len(s.SkippedTopologyEntities))
		}
		r.add(name+".state", "listener chan closed")
	}()
	cc := c.NotifyClose(make(chan *amqp.Error, 4))
	go func() {
		for e := range cc {
			r.add(name+".close", "received *Error: %v (code=%d server=%v recover=%v)", e, e.Code, e.Server, e.Recover)
		}
		r.add(name+".close", "listener chan closed")
	}()
}

// watchChan records channel state changes and NotifyClose events.
func watchChan(r *recorder, name string, ch *amqp.Channel) {
	sc := make(chan *amqp.StateChanged, 64)
	ch.NotifyStateChange(sc)
	go func() {
		for s := range sc {
			r.add(name+".state", "%s -> %s err=%v", s.From, s.To, s.Err)
		}
		r.add(name+".state", "listener chan closed")
	}()
	cc := ch.NotifyClose(make(chan *amqp.Error, 4))
	go func() {
		for e := range cc {
			r.add(name+".close", "received *Error: %v (code=%d server=%v recover=%v)", e, e.Code, e.Server, e.Recover)
		}
		r.add(name+".close", "listener chan closed")
	}()
}

// declareTopology declares the experiment's exchange, quorum queues and bindings.
func declareTopology(t *testing.T, ch *amqp.Channel, exchange string, queues map[string]string) {
	t.Helper()
	if err := ch.ExchangeDeclare(exchange, "direct", true, false, false, false, nil); err != nil {
		t.Fatalf("exchange declare: %v", err)
	}
	for q, key := range queues {
		if _, err := ch.QueueDeclare(q, true, false, false, false, amqp.Table{"x-queue-type": "quorum"}); err != nil {
			t.Fatalf("queue declare %s: %v", q, err)
		}
		if err := ch.QueueBind(q, key, exchange, false, nil); err != nil {
			t.Fatalf("bind %s: %v", q, err)
		}
	}
}

// recvOne waits up to timeout for one delivery.
func recvOne(ds <-chan amqp.Delivery, timeout time.Duration) (amqp.Delivery, bool) {
	select {
	case d, ok := <-ds:
		return d, ok
	case <-time.After(timeout):
		return amqp.Delivery{}, false
	}
}

// queueMessages returns messages_ready + messages_unacknowledged of a queue.
func queueMessages(ctx context.Context, t *testing.T, c *testcontainers.DockerContainer, name string) int {
	t.Helper()
	out := rabbitctl(ctx, t, c, "list_queues", "name", "messages_ready", "messages_unacknowledged", "--no-table-headers")
	for _, l := range strings.Split(out, "\n") {
		f := strings.Fields(l)
		if len(f) == 3 && f[0] == name {
			r, _ := strconv.Atoi(f[1])
			u, _ := strconv.Atoi(f[2])
			return r + u
		}
	}
	t.Fatalf("queue %s not listed:\n%s", name, out)
	return 0
}
