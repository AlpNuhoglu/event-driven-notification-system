//go:build integration

// Shared broker setup for experiment E6: one rabbitmq:4.4.1-alpine container
// started with a SHORT node-wide consumer_timeout injected as a conf.d file,
// so the delivery acknowledgement timeout can be observed in seconds.
//
// Run from experiments/e6:
//
//	go test -tags=integration -count=1 -v ./...
package e6

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const image = "rabbitmq:4.4.1-alpine"

var (
	amqpURL   string
	broker    *testcontainers.DockerContainer
	testStart = time.Now()
)

// envMS reads a millisecond duration from the environment, with a default.
func envMS(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return def
}

// Node-wide consumer_timeout given to the broker (queue A uses it).
var brokerTimeoutMS = envMS("E6_BROKER_TIMEOUT_MS", 30000)

func logf(t *testing.T, format string, args ...any) {
	t.Helper()
	t.Logf("[%s +%6.2fs] %s", time.Now().Format("15:04:05.000"),
		time.Since(testStart).Seconds(), fmt.Sprintf(format, args...))
}

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	ctx := context.Background()
	conf := fmt.Sprintf("consumer_timeout = %d\n", brokerTimeoutMS)
	c, err := testcontainers.Run(ctx, image,
		testcontainers.WithExposedPorts("5672/tcp"),
		testcontainers.WithFiles(testcontainers.ContainerFile{
			Reader:            strings.NewReader(conf),
			ContainerFilePath: "/etc/rabbitmq/conf.d/90-e6.conf",
			FileMode:          0o644,
		}),
		testcontainers.WithWaitStrategyAndDeadline(2*time.Minute,
			wait.ForLog("Server startup complete"),
			wait.ForListeningPort("5672/tcp")),
	)
	broker = c
	if err != nil {
		fmt.Fprintln(os.Stderr, "start broker:", err)
		if c != nil {
			_ = testcontainers.TerminateContainer(c)
		}
		return 1
	}
	defer func() { _ = testcontainers.TerminateContainer(c) }()

	ep, err := c.PortEndpoint(ctx, "5672/tcp", "")
	if err != nil {
		fmt.Fprintln(os.Stderr, "endpoint:", err)
		return 1
	}
	amqpURL = "amqp://guest:guest@" + ep + "/"
	return m.Run()
}

// rabbitmqctl runs a rabbitmqctl command inside the broker container.
func rabbitmqctl(t *testing.T, args ...string) string {
	t.Helper()
	code, r, err := broker.Exec(context.Background(), append([]string{"rabbitmqctl"}, args...))
	if err != nil {
		t.Fatalf("exec rabbitmqctl: %v", err)
	}
	out, _ := io.ReadAll(r)
	if code != 0 {
		t.Fatalf("rabbitmqctl %v exit %d: %s", args, code, out)
	}
	return string(out)
}

func dial(t *testing.T) *amqp.Connection {
	t.Helper()
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func quorumArgs(extra amqp.Table) amqp.Table {
	args := amqp.Table{"x-queue-type": "quorum"}
	for k, v := range extra {
		args[k] = v
	}
	return args
}
