//go:build integration

// Checks that testcontainers-go can start a RabbitMQ container and really talk
// to it through the mapped port, both on the host and from inside the Go
// container started by the root Makefile's test-docker recipe (Ryuk enabled).
//
// Host:      go test -race -tags=integration -count=1 -v ./...   (in experiments/e3)
// Container: make -C experiments/e3 -f ../../Makefile test-docker  (from the repository root)
package e3

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const rabbitImage = "rabbitmq:4.4.1-alpine"

func TestRabbitMQThroughMappedPort(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	_, inContainer := os.Stat("/.dockerenv")
	t.Logf("running in container: %v", inContainer == nil)
	for _, k := range []string{
		"TESTCONTAINERS_HOST_OVERRIDE",
		"TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE",
		"TESTCONTAINERS_RYUK_DISABLED",
		"DOCKER_HOST",
	} {
		t.Logf("env %s=%q", k, os.Getenv(k))
	}

	start := time.Now()
	ctr, err := testcontainers.Run(ctx, rabbitImage,
		testcontainers.WithExposedPorts("5672/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("Server startup complete").WithStartupTimeout(2*time.Minute),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatalf("start %s: %v", rabbitImage, err)
	}
	t.Logf("container %s started in %s", ctr.GetContainerID()[:12], time.Since(start).Round(time.Millisecond))

	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatalf("host: %v", err)
	}
	port, err := ctr.MappedPort(ctx, "5672/tcp")
	if err != nil {
		t.Fatalf("mapped port: %v", err)
	}
	url := fmt.Sprintf("amqp://guest:guest@%s:%s/", host, port.Port())
	t.Logf("dialing %s", url)

	// The broker logs "startup complete" before every listener is guaranteed
	// reachable through the port forward, so poll the dial with a deadline.
	var conn *amqp.Connection
	deadline := time.Now().Add(30 * time.Second)
	for {
		conn, err = amqp.Dial(url)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("dial %s: %v", url, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
	defer func() { _ = conn.Close() }()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("channel: %v", err)
	}
	// Durable: RabbitMQ 4.x refuses transient non-exclusive queues
	// (deprecated feature transient_nonexcl_queues, error 541).
	q, err := ch.QueueDeclare("e3", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("declare: %v", err)
	}
	body := []byte("hello from e3")
	if err := ch.PublishWithContext(ctx, "", q.Name, false, false,
		amqp.Publishing{ContentType: "text/plain", Body: body}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	var msg amqp.Delivery
	var ok bool
	deadline = time.Now().Add(10 * time.Second)
	for !ok {
		msg, ok, err = ch.Get(q.Name, true)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if !ok && time.Now().After(deadline) {
			t.Fatal("no message within deadline")
		}
		if !ok {
			time.Sleep(100 * time.Millisecond)
		}
	}
	if string(msg.Body) != string(body) {
		t.Fatalf("body = %q, want %q", msg.Body, body)
	}
	t.Logf("round trip ok: %q (broker %v)", msg.Body, conn.Properties["version"])
}
