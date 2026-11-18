package testutil

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"
)

func TestRabbitMQImageMatchesCompose(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "docker-compose.yml"))
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?m)^\s*image:\s*(rabbitmq:\S+)\s*$`).FindSubmatch(b)
	if m == nil {
		t.Fatal("no rabbitmq image in docker-compose.yml")
	}
	if got := string(m[1]); got != RabbitMQImage {
		t.Errorf("docker-compose.yml uses %s, testutil.RabbitMQImage is %s", got, RabbitMQImage)
	}
}

// echoServer accepts connections and echoes what it reads.
func echoServer(t *testing.T) string {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = c.Close() }()
				_, _ = io.Copy(c, c)
			}()
		}
	}()
	return ln.Addr().String()
}

func dialProxy(t *testing.T, p *Proxy) net.Conn {
	t.Helper()
	c, err := (&net.Dialer{}).DialContext(context.Background(), "tcp", p.addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// roundTrip writes b and reads len(b) bytes back within timeout.
func roundTrip(c net.Conn, b string, timeout time.Duration) (string, error) {
	if _, err := c.Write([]byte(b)); err != nil {
		return "", err
	}
	return readN(c, len(b), timeout)
}

func readN(c net.Conn, n int, timeout time.Duration) (string, error) {
	_ = c.SetReadDeadline(time.Now().Add(timeout))
	buf := make([]byte, n)
	_, err := io.ReadFull(c, buf)
	return string(buf), err
}

func TestProxy_PauseResumeBreakRestore(t *testing.T) {
	p := NewProxy(t, "amqp://guest:guest@"+echoServer(t)+"/v")
	if want := "amqp://guest:guest@" + p.addr + "/v"; p.URL() != want {
		t.Errorf("URL() = %s, want %s", p.URL(), want)
	}
	c := dialProxy(t, p)
	if got, err := roundTrip(c, "one", time.Second); err != nil || got != "one" {
		t.Fatalf("round trip: %q, %v", got, err)
	}

	p.Pause()
	if _, err := roundTrip(c, "two", 100*time.Millisecond); !isTimeout(err) {
		t.Fatalf("while paused: want a read timeout, got %v", err)
	}
	if p.BytesToBroker() != int64(len("one")+len("two")) {
		t.Errorf("bytes to broker: %d", p.BytesToBroker())
	}
	p.Resume()
	if got, err := readN(c, 3, time.Second); err != nil || got != "two" {
		t.Fatalf("after resume: %q, %v", got, err)
	}

	p.Break()
	if _, err := readN(c, 1, time.Second); err == nil || isTimeout(err) {
		t.Fatalf("after break: want the connection closed, got %v", err)
	}
	if _, err := (&net.Dialer{}).DialContext(context.Background(), "tcp", p.addr); err == nil {
		t.Fatal("after break: a new connection was accepted")
	}
	p.Restore(t)
	c2 := dialProxy(t, p)
	if got, err := roundTrip(c2, "three", time.Second); err != nil || got != "three" {
		t.Fatalf("after restore: %q, %v", got, err)
	}
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}
