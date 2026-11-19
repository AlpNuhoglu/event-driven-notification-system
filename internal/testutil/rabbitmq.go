package testutil

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcrabbitmq "github.com/testcontainers/testcontainers-go/modules/rabbitmq"
)

// RabbitMQImage is the RabbitMQ image of the tests. It must equal the image
// of the rabbitmq service in docker-compose.yml (checked by a unit test).
const RabbitMQImage = "rabbitmq:4.4.1-alpine"

// The test container enables the management plugin in addition to the
// image's defaults: its HTTP API creates and deletes a vhost in milliseconds,
// which keeps a fresh vhost per test cheap. The Compose stack does not use it.
const rabbitPlugins = "[rabbitmq_management,rabbitmq_prometheus].\n"

type rabbitServer struct {
	container *tcrabbitmq.RabbitMQContainer
	amqpBase  *url.URL // amqp://guest:guest@host:port
	httpBase  string   // http://host:port of the management API
	user      string
	pass      string
}

var (
	rabbitOnce   sync.Once
	rabbitShared *rabbitServer
	rabbitErr    error
)

func startRabbit() (*rabbitServer, error) {
	ctx, cancel := context.WithTimeout(context.Background(), startTimeout)
	defer cancel()
	c, err := tcrabbitmq.Run(ctx, RabbitMQImage,
		testcontainers.WithFiles(testcontainers.ContainerFile{
			Reader:            strings.NewReader(rabbitPlugins),
			ContainerFilePath: "/etc/rabbitmq/enabled_plugins",
			FileMode:          0o644,
		}),
	)
	if err != nil {
		if c != nil {
			_ = testcontainers.TerminateContainer(c)
		}
		return nil, fmt.Errorf("start rabbitmq: %w", err)
	}
	amqpURL, err := c.AmqpURL(ctx)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("amqp url: %w", err), testcontainers.TerminateContainer(c))
	}
	base, err := url.Parse(amqpURL)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("parse amqp url: %w", err), testcontainers.TerminateContainer(c))
	}
	base.Path = ""
	httpURL, err := c.HttpURL(ctx)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("http url: %w", err), testcontainers.TerminateContainer(c))
	}
	s := &rabbitServer{container: c, amqpBase: base, httpBase: httpURL, user: c.AdminUsername, pass: c.AdminPassword}
	// The management listener can start a moment after the broker logs
	// that it is ready.
	if !poll(func() bool { return s.api(ctx, http.MethodGet, "/api/overview", "") == nil }, time.Minute) {
		return nil, errors.Join(errors.New("rabbitmq management api did not answer"), testcontainers.TerminateContainer(c))
	}
	onExit("rabbitmq", c)
	return s, nil
}

// api sends one request to the management API and fails on a status >= 300.
func (s *rabbitServer) api(ctx context.Context, method, path, body string) error {
	_, err := s.call(ctx, method, path, body)
	return err
}

// call sends one request to the management API and returns the response body.
func (s *rabbitServer) call(ctx context.Context, method, path, body string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, s.httpBase+path, strings.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("management request: %w", err)
	}
	req.SetBasicAuth(s.user, s.pass)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("management %s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("management %s %s: read body: %w", method, path, err)
	}
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("management %s %s: %s: %s", method, path, resp.Status, b)
	}
	return b, nil
}

func sharedRabbit(t testing.TB) *rabbitServer {
	t.Helper()
	rabbitOnce.Do(func() { rabbitShared, rabbitErr = startRabbit() })
	if rabbitErr != nil {
		t.Fatalf("rabbitmq: %v", rabbitErr)
	}
	return rabbitShared
}

// NewRabbitMQVhost creates a fresh vhost on the shared RabbitMQ container,
// which is started on first use, and returns the AMQP URL of that vhost. Each
// test therefore starts from an empty broker. The vhost, with its queues and
// connections, is deleted when the test ends.
func NewRabbitMQVhost(t testing.TB) string {
	t.Helper()
	s := sharedRabbit(t)
	vhost := "t_" + randomHex(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := s.api(ctx, http.MethodPut, "/api/vhosts/"+vhost, ""); err != nil {
		t.Fatalf("create vhost: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := s.api(ctx, http.MethodDelete, "/api/vhosts/"+vhost, ""); err != nil {
			t.Errorf("delete vhost: %v", err)
		}
	})
	perms := `{"configure":".*","write":".*","read":".*"}`
	if err := s.api(ctx, http.MethodPut, "/api/permissions/"+vhost+"/"+url.PathEscape(s.user), perms); err != nil {
		t.Fatalf("set permissions: %v", err)
	}
	u := *s.amqpBase
	u.Path = "/" + vhost
	return u.String()
}

// RabbitMQQueue returns the management API's description of a queue in the
// vhost of amqpURL (a URL from NewRabbitMQVhost): its type, durability and
// arguments, among other fields.
func RabbitMQQueue(t testing.TB, amqpURL, queue string) map[string]any {
	t.Helper()
	s := sharedRabbit(t)
	u, err := url.Parse(amqpURL)
	if err != nil {
		t.Fatalf("parse amqp url: %v", err)
	}
	vhost := strings.TrimPrefix(u.Path, "/")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	b, err := s.call(ctx, http.MethodGet, "/api/queues/"+url.PathEscape(vhost)+"/"+url.PathEscape(queue), "")
	if err != nil {
		t.Fatalf("queue %s: %v", queue, err)
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("queue %s: decode: %v", queue, err)
	}
	return out
}

// Proxy is a TCP proxy between a client and the broker that a test can
// pause and break, to produce missing confirms and lost connections without
// touching the shared container. NewRedisProxy puts one in front of Redis.
type Proxy struct {
	target string
	addr   string
	url    string

	mu     sync.Mutex
	ln     net.Listener
	conns  map[net.Conn]struct{}
	flow   chan struct{} // closed while traffic from the broker to the client flows
	toBkr  atomic.Int64  // bytes forwarded from the client to the broker
	closed bool
}

// NewProxy starts a proxy in front of the broker of amqpURL and returns it;
// URL is amqpURL with the proxy's address. The proxy is closed when the test
// ends.
func NewProxy(t testing.TB, amqpURL string) *Proxy {
	t.Helper()
	u, err := url.Parse(amqpURL)
	if err != nil {
		t.Fatalf("proxy: parse url: %v", err)
	}
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy: listen: %v", err)
	}
	flow := make(chan struct{})
	close(flow)
	p := &Proxy{target: u.Host, addr: ln.Addr().String(), ln: ln, conns: map[net.Conn]struct{}{}, flow: flow}
	u.Host = p.addr
	p.url = u.String()
	go p.accept(ln)
	t.Cleanup(p.close)
	return p
}

// URL returns the AMQP URL that connects through the proxy.
func (p *Proxy) URL() string { return p.url }

// BytesToBroker returns the number of bytes forwarded from clients to the
// broker so far.
func (p *Proxy) BytesToBroker() int64 { return p.toBkr.Load() }

// Pause holds back everything the broker sends to the clients (confirms,
// returns, deliveries) until Resume. What the clients send still reaches the
// broker.
func (p *Proxy) Pause() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if isClosedChan(p.flow) {
		p.flow = make(chan struct{})
	}
}

// Resume lets the held-back traffic through.
func (p *Proxy) Resume() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !isClosedChan(p.flow) {
		close(p.flow)
	}
}

// Break closes every proxied connection and stops accepting new ones, as if
// the broker had gone away. Restore accepts connections again.
func (p *Proxy) Break() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ln != nil {
		_ = p.ln.Close()
		p.ln = nil
	}
	for c := range p.conns {
		_ = c.Close()
	}
}

// Restore accepts connections again on the same address after Break.
func (p *Proxy) Restore(t testing.TB) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ln != nil || p.closed {
		return
	}
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", p.addr)
	if err != nil {
		t.Fatalf("proxy: listen again: %v", err)
	}
	p.ln = ln
	go p.accept(ln)
}

func (p *Proxy) close() {
	p.Break()
	p.Resume()
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
}

func (p *Proxy) accept(ln net.Listener) {
	for {
		client, err := ln.Accept()
		if err != nil {
			return
		}
		go p.serve(client)
	}
}

func (p *Proxy) serve(client net.Conn) {
	broker, err := (&net.Dialer{Timeout: 10 * time.Second}).DialContext(context.Background(), "tcp", p.target)
	if err != nil {
		_ = client.Close()
		return
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		_ = client.Close()
		_ = broker.Close()
		return
	}
	p.conns[client] = struct{}{}
	p.conns[broker] = struct{}{}
	p.mu.Unlock()

	done := make(chan struct{})
	var once sync.Once
	stop := func() {
		once.Do(func() {
			close(done)
			_ = client.Close()
			_ = broker.Close()
			p.mu.Lock()
			delete(p.conns, client)
			delete(p.conns, broker)
			p.mu.Unlock()
		})
	}
	go func() { // client -> broker
		defer stop()
		_, _ = io.Copy(countingWriter{broker, &p.toBkr}, client)
	}()
	go func() { // broker -> client, held back while paused
		defer stop()
		buf := make([]byte, 32*1024)
		for {
			n, err := broker.Read(buf)
			if n > 0 {
				select {
				case <-p.flowSignal():
				case <-done:
					return
				}
				if _, werr := client.Write(buf[:n]); werr != nil {
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()
}

// flowSignal returns the current flow channel. A forwarder that waits on a
// replaced channel is released by Resume, which closes the channel it holds.
func (p *Proxy) flowSignal() <-chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.flow
}

type countingWriter struct {
	w io.Writer
	n *atomic.Int64
}

func (c countingWriter) Write(b []byte) (int, error) {
	n, err := c.w.Write(b)
	c.n.Add(int64(n))
	return n, err
}

func isClosedChan(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
