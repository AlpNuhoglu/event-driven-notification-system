package provider

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

const (
	phone   = "+905551234567"
	email   = "alice@example.com"
	content = "your code is 424242"
)

func newClient(t *testing.T, url string, mutate func(*Config)) (*Client, *prometheus.Registry) {
	t.Helper()
	cfg := Config{
		URL: url, Name: "primary", IdempotencyHeader: "Idempotency-Key",
		ConnectTimeout: time.Second, Timeout: 5 * time.Second, MaxIdleConnsPerHost: 4,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	reg := prometheus.NewRegistry()
	c, err := New(cfg, reg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.http.CloseIdleConnections)
	return c, reg
}

func req(to string) Request {
	return Request{NotificationID: uuid.New(), Channel: domain.ChannelSMS, To: to, Content: content}
}

func accept(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusAccepted)
	_, _ = io.WriteString(w, `{"messageId":"m-1","status":"accepted","timestamp":"2026-01-01T00:00:00Z"}`)
}

func noPII(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, s := range []string{phone, email, content, "+90555***4567"} {
		if strings.Contains(err.Error(), s) {
			t.Fatalf("error contains personal data: %q", s)
		}
	}
}

func TestSend_BodyHasExactlyThreeFields(t *testing.T) {
	var got map[string]json.RawMessage
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&got)
		accept(w, r)
	}))
	defer srv.Close()
	c, _ := newClient(t, srv.URL, nil)
	c.Send(context.Background(), req(phone))
	if len(got) != 3 {
		t.Fatalf("fields = %v", got)
	}
	for _, k := range []string{"to", "channel", "content"} {
		if _, ok := got[k]; !ok {
			t.Errorf("missing %q", k)
		}
	}
}

func TestSend_ToIsTheRealRecipient(t *testing.T) {
	for _, real := range []string{phone, email} {
		var to string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var b payload
			_ = json.NewDecoder(r.Body).Decode(&b)
			to = b.To
			accept(w, r)
		}))
		rcpt := domain.Recipient(real)
		c, _ := newClient(t, srv.URL, nil)
		c.Send(context.Background(), req(rcpt.Reveal()))
		srv.Close()
		if to != real {
			t.Errorf("to = %q, want %q", to, real)
		}
		if to == rcpt.Masked() {
			t.Errorf("the masked form %q reached the provider", to)
		}
	}
}

func TestSend_Headers(t *testing.T) {
	for _, tc := range []struct{ name, header, trace string }{
		{"default", "", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"},
		{"custom", "X-Idempotency-Key", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var h http.Header
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				h = r.Header.Clone()
				accept(w, r)
			}))
			defer srv.Close()
			c, _ := newClient(t, srv.URL, func(c *Config) { c.IdempotencyHeader = tc.header })
			r := req(phone)
			r.TraceParent = tc.trace
			c.Send(context.Background(), r)
			name := tc.header
			if name == "" {
				name = "Idempotency-Key"
			}
			if h.Get(name) != r.NotificationID.String() {
				t.Errorf("%s = %q", name, h.Get(name))
			}
			if h.Get("Content-Type") != "application/json" {
				t.Errorf("content-type = %q", h.Get("Content-Type"))
			}
			if _, ok := h["Traceparent"]; ok != (tc.trace != "") || h.Get("traceparent") != tc.trace {
				t.Errorf("traceparent = %q", h.Get("traceparent"))
			}
		})
	}
}

// The Retry-After header is handed on verbatim, whatever the status; the
// caller decides whether to use it.
func TestSend_RetryAfterCarried(t *testing.T) {
	for _, tc := range []struct {
		status int
		header string
	}{
		{429, "7"},
		{503, "Wed, 21 Oct 2026 07:28:00 GMT"},
		{500, "soon"},
		{429, ""},
	} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if tc.header != "" {
				w.Header().Set("Retry-After", tc.header)
			}
			w.WriteHeader(tc.status)
		}))
		c, _ := newClient(t, srv.URL, nil)
		r := c.Send(context.Background(), req(phone))
		srv.Close()
		if r.StatusCode != tc.status || r.RetryAfter != tc.header || r.Err != nil {
			t.Errorf("status %d, header %q: response = %+v", tc.status, tc.header, r)
		}
	}
}

func TestSend_AcceptedParsed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(accept))
	defer srv.Close()
	c, _ := newClient(t, srv.URL, nil)
	r := c.Send(context.Background(), req(phone))
	if r.StatusCode != 202 || r.MessageID != "m-1" || r.Err != nil || !r.WroteRequest || r.Latency <= 0 {
		t.Fatalf("response = %+v", r)
	}
	if v := promtest.ToFloat64(c.malformed); v != 0 {
		t.Errorf("malformed = %v", v)
	}
	if n := promtest.CollectAndCount(c.unexpected); n != 0 {
		t.Errorf("unexpected series = %d", n)
	}
}

func TestSend_MalformedAccepted(t *testing.T) {
	for _, body := range []string{"not json", `{"status":"accepted"}`, ""} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusAccepted)
			_, _ = io.WriteString(w, body)
		}))
		c, _ := newClient(t, srv.URL, nil)
		r := c.Send(context.Background(), req(phone))
		srv.Close()
		if r.StatusCode != 202 || r.MessageID != "" || r.Err != nil {
			t.Errorf("body %q: response = %+v", body, r)
		}
		if v := promtest.ToFloat64(c.malformed); v != 1 {
			t.Errorf("body %q: malformed = %v", body, v)
		}
	}
}

func TestSend_Other2xx(t *testing.T) {
	for _, code := range []int{200, 204} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(code)
		}))
		c, _ := newClient(t, srv.URL, nil)
		r := c.Send(context.Background(), req(phone))
		srv.Close()
		if r.StatusCode != code || r.Err != nil || r.MessageID != "" {
			t.Errorf("code %d: response = %+v", code, r)
		}
		if v := promtest.ToFloat64(c.unexpected.WithLabelValues("sms", "primary", strconv.Itoa(code))); v != 1 {
			t.Errorf("code %d: counter = %v", code, v)
		}
		if v := promtest.ToFloat64(c.malformed); v != 0 {
			t.Errorf("malformed = %v", v)
		}
	}
}

func TestSend_Non2xxIsAResponse(t *testing.T) {
	for _, code := range []int{400, 500} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(code)
		}))
		c, _ := newClient(t, srv.URL, nil)
		r := c.Send(context.Background(), req(phone))
		srv.Close()
		if r.StatusCode != code || r.Err != nil {
			t.Errorf("code %d: response = %+v", code, r)
		}
	}
}

func TestSend_RedirectNotFollowed(t *testing.T) {
	var hits atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		accept(w, r)
	}))
	defer target.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer srv.Close()
	c, _ := newClient(t, srv.URL, nil)
	r := c.Send(context.Background(), req(phone))
	if r.StatusCode != 302 || r.Err != nil {
		t.Fatalf("response = %+v", r)
	}
	if hits.Load() != 0 {
		t.Fatalf("redirect target received %d requests", hits.Load())
	}
}

// rawServer answers the first request on each server with 200 and closes the
// connection without answering the second ("/drop" scenario of experiment E5).
type rawServer struct {
	ln    net.Listener
	mu    sync.Mutex
	count int
}

func startRaw(t *testing.T) *rawServer {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &rawServer{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go s.serve(c)
		}
	}()
	return s
}

func (s *rawServer) serve(c net.Conn) {
	defer func() { _ = c.Close() }()
	br := bufio.NewReader(c)
	for {
		r, err := http.ReadRequest(br)
		if err != nil {
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		_ = r.Body.Close()
		s.mu.Lock()
		s.count++
		n := s.count
		s.mu.Unlock()
		if n >= 2 {
			return // close without answering
		}
		_, _ = c.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"))
	}
}

func (s *rawServer) requests() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

func TestSend_SentExactlyOnce(t *testing.T) {
	s := startRaw(t)
	c, _ := newClient(t, "http://"+s.ln.Addr().String(), nil)
	c.Send(context.Background(), req(phone)) // warms the pool

	var reused, seen bool
	ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{
		GotConn: func(i httptrace.GotConnInfo) {
			if !seen { // a replay would call GotConn again on a new connection
				seen, reused = true, i.Reused
			}
		},
	})
	r := c.Send(ctx, req(phone))
	if !reused {
		t.Fatal("the first GotConn was not a reused connection; the test proves nothing")
	}
	if got := s.requests(); got != 2 { // one warm-up, one dropped: no replay
		t.Fatalf("server saw %d requests, want 2", got)
	}
	if r.Err == nil || r.StatusCode != 0 || !r.WroteRequest {
		t.Fatalf("response = %+v", r)
	}
	noPII(t, r.Err)
}

func TestSend_ConnectionsReused(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(accept))
	defer srv.Close()
	c, _ := newClient(t, srv.URL, nil)
	for i := range 4 {
		var reused bool
		ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{
			GotConn: func(info httptrace.GotConnInfo) { reused = info.Reused },
		})
		c.Send(ctx, req(phone))
		if want := i > 0; reused != want {
			t.Fatalf("request %d: reused = %v, want %v", i, reused, want)
		}
	}
}

func TestSend_Timeout(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-release }))
	defer srv.Close()
	defer close(release) // runs before srv.Close, which waits for handlers
	c, _ := newClient(t, srv.URL, func(c *Config) { c.Timeout = 20 * time.Millisecond })
	r := c.Send(context.Background(), req(phone))
	if r.StatusCode != 0 {
		t.Fatalf("response = %+v", r)
	}
	noPII(t, r.Err)
}

func TestSend_CallerContextHonoured(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-release }))
	defer srv.Close()
	defer close(release)
	c, _ := newClient(t, srv.URL, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := c.Send(ctx, req(phone))
	noPII(t, r.Err)
}

func TestSend_NoConnection(t *testing.T) {
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	c, _ := newClient(t, "http://"+addr, nil)
	r := c.Send(context.Background(), req(email))
	if r.StatusCode != 0 || r.WroteRequest {
		t.Fatalf("response = %+v", r)
	}
	noPII(t, r.Err)
}

func TestNew_RejectsBadURL(t *testing.T) {
	for _, u := range []string{"", "/relative", "example.com/x", "ftp://example.com/x", "http://"} {
		if _, err := New(Config{URL: u, Name: "p"}, prometheus.NewRegistry()); err == nil {
			t.Errorf("URL %q accepted", u)
		}
	}
	if _, err := New(Config{URL: "https://example.com/x", Name: "p"}, prometheus.NewRegistry()); err != nil {
		t.Errorf("valid URL rejected: %v", err)
	}
}

// The worker builds one client per channel for the same provider on one
// registry: the second client shares the counters of the first.
func TestNew_ClientsShareCountersOnOneRegistry(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	cfg := Config{URL: srv.URL, Name: "primary", ConnectTimeout: time.Second, Timeout: 5 * time.Second}
	reg := prometheus.NewRegistry()
	first, err := New(cfg, reg)
	if err != nil {
		t.Fatal(err)
	}
	second, err := New(cfg, reg)
	if err != nil {
		t.Fatalf("second client on the same registry: %v", err)
	}
	t.Cleanup(first.http.CloseIdleConnections)
	t.Cleanup(second.http.CloseIdleConnections)

	smsReq, emailReq := req(phone), req(email)
	emailReq.Channel = domain.ChannelEmail
	first.Send(context.Background(), smsReq)
	second.Send(context.Background(), emailReq)
	for _, ch := range []string{"sms", "email"} {
		if v := promtest.ToFloat64(first.unexpected.WithLabelValues(ch, "primary", "200")); v != 1 {
			t.Errorf("channel %s: counter = %v, want 1", ch, v)
		}
	}
}
