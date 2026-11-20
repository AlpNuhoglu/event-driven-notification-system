package provider

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
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
	"go.opentelemetry.io/otel/trace"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/tracing"
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

// accept answers as a provider does, after it has read the request. A server
// that answers on the headers alone can be faster than the client's write of
// the body, and the response then carries WroteRequest false, which no rule
// reads when there is a response (seen in CI in P6).
func accept(w http.ResponseWriter, r *http.Request) {
	_, _ = io.Copy(io.Discard, r.Body)
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

// The provider request is one of the three paths that carry the real
// recipient (ADR-0009 §4): the body on the wire is compared byte for byte
// with the body the real value gives.
func TestSend_BodyCarriesTheRealRecipientByteForByte(t *testing.T) {
	for _, real := range []string{phone, email, "device-token-0123456789abcdef"} {
		var got []byte
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got, _ = io.ReadAll(r.Body)
			accept(w, r)
		}))
		c, _ := newClient(t, srv.URL, nil)
		c.Send(context.Background(), req(domain.Recipient(real).Reveal()))
		srv.Close()
		want := []byte(`{"to":"` + real + `","channel":"sms","content":"` + content + `"}`)
		if string(got) != string(want) {
			t.Errorf("body %d bytes, want the %d bytes of the real value", len(got), len(want))
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
			// The header carries the context of the span in the request's
			// context; the global provider records nothing here, so the
			// client span keeps its parent's context.
			ctx := context.Background()
			if tc.trace != "" {
				ctx = trace.ContextWithRemoteSpanContext(ctx, tracing.ParseTraceParent(tc.trace))
			}
			c.Send(ctx, r)
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

// The body of an answer that is not a 2xx is handed on, up to MaxErrorBody
// bytes, with a mark when it was longer; the body of a 2xx is not.
func TestSend_ErrorBodyIsKeptUpToItsLimit(t *testing.T) {
	long := strings.Repeat("x", MaxErrorBody+100)
	cases := []struct {
		name     string
		code     int
		body     string
		wantBody string
		wantCut  bool
	}{
		{"a 400 with a body", 400, `{"error":"invalid number"}`, `{"error":"invalid number"}`, false},
		{"a 503 without a body", 503, "", "", false},
		{"a body of exactly the limit", 500, long[:MaxErrorBody], long[:MaxErrorBody], false},
		{"a longer body", 500, long, long[:MaxErrorBody], true},
		{"a 202", 202, `{"messageId":"m-1"}`, "", false},
		{"another 2xx", 200, "ok, whatever", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.code)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			c, _ := newClient(t, srv.URL, nil)
			r := c.Send(context.Background(), req(phone))
			if r.StatusCode != tc.code || r.Err != nil || r.ErrorBody != tc.wantBody || r.ErrorBodyCut != tc.wantCut {
				t.Errorf("status %d, err %v, %d bytes of error body (cut %v); want %d bytes (cut %v)",
					r.StatusCode, r.Err, len(r.ErrorBody), r.ErrorBodyCut, len(tc.wantBody), tc.wantCut)
			}
		})
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

// countingListener counts the bytes its connections receive.
type countingListener struct {
	net.Listener
	read *atomic.Int64
}

func (l countingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err //nolint:wrapcheck // a listener's error is passed through
	}
	return countingConn{c, l.read}, nil
}

type countingConn struct {
	net.Conn
	read *atomic.Int64
}

func (c countingConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.read.Add(int64(n))
	return n, err //nolint:wrapcheck // io.Reader
}

// dropAfterRequest starts a server that reads each request to its end and
// then closes the connection without an answer. got receives the number of
// bytes of the request as it arrived on the wire.
func dropAfterRequest(t *testing.T) (url string, got <-chan int64) {
	t.Helper()
	var read atomic.Int64
	ch := make(chan int64, 1)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		ch <- read.Swap(0)
		conn, _, err := w.(http.Hijacker).Hijack() //nolint:forcetypeassert // an HTTP/1 test server
		if err == nil {
			_ = conn.Close()
		}
	}))
	srv.Listener = countingListener{srv.Listener, &read}
	srv.Start()
	t.Cleanup(srv.Close)
	return srv.URL, ch
}

func emailOf(contentBytes int) Request {
	return Request{
		NotificationID: uuid.New(), Channel: domain.ChannelEmail, To: "user@example.com",
		Content: strings.Repeat("a", contentBytes),
	}
}

// The provider cannot have the whole request before the client reports it
// as written: a call that fails right after the provider received the
// request must be WroteRequest true (unknown, not unreachable). Each server
// here reads the whole request and then closes the connection without an
// answer. The sizes are the ones at which the old rule, the transport's
// WroteRequest hook alone, was measured to be late (written): a request of
// exactly the transport's write buffer, one byte more, and the largest
// e-mail the API accepts, beside an ordinary sms, at which it was not.
func TestSend_ProviderCannotHaveTheRequestBeforeWroteRequestIsReported(t *testing.T) {
	const writeBuffer = 4096 // net/http's default; the client does not change it

	// The size of a request on the wire, to choose the content that makes it
	// exactly the buffer: the headers do not change with the content as long
	// as the number of digits of Content-Length stays.
	const probeContent = 3000
	url, got := dropAfterRequest(t)
	c, _ := newClient(t, url, nil)
	c.Send(context.Background(), emailOf(probeContent))
	exact := probeContent + writeBuffer - int(<-got)
	if exact < 1000 || exact > 9999 {
		t.Fatalf("content of %d bytes for a request of %d: the Content-Length would change its digits", exact, writeBuffer)
	}

	cases := []struct {
		name     string
		req      Request
		wantWire int64 // 0: not checked
	}{
		{"an sms", req(phone), 0},
		{"a request of exactly the write buffer", emailOf(exact), writeBuffer},
		{"one byte more than the write buffer", emailOf(exact + 1), writeBuffer + 1},
		{"the largest e-mail the API accepts", emailOf(domain.MaxEmailContentBytes), 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for range 20 {
				url, got := dropAfterRequest(t)
				c, _ := newClient(t, url, nil)
				r := c.Send(context.Background(), tc.req)
				wire := <-got
				if tc.wantWire != 0 && wire != tc.wantWire {
					t.Fatalf("the request was %d bytes on the wire, want %d: the case does not test what it names", wire, tc.wantWire)
				}
				if r.Err == nil || r.StatusCode != 0 {
					t.Fatalf("response = %+v, want an error without a status: the server closes the connection", r)
				}
				if !r.WroteRequest {
					t.Fatalf("the server had read the whole request (%d bytes) and the call reports it as not written: %+v", wire, r)
				}
			}
		})
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// What decides WroteRequest, on a transport the test controls: the body read
// to its end, and no failed write reported. The first case is the one the
// transport's hook alone got wrong: the call returns before the hook has
// run.
func TestSend_WrittenWhenTheBodyWasReadToTheEnd(t *testing.T) {
	errGone := errors.New("connection gone")
	cases := []struct {
		name      string
		transport func(body []byte, r *http.Request)
		want      bool
	}{
		{"the body was read to the end and the transport's hook has not run", func(_ []byte, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
		}, true},
		{"the body was read to the end and the hook reports a write without error", func(_ []byte, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			httptrace.ContextClientTrace(r.Context()).WroteRequest(httptrace.WroteRequestInfo{})
		}, true},
		{"the body was read to the end and the hook reports a failed write", func(_ []byte, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			httptrace.ContextClientTrace(r.Context()).WroteRequest(httptrace.WroteRequestInfo{Err: errGone})
		}, false},
		{"all of the body but its last byte was read", func(body []byte, r *http.Request) {
			_, _ = io.ReadFull(r.Body, make([]byte, len(body)-1))
		}, false},
		{"nothing of the body was read", func([]byte, *http.Request) {}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, _ := newClient(t, "http://provider.invalid/", nil)
			request := emailOf(10_000)
			body, err := json.Marshal(payload{To: request.To, Channel: string(request.Channel), Content: request.Content})
			if err != nil {
				t.Fatal(err)
			}
			c.http.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
				if r.ContentLength != int64(len(body)) {
					t.Errorf("Content-Length %d, want %d", r.ContentLength, len(body))
				}
				tc.transport(body, r)
				return nil, errGone
			})
			r := c.Send(context.Background(), request)
			if r.Err == nil || r.StatusCode != 0 {
				t.Fatalf("response = %+v, want the transport's error", r)
			}
			if r.WroteRequest != tc.want {
				t.Errorf("WroteRequest = %v, want %v", r.WroteRequest, tc.want)
			}
		})
	}
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

// InitSeries creates provider_unexpected_2xx_total of the channel at zero
// for every standard 2xx status but 202, which is not unexpected.
func TestInitSeries_UnexpectedSuccessCodesExistAtZero(t *testing.T) {
	c, reg := newClient(t, "http://127.0.0.1:1", nil)
	c.InitSeries(domain.ChannelEmail)
	if n, err := promtest.GatherAndCount(reg, "provider_unexpected_2xx_total"); err != nil || n != 9 {
		t.Errorf("gathered %d series (%v), want 9 (200, 201, 203 to 208, 226)", n, err)
	}
	if v := promtest.ToFloat64(c.unexpected.WithLabelValues("email", "primary", "204")); v != 0 {
		t.Errorf("204 = %v, want 0", v)
	}
	if n := promtest.CollectAndCount(c.unexpected); n != 9 {
		t.Errorf("%d series after reading one, want 9", n)
	}
}
