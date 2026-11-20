package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/trace"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

// maxBody bounds how much of a response body is read.
const maxBody = 64 << 10

// MaxErrorBody bounds how much of the body of an answer that is not a 2xx
// is kept (Response.ErrorBody). It is several times what is stored (1 KB),
// so that the values to remove from it are still whole when it is cut.
const MaxErrorBody = 8 << 10

// Config configures a Client.
type Config struct {
	// URL is the provider endpoint (PROVIDER_URL); absolute http or https.
	URL string
	// Name is the provider's name, used as a metric label (PROVIDER_NAME).
	Name string
	// IdempotencyHeader is the header carrying the notification id.
	IdempotencyHeader string
	// ConnectTimeout bounds the TCP connect.
	ConnectTimeout time.Duration
	// Timeout is the total time of one request.
	Timeout time.Duration
	// MaxIdleConnsPerHost is the per-channel concurrency of the worker.
	MaxIdleConnsPerHost int
	// TracerProvider records the provider call's span; the global provider
	// when nil.
	TracerProvider trace.TracerProvider
}

// Request is one delivery attempt.
type Request struct {
	// NotificationID is sent as the idempotency header value.
	NotificationID uuid.UUID
	// Channel is the delivery channel.
	Channel domain.Channel
	// To is the real recipient value (from domain.Recipient.Reveal).
	To string
	// Content is the rendered message.
	Content string
}

// Response is what one attempt produced.
type Response struct {
	// StatusCode is the HTTP status; 0 when no response was received.
	StatusCode int
	// MessageID is the "messageId" of a parsed 202 body; empty otherwise.
	MessageID string
	// Latency runs from just before the request until the body was drained.
	Latency time.Duration
	// Err is the transport error; nil when a response was received.
	Err error
	// WroteRequest reports that the request may have reached the provider
	// completely: the transport read the request's body to the end and
	// reported no failed write (written). It is true whenever the provider
	// can have the whole request, also when the call then fails.
	WroteRequest bool
	// RetryAfter is the response's Retry-After header, verbatim; empty when
	// the response has none. The caller decides whether to use it.
	RetryAfter string
	// ErrorBody is the body of a response that is not a 2xx, at most
	// MaxErrorBody bytes of it; ErrorBodyCut reports that the body was
	// longer. It is the provider's own text and can quote the request: the
	// caller removes the message's values from it before it stores it, and
	// nothing logs it (ADR-0009 §4).
	ErrorBody    string
	ErrorBodyCut bool
}

// Client sends notifications to one provider.
type Client struct {
	cfg        Config
	http       *http.Client
	malformed  prometheus.Counter
	unexpected *prometheus.CounterVec
}

// payload is the exact wire body: three fields, nothing else.
type payload struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

type accepted struct {
	MessageID string `json:"messageId"`
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
}

// New validates the configuration and builds a Client with its own
// transport. Its two counters are registered on reg.
func New(cfg Config, reg prometheus.Registerer) (*Client, error) {
	u, err := url.Parse(cfg.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, errors.New("provider: URL must be an absolute http or https URL")
	}
	if cfg.IdempotencyHeader == "" {
		cfg.IdempotencyHeader = "Idempotency-Key"
	}
	idle := max(cfg.MaxIdleConnsPerHost, 1)

	tr := http.DefaultTransport.(*http.Transport).Clone() //nolint:forcetypeassert // the default transport is an *http.Transport
	tr.DialContext = (&net.Dialer{Timeout: cfg.ConnectTimeout, KeepAlive: 30 * time.Second}).DialContext
	tr.MaxIdleConnsPerHost = idle
	tr.MaxIdleConns = idle

	c := &Client{
		cfg: cfg,
		http: &http.Client{
			Transport: newTracedTransport(tr, cfg.TracerProvider, cfg.Name),
			Timeout:   cfg.Timeout,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		malformed: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "provider_malformed_response_total",
			Help:        "202 responses whose body had no parseable messageId.",
			ConstLabels: prometheus.Labels{"provider": cfg.Name},
		}),
		unexpected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "provider_unexpected_2xx_total",
			Help: "2xx responses other than 202.",
		}, []string{"channel", "provider", "code"}),
	}
	if reg != nil {
		if c.malformed, err = register(reg, c.malformed); err != nil {
			return nil, fmt.Errorf("provider: register metrics: %w", err)
		}
		if c.unexpected, err = register(reg, c.unexpected); err != nil {
			return nil, fmt.Errorf("provider: register metrics: %w", err)
		}
	}
	return c, nil
}

// register registers col on reg. The worker builds one client per channel
// for the same provider, so a collector that is already registered is not an
// error: the existing one is returned and shared.
func register[T prometheus.Collector](reg prometheus.Registerer, col T) (T, error) {
	err := reg.Register(col)
	var already prometheus.AlreadyRegisteredError
	if errors.As(err, &already) {
		if existing, ok := already.ExistingCollector.(T); ok {
			return existing, nil
		}
	}
	return col, err
}

// InitSeries creates the series of provider_unexpected_2xx_total of
// channel ch at zero: one per standard 2xx status other than 202, the fixed
// set the code label takes in practice.
func (c *Client) InitSeries(ch domain.Channel) {
	for code := 200; code <= 299; code++ {
		if code != http.StatusAccepted && http.StatusText(code) != "" {
			c.unexpected.WithLabelValues(string(ch), c.cfg.Name, strconv.Itoa(code))
		}
	}
}

// sentBody is the body of one request. It records when the transport has
// read it to the end, which is before the last of it can be on the wire.
type sentBody struct {
	r    *bytes.Reader
	left atomic.Int64 // bytes the transport has not read yet
}

func newSentBody(b []byte) *sentBody {
	s := &sentBody{r: bytes.NewReader(b)}
	s.left.Store(int64(len(b)))
	return s
}

// Read counts the bytes before it hands them to the transport.
func (s *sentBody) Read(p []byte) (int, error) {
	n, err := s.r.Read(p)
	if n > 0 {
		s.left.Add(-int64(n))
	}
	return n, err //nolint:wrapcheck // io.Reader: io.EOF must stay io.EOF
}

// Close implements io.Closer; there is nothing to release.
func (s *sentBody) Close() error { return nil }

// readToEnd reports that the transport has read the whole body.
func (s *sentBody) readToEnd() bool { return s.left.Load() == 0 }

// written decides whether a request counts as written, which is what the
// classification asks when a call fails without a response: written means
// the provider may have the request (unknown), not written means it cannot
// (unreachable; ADR-0007 section 2).
//
// The rule: the transport has read the body to the end, and has not
// reported a failed write. The provider cannot have the whole request before
// its last bytes were read from the body, whatever the transport buffers and
// whichever protocol it speaks, so the first part is true whenever the
// provider can have the request. The second part keeps a write that failed
// (a broken pipe on a kept-alive connection) at "not written"; if the
// transport's report has not arrived when the call returns, the request
// counts as written, which is the safe side.
//
// Until P6 the rule was the transport's WroteRequest hook alone: written when
// the hook had fired without an error. That is only right while the hook
// runs before the request leaves, and net/http (Go 1.27) does that in one
// case only: HTTP/1, a body it knows to be in memory, and a request smaller
// than the connection's write buffer (4096 bytes). Measured in P6 with a
// server that reads the request while the hook is held: a request of 4095
// bytes had not left; one of exactly 4096 bytes had (a buffer that the copy
// of the body fills exactly is flushed at once); so had every larger one,
// the 100 KB e-mail included (once the buffer is full, the rest of the body
// goes to the connection directly). A body net/http does not know to be in
// memory, which is what the tracing transport hands it, is sent as it is
// read, and over HTTP/2 the hook is called after the body's frames. In all
// of these a provider could answer, or close the connection, before the hook
// had run, and a call that failed then was reported as not written.
func written(body *sentBody, writeFailed *atomic.Bool) bool {
	return body.readToEnd() && !writeFailed.Load()
}

// Name returns the provider name.
func (c *Client) Name() string { return c.cfg.Name }

// Send performs exactly one request and reports the result. It never
// classifies the outcome. The request carries the traceparent of its client
// span, a child of the span in ctx.
func (c *Client) Send(ctx context.Context, r Request) Response {
	ctx = withSpanAttrs(ctx, r)
	body, err := json.Marshal(payload{To: r.To, Channel: string(r.Channel), Content: r.Content})
	if err != nil {
		return Response{Err: errors.New("provider: encode request")}
	}
	// Whether the request counts as written: see written. Both of its parts
	// are set on the transport's goroutines, which can still be running when
	// Do returns, so they are atomic.
	sent := newSentBody(body)
	var writeFailed atomic.Bool
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		WroteRequest: func(i httptrace.WroteRequestInfo) {
			if i.Err != nil {
				writeFailed.Store(true)
			}
		},
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.URL, sent)
	if err != nil {
		return Response{Err: errors.New("provider: build request")}
	}
	// net/http knows the length only of its own in-memory readers.
	req.ContentLength = int64(len(body))
	// One attempt is exactly one request: without GetBody the transport
	// cannot replay the request on a broken keep-alive connection.
	req.GetBody = nil
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(c.cfg.IdempotencyHeader, r.NotificationID.String())

	start := time.Now()
	resp, err := c.http.Do(req)
	if err != nil {
		return Response{Latency: time.Since(start), Err: err, WroteRequest: written(sent, &writeFailed)}
	}
	defer func() { _ = resp.Body.Close() }()

	out := Response{
		StatusCode:   resp.StatusCode,
		WroteRequest: written(sent, &writeFailed),
		RetryAfter:   resp.Header.Get("Retry-After"),
	}
	switch {
	case resp.StatusCode == http.StatusAccepted:
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxBody))
		var a accepted
		if json.Unmarshal(raw, &a) != nil || a.MessageID == "" {
			c.malformed.Inc()
		} else {
			out.MessageID = a.MessageID
		}
	case resp.StatusCode/100 == 2:
		c.unexpected.WithLabelValues(string(r.Channel), c.cfg.Name, strconv.Itoa(resp.StatusCode)).Inc()
	default:
		// One byte more than is kept tells a longer body from one that fits.
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, MaxErrorBody+1))
		if len(raw) > MaxErrorBody {
			raw, out.ErrorBodyCut = raw[:MaxErrorBody], true
		}
		out.ErrorBody = string(raw)
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxBody))
	out.Latency = time.Since(start)
	return out
}
