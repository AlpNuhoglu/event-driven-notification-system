package demo

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
)

// webhookHost is the host of the public webhook service that the default
// provider URL points at. Its URL has a request quota, which the probe reads
// from the service's own API.
const webhookHost = "webhook.site"

// webhookFreeLimit is the number of requests a free webhook.site URL keeps.
const webhookFreeLimit = 50

// DialFunc opens a connection to address (host:port), with TLS when useTLS
// is set, and closes it again. It sends nothing over it.
type DialFunc func(ctx context.Context, address string, useTLS bool) error

// WebhookAPI is the part of the webhook service's own API the probe uses.
// Neither call counts as a request to the URL.
type WebhookAPI interface {
	// Exists reports whether the URL still exists (false: deleted or expired).
	Exists(ctx context.Context, token uuid.UUID) (bool, error)
	// Received returns how many requests the URL has received so far.
	// known is false when the answer does not say.
	Received(ctx context.Context, token uuid.UUID) (total int64, known bool, err error)
}

// Prober checks, before anything is created, that the provider URL can take
// the notifications. It never sends a request to the URL itself.
type Prober struct {
	// Dial checks the connection; the default dials with Timeout.
	Dial DialFunc
	// Timeout bounds the default Dial.
	Timeout time.Duration
	// Webhook is asked about a webhook.site URL.
	Webhook WebhookAPI
}

// Check returns notes to print and an error when nothing may be sent. The
// error text is for a person.
func (p Prober) Check(ctx context.Context, providerURL string, count int) ([]string, error) {
	u, err := url.Parse(providerURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" {
		return nil, errors.New("PROVIDER_URL is not an http or https URL; set it to the URL the provider listens on")
	}
	useTLS := u.Scheme == "https"
	port := u.Port()
	if port == "" {
		port = "80"
		if useTLS {
			port = "443"
		}
	}
	address := net.JoinHostPort(u.Hostname(), port)

	dial := p.Dial
	if dial == nil {
		dial = p.defaultDial
	}
	if err := dial(ctx, address, useTLS); err != nil {
		return nil, fmt.Errorf("the provider at %s cannot be reached (%w); nothing was sent", address, err)
	}
	notes := []string{fmt.Sprintf("provider %s: reachable", address)}

	host := strings.TrimPrefix(strings.ToLower(u.Hostname()), "www.")
	if host != webhookHost {
		return notes, nil
	}
	more, err := p.checkWebhook(ctx, u, count)
	return append(notes, more...), err
}

func (p Prober) defaultDial(ctx context.Context, address string, useTLS bool) error {
	ctx, cancel := context.WithTimeout(ctx, p.Timeout)
	defer cancel()
	d := &net.Dialer{}
	var conn net.Conn
	var err error
	if useTLS {
		host, _, _ := net.SplitHostPort(address)
		td := &tls.Dialer{NetDialer: d, Config: &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12}}
		conn, err = td.DialContext(ctx, "tcp", address)
	} else {
		conn, err = d.DialContext(ctx, "tcp", address)
	}
	if err != nil {
		return err
	}
	return conn.Close()
}

func (p Prober) checkWebhook(ctx context.Context, u *url.URL, count int) ([]string, error) {
	first, _, _ := strings.Cut(strings.TrimPrefix(u.Path, "/"), "/")
	token, err := domain.ParseID(first)
	if err != nil || p.Webhook == nil {
		return []string{"webhook.site: the URL has no token in it, so the number of requests left is unknown; going on"}, nil
	}
	exists, err := p.Webhook.Exists(ctx, token)
	if err != nil {
		return []string{fmt.Sprintf("webhook.site: the URL could not be checked (%v); going on", err)}, nil
	}
	if !exists {
		return nil, errors.New("the webhook.site URL has been deleted or has expired; nothing was sent. " +
			"Open https://webhook.site for a new URL and set PROVIDER_URL to it (for example in .env), then start the stack again")
	}
	total, known, err := p.Webhook.Received(ctx, token)
	if err != nil || !known {
		return []string{"webhook.site: the URL exists; how many requests it has received is unknown; going on"}, nil
	}
	left := max(webhookFreeLimit-total, 0)
	if left < int64(count) {
		return nil, fmt.Errorf("the webhook.site URL has %d requests left (it has received %d of %d) and the demo needs %d; nothing was sent. "+
			"Set PROVIDER_URL to a new URL, or lower --count", left, total, webhookFreeLimit, count)
	}
	return []string{fmt.Sprintf("webhook.site: the URL exists and has %d requests left", left)}, nil
}

// HTTPWebhookAPI is WebhookAPI over HTTP.
type HTTPWebhookAPI struct {
	// BaseURL is the service, e.g. https://webhook.site.
	BaseURL string
	Client  *http.Client
}

// Exists answers 200 as true and 404 as false; any other answer is an error.
func (a HTTPWebhookAPI) Exists(ctx context.Context, token uuid.UUID) (bool, error) {
	resp, err := a.get(ctx, "/token/"+token.String())
	if err != nil {
		return false, err
	}
	defer func() { _ = resp.Body.Close() }()
	switch resp.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		return false, fmt.Errorf("the webhook API answered %d", resp.StatusCode)
	}
}

// Received reads the total member of the first page of the URL's requests.
// A total that is missing or not a whole number is "unknown", not an error.
func (a HTTPWebhookAPI) Received(ctx context.Context, token uuid.UUID) (int64, bool, error) {
	resp, err := a.get(ctx, "/token/"+token.String()+"/requests?per_page=1")
	if err != nil {
		return 0, false, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return 0, false, fmt.Errorf("the webhook API answered %d", resp.StatusCode)
	}
	dec := json.NewDecoder(http.MaxBytesReader(nil, resp.Body, 1<<20))
	dec.UseNumber()
	var body map[string]any
	if err := dec.Decode(&body); err != nil {
		return 0, false, nil //nolint:nilerr // an unreadable body means the total is unknown
	}
	n, ok := body["total"].(json.Number)
	if !ok {
		return 0, false, nil
	}
	if i, err := n.Int64(); err == nil && i >= 0 {
		return i, true, nil
	}
	if f, err := n.Float64(); err == nil && f >= 0 && f == float64(int64(f)) {
		return int64(f), true, nil
	}
	return 0, false, nil
}

func (a HTTPWebhookAPI) get(ctx context.Context, path string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(a.BaseURL, "/")+path, http.NoBody)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	return a.Client.Do(req)
}
