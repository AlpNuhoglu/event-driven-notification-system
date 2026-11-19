// Package demo implements the command "make demo": at most ten
// notifications through a running stack, with a check of the provider URL
// before anything is created, a live view of their status over the
// WebSocket and a summary at the end.
package demo

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/config"
)

// MaxCount is the largest number of notifications the demo creates.
const MaxCount = 10

// The reserved fake values of the demo data.
const (
	fakePhone = "+905551234567"
	fakeEmail = "user@example.com"
	fakePush  = "demo-device-token-0001"
)

// Options configure one run.
type Options struct {
	// API is the base URL of the REST API; Gateway the WebSocket URL.
	API     string
	Gateway string
	// Count is the number of notifications, 1 to MaxCount.
	Count int
	// Timeout bounds the wait for the final statuses.
	Timeout time.Duration
	// ProbeTimeout bounds each connection check and call of the probe, and
	// RequestTimeout each call of the API.
	ProbeTimeout   time.Duration
	RequestTimeout time.Duration
	// ProviderURL is the worker's PROVIDER_URL.
	ProviderURL string
	// WebhookAPI is the base URL of the webhook service's own API.
	WebhookAPI string

	// Prober overrides the probe; Now the clock of the printed times.
	Prober *Prober
	Now    func() time.Time
}

// Validate refuses a count outside 1 to MaxCount.
func (o Options) Validate() error {
	if o.Count < 1 || o.Count > MaxCount {
		return fmt.Errorf("--count must be between 1 and %d (got %d)", MaxCount, o.Count)
	}
	return nil
}

// Main parses args, reads PROVIDER_URL through the platform configuration
// and runs the demo. It returns the exit code.
func Main(ctx context.Context, args []string, lookup func(string) (string, bool), out io.Writer) int {
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	fs.SetOutput(out)
	o := Options{}
	fs.StringVar(&o.API, "api", "http://api:8080", "base URL of the REST API")
	fs.StringVar(&o.Gateway, "gateway", "ws://gateway:8082/v1/ws", "URL of the WebSocket gateway")
	fs.IntVar(&o.Count, "count", 5, fmt.Sprintf("number of notifications (1 to %d)", MaxCount))
	fs.DurationVar(&o.Timeout, "timeout", 30*time.Second, "how long to wait for the final statuses")
	fs.DurationVar(&o.ProbeTimeout, "probe-timeout", 5*time.Second, "timeout of each check of the provider URL")
	fs.DurationVar(&o.RequestTimeout, "request-timeout", 10*time.Second, "timeout of each API call")
	fs.StringVar(&o.WebhookAPI, "webhook-api", "https://webhook.site", "base URL of the webhook.site API")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if err := o.Validate(); err != nil {
		_, _ = fmt.Fprintln(out, err)
		return 2
	}
	cfg, err := config.LoadFrom(lookup)
	if err != nil {
		_, _ = fmt.Fprintf(out, "the configuration cannot be read: %v\n", err)
		return 1
	}
	o.ProviderURL = cfg.Provider.URL
	return Run(ctx, o, out)
}

// plan returns the notification of position i. The channels and priorities
// are mixed so that a few notifications already cover all three channels.
func plan(i int) (createRequest, domain.Recipient) {
	channels := []string{"sms", "email", "push"}
	priorities := []string{"high", "normal", "low"}
	ch := channels[i%3]
	r := createRequest{
		Channel:  ch,
		Priority: priorities[(i+i/3)%3],
		Content:  fmt.Sprintf("demo message %d", i+1),
	}
	switch ch {
	case "sms":
		r.Recipient = fakePhone
	case "email":
		r.Recipient = fakeEmail
	default:
		r.Recipient = fakePush
	}
	return r, domain.Recipient(r.Recipient)
}

// Run executes the four steps and returns the exit code: 0 only when every
// notification ended sent.
func Run(ctx context.Context, o Options, out io.Writer) int {
	say := func(format string, args ...any) { _, _ = fmt.Fprintf(out, format+"\n", args...) }
	if err := o.Validate(); err != nil {
		say("%v", err)
		return 2
	}
	now := o.Now
	if now == nil {
		now = time.Now
	}

	say("Step 1: check the provider")
	prober := o.Prober
	if prober == nil {
		prober = &Prober{
			Timeout: o.ProbeTimeout,
			Webhook: HTTPWebhookAPI{BaseURL: o.WebhookAPI, Client: &http.Client{Timeout: o.ProbeTimeout}},
		}
	}
	notes, err := prober.Check(ctx, o.ProviderURL, o.Count)
	for _, n := range notes {
		say("  %s", n)
	}
	if err != nil {
		say("  %v", err)
		return 1
	}

	say("Step 2: create %d notifications", o.Count)
	api := &apiClient{base: o.API, http: &http.Client{Timeout: o.RequestTimeout}}
	items, ok := createAll(ctx, api, o.Count, say)
	if !ok {
		return 1
	}

	say("Step 3: follow them on the gateway (up to %s)", o.Timeout)
	fctx, cancel := context.WithTimeout(ctx, o.Timeout)
	ferr := follow(fctx, o.Gateway, items, out, now)
	cancel()
	failed := false
	switch {
	case errors.Is(ferr, errTimeout):
		say("Timed out after %s; still pending: %s", o.Timeout, pending(items))
		failed = true
	case ferr != nil:
		say("%v", ferr)
		failed = true
	}

	say("Step 4: summary")
	if !summarize(ctx, api, items, say) {
		failed = true
	}
	say("")
	say("Look further: Grafana http://localhost:3000, Prometheus http://localhost:9090, Jaeger http://localhost:16686 (the default host ports)")
	if failed {
		return 1
	}
	return 0
}

// createAll creates the notifications: the last one through the batch
// endpoint, the others one by one, each with an Idempotency-Key that is the
// same on every run, so a re-run replays instead of sending again.
func createAll(ctx context.Context, api *apiClient, count int, say func(string, ...any)) ([]*item, bool) {
	var items []*item
	replays := 0
	add := func(id string, r createRequest, rcpt domain.Recipient, via string, replayed bool) bool {
		if _, err := domain.ParseID(id); err != nil {
			say("  the API returned an id that is not valid")
			return false
		}
		items = append(items, &item{id: id, channel: r.Channel, priority: r.Priority, recipient: rcpt})
		note := ""
		if replayed {
			replays++
			note = "  (replayed)"
		}
		say("  %s  %-5s %-6s %-16s %s%s", shortID(id), r.Channel, r.Priority, rcpt.Masked(), via, note)
		return true
	}
	for i := range count {
		r, rcpt := plan(i)
		if i == count-1 {
			b, replayed, err := api.createBatch(ctx, fmt.Sprintf("demo-batch-%d", i), []createRequest{r})
			if err != nil {
				say("  %v", err)
				return nil, false
			}
			if len(b.Accepted) != 1 || len(b.Rejected) != 0 {
				say("  the batch was not accepted (accepted %d, rejected %d)", len(b.Accepted), len(b.Rejected))
				return nil, false
			}
			if !add(b.Accepted[0].ID, r, rcpt, "POST /v1/notifications/batch", replayed) {
				return nil, false
			}
			continue
		}
		n, replayed, err := api.create(ctx, fmt.Sprintf("demo-%d", i), r)
		if err != nil {
			say("  %v", err)
			return nil, false
		}
		if !add(n.ID, r, rcpt, "POST /v1/notifications", replayed) {
			return nil, false
		}
	}
	if replays > 0 {
		say("  %d of %d requests were replays: their Idempotency-Key was used before, so the API returned the stored answer and sent nothing again", replays, count)
	}
	return items, true
}

// summarize prints the final state of each notification and the delivery
// figures. It reports whether every notification ended sent.
func summarize(ctx context.Context, api *apiClient, items []*item, say func(string, ...any)) bool {
	allSent := true
	for _, it := range items {
		n, err := api.get(ctx, it.id)
		status, attempts := it.status, "?"
		reason := ""
		if err == nil {
			status, attempts = n.Status, fmt.Sprint(n.AttemptCount)
			if n.FailureReason != nil {
				reason = "  (" + *n.FailureReason + ")"
			}
		} else {
			say("  %s: could not be read: %v", shortID(it.id), err)
		}
		if status != "sent" {
			allSent = false
		}
		say("  %s  %-5s %-6s %-10s attempts %s%s", shortID(it.id), it.channel, it.priority, status, attempts, reason)
	}
	s, err := api.summary(ctx)
	if err != nil {
		say("  metrics summary: %v", err)
		return allSent
	}
	say("  delivery attempts of the last %s (computed %s):", s.Window, s.GeneratedAt)
	for _, c := range s.Channels {
		var parts []string
		for k, v := range c.Attempts {
			if v > 0 {
				parts = append(parts, fmt.Sprintf("%s %d", k, v))
			}
		}
		sort.Strings(parts)
		att := "none"
		if len(parts) > 0 {
			att = strings.Join(parts, ", ")
		}
		rate := "n/a"
		if c.SuccessRate != nil {
			rate = fmt.Sprintf("%.0f%%", *c.SuccessRate*100)
		}
		say("    %-5s attempts: %s; success_rate: %s", c.Channel, att, rate)
	}
	return allSent
}
