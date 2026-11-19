package api

import (
	"context"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/api/apigen"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
)

// Defaults of the summary's options when they are zero: the defaults of
// SUMMARY_WINDOW, SUMMARY_CACHE_TTL and SUMMARY_BACKLOG_CACHE_TTL.
const (
	DefaultSummaryWindow     = 5 * time.Minute
	DefaultSummaryCacheTTL   = 2 * time.Second
	DefaultSummaryBacklogTTL = 10 * time.Second
)

// DefaultLaneHWM is the high-water mark reported for a priority that
// Options.LaneHWM does not name (the defaults of LANE_HWM_*).
var DefaultLaneHWM = map[domain.Priority]int{
	domain.PriorityHigh:   500,
	domain.PriorityNormal: 1000,
	domain.PriorityLow:    500,
}

// summarizer is the part of the store that the metrics summary reads.
type summarizer interface {
	SummaryQueued(ctx context.Context) ([]store.LaneCount, error)
	SummaryBacklogDue(ctx context.Context, now time.Time) ([]store.LaneCount, error)
	SummaryWindow(ctx context.Context, since time.Time) (map[domain.Channel]store.ChannelWindow, error)
}

// summaryService computes the metrics summary from Postgres and caches it:
// the whole answer for cacheTTL, and the due backlog (the most expensive
// figure, a count over the waiting rows) for the longer backlogTTL. Requests
// that find no fresh answer share one computation (singleflight).
type summaryService struct {
	src        summarizer
	now        func() time.Time
	window     time.Duration
	cacheTTL   time.Duration
	backlogTTL time.Duration
	dbTimeout  time.Duration
	hwm        map[domain.Priority]int

	flight singleflight.Group

	mu        sync.Mutex
	result    *apigen.MetricsSummary
	resultAt  time.Time
	backlog   []store.LaneCount
	backlogAt time.Time
	// haveBacklog tells that backlog holds a computed value (an empty one
	// is a value).
	haveBacklog bool
}

// get returns the cached summary when it is younger than cacheTTL and
// computes a new one otherwise. A failed computation is not cached.
func (s *summaryService) get(ctx context.Context) (apigen.MetricsSummary, error) {
	if v, ok := s.cached(); ok {
		return v, nil
	}
	v, err, _ := s.flight.Do("summary", func() (any, error) {
		// A flight that finished just before this one started has filled
		// the cache already.
		if v, ok := s.cached(); ok {
			return v, nil
		}
		// The computation serves every waiting request, so one caller's
		// cancellation must not fail it; the time budget still applies.
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.dbTimeout)
		defer cancel()
		return s.compute(ctx)
	})
	if err != nil {
		return apigen.MetricsSummary{}, err
	}
	return v.(apigen.MetricsSummary), nil
}

func (s *summaryService) cached() (apigen.MetricsSummary, bool) {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.result != nil && now.Sub(s.resultAt) < s.cacheTTL {
		return *s.result, true
	}
	return apigen.MetricsSummary{}, false
}

func (s *summaryService) compute(ctx context.Context) (apigen.MetricsSummary, error) {
	now := s.now()
	queued, err := s.src.SummaryQueued(ctx)
	if err != nil {
		return apigen.MetricsSummary{}, err
	}
	backlog, err := s.backlogDue(ctx, now)
	if err != nil {
		return apigen.MetricsSummary{}, err
	}
	win, err := s.src.SummaryWindow(ctx, now.Add(-s.window))
	if err != nil {
		return apigen.MetricsSummary{}, err
	}
	out := apigen.MetricsSummary{
		GeneratedAt: now.UTC().Truncate(time.Second),
		Window:      formatWindow(s.window),
		Lanes:       s.lanes(queued, backlog),
		Channels:    make([]apigen.ChannelSummary, 0, len(domain.Channels())),
	}
	for _, c := range domain.Channels() {
		out.Channels = append(out.Channels, channelSummary(c, win[c]))
	}
	s.mu.Lock()
	s.result, s.resultAt = &out, now
	s.mu.Unlock()
	return out, nil
}

// backlogDue returns the due backlog per lane, reusing the previous values
// while they are younger than backlogTTL.
func (s *summaryService) backlogDue(ctx context.Context, now time.Time) ([]store.LaneCount, error) {
	s.mu.Lock()
	if s.haveBacklog && now.Sub(s.backlogAt) < s.backlogTTL {
		b := s.backlog
		s.mu.Unlock()
		return b, nil
	}
	s.mu.Unlock()
	b, err := s.src.SummaryBacklogDue(ctx, now)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.backlog, s.backlogAt, s.haveBacklog = b, now, true
	s.mu.Unlock()
	return b, nil
}

// lanes lists all nine lanes in the order sms, email, push by high, normal,
// low, with zero for a lane the counts do not mention.
func (s *summaryService) lanes(queued, backlog []store.LaneCount) []apigen.LaneSummary {
	type lane struct {
		c domain.Channel
		p domain.Priority
	}
	q := make(map[lane]int64, len(queued))
	for _, r := range queued {
		q[lane{r.Channel, r.Priority}] = r.Count
	}
	b := make(map[lane]int64, len(backlog))
	for _, r := range backlog {
		b[lane{r.Channel, r.Priority}] = r.Count
	}
	out := make([]apigen.LaneSummary, 0, 9)
	for _, c := range domain.Channels() {
		for _, p := range domain.Priorities() {
			k := lane{c, p}
			out = append(out, apigen.LaneSummary{
				Channel:    apigen.Channel(c),
				Priority:   apigen.Priority(p.String()),
				Queued:     q[k],
				Hwm:        int32(s.hwm[p]), //nolint:gosec // a configured mark is far below the int32 range
				BacklogDue: b[k],
			})
		}
	}
	return out
}

func channelSummary(c domain.Channel, w store.ChannelWindow) apigen.ChannelSummary {
	n := func(o domain.Outcome) int64 { return w.Outcomes[o] }
	var finished int64
	for _, v := range w.Outcomes {
		finished += v
	}
	denom := finished - n(domain.OutcomeAborted)
	var rate *float64
	if denom > 0 {
		r := float64(n(domain.OutcomeSuccess)) / float64(denom)
		rate = &r
	}
	return apigen.ChannelSummary{
		Channel: apigen.Channel(c),
		Attempts: apigen.AttemptCounts{
			Success:       n(domain.OutcomeSuccess),
			Retryable:     n(domain.OutcomeRetryable),
			Permanent:     n(domain.OutcomePermanent),
			ProviderFatal: n(domain.OutcomeProviderFatal),
			Throttled:     n(domain.OutcomeThrottled),
			Unknown:       n(domain.OutcomeUnknown),
			Unreachable:   n(domain.OutcomeUnreachable),
			Aborted:       n(domain.OutcomeAborted),
			InternalError: n(domain.OutcomeInternalError),
		},
		SuccessRate:       rate,
		ProviderLatencyMs: apigen.Percentiles{P50: w.ProviderLatency.P50, P95: w.ProviderLatency.P95},
		EndToEndLatencyMs: apigen.Percentiles{P50: w.EndToEnd.P50, P95: w.EndToEnd.P95},
	}
}

// formatWindow writes a duration in its short form: "5m", "90s", "1h".
func formatWindow(d time.Duration) string {
	s := d.String()
	if strings.HasSuffix(s, "m0s") {
		s = strings.TrimSuffix(s, "0s")
	}
	if strings.HasSuffix(s, "h0m") {
		s = strings.TrimSuffix(s, "0m")
	}
	return s
}

// GetMetricsSummary answers the delivery figures of the last window.
func (h *handlers) GetMetricsSummary(ctx context.Context, _ apigen.GetMetricsSummaryRequestObject) (apigen.GetMetricsSummaryResponseObject, error) {
	v, err := h.summary.get(ctx)
	if err != nil {
		return nil, storeProblem(err)
	}
	return apigen.GetMetricsSummary200JSONResponse{
		Body:    v,
		Headers: apigen.GetMetricsSummary200ResponseHeaders{XRequestID: stateFrom(ctx).correlationID},
	}, nil
}
