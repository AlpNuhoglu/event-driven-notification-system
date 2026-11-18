// Package config loads and validates the service configuration from environment
// variables. The variables and their defaults are specified by .env.example.
package config

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Retry is one retry profile: back-off, failure budget and default TTL.
type Retry struct {
	Base        time.Duration
	Cap         time.Duration
	MaxFailures int
	TTL         time.Duration
}

// Provider configures the external notification provider.
type Provider struct {
	URL               string
	IdempotencyHeader string
	Name              string
	ConnectTimeout    time.Duration
	Timeout           time.Duration
}

// Connections holds the connection URLs of the backing services.
type Connections struct {
	DatabaseURL string
	RabbitMQURL string
	RedisURL    string
	// AMQPRecoveryRetryInterval and AMQPRecoveryMaxRetries are the budget of
	// the RabbitMQ client's automatic connection recovery: the wait before
	// each reconnection attempt and the number of attempts (D5).
	AMQPRecoveryRetryInterval time.Duration
	AMQPRecoveryMaxRetries    int
}

// Ports holds the listen addresses.
type Ports struct {
	APIAddr     string
	AdminAddr   string
	GatewayAddr string
	// MockProviderAddr is the listen address of the mock provider.
	MockProviderAddr string
}

// API configures the REST API.
type API struct {
	MaxBodyBytes          int64
	IdempotencyKeyTTL     time.Duration
	PhoneValidation       string
	SchedulePastTolerance time.Duration
	ScheduleMaxHorizon    time.Duration
	ExpiresAtMinLead      time.Duration
	SummaryWindow         time.Duration
	SummaryCacheTTL       time.Duration
	SummaryBacklogTTL     time.Duration
}

// Relay configures the outbox relay and its housekeeping passes.
type Relay struct {
	TickInterval      time.Duration
	BatchSize         int
	ConfirmTimeout    time.Duration
	LaneHWMHigh       int
	LaneHWMNormal     int
	LaneHWMLow        int
	ExpiryInterval    time.Duration
	ExpiryBatchSize   int
	LeaseSweepEvery   time.Duration
	LeaseSweepBatch   int
	ReconcileMinEvery time.Duration
	BacklogGaugeEvery time.Duration
}

// Worker configures the delivery workers.
type Worker struct {
	Channels            []string
	Replicas            int
	Concurrency         int
	LeaseDuration       time.Duration
	FinalWriteRetryTime time.Duration
	ShutdownDrain       time.Duration
	ConsumerTimeout     time.Duration
}

// RateLimit configures per-channel pacing.
type RateLimit struct {
	PerSecond     int
	Horizon       time.Duration
	RetryInterval time.Duration
	// SlotTolerance is how late a send may start after its reserved slot.
	// Beyond it the slot is given up and a new one is reserved (D77).
	SlotTolerance time.Duration
}

// RetryConfig holds the retry profiles and shared delay bounds.
type RetryConfig struct {
	High                 Retry
	Normal               Retry
	Low                  Retry
	MinDelay             time.Duration
	AfterCap             time.Duration
	ThrottleDefaultDelay time.Duration
	ThrottleMaxDelay     time.Duration
}

// Breaker configures the circuit breaker.
type Breaker struct {
	Window              time.Duration
	MinRequests         int
	FailureRatio        float64
	ConsecutiveFailures int
	OpenBase            time.Duration
	OpenMax             time.Duration
	FatalOpen           time.Duration
}

// Events configures status events and the WebSocket gateway.
type Events struct {
	BufferSize         int
	QueueMaxLength     int
	QueueMessageTTL    time.Duration
	WSOriginPatterns   []string
	WSSendQueueSize    int
	WSMaxSubscriptions int
	WSPingInterval     time.Duration
}

// Templates configures template size limits.
type Templates struct {
	MaxBodyBytes   int64
	MaxOutputBytes int64
}

// Observability configures logging and tracing.
type Observability struct {
	LogLevel         string
	OTLPEndpoint     string
	TracesSampler    string
	TracesSamplerArg float64
}

// Config is the complete service configuration.
type Config struct {
	Provider      Provider
	Connections   Connections
	Ports         Ports
	API           API
	Relay         Relay
	Worker        Worker
	RateLimit     RateLimit
	Retry         RetryConfig
	Breaker       Breaker
	Events        Events
	Templates     Templates
	Observability Observability
}

// loader reads variables through lookup, records every name it reads and
// collects validation errors. Error messages never contain a value.
type loader struct {
	lookup func(string) (string, bool)
	keys   []string
	errs   []error
}

func (l *loader) fail(name, msg string) {
	l.errs = append(l.errs, fmt.Errorf("%s: %s", name, msg))
}

// raw returns the trimmed value, or def when the variable is unset or empty.
func (l *loader) raw(name, def string) (string, bool) {
	l.keys = append(l.keys, name)
	v, ok := l.lookup(name)
	v = strings.TrimSpace(v)
	if !ok || v == "" {
		return def, false
	}
	return v, true
}

func (l *loader) str(name, def string) string {
	v, _ := l.raw(name, def)
	return v
}

func (l *loader) dur(name string, def time.Duration) time.Duration {
	v, set := l.raw(name, "")
	if !set {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		l.fail(name, "not a valid duration")
		return def
	}
	if d <= 0 {
		l.fail(name, "must be greater than zero")
		return def
	}
	return d
}

func (l *loader) int64v(name string, def int64) int64 {
	v, set := l.raw(name, "")
	if !set {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		l.fail(name, "not a valid integer")
		return def
	}
	if n <= 0 {
		l.fail(name, "must be greater than zero")
		return def
	}
	return n
}

func (l *loader) intv(name string, def int) int {
	return int(l.int64v(name, int64(def)))
}

// float reads a float64 that must lie within the given bounds.
func (l *loader) float(name string, def, lo, hi float64, loOpen bool) float64 {
	v, set := l.raw(name, "")
	if !set {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || math.IsNaN(f) {
		l.fail(name, "not a valid number")
		return def
	}
	if f < lo || f > hi || (loOpen && f == lo) {
		l.fail(name, "out of range")
		return def
	}
	return f
}

func (l *loader) oneOf(name, def string, allowed ...string) string {
	v, _ := l.raw(name, def)
	if !slices.Contains(allowed, v) {
		l.fail(name, "unsupported value")
		return def
	}
	return v
}

func (l *loader) list(name string, def []string) []string {
	v, set := l.raw(name, "")
	if !set {
		return def
	}
	var out []string
	for _, p := range strings.Split(v, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func (l *loader) channels(name string, def []string) []string {
	v, set := l.raw(name, "")
	if !set {
		return def
	}
	var out []string
	for _, p := range strings.Split(v, ",") {
		p = strings.TrimSpace(p)
		if !slices.Contains([]string{"sms", "email", "push"}, p) {
			l.fail(name, "must list only sms, email and push, separated by commas")
			return def
		}
		if slices.Contains(out, p) {
			l.fail(name, "contains a duplicate channel")
			return def
		}
		out = append(out, p)
	}
	return out
}

func (l *loader) retry(prefix, ttl string, def Retry) Retry {
	return Retry{
		Base:        l.dur("RETRY_"+prefix+"_BASE", def.Base),
		Cap:         l.dur("RETRY_"+prefix+"_CAP", def.Cap),
		MaxFailures: l.intv("RETRY_"+prefix+"_MAX_FAILURES", def.MaxFailures),
		TTL:         l.dur(ttl, def.TTL),
	}
}

func (l *loader) load() Config {
	var c Config
	c.Provider = Provider{
		URL:               l.str("PROVIDER_URL", ""),
		IdempotencyHeader: l.str("PROVIDER_IDEMPOTENCY_HEADER", "Idempotency-Key"),
		Name:              l.str("PROVIDER_NAME", "primary"),
		ConnectTimeout:    l.dur("PROVIDER_CONNECT_TIMEOUT", 2*time.Second),
		Timeout:           l.dur("PROVIDER_TIMEOUT", 10*time.Second),
	}
	c.Connections = Connections{
		DatabaseURL: l.str("DATABASE_URL", "postgres://notifications:notifications@postgres:5432/notifications?sslmode=disable"),
		RabbitMQURL: l.str("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/"),
		RedisURL:    l.str("REDIS_URL", "redis://redis:6379/0"),

		AMQPRecoveryRetryInterval: l.dur("AMQP_RECOVERY_RETRY_INTERVAL", 5*time.Second),
		AMQPRecoveryMaxRetries:    l.intv("AMQP_RECOVERY_MAX_RETRIES", 24),
	}
	c.Ports = Ports{
		APIAddr:          l.str("API_ADDR", ":8080"),
		AdminAddr:        l.str("ADMIN_ADDR", ":8081"),
		GatewayAddr:      l.str("GATEWAY_ADDR", ":8082"),
		MockProviderAddr: l.str("MOCKPROVIDER_ADDR", ":8090"),
	}
	c.API = API{
		MaxBodyBytes:          l.int64v("MAX_BODY_BYTES", 5242880),
		IdempotencyKeyTTL:     l.dur("IDEMPOTENCY_KEY_TTL", 24*time.Hour),
		PhoneValidation:       l.oneOf("PHONE_VALIDATION", "possible", "possible", "strict"),
		SchedulePastTolerance: l.dur("SCHEDULE_PAST_TOLERANCE", 30*time.Second),
		ScheduleMaxHorizon:    l.dur("SCHEDULE_MAX_HORIZON", 720*time.Hour),
		ExpiresAtMinLead:      l.dur("EXPIRES_AT_MIN_LEAD", 30*time.Second),
		SummaryWindow:         l.dur("SUMMARY_WINDOW", 5*time.Minute),
		SummaryCacheTTL:       l.dur("SUMMARY_CACHE_TTL", 2*time.Second),
		SummaryBacklogTTL:     l.dur("SUMMARY_BACKLOG_CACHE_TTL", 10*time.Second),
	}
	c.Relay = Relay{
		TickInterval:      l.dur("RELAY_TICK_INTERVAL", 100*time.Millisecond),
		BatchSize:         l.intv("RELAY_BATCH_SIZE", 200),
		ConfirmTimeout:    l.dur("RELAY_CONFIRM_TIMEOUT", 5*time.Second),
		LaneHWMHigh:       l.intv("LANE_HWM_HIGH", 500),
		LaneHWMNormal:     l.intv("LANE_HWM_NORMAL", 1000),
		LaneHWMLow:        l.intv("LANE_HWM_LOW", 500),
		ExpiryInterval:    l.dur("EXPIRY_INTERVAL", time.Second),
		ExpiryBatchSize:   l.intv("EXPIRY_BATCH_SIZE", 1000),
		LeaseSweepEvery:   l.dur("LEASE_SWEEP_INTERVAL", 5*time.Second),
		LeaseSweepBatch:   l.intv("LEASE_SWEEP_BATCH_SIZE", 500),
		ReconcileMinEvery: l.dur("RECONCILE_MIN_INTERVAL", 60*time.Second),
		BacklogGaugeEvery: l.dur("BACKLOG_GAUGE_INTERVAL", 10*time.Second),
	}
	c.Worker = Worker{
		Channels:            l.channels("WORKER_CHANNELS", []string{"sms", "email", "push"}),
		Replicas:            l.intv("WORKER_REPLICAS", 1),
		Concurrency:         l.intv("WORKER_CONCURRENCY", 25),
		LeaseDuration:       l.dur("LEASE_DURATION", 30*time.Second),
		FinalWriteRetryTime: l.dur("FINAL_WRITE_RETRY_WINDOW", 60*time.Second),
		ShutdownDrain:       l.dur("SHUTDOWN_DRAIN_TIMEOUT", 15*time.Second),
		ConsumerTimeout:     l.dur("CONSUMER_TIMEOUT", 24*time.Hour),
	}
	c.RateLimit = RateLimit{
		PerSecond:     l.intv("RATE_LIMIT_PER_SECOND", 98),
		Horizon:       l.dur("RATE_LIMIT_HORIZON", 250*time.Millisecond),
		RetryInterval: l.dur("RATE_LIMIT_RETRY_INTERVAL", 50*time.Millisecond),
		SlotTolerance: l.dur("PACER_SLOT_TOLERANCE", 5*time.Millisecond),
	}
	c.Retry = RetryConfig{
		High:                 l.retry("HIGH", "TTL_HIGH", Retry{time.Second, 30 * time.Second, 5, 10 * time.Minute}),
		Normal:               l.retry("NORMAL", "TTL_NORMAL", Retry{5 * time.Second, 10 * time.Minute, 8, 6 * time.Hour}),
		Low:                  l.retry("LOW", "TTL_LOW", Retry{30 * time.Second, time.Hour, 10, 24 * time.Hour}),
		MinDelay:             l.dur("RETRY_MIN_DELAY", time.Second),
		AfterCap:             l.dur("RETRY_AFTER_CAP", 10*time.Minute),
		ThrottleDefaultDelay: l.dur("THROTTLE_DEFAULT_DELAY", 5*time.Second),
		ThrottleMaxDelay:     l.dur("THROTTLE_MAX_DELAY", 2*time.Minute),
	}
	c.Breaker = Breaker{
		Window:              l.dur("BREAKER_WINDOW", 30*time.Second),
		MinRequests:         l.intv("BREAKER_MIN_REQUESTS", 20),
		FailureRatio:        l.float("BREAKER_FAILURE_RATIO", 0.5, 0, 1, true),
		ConsecutiveFailures: l.intv("BREAKER_CONSECUTIVE_FAILURES", 5),
		OpenBase:            l.dur("BREAKER_OPEN_BASE", 5*time.Second),
		OpenMax:             l.dur("BREAKER_OPEN_MAX", 2*time.Minute),
		FatalOpen:           l.dur("BREAKER_FATAL_OPEN", time.Minute),
	}
	c.Events = Events{
		BufferSize:         l.intv("EVENT_BUFFER_SIZE", 1024),
		QueueMaxLength:     l.intv("EVENT_QUEUE_MAX_LENGTH", 10000),
		QueueMessageTTL:    l.dur("EVENT_QUEUE_MESSAGE_TTL", 30*time.Second),
		WSOriginPatterns:   l.list("WS_ORIGIN_PATTERNS", nil),
		WSSendQueueSize:    l.intv("WS_SEND_QUEUE_SIZE", 256),
		WSMaxSubscriptions: l.intv("WS_MAX_SUBSCRIPTIONS", 1000),
		WSPingInterval:     l.dur("WS_PING_INTERVAL", 30*time.Second),
	}
	c.Templates = Templates{
		MaxBodyBytes:   l.int64v("TEMPLATE_MAX_BODY_BYTES", 65536),
		MaxOutputBytes: l.int64v("TEMPLATE_MAX_OUTPUT_BYTES", 524288),
	}
	c.Observability = Observability{
		LogLevel:         l.oneOf("LOG_LEVEL", "info", "debug", "info", "warn", "error"),
		OTLPEndpoint:     l.str("OTEL_EXPORTER_OTLP_ENDPOINT", "http://jaeger:4317"),
		TracesSampler:    l.str("OTEL_TRACES_SAMPLER", "parentbased_traceidratio"),
		TracesSamplerArg: l.float("OTEL_TRACES_SAMPLER_ARG", 1.0, 0, 1, false),
	}
	return c
}

// LoadFrom builds a Config using lookup to read variables. Unset or empty
// variables take their defaults. All validation errors are returned together.
func LoadFrom(lookup func(string) (string, bool)) (Config, error) {
	cfg, _, err := loadFrom(lookup)
	return cfg, err
}

func loadFrom(lookup func(string) (string, bool)) (Config, []string, error) {
	l := &loader{lookup: lookup}
	cfg := l.load()
	keys := slices.Clone(l.keys)
	slices.Sort(keys)
	return cfg, keys, errors.Join(l.errs...)
}

// Keys returns the sorted names of every environment variable the package reads.
func Keys() []string {
	_, keys, _ := loadFrom(func(string) (string, bool) { return "", false })
	return keys
}
