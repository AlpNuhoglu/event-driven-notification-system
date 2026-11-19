//go:build integration

// Package integration holds the full-path tests: the relay and the worker
// role run in-process, wired as the binaries wire them, against real
// Postgres, RabbitMQ and Redis and an httptest provider.
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/events"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/platform/config"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/queue"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/relay"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/store"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/worker"
)

func TestMain(m *testing.M) { testutil.Main(m) }

// Durations are parameters, in milliseconds.
const (
	waitTimeout    = 10_000 * time.Millisecond
	confirmTimeout = 5_000 * time.Millisecond
	testTTL        = 3_600_000 * time.Millisecond
)

// The reserved fake test values.
const (
	testRecipient   = "+905551234567"
	maskedRecipient = "+90555***4567"
	testContent     = "Your code is 1234"
	testTraceParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
)

// env is one test's infrastructure: its own database, vhost, Redis database
// and provider.
type env struct {
	t       *testing.T
	pool    *pgxpool.Pool // the test's own queries; the roles open their own pools
	store   *store.Store
	amqpURL string
	admin   *amqp.Channel // passive declarations
	rdb     *redis.Client
	prov    *recorder
	log     *slog.Logger
}

func newEnv(t *testing.T) *env {
	t.Helper()
	logs := &lockedBuffer{}
	// Registered first, so it runs last: the roles have stopped by then.
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("role logs:\n%s", logs.String())
		}
	})
	pool := testutil.NewPool(t)
	amqpURL := testutil.NewRabbitMQVhost(t)
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Fatalf("admin dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	admin, err := conn.Channel()
	if err != nil {
		t.Fatalf("admin channel: %v", err)
	}
	e := &env{
		t:       t,
		pool:    pool,
		store:   store.New(pool, store.Profiles{}),
		amqpURL: amqpURL,
		admin:   admin,
		rdb:     testutil.NewRedis(t),
		prov:    newRecorder(t),
		log:     slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}
	e.checkGoroutines()
	return e
}

// checkGoroutines fails the test when goroutines started after this call
// (roles, connections, provider clients) are still running once the test's
// later cleanups have stopped them. The shared containers and this test's
// infrastructure exist already, so they are part of the baseline.
func (e *env) checkGoroutines() {
	t := e.t
	base := runtime.NumGoroutine()
	leaked := true
	t.Cleanup(func() {
		if leaked {
			buf := make([]byte, 1<<20)
			t.Logf("goroutines (baseline %d):\n%s", base, buf[:runtime.Stack(buf, true)])
		}
	})
	t.Cleanup(func() {
		// Keep-alive connections are not leaks of the code under test.
		e.prov.srv.CloseClientConnections()
		http.DefaultClient.CloseIdleConnections()
		testutil.Eventually(t, func() bool { return runtime.NumGoroutine() <= base }, waitTimeout)
		leaked = false
	})
}

// config loads the configuration from the given variables on top of the
// test's connection URLs, as the binaries load it from the environment.
func (e *env) config(vars map[string]string) config.Config {
	e.t.Helper()
	all := map[string]string{
		"DATABASE_URL":                 e.pool.Config().ConnString(),
		"RABBITMQ_URL":                 e.amqpURL,
		"PROVIDER_URL":                 e.prov.srv.URL,
		"PROVIDER_NAME":                "mock",
		"RELAY_TICK_INTERVAL":          "10ms",
		"AMQP_RECOVERY_RETRY_INTERVAL": "20ms",
	}
	for k, v := range vars {
		all[k] = v
	}
	cfg, err := config.LoadFrom(func(k string) (string, bool) {
		v, ok := all[k]
		return v, ok
	})
	if err != nil {
		e.t.Fatalf("config: %v", err)
	}
	return cfg
}

// create inserts n due SMS notifications of normal priority.
func (e *env) create(n int) []uuid.UUID {
	e.t.Helper()
	ids := make([]uuid.UUID, n)
	for i := range ids {
		ctx, cancel := e.ctx()
		row, err := e.store.CreateNotification(ctx, store.NewNotification{
			Channel:       domain.ChannelSMS,
			Priority:      domain.PriorityNormal,
			Recipient:     domain.Recipient(testRecipient),
			Content:       testContent,
			TTL:           testTTL,
			MaxFailures:   5,
			CorrelationID: "corr-integration",
			TraceParent:   testTraceParent,
		})
		cancel()
		if err != nil {
			e.t.Fatalf("create: %v", err)
		}
		ids[i] = row.ID
	}
	return ids
}

// ctx returns a context bounded by waitTimeout for one call.
func (e *env) ctx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), waitTimeout)
}

func (e *env) get(id uuid.UUID) store.Notification {
	e.t.Helper()
	ctx, cancel := e.ctx()
	defer cancel()
	n, err := e.store.GetNotification(ctx, id)
	if err != nil {
		e.t.Fatalf("get: %v", err)
	}
	return n
}

func (e *env) attempts(id uuid.UUID) []store.Attempt {
	e.t.Helper()
	ctx, cancel := e.ctx()
	defer cancel()
	a, err := e.store.ListAttempts(ctx, id)
	if err != nil {
		e.t.Fatalf("attempts: %v", err)
	}
	return a
}

// countStatus counts this test's notifications in status s.
func (e *env) countStatus(s domain.Status) int {
	e.t.Helper()
	ctx, cancel := e.ctx()
	defer cancel()
	var n int
	if err := e.pool.QueryRow(ctx, `SELECT count(*) FROM notifications WHERE status = $1`, string(s)).Scan(&n); err != nil {
		e.t.Errorf("count %s: %v", s, err)
		return -1
	}
	return n
}

// waitStatus waits until the notification is in status s.
func (e *env) waitStatus(id uuid.UUID, s domain.Status) {
	e.t.Helper()
	testutil.Eventually(e.t, func() bool { return e.get(id).Status == s }, waitTimeout)
}

// claimLockWaiters counts backends of this test's database that are blocked
// on a lock while running the claim statement.
func (e *env) claimLockWaiters() int {
	e.t.Helper()
	ctx, cancel := e.ctx()
	defer cancel()
	var n int
	err := e.pool.QueryRow(ctx, `
		SELECT count(*) FROM pg_stat_activity
		WHERE datname = current_database()
		  AND pid <> pg_backend_pid()
		  AND wait_event_type = 'Lock'
		  AND query LIKE '%WITH claimed AS%'`).Scan(&n)
	if err != nil {
		e.t.Errorf("pg_stat_activity: %v", err)
		return -1
	}
	return n
}

// ready returns the number of ready messages in the SMS dispatch queue, read
// from the broker at once (a passive declaration).
func (e *env) ready() int {
	e.t.Helper()
	q, err := e.admin.QueueDeclarePassive(queue.DispatchQueue(domain.ChannelSMS), true, false, false, false, nil)
	if err != nil {
		e.t.Fatalf("passive declare: %v", err)
	}
	return q.Messages
}

// queueEmpty stops the worker role and then reports whether the SMS
// dispatch queue holds no message and has no consumer. Stopping the role
// closes its channels, which would requeue every delivery still
// unacknowledged; so no ready message afterwards means that nothing was left
// ready or unacknowledged. (The management API of the test broker returns no
// message counts, so testutil.RabbitMQQueue cannot show them.)
func (e *env) queueEmpty(role *running) bool {
	e.t.Helper()
	role.cancel()
	if err := role.wait(e.t); err != nil {
		e.t.Errorf("worker role returned %v", err)
	}
	q, err := e.admin.QueueDeclarePassive(queue.DispatchQueue(domain.ChannelSMS), true, false, false, false, nil)
	if err != nil {
		e.t.Fatalf("passive declare: %v", err)
	}
	if q.Messages != 0 || q.Consumers != 0 {
		e.t.Logf("dispatch queue: %d ready, %d consumers", q.Messages, q.Consumers)
		return false
	}
	return true
}

// publisher returns a confirming publisher on its own connection, the one
// the relay uses.
func (e *env) publisher() *queue.Publisher {
	e.t.Helper()
	conn := e.dial()
	if err := conn.DeclareTopology(24 * time.Hour); err != nil {
		e.t.Fatalf("declare: %v", err)
	}
	pub, err := queue.NewPublisher(conn, 200)
	if err != nil {
		e.t.Fatalf("publisher: %v", err)
	}
	return pub
}

func (e *env) dial() *queue.Connection {
	e.t.Helper()
	conn, err := queue.Dial(e.amqpURL, 20*time.Millisecond, 1000, e.log)
	if err != nil {
		e.t.Fatalf("dial: %v", err)
	}
	e.t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func (e *env) rolePool(cfg config.Config) *pgxpool.Pool {
	e.t.Helper()
	pool, err := pgxpool.New(context.Background(), cfg.Connections.DatabaseURL)
	if err != nil {
		e.t.Fatalf("pool: %v", err)
	}
	e.t.Cleanup(pool.Close)
	// As the services do at start.
	ctx, cancel := e.ctx()
	defer cancel()
	if err := pool.Ping(ctx); err != nil {
		e.t.Fatalf("ping: %v", err)
	}
	return pool
}

// running is a role running in a goroutine.
type running struct {
	cancel context.CancelFunc
	done   chan error

	mu       sync.Mutex
	finished bool
	err      error
}

func start(t *testing.T, run func(ctx context.Context) error) *running {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	r := &running{cancel: cancel, done: make(chan error, 1)}
	go func() { r.done <- run(ctx) }()
	// Registered after the role's connections, so it runs before they close.
	t.Cleanup(func() {
		r.cancel()
		if err := r.wait(t); err != nil {
			t.Errorf("role returned %v after its context was cancelled", err)
		}
	})
	return r
}

// wait waits until the role has returned and returns its error.
func (r *running) wait(t *testing.T) error {
	t.Helper()
	testutil.Eventually(t, r.returned, waitTimeout)
	return r.err
}

// returned reports whether the role has returned, without waiting.
func (r *running) returned() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.finished {
		select {
		case r.err = <-r.done:
			r.finished = true
		default:
		}
	}
	return r.finished
}

// startWorker wires the worker role with worker.NewRole, the function
// worker.Service uses, on this test's database, vhost and Redis database,
// and runs it. writeMargin replaces worker.DefaultWriteMargin when positive.
func (e *env) startWorker(cfg config.Config, writeMargin time.Duration) (*worker.Role, *running) {
	e.t.Helper()
	role, err := worker.NewRole(cfg, worker.Resources{
		Pool:        e.rolePool(cfg),
		Conn:        e.dial(),
		Redis:       e.rdb,
		Registry:    prometheus.NewRegistry(),
		WriteMargin: writeMargin,
	}, e.log)
	if err != nil {
		e.t.Fatalf("worker role: %v", err)
	}
	return role, start(e.t, role.Run)
}

// startRelay runs relay.Service, the relay binary's service, on cfg.
func (e *env) startRelay(cfg config.Config) *running {
	e.t.Helper()
	return start(e.t, func(ctx context.Context) error { return relay.Service(ctx, cfg, e.log) })
}

// startRelayWith runs a relay built as relay.Service builds it, except that
// its publisher is wrapped by wrap. relay.Service has no seam for that.
func (e *env) startRelayWith(cfg config.Config, wrap func(relay.Publisher) relay.Publisher) *running {
	e.t.Helper()
	pool := e.rolePool(cfg)
	conn := e.dial()
	if err := conn.DeclareTopology(cfg.Worker.ConsumerTimeout); err != nil {
		e.t.Fatalf("declare: %v", err)
	}
	pub, err := queue.NewPublisher(conn, cfg.Relay.BatchSize)
	if err != nil {
		e.t.Fatalf("publisher: %v", err)
	}
	r := relay.New(store.New(pool, store.ProfilesFrom(cfg.Retry)), wrap(pub), conn, events.Nop{}, e.log,
		relay.ConfigFrom(cfg.Relay), func() float64 { return 0.5 })
	return start(e.t, func(ctx context.Context) error {
		var wg sync.WaitGroup
		wg.Go(func() { _ = r.RunPublishing(ctx) })
		wg.Go(func() { _ = r.RunLeaseSweeper(ctx) })
		wg.Go(func() { _ = r.RunExpiry(ctx) })
		wg.Go(func() { _ = r.RunIdempotencyCleanup(ctx) })
		wg.Wait()
		return nil
	})
}

// request is one request the provider received.
type request struct {
	at   time.Time
	body map[string]json.RawMessage
	key  string
}

// recorder is the provider: it records every request and answers 202, or
// holds each request until released while blocked.
type recorder struct {
	srv *httptest.Server

	mu   sync.Mutex
	reqs []request
	hold chan struct{} // non-nil while blocked; closed by the release
}

func newRecorder(t *testing.T) *recorder {
	t.Helper()
	r := &recorder{}
	r.srv = httptest.NewServer(http.HandlerFunc(r.serve))
	t.Cleanup(r.srv.Close)
	return r
}

func (r *recorder) serve(w http.ResponseWriter, req *http.Request) {
	at := time.Now()
	var body map[string]json.RawMessage
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		body = nil
	}
	r.mu.Lock()
	r.reqs = append(r.reqs, request{at: at, body: body, key: req.Header.Get("Idempotency-Key")})
	hold := r.hold
	r.mu.Unlock()
	if hold != nil {
		select {
		case <-hold:
		case <-req.Context().Done():
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"messageId": uuid.NewString(), "status": "accepted", "timestamp": time.Now().UTC().Format(time.RFC3339),
	})
}

// block makes every request wait until the returned release is called. The
// release is also called when the test ends.
func (r *recorder) block(t *testing.T) (release func()) {
	t.Helper()
	hold := make(chan struct{})
	r.mu.Lock()
	r.hold = hold
	r.mu.Unlock()
	var once sync.Once
	release = func() {
		once.Do(func() {
			r.mu.Lock()
			r.hold = nil
			r.mu.Unlock()
			close(hold)
		})
	}
	t.Cleanup(release)
	return release
}

func (r *recorder) requests() []request {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.reqs)
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.reqs)
}

// keys returns how many requests carried each idempotency key.
func keys(reqs []request) map[string]int {
	out := map[string]int{}
	for _, q := range reqs {
		out[q.key]++
	}
	return out
}

// fieldNames returns the sorted top-level field names of a request body.
func fieldNames(body map[string]json.RawMessage) []string {
	names := make([]string, 0, len(body))
	for k := range body {
		names = append(names, k)
	}
	slices.Sort(names)
	return names
}

// lockedBuffer is a bytes.Buffer safe for concurrent writers.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n, err := b.buf.Write(p)
	if err != nil {
		return n, fmt.Errorf("log buffer: %w", err)
	}
	return n, nil
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
