//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/AlpNuhoglu/event-driven-notification-system/internal/domain"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/queue"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/testutil"
	"github.com/AlpNuhoglu/event-driven-notification-system/internal/worker"
)

// countingScripter counts the limiter's script calls: every reservation is
// one EVALSHA (followed by one EVAL only when Redis does not know the script
// yet).
type countingScripter struct {
	redis.Scripter
	calls atomic.Int64
}

func (c *countingScripter) EvalSha(ctx context.Context, sha1 string, keys []string, args ...any) *redis.Cmd {
	c.calls.Add(1)
	return c.Scripter.EvalSha(ctx, sha1, keys, args...)
}

func (c *countingScripter) Eval(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd {
	c.calls.Add(1)
	return c.Scripter.Eval(ctx, script, keys, args...)
}

// poisonDepth returns the number of messages in the SMS poison queue.
func (e *env) poisonDepth() int {
	e.t.Helper()
	q, err := e.admin.QueueDeclarePassive(queue.PoisonQueue(domain.ChannelSMS), true, false, false, false, nil)
	if err != nil {
		e.t.Fatalf("passive declare: %v", err)
	}
	return q.Messages
}

// Open issue 7, through the wired roles: the worker's connection to Postgres
// is cut (a TCP proxy that refuses connections) while three sends are in
// flight at the provider and further messages are delivered. The claims of
// the messages the pacer takes fail and are retried in place: during the
// outage the messages do not cycle through the broker and the number of
// rate-limit calls does not grow. When the database is back, every
// notification is sent once and the poison queue is empty.
//
// The provider holds the in-flight sends until the database is back: a send
// that completed during the outage would lose its final write (it is not
// retried yet) and be sent again after its lease, which is not what this
// test is about.
func TestE2E_WorkerDatabaseOutage(t *testing.T) {
	const (
		concurrency = 6
		inFlight    = 3
		later       = 17
		outage      = 1_000 * time.Millisecond
	)
	e := newEnv(t)
	proxy := testutil.NewProxy(t, e.pool.Config().ConnString())
	vars := map[string]string{
		"WORKER_CONCURRENCY": "6",
		"CLAIM_RETRY_BASE":   "10ms",
		"CLAIM_RETRY_MAX":    "200ms",
	}
	relayCfg := e.config(vars)
	vars["DATABASE_URL"] = proxy.URL()
	workerCfg := e.config(vars)

	// The worker logs to a buffer of its own, which the test reads.
	logs := &lockedBuffer{}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("worker logs:\n%s", logs.String())
		}
	})
	limiter := &countingScripter{Scripter: e.rdb}
	role, err := worker.NewRole(workerCfg, worker.Resources{
		Pool:     e.rolePool(workerCfg),
		Conn:     e.dial(),
		Redis:    limiter,
		Registry: prometheus.NewRegistry(),
	}, slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	if err != nil {
		t.Fatalf("worker role: %v", err)
	}
	release := e.prov.block(t)
	w := start(t, role.Run)
	e.startRelay(relayCfg)

	ids := e.create(inFlight)
	testutil.Eventually(t, func() bool { return e.prov.count() == inFlight }, waitTimeout)
	// The buffer is empty now, so the pacer reserves nothing until the next
	// messages arrive.
	callsBefore := limiter.calls.Load()

	proxy.Break()
	ids = append(ids, e.create(later)...)
	// The pacer reserves a slot for each of the three messages it can take
	// with the free concurrency slots. Their claims fail; prefetch equals
	// concurrency, so three more wait in the buffer and the rest stay ready
	// in the queue.
	testutil.Eventually(t, func() bool { return limiter.calls.Load() >= callsBefore+concurrency-inFlight }, waitTimeout)
	cut := time.Now()
	callsAtCut := limiter.calls.Load()
	testutil.Eventually(t, func() bool { return time.Since(cut) >= outage }, outage+waitTimeout)
	callsAtEnd := limiter.calls.Load()
	readyAtEnd := e.ready()
	processingAtEnd := e.countStatus(domain.StatusProcessing)
	t.Logf("outage of %s: limiter calls %d before it, %d at its start, %d at its end; ready %d; processing %d",
		time.Since(cut).Round(time.Millisecond), callsBefore, callsAtCut, callsAtEnd, readyAtEnd, processingAtEnd)
	if callsAtEnd != callsAtCut {
		t.Errorf("the limiter was called %d times during the outage, want 0", callsAtEnd-callsAtCut)
	}
	if wantReady := later - 2*(concurrency-inFlight); readyAtEnd != wantReady {
		t.Errorf("%d messages ready at the end of the outage, want %d: messages cycled through the broker", readyAtEnd, wantReady)
	}
	if processingAtEnd != inFlight {
		t.Errorf("%d notifications processing during the outage, want %d", processingAtEnd, inFlight)
	}
	if n := strings.Count(logs.String(), "claim failed; retrying"); n != concurrency-inFlight {
		t.Errorf("%d claim-failure lines, want one per failing claim, %d", n, concurrency-inFlight)
	}

	proxy.Restore(t)
	// The retried claims succeed and their sends reach the provider (held
	// too); then the held sends are released and their final writes reach
	// the database.
	testutil.Eventually(t, func() bool { return e.prov.count() == concurrency }, waitTimeout)
	release()
	testutil.Eventually(t, func() bool { return e.countStatus(domain.StatusSent) == inFlight+later }, waitTimeout)

	byKey := keys(e.prov.requests())
	for _, id := range ids {
		if byKey[id.String()] != 1 {
			t.Errorf("notification %s reached the provider %d times, want once", id, byKey[id.String()])
		}
	}
	if n := e.prov.count(); n != inFlight+later {
		t.Errorf("provider received %d requests, want %d", n, inFlight+later)
	}
	recovered := 0
	for line := range strings.SplitSeq(logs.String(), "\n") {
		var l struct {
			Msg   string `json:"msg"`
			Tries int    `json:"tries"`
		}
		if json.Unmarshal([]byte(line), &l) == nil && l.Msg == "worker: claim succeeded after retries" {
			recovered++
			if l.Tries < 2 {
				t.Errorf("a recovered claim reports %d tries", l.Tries)
			}
		}
	}
	if recovered != concurrency-inFlight {
		t.Errorf("%d claims recovered after retries, want %d", recovered, concurrency-inFlight)
	}
	if !e.queueEmpty(w) {
		t.Error("the dispatch queue is not empty")
	}
	if n := e.poisonDepth(); n != 0 {
		t.Errorf("the poison queue holds %d messages, want 0", n)
	}
}
