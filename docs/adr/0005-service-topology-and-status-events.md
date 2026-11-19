# ADR-0005: Service topology and status events

- **Status:** Accepted
- **Date:** 2026-11-18
- **Related:** [ADR-0001](0001-rabbitmq-as-dispatch-broker.md), [ADR-0002](0002-postgres-as-outbox-scheduler-and-retry-delay.md), [ADR-0003](0003-bounded-relay-buffer-and-sweepers.md), [ADR-0004](0004-data-model-state-machine-and-fencing.md), [ADR-0006](0006-api-contract-and-three-layer-idempotency.md), [ADR-0007](0007-retry-policy-and-circuit-breaker.md), [ADR-0009](0009-observability.md)

## Context

The previous ADRs define *what* happens: the relay publishes and recovers, workers claim and deliver, the api accepts, cancels and redrives, and status events reach real-time clients. This ADR decides *where* each responsibility runs, how each part is started and stopped, and how status events reach clients in a usable order.

The workloads have very different profiles:

| Workload | Profile |
|---|---|
| REST API | Stateless request/response; scales with request rate and database capacity |
| Relay and recovery | Control-plane loops; low throughput (at most about 300 publishes per second) |
| Delivery | I/O-bound and provider-specific; channels must not affect each other |
| WebSocket | Long-lived connections; scales with connection count and memory |

## Decision

### Guiding principle: correctness lives in the data layer

Every correctness guarantee is enforced in Postgres: `SKIP LOCKED`, conditional transitions, the claim guard and the fencing token ([ADR-0004](0004-data-model-state-machine-and-fencing.md)). **Two instances doing the same work at the same time can never cause a corrupt state, and no guarantee depends on a process being the only one of its kind.** That is why the relay needs no leader election: it runs once, and if a second copy ever ran beside it, the result would be redundant queries, not errors.

### Processes

One Go module, one Docker image, one binary per role under `cmd/`. Compose runs the same image with different commands.

| Service | Responsibilities | Instances in Compose | Required dependencies |
|---|---|---|---|
| **api** | REST endpoints, validation, template rendering, `INSERT`, cancel, redrive, listing, metrics summary, health endpoints on the public port | 1 (stateless; can be replicated) | **Postgres only.** Its status-event publisher is optional and never affects readiness. |
| **relay** | Outbox publishing on a 100 ms ticker, expiry pass, reconciliation, lease sweeper, idempotency-key cleanup (every `IDEMPOTENCY_CLEANUP_INTERVAL`, 1 minute by default; D78), backlog gauges | **1**, with `restart: unless-stopped` | Postgres, RabbitMQ. **No Redis.** |
| **worker** | Consumption, pacing, rate limiting, circuit breaking, provider calls, delivery transitions | **1 container serving all channels** (`WORKER_CHANNELS=sms,email,push`) | Postgres, RabbitMQ; Redis for the shared rate limit (a fallback exists) |
| **gateway** | WebSocket connections, snapshot on subscribe, fan-out of status events | 1 (can be replicated) | RabbitMQ, Postgres (read-only) |
| **migrate** | Applies database migrations, then exits | One-shot | Postgres. Other services wait with `depends_on: condition: service_completed_successfully`. |
| **mockprovider** | Configurable fake provider with fault injection | Not in the default stack; added by the mock override (below) for automated tests, load tests and larger examples | — |

**Channel isolation inside one worker.** The worker runs one pacer loop, one concurrency semaphore, one HTTP client and one circuit breaker **per channel** (below). A failing SMS provider opens the SMS circuit and stops only the SMS pacer. The channel-isolation load test is the evidence for this ([ADR-0010](0010-testing-and-ci.md)). Per-channel worker deployments (`WORKER_CHANNELS=sms`, …) remain possible; they live in a Compose override file that one chaos scenario uses.

**Provider target.** The default provider is **webhook.site**, as the assessment asks (R23). The default lives in `docker-compose.yml` as `${PROVIDER_URL:-https://webhook.site/<uuid>}`, so a clean clone works without a `.env` file.

**Mock profile.** `docker-compose.mock.yml` is an override file that adds `mockprovider` and points `PROVIDER_URL` at it; `make up-mock` starts the stack with it. Automated tests, load tests and the examples that send more than a few requests use this profile ([ADR-0007](0007-retry-policy-and-circuit-breaker.md) §6).

**Infrastructure:** Postgres, RabbitMQ, Redis, Prometheus, Grafana and Jaeger. Traces are sent over OTLP **directly to Jaeger**, with no separate OpenTelemetry Collector ([ADR-0009](0009-observability.md)).

### Compose

| Topic | Rule |
|---|---|
| File and command | `docker-compose.yml`; the README uses the assessment's command, `docker-compose up` (R33), and states the minimum Compose version. CI runs that command once with the standalone binary ([ADR-0010](0010-testing-and-ci.md)). |
| Image versions | **Every image is pinned to an exact tag:** Postgres 18.x, RabbitMQ 4.4.x, Redis, Prometheus, Grafana and Jaeger 2.x (not the v1 all-in-one image). The Go toolchain is 1.26 or later. |
| RabbitMQ | A **fixed hostname and a named volume**. RabbitMQ stores its data under the node name, which defaults to the hostname; without a fixed hostname a recreated container would start with an empty broker. `rabbitmq_prometheus` is enabled explicitly ([ADR-0009](0009-observability.md)). |
| Ports | The api listens on `:8080` and the gateway on `:8082`; both are published to the host. Every service has an admin port, `:8081`, which is internal only ([ADR-0009](0009-observability.md) §8). |
| Restart policy | `restart: unless-stopped` on every application service |
| Health checks | Every container has one. The Go binaries have a **`healthcheck` subcommand**, so minimal images need no `curl`. |
| Configuration | The binaries carry the default of every variable except `PROVIDER_URL`, whose default is in the Compose file. An optional `.env` file reaches every application container through `env_file` with `required: false`; this entry is what sets the minimum Compose version, v2.24.0. |
| Start order | The `depends_on` chain rests on the health of Postgres, RabbitMQ and Redis and on the completion of `migrate` |
| Stop | `stop_grace_period: 20s` for the worker, longer than its 15-second drain |

### Relay

A single process with four loops: the publishing cycle on a 100 ms ticker ([ADR-0002](0002-postgres-as-outbox-scheduler-and-retry-delay.md)), the expiry pass, the lease sweeper, and reconciliation on every (re)connection to RabbitMQ ([ADR-0003](0003-bounded-relay-buffer-and-sweepers.md)). It also deletes expired idempotency keys, in bounded batches every `IDEMPOTENCY_CLEANUP_INTERVAL` (D78, [ADR-0006](0006-api-contract-and-three-layer-idempotency.md) §11), and measures the backlog gauges.

The relay binary carries one operator subcommand, **`requeue-stale`**, run as `docker-compose run --rm relay requeue-stale`. It returns `queued` rows older than 5 minutes to `pending` (`--older-than=5m` by default; `--all` takes every `queued` row), for the cases reconciliation does not cover ([ADR-0003](0003-bounded-relay-buffer-and-sweepers.md) §3).

If the relay stops, nothing is published and nothing is lost: notifications accumulate as `pending` in Postgres. The container runtime restarts it, and the alert `up{job="relay"} == 0` fires meanwhile ([ADR-0009](0009-observability.md)).

### Worker internals

For each channel the worker runs **one pacer loop** and one semaphore:

```
consumer (prefetch = 25) ──► local buffer, ordered by priority
                              (each delivery is stamped with the connection epoch)

pacer loop (one per channel):
  1. acquire a concurrency slot              (semaphore, 25 per channel)
  2. wait until the buffer holds a message   (a signal; nothing is taken yet)
  3. circuit open? ── yes ──► wait until its next half-open time
  4. reserve a rate-limit slot               (GCRA, at most 250 ms ahead)
  5. take the highest-priority buffered message; the pacer loop goes back to 1,
     and the message's own goroutine does the rest:
     claim it during the wait
        claim affects 0 rows ──► ack, drop; keep the slot for the next buffered
                                 message, unless the slot is stale by more than
                                 the tolerance (then it is given up)
  6. ack, unless the delivery is from an older connection epoch; emit the processing event
  7. wait for the slot:
        send deadline reached (15 s after issuing the claim call)
                              ──► attempt 'aborted', row back to retrying, nothing sent
        slot missed by more than the tolerance (5 ms)
                              ──► give the slot up, reserve a new one (step 4), wait again
     at the slot time (also a re-reserved one):
        circuit opened meanwhile ──► attempt 'aborted', row back to retrying
        otherwise ──► call the provider ──► final transition (fenced) ──► emit event
  8. release the concurrency slot
```

- **One pacer, not N competing goroutines.** A rate-limit slot is reserved only when a message is known to be waiting, and each reserved slot is used at its own time. Idle goroutines never hold slots they reserved earlier, which would let them send in a burst when messages arrive.
- **The message is chosen after the slot is reserved,** so the highest-priority message available at that moment is the one that is sent.
- **The slot is kept when a claim affects zero rows.** A duplicate or cancelled message does not waste capacity. A kept slot that is stale by more than the tolerance when the next message is ready is dropped.
- **A send starts at a slot reserved for it, or not at all (D77).** A claim can return after its slot has passed, typically because it waited on the relay's row lock while the pacer went on reserving later slots. Within `PACER_SLOT_TOLERANCE` (5 ms) it sends at once. Beyond it the slot is given up, never returned to the limiter, and the goroutine reserves a new one through the same limiter call, horizon included. Meanwhile it holds its message: claimed, acked, the row `processing` ([ADR-0007](0007-retry-policy-and-circuit-breaker.md) §5).
- **Send deadline (D77).** If the send cannot start within `lease − HTTP timeout − 5 s` of issuing the claim call (15 s with the defaults; derived, not configured; measured on the worker's monotonic clock), nothing is sent: the attempt is closed as `aborted` and the row returns to `retrying`, due at once, without consuming retry budget. What remains of the lease always covers the HTTP timeout and the final write, even when the lease began while the claim was still waiting on a row lock.
- **Circuit opening between claim and send:** the breaker is asked again at every slot time, including a re-reserved one; if it is open, nothing is sent. The row returns to `retrying` without consuming retry budget, with `due_at` at the circuit's next half-open time, and the attempt is closed with outcome `aborted` ([ADR-0007](0007-retry-policy-and-circuit-breaker.md) §2).
- **Concurrency is 25 per channel per replica, and prefetch equals concurrency.** By Little's law, reaching 98 msg/s with about 200 ms of provider latency needs `98 × 0.2 ≈ 20` concurrent requests.
- **The message is acked as soon as the claim commits.** From that point the database owns the work: the row is `processing` with a lease, and the lease sweeper recovers it if the worker dies ([ADR-0003](0003-bounded-relay-buffer-and-sweepers.md)). Only unclaimed messages are ever unacknowledged.
- **The final write is retried** with back-off for up to 60 seconds if Postgres is unavailable ([ADR-0004](0004-data-model-state-machine-and-fencing.md) §4).
- **A claim that fails with a database error is retried in place** (step 5). The message's goroutine keeps its delivery and its concurrency slot and issues the claim again after a back-off (`CLAIM_RETRY_BASE`, 100 ms, doubling to `CLAIM_RETRY_MAX`, 5 s). It reserves no rate-limit slot per try: when the claim finally succeeds, the slot of the first try is long stale, the missed-slot rule gives it up and reserves one new slot, and the send deadline counts from the claim call that succeeded. While every concurrency slot is held this way the pacer takes no further message, so during a Postgres outage messages wait in the local buffer and in the broker instead of cycling between them. Nacking at once, as the first implementation did, made every buffered message circle between broker and worker, each round using a rate-limit slot.
- **The breaker is asked twice** ([ADR-0007](0007-retry-policy-and-circuit-breaker.md) §7, §8): in step 3 whether a message may be prepared at all (this consumes nothing), and at the slot time for a permit to send. The outcome of every admitted send is recorded on its permit exactly once, also when the send panics: in a half-open circuit the admitted send is the probe, and a probe that is never recorded would keep the circuit half-open for good.
- **Redis is optional at run time and at start.** The limiter is the Redis script with an in-process fallback ([ADR-0007](0007-retry-policy-and-circuit-breaker.md) §5). A worker that starts while Redis is unreachable logs a warning and starts on the fallback; a worker that restarted during a Redis outage would otherwise stop delivery.

#### Connection epoch (D76)

After a recovery the client library keeps the same channel object and the broker's delivery tags start again at 1. An ack sent for a delivery that arrived before the reconnect therefore acknowledges whichever message carries that tag on the recovered channel, a different one (observed in experiment E4). The worker prevents this with a connection epoch:

- The worker keeps a connection epoch, a counter that advances every time the connection is lost. A channel that the library reopens after a channel error also restarts its delivery tags, so it advances the epoch in the same way.
- **Each delivery carries the epoch it arrived on.**
- **On a reconnect, deliveries from an older epoch are removed from the pacer's local buffer without an ack or a nack.** The broker requeued them when the connection dropped and delivers them again.
- **A delivery from an older epoch is never acked.** If the connection is lost between a claim and its ack (steps 5 and 6), the claim stands: the row is `processing`, the send goes ahead, and no ack is sent.
- **A redelivered duplicate is handled by the claim guard.** Its claim affects zero rows, and it is acked and dropped (step 5, [ADR-0004](0004-data-model-state-machine-and-fencing.md)).

The test is in [ADR-0010](0010-testing-and-ci.md): the connection is broken while messages sit in the local buffer; every notification ends `sent`, and no ack from an older epoch reaches the broker.

### Connections to RabbitMQ

- Every role declares the topology it uses at start-up and on every (re)connection ([ADR-0001](0001-rabbitmq-as-dispatch-broker.md)).
- Reconnection uses the client library's automatic recovery, including its topology recovery. The library has it off by default and marks it experimental (experiment E4), so every role enables it explicitly (`Config.Recovery`).
- **The recovery budget is configuration:** `AMQP_RECOVERY_MAX_RETRIES` = 24 attempts, each after a wait of `AMQP_RECOVERY_RETRY_INTERVAL` = 5 s, which is about 2 minutes. The library's own default, 5 attempts at 5 s, gave up 26 s into a 60-second outage.
- A role learns about the connection from the library's state notifications (`NotifyStateChange`): `open → reconnecting` when it is lost, `reconnecting → open` when it is back, and `→ closed` with an error when recovery has given up.
- **If recovery gives up, the relay, the worker and the gateway exit with a non-zero code** and are restarted by the container runtime. The new process declares the topology at start-up, and the relay runs reconciliation on its first connection ([ADR-0003](0003-bounded-relay-buffer-and-sweepers.md) §3). This exit-and-restart path is also the fallback if the experimental recovery misbehaves ([future work](../future-work.md)).
- **The relay also exits when its publisher's channel is closed for good** (the return listener is gone, so no later batch could be checked for unroutable messages). A channel error from the broker does not cause this: the library reopens the channel and the listener keeps working (tested). P3 may replace the exit with rebuilding the publisher.
- **The worker never acks a delivery that arrived before a reconnect** (the connection epoch, above).
- **The api does not exit.** Its only use of RabbitMQ is the optional event publisher, so it logs the state, keeps serving and reconnects lazily.
- A worker that receives a server-side consumer cancellation resubscribes ([ADR-0001](0001-rabbitmq-as-dispatch-broker.md)).

### Graceful shutdown

| Service | On `SIGTERM` |
|---|---|
| worker | Cancel the consumers; nack prefetched messages that have not been claimed (requeue), including, at once and exactly once, a message whose claim is being retried after a database error; finish in-flight sends within 15 s, which is under the 30 s lease and the 20 s stop grace period; exit. A claimed notification whose send has not started when the 15 s end is closed as `aborted` and returns to `retrying` without consuming budget. A send that is still running then is cancelled and recorded as `unknown`: waiting for it could take the drain time plus the HTTP timeout, longer than the stop grace period. |
| relay | Finish the current lane's transaction, start no further lane, and exit. |
| api | `http.Server.Shutdown` drains in-flight requests. |
| gateway | Send a WebSocket close frame. Clients reconnect with jittered back-off. |

### Status events

**Every transition returns a `StatusChanged` value**, with the repository signature `(StatusChanged, error)` ([ADR-0004](0004-data-model-state-machine-and-fencing.md)). The caller publishes it as `notification.status_changed` to the `notifications.events` topic exchange **after commit**, best-effort, with routing key `status.{channel}.{status}` ([ADR-0001](0001-rabbitmq-as-dispatch-broker.md)). A test asserts that all 12 transitions produce an event.

| Publisher | Transitions |
|---|---|
| relay | 1, 2 (`queued`), 7 (`retrying` or `failed`), 8 (`queued → pending`), 10 (`failed / expired`) |
| worker | 3, 4, 5, 6, 12 |
| api | 9 (`cancelled`), 11 (redrive) |

**Publishers are optional by construction.** Each one connects lazily, publishes through a bounded in-memory buffer and drops events when the buffer is full. A publisher never blocks a transition and never affects start-up or readiness.

Events are not routed through the outbox: that would add a database write to every transition for a channel that is best-effort by design.

#### Ordering

Events are published from different processes and connections, so they can arrive out of order (for example `sent` before `queued`). Every event carries the row's `version` ([ADR-0004](0004-data-model-state-machine-and-fencing.md)), which is strictly monotonic because each transition increments it under the row lock.

- **Client rule:** ignore any event whose `version` is less than or equal to the version already known. `GET /v1/notifications/{id}` also returns `version`.
- The gateway keeps no per-notification state and does not filter by version.

#### Gateway

| Topic | Rule |
|---|---|
| Event delivery | Each gateway instance consumes through its own exclusive, auto-delete queue with `x-max-length` and a short `x-message-ttl` ([ADR-0001](0001-rabbitmq-as-dispatch-broker.md)) |
| **Snapshot on subscribe** | The gateway **registers the subscription first, then reads Postgres and sends a snapshot**: a `status_changed` message with the current `version`. For a batch subscription it sends one message per notification (at most 1,000). A client that subscribes after creating a notification therefore never misses its state, and the `version` rule discards whichever of snapshot and live event is older. |
| Database access | Read-only |
| Slow clients | Each connection has a bounded send queue. When it is full, the connection is closed; the client reconnects and receives a fresh snapshot. |
| Origins | Allowed origins come from configuration (`OriginPatterns`) |
| Limits | At most 1,000 subscriptions per connection ([ADR-0006](0006-api-contract-and-three-layer-idempotency.md) §10) |

### Code layout

```
cmd/{api,relay,worker,gateway,migrate,mockprovider}/main.go   ← wiring only
internal/
  domain/      state machine, decideRetry, error classification: pure, no I/O
  store/       sqlc-generated queries and transition functions returning StatusChanged
  queue/       RabbitMQ topology, publisher (confirms, mandatory), consumers, event publisher
  ratelimit/   GCRA in Redis (Lua) and the in-process fallback
  breaker/     in-process circuit breaker: a pure state machine with an injected clock
  provider/    Provider interface; webhook implementation
  api/ relay/ worker/ gateway/    role-specific logic
  platform/    config (environment variables), logging, metrics, tracing, shutdown
api/openapi.yaml
migrations/
```

`domain/` imports nothing that performs I/O. The core rules can therefore be tested without a database or a broker, and this is where most unit tests live ([ADR-0010](0010-testing-and-ci.md)).

## Alternatives considered

| Alternative | Why it was rejected |
|---|---|
| **Two relay replicas, active-passive, with leader election through a Postgres advisory lock** | Leadership served only to make the high-water mark exact, and the mark does not need to be exact. In Compose, a single relay with a restart policy recovers as fast as a standby would take over. The election needed a dedicated session connection, keepalive probes and step-down logic, none of which produced a difference a test could show. Recorded in [future work](../future-work.md) for multi-node deployments. |
| **One worker deployment per channel as the default** | Process-level isolation protects against a memory leak in one channel's client, which this system has no evidence of; flow-control isolation is already per channel inside the process. One container keeps the default stack small. The per-channel layout is kept as an override. |
| **N worker goroutines that each reserve a slot and then block on the buffer** | Idle goroutines hold slots reserved in the past. When messages arrive, they all send at once, which breaks the rate limit (R15). |
| **A per-connection `last_version` map in the gateway** | It let late events through once an entry had been removed at a final state, and it was not bounded as intended. The client-side `version` rule is sufficient, and the snapshot covers the case the map could not: a client that subscribes after the events were published. |
| **`mockprovider` as the default provider** | Works offline, but the assessment says to use webhook.site. The default follows the text; the mock is one profile away. |
| **A single process running everything** | Simplest to run, but an api deploy would drop every WebSocket connection, and nothing could be scaled or restarted separately. |
| **Gateway inside the api** | One container fewer, but it mixes connection-bound and request-bound scaling, and api restarts would cause reconnect storms. |
| **Running migrations at api start-up** | Several replicas would race to migrate. |
| **Routing status events through the outbox** | Gives guaranteed delivery for a best-effort channel at the cost of a write on every transition. Clients can always fall back to `GET`. |
| **OpenTelemetry Collector container** | Jaeger accepts OTLP directly. The application-side batch span processor is enough at this scale; a collector is a production addition ([ADR-0009](0009-observability.md)). |

## Consequences

**Positive**

- No component needs coordination with a peer: there is no election, no shared lock and no session-bound Postgres feature.
- Channels are isolated where it matters for delivery: pacing, concurrency, circuit state and provider connections.
- A client can subscribe at any time and still learn the current state.
- The default stack is small: four long-running application containers, one one-shot migration and six infrastructure containers.

**Negative / costs**

- **The relay is a single point of delay.** While it restarts, publishing, expiry and recovery pause. Nothing is lost, but latency grows by the restart time.
- One worker process serves all channels, so a crash of that process stops all three until it restarts.
- Circuit state is per worker replica ([ADR-0007](0007-retry-policy-and-circuit-breaker.md) §7).
- Status events are best-effort; clients must apply the `version` rule.
- The default provider is an external service with a request cap and an expiry date ([ADR-0007](0007-retry-policy-and-circuit-breaker.md) §6).

## Review history

Changes made after the design review, recorded in the [design review log](../ai/design-review-log.md):

- Leader election and the standby relay were removed in favour of a single relay with a restart policy, because leadership only made an approximate limit exact; the document was renamed accordingly. (D4)
- The worker loop was specified as one pacer per channel with a fixed order of operations, because the earlier description allowed a reading in which idle goroutines sent in bursts. (D23)
- The default Compose stack runs one worker container for all channels and uses webhook.site as the default provider. (D46, D44)
- The gateway sends a snapshot on subscribe and no longer keeps a `last_version` map, because a client learns a notification's `id` only after the first events may have been published. (D37)
- Event publishers became lazy and optional, connection-recovery failure became an exit, and Compose details (fixed RabbitMQ hostname, `healthcheck` subcommand, stop grace period, pinned images) were added. (D11, D5, D9, D47, D48)
- After experiment E4 (P0), the client's recovery is enabled explicitly with a configured budget of 24 attempts at 5 s, and the worker gained the connection epoch: a delivery that arrived before a reconnect is dropped from the local buffer and never acked, because delivery tags restart on the recovered channel. (D5, D76)
- In P1 the worker loop gained the missed-slot rule and the send deadline: a claim that returns more than 5 ms after its slot gives the slot up and reserves a new one, and a send that cannot start within 15 s of the claim call is aborted. Sending late at once could put 101 arrivals into a one-second window. (D77)
