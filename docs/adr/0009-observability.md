# ADR-0009: Observability

- **Status:** Accepted
- **Date:** 2026-11-18
- **Related:** [ADR-0002](0002-postgres-as-outbox-scheduler-and-retry-delay.md), [ADR-0003](0003-bounded-relay-buffer-and-sweepers.md), [ADR-0004](0004-data-model-state-machine-and-fencing.md), [ADR-0005](0005-service-topology-and-status-events.md), [ADR-0006](0006-api-contract-and-three-layer-idempotency.md), [ADR-0007](0007-retry-policy-and-circuit-breaker.md), [ADR-0010](0010-testing-and-ci.md)

## Context

The requirements ask for:

- a **real-time metrics endpoint** with queue depth, success and failure rates, and latency (R20);
- **structured logging with correlation IDs** (R21);
- a **health check endpoint** (R22);
- **distributed tracing** as a bonus (R29).

The business context adds that delivery status must be visible both to internal teams and to API consumers (R6).

Three properties of this system make observability harder than in a request/response service:

- **Work is asynchronous.** A request finishes long before its notifications are delivered, sometimes hours later.
- **It crosses process boundaries through both Postgres and RabbitMQ:** api → relay → worker → provider.
- **The data is personal.** Recipients are phone numbers, email addresses and device tokens, and content can contain one-time passwords.

## Decision

### 1. Metrics (Prometheus)

**Cardinality rule.** Label values come only from small, fixed sets: `channel`, `priority`, `provider`, `outcome`, `source`, `status` (a final notification status), `status_class` (`2xx`/`4xx`/`5xx`), `method`, `code` (an HTTP status code), `route`, `kind`. **`id`, `recipient`, `batch_id`, `template_name` and tenant identifiers are never labels.** `route` is the route template (`/v1/notifications/{id}`), never the raw path, which would create one time series per ID.

| Service | Metric | Type | Answers |
|---|---|---|---|
| api | `http_requests_total{route,method,code}`, `http_request_duration_seconds{route,method}` | counter, histogram | API health |
| api | `notifications_created_total{channel,priority,source}` (`source` is `single` or `batch`), `notifications_rejected_total{channel,reason}`, `idempotent_replays_total` | counter | Ingest volume, validation failures, client retries |
| relay | `lane_queued{channel,priority}` | gauge | **Queue depth:** occupancy of each lane's buffer against its HWM |
| relay | `lane_oldest_queued_age_seconds{channel,priority}` | gauge | Whether queued work is being consumed; the signal for messages lost without a reconnection ([ADR-0003](0003-bounded-relay-buffer-and-sweepers.md)) |
| relay | `backlog_due{channel,priority}`, `scheduled_future` | gauge | Work waiting in Postgres; measured every 10 s |
| relay | `relay_published_total{channel,priority}`, `reconciliation_runs_total`, `reconciled_rows_total`, `sweeper_recovered_total{kind}`, `expired_total{channel,priority}` | counter | Control-plane health |
| api, relay, worker | `notifications_finished_total{channel,priority,status}` | counter | Notifications reaching a final status (`sent`, `failed`, `cancelled`), incremented by whichever service performs the final transition. It counts transitions, so a notification that is redriven, or that becomes `sent` through a late success, is counted more than once. It is the denominator of the expiry alert. |
| worker | `delivery_attempts_total{channel,provider,outcome}` | counter | **Success and failure rates**, via `rate()`. `outcome` takes every final value of [ADR-0007](0007-retry-policy-and-circuit-breaker.md) §2, including `unreachable` and `aborted`. |
| worker | `provider_request_duration_seconds{channel,provider}` | histogram | Provider latency |
| worker | `end_to_end_latency_seconds{channel,priority}` | histogram | **End-to-end latency**, from `COALESCE(scheduled_at, created_at)` to `sent_at` (see below) |
| worker | `queue_wait_seconds{channel,priority}` | histogram | From `queued_at` to claim. Rows claimed directly have no `queued_at` and are **not observed** ([ADR-0004](0004-data-model-state-machine-and-fencing.md)). |
| worker | `ratelimit_wait_seconds{channel}`, `circuit_state{channel,provider}`, `limiter_fallback_active`, `inflight{channel}` | histogram, gauge | Flow control. `circuit_state` is the only source of circuit state. |
| worker | `pacer_slots_missed_total{channel}` | counter | Rate-limit slots given up because the claim returned more than the tolerance after them (D77, [ADR-0007](0007-retry-policy-and-circuit-breaker.md) §5). A steady rate above zero means claims are slow: relay batches that take long to commit, or a slow database. |
| worker | `provider_unexpected_2xx_total{channel,provider,code}`, `provider_malformed_response_total{provider}` | counter | Provider anomalies that still count as success: a `2xx` other than `202` (`code` is the status code, a small fixed set), and a `202` whose body cannot be parsed |
| gateway | `ws_connections`, `ws_subscriptions` | gauge | Real-time layer |
| RabbitMQ | Queue depth, unacked messages, poison queue depth | | **Not re-implemented:** scraped from RabbitMQ's `rabbitmq_prometheus` plugin, which is enabled explicitly. Prometheus scrapes the **per-object (detailed) endpoint**; the default endpoint is aggregated and has no per-queue series. |

**Why `COALESCE(scheduled_at, created_at)` is the start point:**

- `created_at` alone would make scheduled notifications look hours late.
- `due_at` cannot be used either: it is overwritten on every retry ([ADR-0004](0004-data-model-state-machine-and-fencing.md)). A message retried for four hours would be reported as delivered in 120 ms.
- The chosen start point is immutable, so retry and queueing time remain visible.

Histogram buckets run from 100 ms to 3 hours, so a burst backlog is visible rather than collapsed into the last bucket.

### 2. Metrics summary as JSON

The Prometheus endpoint exposes raw counters; rates and percentiles require PromQL. Someone who simply runs `curl` should see the required figures directly. The api therefore serves `GET /v1/metrics/summary`, which is the "real-time metrics endpoint" of the requirements:

```json
{
  "generated_at": "2026-11-18T10:05:02Z",
  "window": "5m",
  "lanes": [
    { "channel": "sms", "priority": "high", "queued": 12, "hwm": 500, "backlog_due": 0 }
  ],
  "channels": [
    {
      "channel": "sms",
      "attempts": {
        "success": 29871, "retryable": 112, "permanent": 9, "provider_fatal": 0,
        "throttled": 0, "unknown": 2, "unreachable": 0, "aborted": 0, "internal_error": 0
      },
      "success_rate": 0.9959,
      "provider_latency_ms": { "p50": 148, "p95": 402 },
      "end_to_end_latency_ms": { "p50": 920, "p95": 4100 }
    }
  ]
}
```

- **Every outcome is listed,** including those that are zero.
- **`success_rate`** is `success / (all attempts finished in the window except aborted)`. An `aborted` attempt sent nothing ([ADR-0007](0007-retry-policy-and-circuit-breaker.md) §2), so it is neither a success nor a failure. With an empty denominator the value is `null`.
- Computed **from the database**, not from Prometheus, so the endpoint has no dependency on the monitoring stack:
  - lane counts use the partial indexes ([ADR-0004](0004-data-model-state-machine-and-fencing.md));
  - rates and percentiles use `notification_attempts` through its `(channel, finished_at)` index and `percentile_cont`; attempts closed by the lease sweeper have a `finished_at` and are included;
  - end-to-end latency joins the window's successful attempts to their notifications.
- **Postgres is the endpoint's only dependency.** There is no `circuits` field: circuit state lives in the worker processes and is exposed by their `circuit_state` metric.
- **Caching.** The result is cached for **2 seconds**, behind **`singleflight`**, so concurrent requests share one computation. **`backlog_due` is cached separately for 10 seconds,** because counting due rows on a table with a large backlog is the most expensive part.

### 3. Dashboard and alerts

Shipped in the repository and provisioned automatically, so they are available right after `docker-compose up`.

**One Grafana dashboard, "Overview":** ingest rate, lane depth against HWM, oldest queued age, backlog in Postgres, attempts by outcome, sent and failed rates, provider latency, end-to-end p95 per priority, circuit state, rate-limit wait, in-flight sends, relay publish rate, reconciliations and expiries, RabbitMQ queue and poison-queue depth.

**Alerts are Prometheus rules only;** there is no Alertmanager.

| Alert | Condition |
|---|---|
| Relay down | `up{job="relay"} == 0` |
| `high` lane stalled | `lane_oldest_queued_age_seconds{priority="high"}` above 2 minutes, sustained for 2 minutes |
| Poison messages | Any poison queue depth above zero for 5 minutes |
| Expiry spike | Any `high`-priority expiry within a 10-minute window (`expired_total{priority="high"}`), or `normal`-priority expiries above 1 % of finished notifications over 15 minutes (`expired_total` against `notifications_finished_total`, both with `priority="normal"`) |
| Circuit not closed | A circuit **not closed** (open or half-open) for more than 5 minutes. The rule is written on "not closed" because one open period lasts at most 2 minutes ([ADR-0007](0007-retry-policy-and-circuit-breaker.md) §7): during a long outage the circuit alternates between open and half-open and is never continuously open for 5 minutes. |
| Limiter fallback | `limiter_fallback_active == 1` |

The thresholds are first values, set when the rules are written and tuned against the load and chaos results. Each rule carries a one-line runbook note in `alerts.yml`.

**Both are tested** ([ADR-0010](0010-testing-and-ci.md)): after the end-to-end run, a smoke test executes every dashboard panel query and fails if one returns no data, and `promtool check rules` validates the alert file.

### 4. Structured logging

- **`log/slog` with the JSON handler**, from the standard library, wrapped by a handler that reads `correlation_id`, `trace_id` and `span_id` from the context and adds them to every record.
- **Standard fields:** `service`, `version` (git SHA), `notification_id`, `batch_id`, `channel`, `priority`, `attempt`.
- **Levels:** at about 300 msg/s, logging every transition at `info` would mean about 1,500 lines per second. `info` is therefore reserved for final outcomes, errors and lifecycle events such as reconnections and reconciliation runs; intermediate transitions are `debug`.

#### PII protection

| Layer | Rule |
|---|---|
| **Typed redaction** | Recipients are carried in a `Recipient` type that implements `slog.LogValuer`. However it reaches a log call (as an attribute, inside a struct, inside a group), it is written in masked form: `+90555***4567`, `a***@example.com`, the first six characters of a device token. The type masks itself in **every** formatting and encoding path, `encoding/json` included (the JSON log handler marshals a value nested in a struct); only `Reveal()` returns the real value. Code that must pass the real value on (the database, the provider request, the API response) calls it explicitly, and each of those three paths has a test that compares the value byte for byte. |
| **Key-based redaction** | A single `ReplaceAttr` function in the central handler drops `content`, `variables` and `template_vars`, and masks any attribute named `recipient`. It is the backstop for values that are passed as plain strings. |
| **Value-based redaction** | Neither rule can catch personal data inside free text, such as a provider error saying `"invalid number +905551234567"`. Before a provider error body is stored, every occurrence of that message's own `recipient` and `content` is replaced with its masked form, and the body is truncated to 1 KB. It is stored only in `notification_attempts.error`. |
| **Never logged** | Provider response bodies are never logged. Logs record only the status code and the error class. |
| **Canary scan** | The end-to-end run sends notifications with a unique recipient and unique content, then searches **all service logs, all exported spans and every `/metrics` output** for those values. Any match fails the run ([ADR-0010](0010-testing-and-ci.md)). |

A one-time password appearing in a log is a security incident, which is why `content` is never logged at any level.

### 5. Correlation IDs

- `X-Request-ID` is accepted from the client if it is at most 128 characters of `[A-Za-z0-9._-]`; otherwise it is ignored and a UUIDv7 is generated. An unvalidated header would let a client write arbitrary text into every log line. It is echoed in the response and stored in `notifications.correlation_id`.
- It reaches workers **through the database row, not the message**: the message carries only the `id` and `traceparent` ([ADR-0001](0001-rabbitmq-as-dispatch-broker.md)).
- All notifications in a batch share the request's correlation ID.
- `correlation_id` is immutable ([ADR-0004](0004-data-model-state-machine-and-fencing.md)).

### 6. Distributed tracing (OpenTelemetry)

Tracing a whole asynchronous lifecycle as one trace (request → publish → delivery → retries hours later) produces traces that span hours and cannot be read. Splitting every delivery from its request loses the simplest view: one request followed to the provider. The design uses a **hybrid rule**:

```
First attempt of a non-scheduled notification: one trace

[Trace A]  POST /v1/notifications ── validate ── INSERT (otelpgx)
              └─ relay.publish ── worker.process ── provider HTTP (otelhttp) ── DB transition
                 (child of notifications.trace_parent; traceparent header on the AMQP message)

Retries and scheduled deliveries: a new trace, linked to the request

[Trace B]  relay.publish ── worker.process ── provider HTTP ── DB transition
              ┆ span link → Trace A (via notifications.trace_parent)
```

| Case | The relay's publish span |
|---|---|
| First attempt (`attempt_count = 0`) of a notification without `scheduled_at` | A **child** of the stored `trace_parent`: request, relay, worker and provider form one trace |
| A retry, or any delivery of a scheduled notification | A **new root span linked** to the stored `trace_parent` |

- In both cases the relay puts **its own span's context** in the AMQP `traceparent` header, and the worker continues from it.
- `notifications.trace_parent` is written once at creation and **never updated** ([ADR-0004](0004-data-model-state-machine-and-fencing.md)), so every link points to the original request.
- The `otelhttp` transport propagates the context to the provider. On webhook.site the `traceparent` header is visible in the request inspector.
- **Span attributes:** `notification.id`, `channel`, `priority`, `attempt`, `outcome`, `provider`. Never the recipient or the content.
- **Export:** OTLP directly to **Jaeger 2.x** (the image is pinned; the v1 all-in-one image is not used), with the SDK's `BatchSpanProcessor`. There is no Collector container ([ADR-0005](0005-service-topology-and-status-events.md)); a production deployment would add one for tail sampling and routing.
- **Sampling:** parent-based, configured with the standard `OTEL_TRACES_SAMPLER` variables: 100 % for the demo, about 1 % during load tests to protect Jaeger's in-memory storage.

### 7. Health checks

`/healthz` and `/readyz` are served on **the api's public port** and are part of the OpenAPI spec, so the health check endpoint is reachable by anyone who can reach the API. Every service also serves both on its admin port for the container runtime; the Go binaries' `healthcheck` subcommand calls them ([ADR-0005](0005-service-topology-and-status-events.md)).

| Service | `/healthz` (liveness) | `/readyz` (readiness) |
|---|---|---|
| All | The process is running. **No dependency checks**: a database outage must not make the runtime restart every container in a loop. | |
| api | | **Postgres only.** Not RabbitMQ (its event publisher is optional) and not Redis. |
| worker | | RabbitMQ, Postgres, **and the consumer loop is active** (an `atomic.Bool` set by the consumer goroutine). An open TCP connection alone does not prove the consumer is running; it may be in the middle of reconnecting. Redis is not required (a fallback exists). |
| relay | | Postgres |
| gateway | | RabbitMQ and Postgres (for snapshots), and the event consumer is active (same flag) |

The readiness response is JSON, with the status and duration of each check. On the api's public port a failed readiness is a `503` problem document ([ADR-0006](0006-api-contract-and-three-layer-idempotency.md) §9) that carries the same list in its `checks` member.

### 8. Admin port

Each service serves `/metrics`, `/healthz`, `/readyz` and `/debug/pprof/*` on a separate admin port (`:8081`), apart from the public API (`:8080`) and the gateway's WebSocket port (`:8082`).

- In Compose the admin port is **not published to the host**; Prometheus scrapes it over the internal network.
- **`/metrics` and `pprof` stay on the admin port only.** `pprof` exposes profiling data and can be used to load the process.

## Alternatives considered

| Alternative | Why it was rejected |
|---|---|
| **Health checks on the admin port only** | The port is not published, so the required health check endpoint would be unreachable from the host. |
| **Four dashboards** (overview, delivery, control plane, RabbitMQ) | Each untested panel is a panel that can silently break. One dashboard whose every query is exercised by a smoke test is worth more than four that are not. |
| **Circuit state in the summary endpoint** | The breaker is in-process ([ADR-0007](0007-retry-policy-and-circuit-breaker.md) §7), so the api cannot read it without a new dependency. The worker's metric already exposes it. |
| **Key-based redaction alone** | Misses a recipient inside a struct, an error string, a span attribute or a metric label. The typed value and the canary scan cover those paths. |
| **One trace per attempt, always linked** | The first delivery of an ordinary notification would never appear under its request, which is the first thing a reader of a trace looks for. |
| **One trace per notification lifecycle** | Traces lasting hours are unreadable and break trace-duration tooling. |
| **Scraping RabbitMQ's default metrics endpoint** | It is aggregated; per-queue panels and the poison-queue alert need per-object series. |
| **Prometheus `/metrics` only** | Satisfies "metrics endpoint" literally, but the required rates and percentiles are not directly readable without PromQL. |
| **Summary computed from the Prometheus HTTP API** | Makes the api depend on the monitoring stack. |
| **High-cardinality labels** (`id`, `template_name`, tenant) | Unbounded series growth. |
| **End-to-end latency from `created_at` or `due_at`** | Scheduled messages look late, or retries look instant (section 1). |
| **OpenTelemetry Collector** | Not needed at this scale; Jaeger accepts OTLP directly. Recommended for production. |
| **`zap` or `zerolog`** | `slog` is in the standard library and fast enough at this volume. |
| **Every transition at `info`** | About 1,500 lines per second of low-value noise. |
| **Liveness probes that check dependencies** | Turn a dependency outage into a restart storm. |
| **Readiness based only on the TCP connection** | Reports ready while the consumer loop is down. |

## Consequences

**Positive**

- Every figure the requirements ask for is available two ways: as Prometheus metrics for the dashboard and alerts, and as JSON for direct inspection.
- Latency measurements are honest across scheduling and retries.
- An ordinary delivery is one trace from the request to the provider; retries and scheduled deliveries stay readable and linked.
- Personal data is protected by a typed value and a central handler, and the protection is checked against real output by the canary scan.
- The dashboard and the alert rules are tested, not only shipped.

**Negative / costs**

- The summary endpoint runs percentile queries; caching and `singleflight` bound the cost, but it is still database load.
- Circuit state is visible only through Prometheus, not through the JSON summary.
- Instrumentation spans every component and must be kept up to date as code changes.
- Value-based redaction protects only against values the worker knows (the message's own recipient and content), not arbitrary personal data a provider might return.
- Sampling must be tuned per environment.

## Review history

Changes made after the design review, recorded in the [design review log](../ai/design-review-log.md):

- Health endpoints are also served on the api's public port, because the admin port is not published and the required endpoint was unreachable from the host. (D33)
- Four dashboards and five untested alerts became one "Overview" dashboard with a panel smoke test and `promtool`-checked rules; metrics for leadership and stale events were replaced by reconciliation and oldest-queued-age metrics, and the alerts received first thresholds, with the two metrics the expiry rule needs. (D41, D74, D75)
- Redaction gained a `Recipient` log type and a canary scan over logs, spans and metrics, because key-based redaction misses values outside named attributes. (D42)
- The trace rule became hybrid: the first attempt of a non-scheduled notification sits under the request's trace, and retries and scheduled deliveries start linked traces. (D13)
- The summary dropped its `circuits` field and gained a defined `success_rate` and a separate `backlog_due` cache; RabbitMQ is scraped per object; `X-Request-ID` is validated; Jaeger is pinned to 2.x. (D39, D9, D35, D43)
