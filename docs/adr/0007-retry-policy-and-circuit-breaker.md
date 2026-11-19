# ADR-0007: Retry policy, rate limiting and circuit breaker

- **Status:** Accepted
- **Date:** 2026-11-18
- **Related:** [ADR-0001](0001-rabbitmq-as-dispatch-broker.md), [ADR-0002](0002-postgres-as-outbox-scheduler-and-retry-delay.md), [ADR-0003](0003-bounded-relay-buffer-and-sweepers.md), [ADR-0004](0004-data-model-state-machine-and-fencing.md), [ADR-0005](0005-service-topology-and-status-events.md), [ADR-0009](0009-observability.md)

## Context

The business context requires failed deliveries to be retried intelligently (R5), and the delivery and retry logic is explicitly left to the candidate's design (R19). Delivery is also bounded by a limit of **at most 100 messages per second per channel** (R15), the provider's request and response formats are given (R23, R24), and "Failure Handling" is listed as a bonus feature (R25).

A broker supplies only the mechanics: holding messages, redelivery and dead-lettering. The intelligence has to live in the worker:

- deciding *whether* to retry;
- deciding *when*;
- deciding *how many times*;
- telling a broken message apart from a broken provider.

## Decision

### 1. Classify failures by scope, not only by retryability

A binary "retryable or not" split is insufficient. A failure is either:

- **message-scoped:** the problem is this message (an invalid recipient). Only this message is affected, and it consumes this message's retry budget.
- **provider-scoped:** the problem is the provider (bad credentials, throttling, an unreachable host). The circuit breaker opens, messages wait, and **their retry budgets are not consumed**.

Without this distinction, a single misconfigured API key would push every queued message through its retry budget into permanent `failed` within minutes, although the only fault is one configuration value.

The scope is certain for some results (`401`, `429`, a refused connection) and a judgement for others: a `5xx` or a timeout after the request was sent may be caused by the message or by the provider. Those are treated as message-scoped, so they consume budget, and they count toward the breaker so that many of them together are recognised as a provider problem.

### 2. Classification table

| Provider result | Outcome | Scope | Consumes budget | Counts toward breaker | Handling |
|---|---|---|---|---|---|
| `202` with the expected body | `success` | — | — | Success | `sent`; `messageId` is stored as `provider_message_id` (section 6) |
| Any other `2xx`, or a `202` whose body cannot be parsed | `success` | — | — | Success | `sent`, with a warning: another `2xx` increments `provider_unexpected_2xx_total{channel,provider,code}`, an unparseable body increments `provider_malformed_response_total{provider}` ([ADR-0009](0009-observability.md) §1). The provider accepted the message, so resending would create a duplicate. |
| `3xx` | `provider_fatal` | **Provider** | No | Opens it | Redirects are **not followed** (section 11). In this provider contract a redirect means a wrong endpoint URL. |
| `400`, `422`, other `4xx` not listed below | `permanent` | Message | — | No | `failed`, `failure_reason = 'provider_rejected'` |
| `401`, `403`, `404`, `410` | `provider_fatal` | **Provider** | No | Opens it | Wrong credentials or a wrong or expired endpoint. The message goes to `retrying` without incrementing `failures`; the circuit opens **immediately** for 1 minute; an alert-level log is written |
| `408`, `409`, `425` | `retryable` | Message | Yes | Yes | Back-off |
| `429` | `throttled` | **Provider** | No | Opens it | The message goes to `retrying` without incrementing `failures`, deferred by `Retry-After`; the circuit opens for the same duration (section 4) |
| `500`, `502`, `503`, `504` | `retryable` | Message | Yes | Yes | Back-off; `Retry-After` honoured on `503` |
| Connection refused, DNS or TLS failure, or any error **before the request was written** | `unreachable` | **Provider** | No | Yes | Nothing was sent, so there is no duplicate risk and the message is not at fault. The message goes to `retrying` with a back-off delay. |
| Timeout or connection error **after the request was written** | `unknown` | Message | Yes | Yes | Delivery is ambiguous; retried with the same provider idempotency key (section 6) |
| Circuit opened between claim and send, or the send deadline passed before the send could start | `aborted` | — | No | No | Nothing was sent. The row returns to `retrying`: with `due_at` at the circuit's next half-open time (section 8), or due at once after a missed send deadline (section 5). Excluded from success and failure rates. |
| Panic recovered in the worker | `internal_error` | Message | Yes | No | It may be a persistent bug; the row fails when the budget runs out |

**Detecting whether the request left the process.** The worker uses `net/http/httptrace`'s `ClientTrace.WroteRequest`. The request counts as *sent* only if the hook fired **and** `WroteRequestInfo.Err == nil`. The hook also fires on a failed write (for example a broken pipe), and such a request is treated as not sent (`unreachable`).

**A response decides.** When a response arrived, its status is classified and `WroteRequest` is not consulted; the flag matters only when there is no response. The table is implemented once, in `domain.Classify`, and the three columns "scope", "consumes budget" and "counts toward breaker" are properties of the outcome (`Outcome.Scope`, `ConsumesBudget`, `Breaker`), so the retry decision and the breaker read the same source.

**Counters.** `attempt_count` is the fencing token and increments on every claim. `failures` is the retry budget and increments only for budget-consuming outcomes ([ADR-0004](0004-data-model-state-machine-and-fencing.md)).

**webhook.site limits.** The default provider is a free webhook.site URL, which has a request cap and expires. Whatever status it returns in those cases is classified as provider-scoped, so a capped or expired URL opens the circuit instead of failing notifications.

### 3. Back-off profiles per priority

The delay uses **full jitter**: `delay = jitter × min(cap, base × 2^failures)`, with a floor of 1 second, where `jitter` is a random number in `[0, 1)` supplied by the caller ([ADR-0004](0004-data-model-state-machine-and-fencing.md) §5). The floor exists because full jitter can produce a delay close to zero, which would retry at once against a provider that has just failed; one second protects the provider. AWS's analysis of back-off strategies shows that full jitter minimises total work under contention.

| Priority | `base` | `cap` | `max_failures` | Default TTL (`expires_at`) | Largest burst one channel can drain within the TTL |
|---|---|---|---|---|---|
| `high` | 1 s | 30 s | 5 | due time + **10 minutes** | 600 s × 98 ≈ 58,800 |
| `normal` | 5 s | 10 min | 8 | due time + **6 hours** | 21,600 s × 98 ≈ 2.1 M |
| `low` | 30 s | 1 h | 10 | due time + **24 hours** | 86,400 s × 98 ≈ 8.5 M |

- **In the formula, `failures` is the count before the current outcome** (the argument of `decideRetry`). The first retry of a `high` notification therefore draws from `[0, 1 s)` and is lifted to the floor; the second draws from `[0, 2 s)`. For an outcome that does not consume budget the count does not grow, so its back-off stays where the last budget-consuming failure left it.
- **`max_failures` is the total number of budget-consuming failures** a notification may have: the row fails on the failure that makes `failures` equal to `max_failures` (with `max_failures = 1`, on the first one). `failures` is also incremented on that last failure, so an exhausted row shows `failures = max_failures` ([ADR-0004](0004-data-model-state-machine-and-fencing.md) §5).
- **Every notification has an `expires_at`.** The due time is `scheduled_at` if set, otherwise creation time. Because provider-scoped failures do not consume budget, the TTL is what guarantees that nothing retries forever.
- **The TTL also caps the burst a priority can absorb.** The last column assumes the channel carries nothing else and nothing is retried; messages beyond it expire before they are sent. A client that submits a larger burst must set a longer `expires_at`. The README states these figures.
- **The reason of an exhausted budget names the last outcome:** `max_failures_unknown_outcome` after `unknown`, `internal_error` after `internal_error`, otherwise `max_failures`.
- **Expiry shortcut:** if the retry would fall at or after `expires_at`, `decideRetry` fails the notification immediately (`failed / expired`) instead of scheduling a retry that can never happen. The delay compared is the final one, hint included. When the budget is exhausted by the same outcome, the budget is the reason reported.
- A retried notification returns to **its own lane** and is subject to that lane's HWM ([ADR-0003](0003-bounded-relay-buffer-and-sweepers.md)).
- The profiles and TTLs are configuration, and all of this logic lives in the deterministic `decideRetry` function.

### 4. `Retry-After` and throttling

- Both forms are parsed: delta-seconds and HTTP-date. A value that is malformed, zero or a date in the past is not usable and counts as absent. An HTTP-date is compared with the worker's wall clock; the cap below bounds what a skewed clock can do.
- The header is used on `429` and `503` only (section 2); on any other status it is ignored.
- The retry delay is `max(backoff, retry_after, time until the circuit's next half-open probe)`, never below the floor. The worker passes the larger of the last two to `decideRetry` as its one hint.
- `Retry-After` is **capped at 10 minutes**, so an absurd value cannot stall a channel. The cap applies to the hint, after the default below.
- **`429` without a usable `Retry-After`** gets a default of 5 seconds, **doubling on consecutive `429`s up to 2 minutes**. This matters because throttling does not increase `failures`, so the back-off alone would stay at `base`, and the provider would be hit again every second.

### 5. Rate limiting: GCRA in Redis

**The guarantee.** "Maximum 100 messages per second per channel" is read as: across all workers, no one-second window at the provider contains more than 100 messages of one channel. The system guarantees this by **pacing each channel at 98 msg/s with zero burst**. Pacing at exactly 100 cannot give the guarantee: with a 10 ms interval, any timing jitter between the slot and the arrival puts 101 messages into some one-second window. The margin of two is derived from the jitter that the load-smoke test measures, and the rate is configurable.

| Aspect | Rule |
|---|---|
| Algorithm | GCRA (generic cell rate algorithm), implemented as a Lua script in Redis with key `rl:{channel}` |
| Rate | **98 msg/s per channel**, one slot about every 10.2 ms |
| Burst tolerance | **Zero.** Sends are spaced evenly. |
| Clock | The script reads Redis `TIME`, so every worker uses the same clock and local clock skew is irrelevant |
| Result | A **relative wait** until the caller's slot, not an absolute time, so the caller's clock does not matter either |
| Reservation horizon | If the wait would exceed 250 ms, the script does not reserve; the pacer asks again after 50 ms. This bounds how many future slots a crashed process can waste. |
| Missed slot (D77) | A send starts within `PACER_SLOT_TOLERANCE` (5 ms) after its slot. A claim that returns later than that gives the slot up (it is not returned to the limiter) and reserves a new one through the same script, horizon included. The same holds for a slot kept after a zero-row claim that is stale when the next message is ready. Counted in `pacer_slots_missed_total{channel}` ([ADR-0009](0009-observability.md)). |
| Send deadline (D77) | If the send cannot start within `lease − HTTP timeout − 5 s` of issuing the claim call (15 s with the defaults; derived, not configured), the attempt is closed as `aborted` and the row returns to `retrying` without consuming budget (section 2) |
| Never nack on limit | A message is never pulled and then put back because of the rate limit: that would cause a spin loop or needless write amplification. A late claim **holds its message** (claimed, acked, the row `processing`) while it waits for a new slot. |
| Redis unavailable | Fall back to an **in-process limiter** at `98 / WORKER_REPLICAS`. Sending does not stop. The limit is then **approximate**: replicas no longer share a clock or a schedule, and while some replicas use Redis and others the fallback, the channel can briefly exceed its rate. `limiter_fallback_active` is set and alerts ([ADR-0009](0009-observability.md)). |

**Why a missed slot is given up.** The pacer claims during the wait, and a claim can outlast it: a worker that receives a message before the relay commits waits on the relay's row lock ([ADR-0002](0002-postgres-as-outbox-scheduler-and-retry-delay.md)) while the pacer goes on reserving the following slots. If each late claim sent at once, several sends would leave together, next to slots that are used on time. Three are enough: a batch arrives on an idle channel, the relay commits 30 ms later, the claims for the slots at 0, 10 and 20 ms send at 30 ms, and the window `[30 ms, 1030 ms)` holds 101 arrivals. With the rule, every send leaves at a slot that was reserved for it, so the spacing of the script is also the spacing at the provider, up to the tolerance.

**Why 5 ms.** At 98 msg/s the limit of 100 leaves two slots, about 20 ms, of total lateness in a window. The tolerance must stay well below that; the rest is for scheduler and network jitter, which `load-smoke` measures.

Adding workers beyond the point where the limit is reached does not increase throughput. Workers scale to *reach* the limit (by Little's law, about 20 concurrent sends per channel at 200 ms latency) and for availability, not to exceed it.

### 6. Provider contract

The request and response follow the assessment's formats exactly (R23, R24).

| Part | Rule |
|---|---|
| Request | `POST {PROVIDER_URL}` with the JSON body **exactly** `{"to", "channel", "content"}`: the recipient, the channel name and the rendered content. No other field is sent. |
| Headers | `Idempotency-Key: <notification_id>` (the header name is configurable per provider) and `traceparent` |
| Expected response | `202` with `{"messageId", "status", "timestamp"}`. The body is parsed; `messageId` is stored as `provider_message_id`, which is **not** assumed to be unique. |
| Other `2xx` | Treated as success; counted in `provider_unexpected_2xx_total{channel,provider,code}` (section 2) |
| Default target | A **webhook.site** URL configured to answer `202` with the body above ([ADR-0005](0005-service-topology-and-status-events.md)). The README shows the URL's creation date, and the URL is renewed right before submission. |
| `make demo` | Probes the URL first and sends at most 10 requests, to stay well inside the free URL's request cap |
| `mockprovider` | Started by the mock profile (`make up-mock`, [ADR-0005](0005-service-topology-and-status-events.md)). Used by automated tests, load tests and the batch and load examples ([ADR-0010](0010-testing-and-ci.md) §2). It implements the same contract and uses the idempotency key to count duplicates. |

**Provider idempotency key.** The value is the same on every attempt, because the goal is that the notification is delivered once, not that each attempt is unique. Some providers deduplicate on such a key, which makes an `unknown` outcome safe to retry. **webhook.site ignores the header**, so against the default provider every attempt that ends `unknown` can produce a duplicate; the README says so.

### 7. Circuit breaker: in-process, keyed by `(channel, provider)`

The breaker is a **pure state machine with an injected clock**, held in the worker process. Redis is not involved; it is used only for the rate limit.

| State | Behaviour |
|---|---|
| **Closed** | Outcomes are counted in a 30-second sliding window. The circuit opens when the window has **at least 20 requests and a failure rate of at least 50 %**, **or after 5 consecutive failures**. The second rule is what protects low traffic: three urgent messages during an outage must not burn their budgets while the window fills. Counted as failures: `retryable`, `unknown`, `unreachable`, `throttled`. **`permanent` is not counted** (invalid recipients must not open the circuit), and neither are `aborted` and `internal_error`. |
| **Open** | The open duration starts at 5 s and doubles on each failed probe, up to 2 minutes. `provider_fatal` opens the circuit directly for 1 minute; `throttled` for the `Retry-After` value (section 4). |
| **Half-open** | When the open duration ends, **one** real message is sent as the probe. Success closes the circuit and resets the window; failure opens it again with a doubled duration. |

- The pacer checks the breaker before it reserves a rate-limit slot (section 8).
- **State is per worker replica.** With several replicas, each one discovers an outage on its own, at a cost of a few extra failed requests per replica.
- **Why the provider is part of the key:** attempt records and breaker state are provider-aware from the start, so provider failover can be added later without changing either ([future work](../future-work.md)).

### 8. Order of operations in the worker

The pacer loop per channel is defined in [ADR-0005](0005-service-topology-and-status-events.md). The rules that concern this ADR:

1. **Breaker before limiter.** While the circuit is open, the pacer waits; it does not reserve slots and does not claim.
2. **A slot is reserved only when a message is waiting** in the local buffer.
3. **The claim happens during the wait for the slot,** so the claim's latency does not shift the send away from its slot. **If the claim returns more than the tolerance after the slot, the slot is given up and a new one is reserved** (section 5); the send never happens "now" on a slot that has passed. The breaker is asked again at the new slot's time.
4. **A claim that affects zero rows keeps its slot** for the next message, unless the slot is stale by more than the tolerance when that message is ready; then it is dropped.
5. **If the circuit opens between the claim and the send,** nothing is sent: the attempt is closed as `aborted` and the row returns to `retrying` without consuming budget, due at the circuit's next half-open time.

**While a circuit is open,** prefetched messages wait unacknowledged in the worker and further messages wait in the queue. The dispatch queues carry a 24-hour consumer timeout so that the broker does not cancel the consumer during a long outage ([ADR-0001](0001-rabbitmq-as-dispatch-broker.md)). Notifications that expire while waiting are failed by the expiry pass, and their messages are dropped at claim.

### 9. Poison messages

- Panics in a worker are recovered per message and recorded as `internal_error`, which consumes budget.
- A message that kills the process during its send has already been acknowledged ([ADR-0005](0005-service-topology-and-status-events.md)). Its row is `processing`; the lease sweeper marks the attempt `unknown`, increments `failures` and retries it, and after `max_failures` the row is `failed` with `max_failures_unknown_outcome` ([ADR-0003](0003-bounded-relay-buffer-and-sweepers.md)). **The retry budget and the lease sweeper are the poison defence.**
- The broker's dead-letter queues are a safety net for deliveries that fail before the claim. Their depth is an alerting metric ([ADR-0009](0009-observability.md)).

### 10. Dependency failure matrix

| Failed dependency | api | relay | worker | Outcome |
|---|---|---|---|---|
| **RabbitMQ** | Keeps accepting; its event publisher drops events | Starts no transactions; reconnects; reconciles on reconnection | Stops consuming; reconnects | Backlog accumulates in Postgres; nothing is lost |
| **Postgres** | `/readyz` fails; requests return `503` | Cycles fail and are retried | Cannot claim; final writes are retried for up to 60 s | Messages wait in RabbitMQ; sends that finished during a long outage may be repeated |
| **Redis** | Unaffected | Unaffected | In-process limiter; the rate limit is approximate | Sending continues |
| **Provider** | Unaffected | Unaffected | Circuit opens; budgets are preserved | The backpressure chain absorbs the outage ([ADR-0003](0003-bounded-relay-buffer-and-sweepers.md)) |
| **A worker dies** | Unaffected | The lease sweeper recovers its rows | Restarted by the container runtime | Transition 7 ([ADR-0004](0004-data-model-state-machine-and-fencing.md)) |
| **The relay dies** | Unaffected | Restarted by the container runtime | Drains what is queued, then idles | Publishing, expiry and recovery pause; nothing is lost |

The RabbitMQ, Postgres, Redis, provider and worker rows each have a chaos scenario ([ADR-0010](0010-testing-and-ci.md) §1). The relay row is covered by integration tests of its start-up path, not by a chaos scenario.

### 11. HTTP client

| Setting | Value and reason |
|---|---|
| Timeouts | 2 s to connect, 10 s in total. Both are below the 30 s lease. |
| Redirects | **Not followed:** `CheckRedirect` returns `http.ErrUseLastResponse`. Go would otherwise follow `301`, `302` and `303` by repeating the request as a `GET` without a body, and a `2xx` from the redirect target would be recorded as a false `sent`. |
| Transport replays | `req.GetBody = nil`. One attempt must be exactly one request. Go's transport can replay a request on a broken connection when the request carries an `Idempotency-Key` header and its body can be rebuilt; removing `GetBody` is expected to prevent that. An experiment confirms it before the client is written, with disabled keep-alives as the fallback ([implementation plan](../plan/implementation-plan.md), E5). |
| `MaxIdleConnsPerHost` | Set to the per-channel concurrency (25). **Go's default is 2**, which with 25 concurrent requests to one host causes constant connection churn. |
| Body handling | Every response body is drained before closing: `io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))`, then `Close()`. A body that is closed without being read to the end is not returned to the pool. A test asserts `httptrace.GotConnInfo.Reused`. |
| Tracing | The `otelhttp` transport adds `traceparent` to provider requests ([ADR-0009](0009-observability.md)). |

## Alternatives considered

| Alternative | Why it was rejected |
|---|---|
| **A circuit breaker shared in Redis** (window buckets and a probe lock maintained by Lua scripts) | Its first reason was that the relay's stale-queued sweeper needed to read circuit state; that sweeper no longer exists ([ADR-0003](0003-bounded-relay-buffer-and-sweepers.md)). Its second reason, that each replica rediscovers an outage, costs a few requests per replica. An in-process breaker had to exist anyway as the fallback for a Redis outage. Recorded in [future work](../future-work.md). |
| **Pacing at exactly 100 msg/s** | Any jitter produces a one-second window with 101 arrivals, so the limit and its test could not hold (section 5). |
| **Classifying connection failures as message-scoped** | At low traffic the breaker's request threshold is never reached, so an outage would consume the retry budgets of the few messages in flight although nothing was wrong with them. |
| **Following redirects** | Produces a false `sent` (section 11). |
| **Provider failover in this version** | A second provider with routing while the primary's circuit is open. It needs a second real target to demonstrate and tests of its own. Breaker keys and attempt records are provider-aware, so it can be added later; recorded in [future work](../future-work.md). |
| **Sending `subject` or other extra fields to the provider** | The assessment gives the request body as three fields. |
| **Binary retryable / non-retryable classification** | Provider-scoped failures would consume every message's budget (section 1). |
| **Fixed or plain exponential back-off** | Synchronised retries hammer a recovering provider (thundering herd). |
| **Counting `429` against the retry budget** | Throttling is the provider's state, not the message's; counting it would fail messages because of load. |
| **Stop sending when Redis is down** | A cache outage would stop all delivery. |
| **No limiter when Redis is down** | Abandons the rate limit entirely instead of approximating it. |
| **A static per-worker quota at all times** | Fragile when workers crash or scale, and it wastes capacity under uneven load. It is used only as the fallback. |
| **Token bucket with a burst** | Permits more than 100 messages in a one-second window. |
| **Nack and requeue when the limit is hit** | Spin loop, or write amplification if delayed. |
| **Holding a chosen message while waiting for a slot** | A lower-priority message would be sent although a more urgent one arrived during the wait. |
| **Adaptive limits (AIMD) based on `429`s** | Useful if the provider's real limit is below ours. Recorded in [future work](../future-work.md). |

## Consequences

**Positive**

- One configuration error or provider outage cannot destroy the queue's retry budgets, at high or low traffic.
- Retries are spread out, bounded in count and bounded in time.
- With Redis available, no one-second window exceeds 100 messages per channel, with a measured margin.
- The provider request is exactly what the assessment specifies, and one attempt is exactly one HTTP request.
- Outages are absorbed without operator action, and recovery is automatic through half-open probes.

**Negative / costs**

- Each channel delivers at 98 msg/s, not 100.
- While Redis is down the rate limit is approximate.
- Breaker state is per replica.
- Against a provider that ignores idempotency keys, including the default one, every `unknown` attempt can produce a duplicate.
- The default TTLs cap the burst each priority can absorb; the Postgres backlog itself is not limited.
- The GCRA Lua script is custom code and needs integration tests against a real Redis ([ADR-0010](0010-testing-and-ci.md)).
- Many parameters (profiles, thresholds, durations) must be tuned. All of them are configuration and are validated by the load and chaos tests.

## Review history

Changes made after the design review, recorded in the [design review log](../ai/design-review-log.md):

- The rate became 98 msg/s per channel and the guarantee was restated, because pacing at exactly 100 cannot keep every one-second window at or below 100; the Redis fallback is described as approximate. (D24, D25)
- The circuit breaker moved from Redis into the worker process and gained a consecutive-failure rule; connection failures before the request is written became the provider-scoped outcome `unreachable`. (D26, D27)
- Redirects are no longer followed and transport replays are disabled, because either could turn one attempt into a different or repeated request. (D27, D29)
- The provider contract (request body, `202` response, other `2xx`, headers, webhook.site as default) was written down; it had been defined nowhere. (D28, D44)
- The pacer's order of operations and the `aborted` outcome were defined, provider failover moved to future work, and the default TTLs were tied to the burst each priority can absorb. (D23, D32, D31)
- In P3 the classification table and `decideRetry` were implemented in full. Stated here as a result: a response decides before `WroteRequest`; the exponent of the back-off is the failure count before the current outcome; the reason of an exhausted budget names the last outcome; budget exhaustion is reported in preference to the expiry shortcut; `Retry-After` is used on `429` and `503` only and an unusable value counts as absent. (D17, D27, D30)
- In P1 the rate limit gained the missed-slot rule and the send deadline, after the implementation showed that a claim can outlast its slot while it waits on the relay's row lock; `max_failures` was stated as the total number of budget-consuming failures. (D77)
