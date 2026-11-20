# Known limitations and future work

This document collects what the current design does not do. It has three parts:

- [Known limitations](#known-limitations-of-the-current-implementation): behaviour a user or operator should know about.
- [Interpretations of the assessment](#interpretations-of-the-assessment): places where the requirements leave a choice open, and the choice made.
- [Future work](#future-work): items left out of scope, with the reason and the point where each would connect to the current design.

Requirements are referred to by the R-IDs of the requirements map in the [architecture overview](architecture.md#requirements-map).

## Known limitations of the current implementation

| Limitation | What it means | Why it is acceptable here | Reference |
|---|---|---|---|
| **Each priority can absorb only a bounded burst** | A notification expires after its TTL (high 10 min, normal 6 h, low 24 h by default). One channel drains 98 msg/s, so at most about 58,800 `high`, 2.1 M `normal` or 8.5 M `low` messages can be sent within their default TTL; the rest expire unsent. | The TTLs are configurable and a client can set `expires_at` per notification. Sending an expired one-time password would be worse than failing it. | [ADR-0007](adr/0007-retry-policy-and-circuit-breaker.md) §3 |
| **The backlog in Postgres is not limited** | The API accepts notifications faster than they can be delivered, without a ceiling. Expiry is the only bound. | A burst is the expected workload, and Postgres holds millions of pending rows comfortably. Admission control is future work (item 6). | [ADR-0003](adr/0003-bounded-relay-buffer-and-sweepers.md) §2 |
| **Strict priority can make `low` wait** | While `high` traffic fills a channel's full rate, `normal` and `low` messages of that channel are not sent. | Strict priority is the reading of "priority queue" adopted here (R16); `low` traffic is bounded by its expiry. | [ADR-0001](adr/0001-rabbitmq-as-dispatch-broker.md) |
| **A requeued message loses its priority** | A message returned to the queue (a worker shutting down, a lost connection) is delivered before the messages that are already waiting, whatever their priority (observed in experiment E1). A returned `low` message can therefore be sent before a waiting `high` one. | At most one prefetch buffer (25 messages per channel and replica) is returned at a time, so the effect is small and short. | [ADR-0001](adr/0001-rabbitmq-as-dispatch-broker.md) |
| **The rate limit is enforced when a send starts, not when it arrives** | The pacer starts each send at its own slot, at most 98 per second and channel. Arrivals at the provider can still bunch: when several sends wait for a cold connection (the TCP and TLS handshake after an idle period), their requests are written together once the connections are up. | Against the mock provider in Compose the effect is negligible: connections are local and are reused (`MaxIdleConnsPerHost` equals the concurrency). `load-smoke` measures arrivals at the provider and logs the largest one-second window. Pre-warming provider connections is future work. | [ADR-0007](adr/0007-retry-policy-and-circuit-breaker.md) §5, §11 |
| **The rate limit is approximate while Redis is down** | Each worker replica then limits itself to `98 / replicas`. Replicas no longer share a schedule, and during the switch-over a channel can briefly exceed its rate. | Stopping all delivery because a cache is down would be worse. An alert fires while the fallback is active. | [ADR-0007](adr/0007-retry-policy-and-circuit-breaker.md) §5 |
| **A single relay** | While the relay restarts, publishing, expiry and recovery pause. | Nothing is lost: work accumulates in Postgres, and the container runtime restarts the relay. Leader election is future work (item 15). | [ADR-0005](adr/0005-service-topology-and-status-events.md) |
| **The client library's connection recovery is opt-in and experimental** | Reconnecting to RabbitMQ and redeclaring the topology rely on the automatic recovery of `amqp091-go`. The library has it off by default and documents it as experimental: its behaviour or its API may change in a later release. | The library version is pinned in `go.mod`, and the behaviour the design relies on was run in experiment E4. The exit-and-restart path is the fallback: when recovery gives up after its budget (24 attempts at 5 s), the relay, the worker and the gateway exit, are restarted, declare the topology again and reconcile. | [ADR-0005](adr/0005-service-topology-and-status-events.md), [ADR-0003](adr/0003-bounded-relay-buffer-and-sweepers.md) §3 |
| **Reconciliation covers reconnections only** | A message that disappears while the relay stays connected (for example one dead-lettered by the broker) is not republished automatically. | An alert on the oldest queued age of `high` lanes, the manual command `docker-compose run --rm relay requeue-stale`, and expiry as the final bound. | [ADR-0003](adr/0003-bounded-relay-buffer-and-sweepers.md) §3 |
| **Circuit breaker state is per worker replica** | With several replicas, each one discovers a provider outage separately. | The cost is a few failed requests per replica. A shared breaker is future work (item 16). | [ADR-0007](adr/0007-retry-policy-and-circuit-breaker.md) §7 |
| **Duplicate sends are possible** | Delivery is at-least-once. Every attempt that ends `unknown` (a timeout after the request was sent, a worker that died after the provider answered, a database outage longer than the final-write retries) is retried, and the retry is a duplicate unless the provider deduplicates on the idempotency key. | Some providers honour idempotency keys; **webhook.site does not**. Load and chaos tests count duplicates at the provider and report them. | [ADR-0007](adr/0007-retry-policy-and-circuit-breaker.md) §6 |
| **Whether a failed request "was sent" is read at one instant** | Without a response, the attempt is `unreachable` (nothing was sent, no budget consumed) or `unknown`, decided by whether the client had finished writing the request when the call returned. A request that the timeout cuts off in the middle of its write is recorded `unreachable`, although part of it left the process. A `Retry-After` given as a date is compared with the worker's own wall clock. | A provider cannot act on a partial request, and the two outcomes differ only in budget accounting: both are retried with the same idempotency key. The cap on `Retry-After` (10 minutes) bounds the effect of a skewed clock. | [ADR-0007](adr/0007-retry-policy-and-circuit-breaker.md) §2, §4 |
| **After an idle period, the first minute of a large burst runs at about a third of the rate** | Every pool plans each execution with the current table size (D79), which removed the plans cached while a table was small. What is left is a planner misled by its statistics: when the tables were last analysed while the outbox was empty (the normal idle state) and then 1,000,000 rows arrive, the claim looks its row up through the partial expiry index, which the statistics call empty. On the Compose stack each claim then took about 290 ms and each channel delivered about 35 messages per second instead of 98, until the next `ANALYZE`. The lease sweeper's per-row update (29 ms per row with 200,000 expired leases) and the relay's count are misled the same way. | It ends by itself with autovacuum's `ANALYZE` (with the default settings at most about a minute after the burst), nothing is lost or sent twice, and a burst of the size `load-smoke` sends does not show it. Measured so far, in the first minute after the burst: 6,344 notifications sent, against 18,253 on tables that were never analysed and 5,846 before D79. Not measured yet: the claim's cost at smaller backlogs (1,000 to 100,000 pending rows) and the three remedies (an explicit `ANALYZE` by the relay, per-table analyse settings, a changed look-up in the claim). Both are named measurements of the tuning of P6 (the author's decision on open issue 11 of P3); until then the behaviour stays as described. | [ADR-0003](adr/0003-bounded-relay-buffer-and-sweepers.md) §6 |
| **A claim that keeps failing holds its concurrency slot** | A claim that fails with a database error is retried in place with back-off until it succeeds or the worker shuts down. The worker does not tell a transient error from a permanent one: a notification whose claim can never succeed would hold one of the channel's 25 slots for good. | A claim is one fixed statement on one row; an error other than an unreachable or failing database would be a defect, which the claim's log lines show. The earlier behaviour (nack and redeliver at once) looped on such a message as well, at the cost of a rate-limit slot per round. | [ADR-0005](adr/0005-service-topology-and-status-events.md), worker internals |
| **Two narrow windows in the connection epoch** | A delivery from before a reconnect is never acked: the epoch advances inside the client library's recovery, before the channel is reset, and the check is one step with the ack. Two windows remain, both only after a channel error on the consumer's own channel, which the worker does not cause in normal operation: a full connection loss and recovery (at least 5 s) falling inside the few statements between the library's channel-close hook and its reopen of that channel, after which an ack could carry an old tag; and an ack written in the microseconds between the broker's channel close and the library marking the channel closed, which makes the broker close the connection. | The first would need a hook at the library's channel reset, which `amqp091-go` v1.17.0 does not offer; its consequence would be one message removed unclaimed, which reconciliation or `requeue-stale` recovers. The second only causes a reconnect. The library's recovery is opt-in and marked experimental; exit and restart remains the fallback. | [ADR-0005](adr/0005-service-topology-and-status-events.md), connection epoch |
| **The default provider URL expires and is capped** | The stack sends to a free webhook.site URL by default. Free URLs expire after about a week and accept a limited number of requests. | The README shows the URL's creation date and how to create a new one; `make demo` sends at most 10 requests; batch and load examples use `mockprovider`. An expired or capped URL opens the circuit instead of failing notifications. | [ADR-0007](adr/0007-retry-policy-and-circuit-breaker.md) §6 |
| **Requests to the default provider URL are publicly visible** | The default webhook.site URL is in a public repository, and anyone who has the URL can see the requests it received, including recipients and content. | Demo and test data use only reserved fake values (`+905551234567`, `example.com` addresses). Real recipients must never be sent through the default URL; set `PROVIDER_URL` to a private provider or use the mock profile (`make up-mock`). | [ADR-0005](adr/0005-service-topology-and-status-events.md) |
| **No authentication or authorisation** | Anyone who can reach the API can use it. | Not part of the assessment; adding it properly means introducing tenants (items 1 and 2). | [ADR-0006](adr/0006-api-contract-and-three-layer-idempotency.md) |
| **`cancelled` and `failed` can become `sent`** | If a late result proves that a message was delivered, its status changes to `sent` even after a cancel or a failure. | The status then matches what happened at the provider. Documented in the API. | [ADR-0004](adr/0004-data-model-state-machine-and-fencing.md) §4 |
| **Status events can be lost** | WebSocket updates are best-effort. An event publisher drops events when its buffer is full and while it has no connection to the broker, which can last up to one `AMQP_RECOVERY_RETRY_INTERVAL` (5 s) after the broker is back. Events published while the gateway is reconnecting are lost too, and a client that stays connected through that is **not** sent a new snapshot: it learns of a status it missed only from the next event of that notification, or from `GET`. | The gateway sends a snapshot on subscribe, and `GET` is authoritative. Sending every subscription a new snapshot after a reconnection is the remedy, at the cost of a read per subscription. | [ADR-0005](adr/0005-service-topology-and-status-events.md) |
| **The summary's and the gauges' counts grow with the backlog** | `GET /v1/metrics/summary` and the relay's lane gauges count the waiting rows. Their statements read only rows that are not finished, through the partial indexes, but on every dataset tried the planner chose the expiry index with a status filter, not the lanes' own indexes: the cost grows with the number of waiting rows. The summary's end-to-end percentiles join the window's successful attempts to their notifications; with a small table the planner scanned `notifications` for it. | The answer is cached (2 s, the due backlog 10 s) and the gauges are measured every 10 s. Their latency on the 1,000,000-row dataset is measured in the load phase, with the remedy if one is needed. | [ADR-0009](adr/0009-observability.md) §1, §2 |
| **A metric can differ from the database** | `delivery_attempts_total` counts an attempt when the provider call returns, before the final write; `circuit_state` reads "open" for a circuit whose open period has ended until the next message asks. | Metrics show rates and trends; the database is authoritative, and the circuit alert is written on "not closed". | [ADR-0009](adr/0009-observability.md) §1 |
| **The per-channel worker layout runs with the mock provider only** | `docker-compose.override.worker-per-channel.yml` points its three workers at `mockprovider` and depends on it, so it is used on top of the mock profile; without `docker-compose.mock.yml` Compose refuses the configuration. A per-channel layout against webhook.site or another real provider is not offered. | An override cannot inherit the mock profile's provider URL for services it adds (`extends` reads `docker-compose.yml` only), and a layout that silently sent two channels to the public webhook.site URL would be worse. The layout exists for tests and load runs, which use the mock; the default stays one worker. | [ADR-0005](adr/0005-service-topology-and-status-events.md), [ADR-0010](adr/0010-testing-and-ci.md) §1 |
| **Single-node RabbitMQ, Postgres and Redis in Compose** | No replication happens on one node. | The configuration is production-shaped (quorum queues, confirms); item 14 lists the production setup. | [ADR-0001](adr/0001-rabbitmq-as-dispatch-broker.md) |
| **A deactivated template can still be used by a request that is in flight** | A create reads its template before its transaction. A request that had loaded the template when the deactivation committed still creates its notifications; every later request is refused. | Deactivation stops new use; it is not a recall. Notifications already created are unaffected by design, and the window is one request long. | [ADR-0008](adr/0008-templates-and-scheduling.md) §5, §7 |
| **A retry that overtakes its own first request renders again** | A request with an `Idempotency-Key` that uses a template looks the key up before it renders. If the retry arrives before the first request has committed, it does not see the key and renders with the templates as they are then; if the template was deactivated in between, it is answered `422` although the first request succeeds. | It needs a retry within milliseconds of its original and a deactivation between the two. The claim inside the transaction still prevents a second notification. | [ADR-0008](adr/0008-templates-and-scheduling.md) §5 |
| **A template can only ask whether a variable is empty** | `{{ if .flag }}` is false for an empty value: `false`, `""` or an optional variable that is left out. Every other value is true, the number `0` and the string `"false"` included. There are no comparisons and no negation: the allow-list refuses every builtin function. | The safe subset is deliberately small, and the boolean a client sends behaves as it reads (D40 as amended). The API description says what counts as empty. | [ADR-0008](adr/0008-templates-and-scheduling.md) §2, §4 |
| **`truncate` counts code points** | A character built from several code points (some emoji, combining marks) can be cut in the middle of what a reader sees as one character; a code point is never split. | Enough for shortening names and titles; grapheme clusters would need a Unicode segmentation table. | [ADR-0008](adr/0008-templates-and-scheduling.md) §3 |
| **`scheduled_at` and `expires_at` are judged by the API's clock** | The API decides whether a time is in the past or too close; the database's clock decides when a row is due and when it expires. With the two clocks apart, a time accepted as "a moment ahead" can already be due. | The 30-second tolerance exists for this skew, and the rules are limits for clients, not delivery guarantees: delivery always follows the database's clock. One clock for both would cost a database round trip per request. | [ADR-0008](adr/0008-templates-and-scheduling.md), scheduling |
| **Scheduling precision is about 100 ms** | A notification is published up to one relay tick after it becomes due. | Sufficient for notifications. | [ADR-0002](adr/0002-postgres-as-outbox-scheduler-and-retry-delay.md) |
| **Value-based redaction only knows the message's own recipient and content** | Arbitrary personal data in a provider's error text is not detected, and neither is the message's own value in another encoding (URL-encoded, base64, a number re-formatted with spaces). A phone number without its plus sign is. | Covers the common case of a provider echoing its input back, in plain text or JSON. | [ADR-0009](adr/0009-observability.md) §4 |
| **Key-based redaction cannot see inside a struct** | A struct with a content field, logged as one value, would be written by the JSON encoder as it is. No such struct is logged, the role-level log-capture tests and the canary scan would show one, but nothing in the type system prevents it. | The recipient is protected by its type wherever it goes; giving the types that carry a content a log form of their own is the remedy. | [ADR-0009](adr/0009-observability.md) §4 |
| **An image built without `make` reports version `dev`** | The version in every log line and span is the commit, passed to the build by `make`. A plain `docker-compose up` builds without it. | The stack runs the same; `GIT_SHA=$(git rev-parse HEAD) docker-compose up --build` sets it. | [ADR-0009](adr/0009-observability.md) §4 |
| **No data retention** | Rows accumulate until partitioning and retention are added (item 7). | Not needed for the lifetime of an assessment. | [ADR-0004](adr/0004-data-model-state-machine-and-fencing.md) |

## Interpretations of the assessment

Where the assessment leaves a point open, the design adopts one reading and states it. None of these contradicts the text.

| Requirement | What is left open | Interpretation |
|---|---|---|
| R12, cancel "pending" notifications | Whether "pending" is one state or "not yet sent" | Cancel is allowed from `pending`, `queued` and `retrying`: any state in which no worker holds the notification. From `processing`, `sent` or `failed` it returns `409`. |
| R9, batch creation | Whether a batch is all-or-nothing | **Partial success.** Valid items are accepted and invalid ones are reported per index; a batch with no valid item returns `422`. |
| R6, R24, delivery status | What the success state means | `sent` means **the provider accepted the message**. It does not mean the message reached the user; delivery receipts are future work (item 3). |
| R24, expected response `202` | What to do with another `2xx` | Treated as success, with a warning metric. The provider accepted the message, and resending would duplicate it. |
| R28, WebSocket updates | Whether updates are guaranteed | **Best-effort events, plus a snapshot of the current state on subscribe.** A client can always reconcile with `GET`. |
| R5, R19, retry logic | Whether a notification can be retried indefinitely | Every notification has an **`expires_at`**, defaulted per priority and settable by the client. After it, the notification fails as `expired` instead of being sent late. |
| R15, rate limit | Scope and window of the limit | One limit per channel across all workers: no one-second window at the provider exceeds 100. Achieved by pacing at 98 msg/s. |
| R16, priority queue | Strict or weighted | Strict. |
| R8, notification fields | Whether an email subject or push title exists | A notification has recipient, channel, content and priority only; there is no subject (item 17). |
| R18, idempotency | Where duplicates are prevented | At the request (`Idempotency-Key`), at the record (`dedupe_key`) and at the send (claim guard, fencing, provider header). |
| R20, metrics endpoint | One endpoint or several; the format | `GET /v1/metrics/summary` returns queue depth, success and failure rates and latency as JSON; Prometheus `/metrics` exists in addition. |
| R22, health check | Which port | The api's public port. |
| R23, provider request | Headers; which URL is the default | The body is exactly the three fields; headers carry the idempotency key and trace context; the default target is a webhook.site URL. |
| R7, Go version | Which Go 1.2x release | Go 1.26 or later, because the code generator requires it. |
| R33, one-command setup | Compose file name and command | `docker-compose.yml` and `docker-compose up`, as written. |

## Future work

### 1. Multi-tenancy and fair scheduling

**Problem.** A single client running a flash sale can delay every other client's campaign traffic behind its own backlog (noisy neighbour). Priority alone does not solve this: both clients' messages may be `low`.

**Sketch.**

- **Tenant identity from API keys** (item 2), and a `tenant_id` column on notifications, batches and templates.
- **Fair selection in the relay.** The lane query takes a bounded number of rows per tenant with a `LATERAL` sub-query, so each tenant costs one index probe on `(tenant_id, channel, priority, due_at)`:

  ```sql
  SELECT c.id, c.trace_parent
  FROM tenants_with_due_work t
  CROSS JOIN LATERAL (
    SELECT id, trace_parent
    FROM notifications
    WHERE tenant_id = t.tenant_id
      AND channel = $1 AND priority = $2
      AND status IN ('pending', 'retrying') AND due_at <= now()
    ORDER BY due_at
    LIMIT $per_tenant_quota
    FOR UPDATE SKIP LOCKED
  ) c
  LIMIT $headroom;
  ```

  This avoids ranking every due row on each relay cycle, which a window function over the whole lane would do. `tenants_with_due_work` stands for a small maintained set of tenants that currently have a backlog. The query is a sketch and has not been run. Weighted or deficit round-robin between tenants is a refinement of the per-tenant quota.

- **Scoping:**
  - idempotency keys become `(tenant_id, key)`;
  - `dedupe_key` uniqueness becomes `(tenant_id, dedupe_key)`;
  - templates are namespaced per tenant;
  - per-tenant rate limits are optional.
- **Metrics:** `tenant_id` is **not** added as a Prometheus label, because it would cause unbounded cardinality. Per-tenant usage comes from the database or an analytics pipeline instead.

**Hooks into:** the relay lane query ([ADR-0002](adr/0002-postgres-as-outbox-scheduler-and-retry-delay.md), [ADR-0003](adr/0003-bounded-relay-buffer-and-sweepers.md)), the idempotency and dedupe indexes ([ADR-0006](adr/0006-api-contract-and-three-layer-idempotency.md)) and the cardinality rules ([ADR-0009](adr/0009-observability.md)).

### 2. Authentication and authorisation

- **API keys**, stored hashed, with scopes:
  - `notifications:write` to send;
  - `notifications:read` to query;
  - `admin` for redrive, templates and batch cancel.
- **Rate limiting on ingress per key**, to protect the API itself, separately from the delivery rate limit.
- The WebSocket gateway authenticates on connect and only allows subscriptions to the caller's own notifications.

### 3. Delivery receipts (`delivered` / `undelivered`)

- Providers report final delivery asynchronously through delivery-receipt webhooks. A receipts endpoint would:
  - verify the provider's signature;
  - map the receipt to a notification through `provider_message_id` (which would then need to be unique per provider);
  - apply new transitions `sent → delivered` and `sent → undelivered`.
- The `delivered` state name is already reserved, and `provider_message_id` is already stored for this purpose.

**Hooks into:** the state machine ([ADR-0004](adr/0004-data-model-state-machine-and-fencing.md)).

### 4. Status webhooks for API consumers

Clients that cannot hold a WebSocket open would register a callback URL and receive signed `status_changed` payloads.

There are two ways to build this, with different guarantees:

| Approach | Integration cost | Guarantee |
|---|---|---|
| **Bind a durable queue to `notifications.events`** and deliver webhooks from a consumer | Minimal: no change to the transition code | Inherits the best-effort publishing: an event dropped between commit and publish is never delivered |
| **Write to a `webhook_outbox` table inside the transition's own transaction**, delivered by a relay-style publisher with retries and back-off | The transition functions also write the outbox row | **Guaranteed:** the webhook exists if and only if the transition committed |

The topic exchange is the quick extension point. Where clients rely on webhooks for correctness rather than convenience, the outbox is the right choice.

**Hooks into:** [ADR-0001](adr/0001-rabbitmq-as-dispatch-broker.md), [ADR-0005](adr/0005-service-topology-and-status-events.md).

### 5. Provider failover routing

Route a channel's traffic to a secondary provider while the primary's circuit is open. The breaker keys `(channel, provider)` and the attempt records already include the provider, so this adds routing logic and its tests. The rate limit is per channel and is our own policy, so failover does not change it.

**Hooks into:** [ADR-0007](adr/0007-retry-policy-and-circuit-breaker.md) §7.

### 6. Admission control

**Problem.** The API accepts work without regard to whether it can be delivered before it expires. During a very large burst, clients learn that their messages expired only hours later.

**Sketch.** At creation, compare the lane's backlog with what the channel can drain within the notification's lifetime (`backlog_due` against TTL × rate). Beyond a configurable ratio, reject with `429` and a `Retry-After`, or accept with a warning in the response. `high` traffic would be exempt or have its own threshold.

**Hooks into:** the api's create path ([ADR-0006](adr/0006-api-contract-and-three-layer-idempotency.md)) and the backlog gauges ([ADR-0009](adr/0009-observability.md)).

### 7. Partitioning and retention

**Problem.** At millions of rows per day, `notifications` and `notification_attempts` grow without bound, and deleting old rows causes heavy bloat.

**Sketch.**

- Partition both tables by time (monthly or weekly, for example with `pg_partman`), and drop whole partitions after a retention period.
- **The conflict to solve:** a unique index on a partitioned table must include the partition key, which breaks global `dedupe_key` uniqueness. Two options:
  - move dedupe keys into a small non-partitioned `dedupe_keys` table with its own retention, which also turns permanent uniqueness into a bounded window;
  - accept per-partition uniqueness and document the window.

**Hooks into:** [ADR-0004](adr/0004-data-model-state-machine-and-fencing.md), [ADR-0006](adr/0006-api-contract-and-three-layer-idempotency.md).

### 8. Splitting hot and cold columns

`notifications` holds both the immutable request (recipient, content, template data) and the delivery state that is rewritten several times per notification. At 294 msg/s the cost of rewriting whole rows is small. At a much higher rate, moving the changing columns (`status`, `due_at`, `attempt_count`, `lease_expires_at`, `version`) into a narrow table would reduce write volume and bloat, at the cost of a join on every listing query.

**Hooks into:** [ADR-0004](adr/0004-data-model-state-machine-and-fencing.md) §6.

### 9. Data protection (KVKK / GDPR)

- **Content minimisation:** after a notification reaches a final state and a grace period has passed, purge `content` and `template_vars`, keeping metadata for audit.
- **Erasure:** a procedure to remove or anonymise a recipient's data on request.
- **Encryption at rest** for `recipient` and `content`, either at the column level or through storage-level encryption, with key management.

### 10. Campaign-level operations

A flash sale of 1 M messages is 1,000 batches. Cancelling or monitoring it batch by batch is impractical.

- A `campaign_id` (or label) shared by many batches.
- **Chunked cancel:** loop over `UPDATE ... WHERE id IN (SELECT ... LIMIT 1000 FOR UPDATE SKIP LOCKED)` until nothing remains, so no single transaction holds locks on hundreds of thousands of rows. Rows skipped because they were locked are retried in a final pass.
- Campaign-level progress endpoints and metrics.

**Hooks into:** [ADR-0006](adr/0006-api-contract-and-three-layer-idempotency.md) §7.

### 11. Scheduling extensions

- **Per-recipient time zones:** "send at 10:00 local time".
- **Quiet hours:** no campaign traffic between, for example, 22:00 and 09:00 in the recipient's time zone; transactional traffic is exempt.
- **Recurring schedules.**
- **Rescheduling** a pending notification, instead of cancelling and recreating it.

**Hooks into:** [ADR-0008](adr/0008-templates-and-scheduling.md).

### 12. SMS encoding: national language shift tables

GSM 03.38 defines national language shift tables, including Turkish, that encode characters such as `ş`, `ğ` and `ı` within GSM-7. Using them keeps Turkish text in GSM-7 instead of UCS-2 (67 characters per concatenated segment). The shift tables need an extra header in each message, so a concatenated segment then holds somewhat fewer than 153 characters; the segment count is still roughly halved. This requires provider support for the corresponding headers.

**Hooks into:** [ADR-0006](adr/0006-api-contract-and-three-layer-idempotency.md) §5.

### 13. Template extensions

- **Localisation:** language variants of a template, selected per recipient.
- **Locale-aware case functions** (`upper`, `lower`), which need the recipient's language to be correct.
- **Controlled iteration:** lists in variables with a bounded `range` (maximum length enforced before execution), behind the same allow-list validation.
- **A plain-text alternative part** for email (multipart), generated from the HTML body or authored separately.

**Hooks into:** [ADR-0008](adr/0008-templates-and-scheduling.md).

### 14. Production infrastructure

| Component | Production setup |
|---|---|
| RabbitMQ | 3-node cluster, so quorum queues actually replicate |
| Postgres | HA (for example Patroni), with a read replica for listing and summary queries |
| Redis | Sentinel or Cluster. The rate-limit script uses one key per channel, so it works in Cluster mode. |
| Tracing | OpenTelemetry Collector with tail sampling, and Jaeger with persistent storage |
| Logging | Shipping to a log store (for example Loki), with trace IDs linking logs to traces |
| Alerting | Alertmanager with routing; the current rules are evaluated by Prometheus only |
| Deployment | Kubernetes manifests or a Helm chart; secrets from a secret manager rather than environment files |

### 15. Relay leader election

Run two or more relay replicas with one active at a time, for example through a Postgres advisory lock held on a dedicated session connection. Correctness does not need it (`SKIP LOCKED` and conditional transitions already make concurrent relays safe); it would shorten the pause when a relay's host fails, in a deployment where restarting the container is slower than a standby taking over. A session-level lock rules out transaction-mode connection poolers in front of the relay.

**Hooks into:** [ADR-0005](adr/0005-service-topology-and-status-events.md).

### 16. Shared circuit breaker

Keep breaker state in Redis so that all worker replicas open and close a circuit together and a half-open probe is sent by one replica only. It becomes worthwhile with many replicas, where per-replica discovery multiplies the failed requests sent into an outage. The in-process breaker would remain as the fallback.

**Hooks into:** [ADR-0007](adr/0007-retry-policy-and-circuit-breaker.md) §7.

### 17. Subject and title

An email subject and a push title, as optional fields of a notification and of a template version. They are left out because the assessment's notification and provider request have no such field. A real email or push provider integration would add them, together with their validation (length, no line breaks in a subject) and a plain-text template engine for subjects.

**Hooks into:** [ADR-0006](adr/0006-api-contract-and-three-layer-idempotency.md) §3, [ADR-0008](adr/0008-templates-and-scheduling.md).

### 18. Broker-side delays (RabbitMQ 4.3 and 4.4)

RabbitMQ 4.3 added delayed retry for quorum queues and 4.4 added a per-message delivery time, without a plugin. The current design keeps every delay in Postgres so that waiting work is visible, cancellable and expirable in one place. If the relay's polling ever became a cost worth removing, short retry delays could move into the broker while scheduled notifications stay in Postgres. This would need a rule for cancelling and expiring a notification whose message is waiting inside the broker.

**Hooks into:** [ADR-0001](adr/0001-rabbitmq-as-dispatch-broker.md), [ADR-0002](adr/0002-postgres-as-outbox-scheduler-and-retry-delay.md).

### 19. Adaptive rate limiting (AIMD)

If a provider returns `429` while we send below our own limit, its real limit is lower than ours. An additive-increase / multiplicative-decrease controller would lower the channel's rate when throttled and probe back up gradually, instead of relying only on the circuit breaker.

**Hooks into:** [ADR-0007](adr/0007-retry-policy-and-circuit-breaker.md) §4 and §5.

### 20. Operations

- **SLOs and error budgets**, for example: 99 % of `high` notifications sent within 5 seconds.
- **Exemplars** linking latency histograms to the traces of slow requests.
- A **capacity plan** based on production-scale load tests.

**Hooks into:** [ADR-0009](adr/0009-observability.md), [ADR-0010](adr/0010-testing-and-ci.md).
