# ADR-0001: RabbitMQ (quorum queues) as the dispatch broker

- **Status:** Accepted
- **Date:** 2026-11-18
- **Related:** [ADR-0002](0002-postgres-as-outbox-scheduler-and-retry-delay.md), [ADR-0003](0003-bounded-relay-buffer-and-sweepers.md), [ADR-0005](0005-service-topology-and-status-events.md), [ADR-0007](0007-retry-policy-and-circuit-breaker.md)

## Context

The service accepts notification requests for three channels (SMS, Email, Push) and delivers them asynchronously through an external provider. The requirements that shape the choice of messaging technology are:

- Asynchronous processing by queue workers (R14).
- A rate limit of at most **100 messages per second per channel** (R15).
- Priority support: `high`, `normal`, `low` (R16).
- Resilience to burst traffic such as flash sales and breaking news (R3).
- Intelligent retries of failed deliveries (R5).

### Capacity analysis

The rate limit, not the broker, is the throughput ceiling. Each channel is paced at 97 msg/s so that no one-second window at the provider exceeds 100 ([ADR-0007](0007-retry-policy-and-circuit-breaker.md) §5):

| Quantity | Value |
|---|---|
| Delivery capacity | 3 channels × 97 msg/s = **291 msg/s** |
| Per hour | ≈ 1.06 M |
| Per day | ≈ **25.4 M** |

"Millions per day" is within the average capacity. Bursts are the problem: 1 M SMS submitted within ten minutes take 1,000,000 / 97 ≈ 10,300 s ≈ **2.9 hours** to drain through the SMS channel.

**The system therefore accumulates backlog by design.** Priority exists because of that backlog: a one-time password must not wait behind 1 M campaign messages. The messaging layer is judged by how well it orders and isolates work under backlog, not by raw throughput.

### Nature of the workload

This is a **work-queue** problem, not a **log/stream** problem:

- Each message is processed by exactly one consumer and is then done.
- There is no need to replay history or to fan the same delivery out to independent consumer groups.
- What is needed is per-message acknowledgement, competing consumers, priority and channel isolation.

The one place where fan-out *is* needed is **status-change events** (`notification.status_changed`), which every real-time gateway instance must receive.

## Decision

Use **RabbitMQ 4.4.x** as the dispatch broker, pinned to an exact image tag in `docker-compose.yml`, with `rabbitmq/amqp091-go` ≥ v1.13.0 as the client (automatic connection recovery since v1.12.0, topology recovery since v1.13.0). The recovery is off by default and the library marks it experimental; it is enabled explicitly, with the budget given in [ADR-0005](0005-service-topology-and-status-events.md).

### Delivery topology

| Element | Definition |
|---|---|
| Exchange | `notifications.dispatch` (direct, durable) |
| Queues | **One per channel:** `dispatch.sms`, `dispatch.email`, `dispatch.push` |
| Routing key | `{channel}` |
| Queue type | **Quorum queues** (`x-queue-type: quorum`) |
| Priority | The quorum queue's built-in strict priorities (RabbitMQ 4.3+); no queue argument is needed |
| Consumer timeout | `x-consumer-timeout` of 24 hours on each dispatch queue (see below) |
| Dead letters | `notifications.dlx` → `dispatch.{channel}.poison`. The delivery limit is the broker default (20). |
| Messages | Persistent; published with **publisher confirms** and **`mandatory=true`**; `MessageId` is the notification `id`; `priority` is set on every message |
| Payload | Minimal: the notification `id` in the body and the W3C `traceparent` as a header. The database row is authoritative; workers load the full row when they claim it ([ADR-0004](0004-data-model-state-machine-and-fencing.md)). |

**Who declares the topology.** Every role (api, relay, worker, gateway) declares the exchanges, queues and bindings it uses, idempotently, **at start-up and on every (re)connection**. A broker that comes back empty therefore has its topology again before the first publish.

### Priority: strict, in the broker

Priority is interpreted as **strict** (R16): a `high` message is always delivered before a `normal` or `low` one in the same channel.

- The AMQP priority is `6 − db_priority`, that is `high` = 6, `normal` = 5, `low` = 4 ([ADR-0004](0004-data-model-state-machine-and-fencing.md) stores `high` as 0).
- **The range starts at 4 on purpose.** RabbitMQ treats a message without a priority as priority 4, and the client library, `amqp091-go`, leaves the priority property off the wire when its value is 0, so a priority of 0 cannot be sent at all. With `low` at 4, a message that loses its priority through a defect is delivered as `low`; it never overtakes `high`. Experiment E1 ran this on RabbitMQ 4.4.1: with priorities 4, 5 and 6 and messages without a priority interleaved in one queue, every 6 was delivered before every 5 and every 5 before every 4, and the messages without a priority were delivered among the 4s in publishing order.
- **The priority is set explicitly on every message.** A test asserts that every published message carries the field, with the value that belongs to its row ([ADR-0010](0010-testing-and-ci.md)).
- The worker's pacer picks the highest-priority message in its local prefetch buffer ([ADR-0005](0005-service-topology-and-status-events.md)), so strictness also holds for the messages already delivered to a consumer. In that buffer a delivery without a priority property counts as 4, as it does in the broker.
- Per-priority buffer limits (high-water marks) are counted in Postgres, not in the broker ([ADR-0003](0003-bounded-relay-buffer-and-sweepers.md)).

Two limitations follow and are documented in [future work](../future-work.md):

- Under sustained `high` traffic at the channel's full rate, `low` waits. Its expiry time bounds the wait.
- A message that is returned to the queue (a nack on `SIGTERM`, a lost connection) loses its priority: the broker delivers returned messages first, before messages that are already waiting, whatever their priority (observed in experiment E1).

### Unroutable publishes

Publisher confirms alone do not protect against loss: the broker also confirms a message that it could not route to any queue. Every dispatch publish therefore sets `mandatory=true`.

- An unroutable message comes back as a `basic.return`. **A returned message's row is not marked `queued`**; it stays `pending` and is published again on a later cycle ([ADR-0002](0002-postgres-as-outbox-scheduler-and-retry-delay.md)).
- Returns are collected per batch and matched to rows by `MessageId` after the confirms have arrived.
- Implementation: the return channel (`NotifyReturn`) is buffered, and each confirm is processed only after a non-blocking drain of pending returns. This relies on the client library handing over a `basic.return` before the matching confirm. The broker sends them in that order; the client-side ordering is an assumption that the integration test repeats 100 times ([ADR-0010](0010-testing-and-ci.md)). If the assumption fails, the result is a row marked `queued` with no message, which reconciliation recovers ([ADR-0003](0003-bounded-relay-buffer-and-sweepers.md)).

### Consumer timeout

While a provider's circuit is open, prefetched messages wait unacknowledged in the worker ([ADR-0007](0007-retry-policy-and-circuit-breaker.md) §8). RabbitMQ cancels a consumer whose delivery stays unacknowledged longer than the consumer timeout, 30 minutes by default. The dispatch queues are therefore declared with `x-consumer-timeout` set to 24 hours, which is longer than any notification's default lifetime. There is no cancel-and-resubscribe logic. As a safety net, a worker that receives a server-side consumer cancellation resubscribes.

### Poison messages

The broker's dead-letter path is a safety net, not the main defence. The worker acknowledges a message as soon as its claim commits ([ADR-0005](0005-service-topology-and-status-events.md)), so a message that crashes a worker during the send is no longer in the queue; that case is handled by the retry budget and the lease sweeper ([ADR-0003](0003-bounded-relay-buffer-and-sweepers.md), [ADR-0007](0007-retry-policy-and-circuit-breaker.md) §9).

- `dispatch.{channel}.poison` queues are classic and durable, and have no consumer. Their depth is a metric and an alert ([ADR-0009](0009-observability.md)).
- A delivery whose body is not a notification id cannot belong to a row. The worker rejects it without requeue, so it goes to the poison queue, and logs its size, never its content.
- **Requeues use `basic.nack`, not `basic.reject`.** The worker returns a message it has not claimed with a nack and requeue (shutdown, a claim that could not be made). Since RabbitMQ 4.3 a nack with requeue does not increment a quorum queue's delivery count, while a reject does, and the delivery limit is 20: with reject, a message returned often enough during an outage would be dead-lettered although nothing is wrong with it. **Checked on RabbitMQ 4.4.1 in P3** (`TestConsumer_NackWithRequeueDoesNotCountTowardDeliveryLimit`): one message nacked with requeue 25 times is redelivered 25 times, carries no `x-delivery-count` header at all, and is not dead-lettered. The control (`TestConsumer_RejectWithRequeueCountsTowardDeliveryLimit`) shows the count that a reject builds: `x-delivery-count` rises from 1 to 20 and the 21st reject sends the message to the poison queue. Since P3 the worker also no longer nacks a claim that failed with a database error: it retries the claim in place ([ADR-0005](0005-service-topology-and-status-events.md), worker internals), so a nack is left for shutdown only. `basic.reject` without requeue is used only for a delivery that can never be processed (the bullet above).
- A dead-lettered message leaves its row in `queued`. The row is recovered by reconciliation or by the manual `requeue-stale` command ([ADR-0003](0003-bounded-relay-buffer-and-sweepers.md)).

### Status events

| Element | Definition |
|---|---|
| Exchange | `notifications.events` (topic, durable) |
| Event name | `notification.status_changed` |
| Routing key | `status.{channel}.{status}` |
| Message | The event's name is the message's `type` property. The body is JSON with exactly these members, all always present: `id`, `batch_id` (`null` without a batch), `channel`, `priority`, `status`, `version`, `failure_reason` (`null` unless `failed`), `attempt_count`, `updated_at` (RFC 3339, UTC). No recipient and no content. Delivery is transient, published without confirms and without `mandatory`: nobody bound is normal. A consumer ignores a member it does not know and drops a body without an `id`, with a `version` below 1 or with a channel, priority or status it does not know. |
| Consumers | Each gateway instance binds its own **exclusive, auto-delete** queue, with `x-max-length` and a short `x-message-ttl`. These are classic queues, because quorum queues cannot be exclusive; they are transient by nature, so replication is not needed. The broker names the queue, and every (re)subscription declares a new one: a fixed name could meet the queue of the dead connection, which the broker had not removed yet, and be refused as locked. Deliveries are acknowledged automatically. |
| Publishers | api, relay and worker. Each connects lazily, publishes through a bounded buffer, drops events when the buffer is full, and never affects start-up or readiness ([ADR-0005](0005-service-topology-and-status-events.md)). |
| Guarantee | **Best-effort.** A client that misses an event can always fetch the current state with `GET`. Ordering is handled by a per-row `version` ([ADR-0004](0004-data-model-state-machine-and-fencing.md)). |

The topic exchange is also the extension point for future durable consumers such as status webhooks or analytics. None is added now: audit data already lives in `notification_attempts` and metrics are collected in-process.

### What RabbitMQ is *not* used for

- **Holding the burst backlog.** The backlog stays in Postgres; RabbitMQ holds a bounded buffer ([ADR-0003](0003-bounded-relay-buffer-and-sweepers.md)).
- **Delayed delivery.** Neither scheduled notifications nor retry back-off use broker delays ([ADR-0002](0002-postgres-as-outbox-scheduler-and-retry-delay.md)). Only messages that are ready to be processed now ever enter a queue.

## Alternatives considered

| Alternative | Why it was rejected |
|---|---|
| **Postgres `FOR UPDATE SKIP LOCKED` as the only queue** | At this volume (291 msg/s) a Postgres-only queue would be sufficient, and it is simpler: priority through `ORDER BY`, delay through a timestamp, no outbox and no second stateful system. The broker is chosen for three reasons that are not about capacity: the assessment frames the system as event-driven with queue workers; a broker gives standard consumer semantics (prefetch, acknowledgement, competing consumers) instead of hand-written polling workers; and its topic exchange is the extension point for status events. Much of the Postgres design is kept: Postgres remains the source of truth, the scheduler and the retry-delay store ([ADR-0002](0002-postgres-as-outbox-scheduler-and-retry-delay.md)). |
| **Nine lane queues (channel × priority) with a weighted 6:3:1 dispatcher in the worker** | Under contention `high` would receive only 60 % of the channel's capacity, which contradicts the goal that an urgent message does not wait behind bulk traffic. The dispatcher is also custom code that needs its own tests. Built-in strict priority in quorum queues (RabbitMQ 4.3+) gives the stricter behaviour with no application code. |
| **A custom `x-delivery-limit` of 5** | It cannot catch a real poison message, because the worker acknowledges before it sends, so the crashing delivery is never redelivered. A low limit can instead dead-letter healthy messages that were prefetched by a worker that crashed for another reason. The broker default (20) stays as a backstop. |
| **Cancel and resubscribe the consumer during a long open circuit** | Extra state and code on the consume path to avoid a timeout that a queue argument removes. Kept only as the fallback if the argument does not behave as documented ([implementation plan](../plan/implementation-plan.md), experiment E6). |
| **Kafka** | Its strengths (replay, multi-group fan-out, very high throughput) are not needed here. It has no native message priority and no native delayed redelivery. Share groups (production-ready in Kafka 4.2) do provide per-message acknowledgement, so head-of-line blocking is no longer an argument against it; priority and operational weight are. |
| **Redis Streams** | Consumer groups with `XACK` and `XAUTOCLAIM` are adequate, but priority needs several streams and dead-letter handling is manual. Redis is already in the stack, so this would save a component at the cost of re-implementing broker features. |
| **NATS JetStream** | A strong candidate with native per-message delayed redelivery (`NakWithDelay`). It has no native priority, and its main advantage does not apply because delays are handled in Postgres. |
| **RabbitMQ classic queues with `x-max-priority`** | Classic queues are not replicated in RabbitMQ 4.x (classic mirroring was removed in 4.0). Quorum queues have had strict priorities since 4.3. |
| **Retry and scheduling delays in RabbitMQ** | Possible in several ways: TTL plus dead-letter tiers (fixed delays only; per-message TTL expires only at the head of the queue), delayed retry (4.3) and per-message delivery time (4.4). Rejected because waiting work would then live in two places. Postgres must hold `due_at` anyway so that scheduled and retrying notifications can be listed, cancelled and expired; keeping every delay there gives one code path ([ADR-0002](0002-postgres-as-outbox-scheduler-and-retry-delay.md)). The 4.3 and 4.4 features are recorded in [future work](../future-work.md). |

## Consequences

**Positive**

- Strict priority, per-message acknowledgement, competing consumers and dead-lettering come from standard broker features rather than custom code.
- One queue per channel keeps the topology small: three dispatch queues, three poison queues and the events exchange.
- Quorum queues, persistent messages, publisher confirms and `mandatory` together protect against silent loss between the relay and the queue.

**Negative / costs**

- **Two stateful systems** (Postgres and RabbitMQ) must be kept consistent. This requires a transactional outbox ([ADR-0002](0002-postgres-as-outbox-scheduler-and-retry-delay.md)) and recovery paths ([ADR-0003](0003-bounded-relay-buffer-and-sweepers.md)). This is the main cost of choosing a broker over a Postgres-only queue.
- Strict priority can make `low` wait for as long as `high` traffic fills the channel.
- The design depends on RabbitMQ 4.3+ behaviour (strict priorities, the default priority of 4, the per-queue consumer timeout). These are verified by experiments before any feature work ([implementation plan](../plan/implementation-plan.md), E1 and E6).
- No replay of past messages. This is acceptable because Postgres holds the full history.
- In the single-node Docker Compose setup, quorum queues provide no actual replication. A real deployment needs a three-node cluster.

## Review history

Changes made after the design review, recorded in the [design review log](../ai/design-review-log.md):

- Nine lane queues with a weighted 6:3:1 dispatcher became one queue per channel with built-in strict priority, because the weighting contradicted the goal for urgent messages and quorum queues have supported strict priorities since RabbitMQ 4.3. (D7)
- After experiment E1 (P0), the AMQP priority changed from `2 − db_priority` to `6 − db_priority`: the client library cannot send a priority of 0, which the broker would then sort as 4, ahead of `high`. The description of a requeued message was corrected to what the experiment observed. (D7)
- After experiment E4 (P0), the client's recovery is described as what it is: opt-in and experimental, enabled explicitly. (D5)
- Publishes became `mandatory`, and every role now declares the topology on each connection, because publisher confirms also confirm unroutable messages. (D2)
- The custom delivery limit of 5 was removed: it could not catch a real poison message and could dead-letter healthy ones. (D6)
- A 24-hour consumer timeout was added so that messages waiting during an open circuit do not get their consumer cancelled after 30 minutes. (D8)
- The broker version was pinned to 4.4.x, and the Postgres-only, Kafka and broker-delay alternatives were rewritten to match current releases and the real reasons for the choice. (D48)
