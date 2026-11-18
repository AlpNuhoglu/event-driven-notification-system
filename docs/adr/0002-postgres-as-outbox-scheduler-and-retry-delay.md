# ADR-0002: Postgres as the outbox, scheduler and retry-delay store

- **Status:** Accepted
- **Date:** 2026-11-18
- **Related:** [ADR-0001](0001-rabbitmq-as-dispatch-broker.md), [ADR-0003](0003-bounded-relay-buffer-and-sweepers.md), [ADR-0004](0004-data-model-state-machine-and-fencing.md), [ADR-0005](0005-service-topology-and-status-events.md), [ADR-0007](0007-retry-policy-and-circuit-breaker.md), [ADR-0008](0008-templates-and-scheduling.md), [ADR-0009](0009-observability.md)

## Context

Postgres is the source of truth for notifications, and RabbitMQ is the dispatch broker ([ADR-0001](0001-rabbitmq-as-dispatch-broker.md)). Three separate problems need a solution:

1. **Dual write.** Accepting a notification means writing it to Postgres *and* making it available to workers through RabbitMQ. If the process crashes between the two writes, or the broker is unavailable, the notification is either lost (stored but never published) or phantom (published but never stored).
2. **Scheduled notifications.** Notifications may be scheduled for future delivery, up to 30 days ahead ([ADR-0008](0008-templates-and-scheduling.md)).
3. **Retry delays.** Failed deliveries are retried after an exponential back-off with jitter ([ADR-0007](0007-retry-policy-and-circuit-breaker.md)). RabbitMQ 4.4 can delay an individual message, but a delayed message waits inside the broker, where it cannot be listed, cancelled or expired together with the notification it belongs to ([ADR-0001](0001-rabbitmq-as-dispatch-broker.md)).

All three are the same problem in disguise: *"make this row available to workers at time T."* For a new notification T is now; for a scheduled one T is `scheduled_at`; for a retry T is now plus the back-off.

## Decision

Use **Postgres as a state-based outbox**, with a single scheduling key, `due_at`, and a single component, the **relay**, that publishes rows to RabbitMQ when they become due.

### One scheduling key

| Case | `due_at` | Written by |
|---|---|---|
| New notification | `now()` | api |
| Scheduled notification | `scheduled_at` | api |
| Retry | `now() + delay` | worker or lease sweeper |

The only difference between "send now", "send later" and "retry later" is the value of `due_at`.

**One clock.** `due_at` is always computed in SQL from the database's `now()`. The retry decision function returns a *delay*, not a timestamp, and receives the database `now()` read in the same transaction ([ADR-0004](0004-data-model-state-machine-and-fencing.md) §5). No process clock takes part in scheduling.

### The notifications table *is* the outbox

There is no separate outbox table. A row with `status IN ('pending', 'retrying') AND due_at <= now()` is, by definition, an unpublished message. This avoids duplicating every notification into a second table and removes one write per notification.

### Relay cycle

The relay runs on a **fixed 100 ms ticker**. On each tick it visits every channel and, within a channel, the priorities in the order `high`, `normal`, `low`. For each lane (channel × priority) it runs one transaction:

```sql
BEGIN;

SELECT id, trace_parent
FROM notifications
WHERE channel = $1 AND priority = $2
  AND status IN ('pending', 'retrying')
  AND due_at <= now()
ORDER BY due_at
LIMIT $3                -- min(200, lane headroom); see ADR-0003
FOR UPDATE SKIP LOCKED;

-- publish each id to notifications.dispatch: routing key {channel}, mandatory,
-- persistent, MessageId = id, priority = 6 - db priority;
-- wait for publisher confirms (at most 5 s), then collect basic.return frames

UPDATE notifications
SET status = 'queued', queued_at = now(), updated_at = now(), version = version + 1
WHERE id = ANY($confirmed_and_not_returned);

COMMIT;
-- after commit: emit notification.status_changed events (best-effort)
```

| Rule | Value |
|---|---|
| Rows per lane per cycle | At most **200** |
| Publisher-confirm timeout | **5 seconds** |
| AMQP channel down | **No transaction is started** |
| Connection drops while confirms are outstanding | Those publishes count as failed; the transaction rolls back |
| Unroutable message (`basic.return`) | Its row is **not** marked `queued` ([ADR-0001](0001-rabbitmq-as-dispatch-broker.md)) |
| Dependencies | Postgres and RabbitMQ. **The relay does not use Redis.** |

**Row locks are held across the publish.** This is the one documented exception to the rule that no lock is held across a network call ([ADR-0004](0004-data-model-state-machine-and-fencing.md) §1), and it is deliberate. A worker can receive a message before the relay has committed `queued`. Its claim then waits on the relay's row lock and proceeds after the commit, instead of finding a row it may not claim and dropping the message ([ADR-0004](0004-data-model-state-machine-and-fencing.md), transition 3). The lock duration is bounded by the batch size and the confirm timeout.

**Failure semantics:**

| Failure | Effect | Why it is safe |
|---|---|---|
| Broker unavailable or confirms time out | Transaction rolls back; rows stay `pending` (or `retrying`) | They are picked up again on a later tick |
| Crash or rollback after publish, before commit | The rows stay `pending` (or `retrying`) while their messages are already in the queue | The claim guard accepts `pending` and `retrying` rows that are due, so the worker claims the row directly. If the relay publishes the row again first, the second copy finds the row already claimed and is dropped ([ADR-0004](0004-data-model-state-machine-and-fencing.md)). Publishing is **at-least-once**. |
| A worker receives the message before the relay commits | The claim waits on the row lock | After the commit the row is `queued` and the claim succeeds |
| Unroutable publish | Row stays `pending` | Every role declares the topology on connection, so the next cycle can route it |
| Two relay instances at once | `SKIP LOCKED` keeps them from selecting the same rows | Correctness does not depend on running a single relay ([ADR-0005](0005-service-topology-and-status-events.md)) |

### Pacing

There is no wake-up mechanism. The ticker gives new, scheduled and retried rows the same worst-case delay of about 100 ms before they are published, and the same code path whether a lane is empty, busy or at its high-water mark. **Scheduling precision is therefore about 100 ms.**

### Expiry (transition 10)

The relay also moves rows in `pending`, `retrying` or `queued` with `expires_at <= now()` to `failed` (`failure_reason = 'expired'`).

| Aspect | Rule |
|---|---|
| Cadence | Every second |
| Batch | At most 1,000 rows, selected with `FOR UPDATE SKIP LOCKED` |
| Backlog of expired rows | When a batch comes back full, the next one runs immediately |
| Dependencies | None on Redis or on circuit state. Clients see an expired notification as `failed` when it expires, not when the provider recovers. |

Workers additionally check expiry at claim time, so a stale message that is still in RabbitMQ is never sent ([ADR-0004](0004-data-model-state-machine-and-fencing.md), transition 3). Like every other transition, expiry emits a best-effort status event after commit.

### Status events

Every status change is announced as `notification.status_changed` on the `notifications.events` exchange with routing key `status.{channel}.{status}` ([ADR-0001](0001-rabbitmq-as-dispatch-broker.md)). Events are published **after commit**, outside the outbox. Every publisher (api, relay, worker) connects lazily, uses a bounded buffer and drops events when it is full; an event publisher never affects start-up or readiness ([ADR-0005](0005-service-topology-and-status-events.md)).

### Trace context

The relay is asynchronous, so each row stores the request's `trace_parent` ([ADR-0004](0004-data-model-state-machine-and-fencing.md)). When the relay publishes a row ([ADR-0009](0009-observability.md) §6):

- for the **first attempt of a non-scheduled notification**, its publish span is a **child** of the stored `trace_parent`, so request, relay, worker and provider form one trace;
- for **retries and scheduled deliveries**, it starts a **new root span linked** to the stored `trace_parent`.

In both cases the relay puts its own span's context in the AMQP `traceparent` header.

## Alternatives considered

| Alternative | Why it was rejected |
|---|---|
| **`LISTEN`/`NOTIFY` wake-ups plus "sleep until the earliest `due_at`"** | The sleep computation degenerates when lanes are at their high-water mark: due rows exist, so the earliest `due_at` is in the past and the relay spins. A `NOTIFY` per single create wakes the relay on every request under load, and listening needs a dedicated session connection. A fixed ticker bounds the delay to the same order of magnitude with one code path and no session state. |
| **Commit `queued` first, then publish** | A crash between the two leaves a row marked `queued` with no message, which is the failure the outbox exists to prevent. Publishing inside the transaction leaves only the opposite, harmless case: a message whose row is not yet `queued`. |
| **Synchronous publish in the API; return `503` if the broker is down** | Couples API availability to the broker, and does not remove the dual-write window: a crash between the database commit and the publish still loses the message. |
| **Separate outbox table** | Duplicates each notification and adds a write. The notification row already carries every field the relay needs. |
| **CDC (Debezium or logical replication)** | Adds a connector runtime and its configuration for a publish rate of at most about 300 msg/s, which polling handles easily. |
| **Delays in RabbitMQ** (TTL tiers, delayed retry, per-message delivery time) | Waiting work would live in two places; see [ADR-0001](0001-rabbitmq-as-dispatch-broker.md). |
| **Delays in a Redis sorted set** | Creates a second source of truth for pending work, with weaker durability than Postgres. Pending retries would no longer be queryable alongside the notification itself. |
| **A separate scheduler service with its own store** | Adds a component and a second copy of state for what is, in Postgres, a single indexed column. |

## Consequences

**Positive**

- **The API keeps accepting notifications while RabbitMQ is down.** The backlog accumulates in Postgres and drains once the broker returns, with no loss.
- New, scheduled and retried notifications share **one code path**, which is tested once.
- Retry delays are exact (no tiers), so full jitter is possible.
- Pending work is durable and visible: "how many retries are waiting on SMS?" is a single SQL query.
- RabbitMQ is used only for what it does well ([ADR-0001](0001-rabbitmq-as-dispatch-broker.md)): every message in a queue is ready to be processed now.

**Negative / costs**

- **Polling load.** Each tick runs one count query and up to nine lane queries, ten times per second. Partial indexes keep each query cheap ([ADR-0004](0004-data-model-state-machine-and-fencing.md)).
- **Row locks are held across a network call** for up to the confirm timeout. Cancel and expiry on those rows wait for the relay's transaction.
- **Write amplification.** Every notification is updated several times (`queued`, `processing`, final state). This causes table bloat, which is handled by autovacuum tuning ([ADR-0004](0004-data-model-state-machine-and-fencing.md)).
- **Up to about 100 ms of added latency** before a new notification is published.
- **The relay is a critical component.** It runs as a single instance that the container runtime restarts ([ADR-0005](0005-service-topology-and-status-events.md)); while it is down nothing is published, and nothing is lost.
- **At-least-once publishing** means duplicate messages can appear in RabbitMQ. Every consumer must tolerate them; the claim guard enforces this ([ADR-0004](0004-data-model-state-machine-and-fencing.md)).

## Review history

Changes made after the design review, recorded in the [design review log](../ai/design-review-log.md):

- The claim guard was widened so that a worker which receives a message before the relay commits waits and then claims, instead of dropping the message. This was the review's only blocker; the relay's row locks across the publish are now a stated part of the design. (D1, D10)
- `LISTEN`/`NOTIFY` and the "sleep until the earliest `due_at`" logic were replaced by a fixed 100 ms ticker, because the sleep rule was undefined when lanes were full. (D3)
- Publishes became `mandatory`, with a batch size, a confirm timeout and a rule for unroutable messages. (D2, D10)
- The expiry pass received a batch size, a cadence and a locking rule. (D12)
- `due_at` is computed from the database clock only, and the trace-context rule for the relay's publish span was made explicit. (D17, D13)
