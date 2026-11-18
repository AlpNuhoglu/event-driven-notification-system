# ADR-0003: Bounded relay buffer and recovery sweepers

- **Status:** Accepted
- **Date:** 2026-11-18
- **Related:** [ADR-0001](0001-rabbitmq-as-dispatch-broker.md), [ADR-0002](0002-postgres-as-outbox-scheduler-and-retry-delay.md), [ADR-0004](0004-data-model-state-machine-and-fencing.md), [ADR-0005](0005-service-topology-and-status-events.md), [ADR-0007](0007-retry-policy-and-circuit-breaker.md), [ADR-0009](0009-observability.md)

## Context

The relay publishes due rows from Postgres to RabbitMQ ([ADR-0002](0002-postgres-as-outbox-scheduler-and-retry-delay.md)). Two questions remain open:

1. **How much should the relay publish?** If it publishes everything that is due, a 1 M message burst moves wholesale into RabbitMQ and waits there for about 2.8 hours ([ADR-0001](0001-rabbitmq-as-dispatch-broker.md)).
2. **How is work recovered when something goes wrong?** Two failures need automatic recovery:
   - Messages lost between the broker and the worker, most plausibly because the broker lost its data or was replaced. Postgres is the source of truth and must be able to repair it.
   - A worker that dies after claiming a notification, which leaves the row stuck in `processing`.

The two questions are coupled. A recovery path that puts `queued` rows back into the publishing cycle is only cheap and safe if the number of `queued` rows is small and bounded.

## Decision

### 1. A high-water mark (HWM) per lane

A **lane** is a channel × priority pair. The broker has one queue per channel ([ADR-0001](0001-rabbitmq-as-dispatch-broker.md)); lanes are counted in Postgres. The relay publishes rows of a lane only while that lane's `queued` count is below its high-water mark:

| Lane (per channel) | Default HWM |
|---|---|
| `high` | 500 |
| `normal` | 1,000 |
| `low` | 500 |
| **Total per channel** | **2,000** |

- **Per lane, not per channel.** With a single per-channel limit, 2,000 `low` messages could fill the buffer and stop the relay from publishing a newly arrived `high` message. Per-lane limits keep headroom for `high` at all times; the broker's strict priority then delivers it first.
- **Headroom calculation.** Each relay cycle runs a single query, `SELECT channel, priority, count(*) FROM notifications WHERE status = 'queued' GROUP BY channel, priority`, served by the partial index `(channel, priority, queued_at) WHERE status = 'queued'`. Each lane's `LIMIT` is `min(200, HWM − queued count)` ([ADR-0002](0002-postgres-as-outbox-scheduler-and-retry-delay.md)).
- **Drain time.** A full channel buffer of 2,000 messages is about 20 seconds of work at 98 msg/s.
- **The values are starting points,** chosen so that a full buffer is seconds of work, not derived from a model. They are configurable, and the load tests check that the relay refills the buffer faster than the workers drain it ([ADR-0010](0010-testing-and-ci.md)).
- **Soft limit.** If two relay instances ever run at the same time, both may see the same headroom and exceed the HWM by up to one batch each. This does not affect correctness.

### 2. The backpressure chain

With bounded buffers, a provider outage slows the system down step by step without any explicit coordination:

```
provider fails → circuit opens → the pacer stops claiming → messages wait in the queue
→ lanes reach their HWM → relay stops publishing → backlog accumulates in Postgres
→ expired rows fail (ADR-0002)
```

The burst backlog lives in Postgres, the source of truth. RabbitMQ holds about 6,000 distinct notifications at most (three channels × 2,000); duplicate copies of a notification are possible on top of that, because publishing is at-least-once.

The backlog in Postgres itself is not limited. A notification's expiry time is the only bound on how long it waits ([ADR-0007](0007-retry-policy-and-circuit-breaker.md) §3).

### 3. Reconciliation on reconnection (transition 8)

Recovers messages that were published but can no longer be consumed.

| Aspect | Rule |
|---|---|
| Trigger | **Every (re)connection of the relay to RabbitMQ**, including the first connection after start-up. The relay subscribes to the client library's recovery state notifications (`NotifyStateChange`). |
| Action | **All** `queued` rows return to `pending` (`version + 1`, event emitted). The normal relay cycle then publishes them again. Reconciliation itself never publishes. |
| Rate | At most once per 60 seconds. **A trigger that arrives inside that window is deferred, not skipped:** it runs once when the window ends. |
| Duplicates | A copy of the message may still be in RabbitMQ. Whichever copy is claimed first wins; the other is dropped by the claim guard ([ADR-0004](0004-data-model-state-machine-and-fencing.md)). |
| When recovery gives up | If the library's connection recovery gives up, the relay exits with a non-zero code. The container runtime restarts it, and the new process reconciles on its first connection ([ADR-0005](0005-service-topology-and-status-events.md)). |
| Dependencies | Postgres and RabbitMQ only |

The rule is deliberately coarse. After a reconnection the relay cannot know whether the broker kept its data, so it treats every `queued` row as possibly lost. The HWM keeps that set small (about 6,000 rows), and the claim guard makes the resulting duplicates harmless.

**What reconciliation does not cover.** A message that disappears while the connection stays up is not detected automatically: a message dead-lettered to a poison queue ([ADR-0001](0001-rabbitmq-as-dispatch-broker.md)), or a publish wrongly recorded as `queued`. For these there are two tools and one bound:

- the gauge `lane_oldest_queued_age_seconds{channel,priority}`, with an alert on the `high` lanes ([ADR-0009](0009-observability.md));
- a manual **`requeue-stale`** command that applies transition 8 on demand;
- expiry: a row that is never recovered fails as `expired` when its time runs out.

### 4. Lease sweeper (transition 7)

Recovers notifications whose worker died after claiming them.

| Aspect | Rule |
|---|---|
| Candidate rows | `status = 'processing' AND lease_expires_at < now()`, served by the partial index on `(lease_expires_at) WHERE status = 'processing'` |
| Lease length | 30 seconds, set at claim time. It covers the wait for the rate-limit slot after the claim (at most 250 ms), the provider HTTP timeout (10 s) and a margin for the final write ([ADR-0007](0007-retry-policy-and-circuit-breaker.md)). |
| Action | In **one transaction**: the attempt row goes from `in_flight` to `unknown` with `finished_at = now()`; `failures` is incremented, since an unknown outcome consumes retry budget; `decideRetry` then sets the row to `retrying` (with a back-off `due_at`) or to `failed` (`max_failures_unknown_outcome` or `expired`). An event is emitted after commit. |
| Dependencies | **None on Redis or on circuit state.** A dead worker is a fact regardless of the provider's health, so this sweep always runs. |
| Late workers | A slow (not dead) worker whose lease expired cannot overwrite the new state: the fencing token rejects its write, and a late success can still move the row to `sent` through transition 12 ([ADR-0004](0004-data-model-state-machine-and-fencing.md)). |
| Cadence | Every 5 seconds; at most 500 rows per pass, selected with `SKIP LOCKED` |

Rows reach this sweeper whenever a worker stops without finishing its sends: `SIGKILL`, an out-of-memory kill, a host or container failure, a shutdown that exceeds its grace period, or a Postgres outage that outlasts the worker's final-write retries ([ADR-0004](0004-data-model-state-machine-and-fencing.md) §4). On `SIGTERM`, workers stop consuming, finish their in-flight sends and nack any prefetched messages they have not claimed ([ADR-0005](0005-service-topology-and-status-events.md)).

### 5. Where the recovery paths run

Reconciliation, the lease sweeper and the expiry pass ([ADR-0002](0002-postgres-as-outbox-scheduler-and-retry-delay.md)) run as goroutines inside the **relay** process ([ADR-0005](0005-service-topology-and-status-events.md)). They are counted in `reconciliation_runs_total`, `reconciled_rows_total`, `sweeper_recovered_total{kind}` and `expired_total{channel}` ([ADR-0009](0009-observability.md)).

## Alternatives considered

| Alternative | Why it was rejected |
|---|---|
| **A periodic stale-queued sweeper** (rows `queued` for more than 5 minutes return to `pending`, provided the channel's circuit is closed and workers recently reported progress in Redis) | It failed in both directions. The progress signal was refreshed only when workers claimed, so a lane whose messages were all lost was never swept, and its lost rows could fill the HWM and block the lane until they expired. The conditions were instantaneous while row age is cumulative, so after any outage longer than the threshold, legitimately waiting rows were swept on the first pass. The threshold also assumed a provider that drains at full rate. It needed Redis and a breaker state shared with the relay. |
| **Repairing that sweeper with a worker heartbeat and a "healthy since" window** | Removes the two failures above at the cost of more shared state and timing rules. Reconciliation on reconnection is simpler, deterministic and testable with one scenario (wipe the broker's data and restart it). |
| **Unbounded publishing** (the backlog lives in RabbitMQ) | A long queue is not a memory problem for quorum queues, which do not keep message bodies in memory. The reasons are elsewhere: the backlog would not be queryable, cancellable or expirable in one place; a broker that loses its data would lose the position of the whole backlog; and reconciliation would have to republish millions of rows instead of thousands. |
| **One HWM per channel** | A full buffer of `low` messages would stop the relay from publishing `high` ones (see Decision 1). |
| **Manual recovery only** | Works, but gives up automatic recovery from the most likely loss, a broker restart without its data. The manual command remains for the cases reconciliation does not cover. |
| **Read queue depth from the RabbitMQ management API** | Adds a dependency and returns an eventually consistent value. A count in Postgres is cheap and local. |
| **Recovery path republishes directly** | Two publishers would make the HWM and the ordering harder to reason about. Returning the row to `pending` reuses the relay's path. |
| **Detect stuck `processing` rows with `updated_at` alone** | A slow worker (GC pause, network stall) is not a dead worker. Without a lease and a fencing token, a worker that wakes up late could overwrite the state of a newer attempt ([ADR-0004](0004-data-model-state-machine-and-fencing.md)). |

## Consequences

**Positive**

- RabbitMQ holds a small, bounded number of distinct notifications regardless of how large a burst is.
- Recovery from a broker that lost its data is automatic and bounded in cost.
- Priority is preserved at the relay: `high` always has headroom.
- A provider outage is absorbed by the backpressure chain without custom coordination logic.
- Neither recovery path depends on Redis.

**Negative / costs**

- Every relay cycle runs a count query. It is cheap, but it is extra load.
- The relay must refill buffers faster than workers drain them (up to 98 msg/s per channel). Batch size and tick interval are tuned together and verified by the load tests ([ADR-0010](0010-testing-and-ci.md)).
- Reconciliation covers reconnections only. A message lost while the connection stays up waits for the alert and the manual command, or for expiry.
- Every reconnection republishes up to an HWM's worth of rows per lane. The duplicates are harmless, but they are wasted work.
- The Postgres backlog has no upper limit other than expiry; admission control is [future work](../future-work.md).

## Review history

Changes made after the design review, recorded in the [design review log](../ai/design-review-log.md):

- The periodic stale-queued sweeper was replaced by reconciliation on every relay reconnection, plus a manual `requeue-stale` command and an oldest-queued-age alert, because the sweeper's conditions failed in both directions. (D5)
- The recovery paths no longer run in a relay leader: there is one relay, restarted by the container runtime. (D4)
- The lease now accounts for the rate-limit wait after the claim, and the lease sweeper sets `finished_at` on the attempts it marks `unknown`. (D20)
- Lanes are counted in Postgres over one broker queue per channel; "about 6,000 messages" became "about 6,000 distinct notifications; duplicates possible". (D7)
- The rejection of unbounded publishing no longer cites memory pressure, which does not apply to quorum queues; unbounded backlog in Postgres is now listed as a known limitation. (D56)
