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
- **Drain time.** A full channel buffer of 2,000 messages is about 22 seconds of work at 90 msg/s.
- **The values are starting points,** chosen so that a full buffer is seconds of work, not derived from a model. They are **counted per lane and configured per priority**: one value for `high`, one for `normal` and one for `low`, each applied to every channel. The load tests check that the relay refills the buffer faster than the workers drain it ([ADR-0010](0010-testing-and-ci.md)).
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
| Trigger | **Every (re)connection of the relay to RabbitMQ**, including the first connection after start-up. The relay subscribes to the client library's recovery state notifications (`NotifyStateChange`); the trigger after an outage is the connection's change from `reconnecting` to `open`. `NotifyClose` is not used as the signal: during a recovery it can deliver extra errors, and it is closed without a value both when recovery gives up and on a normal close (experiment E4). |
| Action | **All** `queued` rows return to `pending` (`version + 1`, event emitted). The normal relay cycle then publishes them again. Reconciliation itself never publishes. |
| Rate | At most once per 60 seconds. **A trigger that arrives inside that window is deferred, not skipped:** it runs once when the window ends. |
| Duplicates | A copy of the message may still be in RabbitMQ. Whichever copy is claimed first wins; the other is dropped by the claim guard ([ADR-0004](0004-data-model-state-machine-and-fencing.md)). |
| When recovery gives up | The recovery budget is 24 attempts at 5 s, about 2 minutes (`AMQP_RECOVERY_MAX_RETRIES`, `AMQP_RECOVERY_RETRY_INTERVAL`). If it is used up, the connection's state changes to `closed` with an error and the relay exits with a non-zero code. The container runtime restarts it, and the new process declares the topology and reconciles on its first connection ([ADR-0005](0005-service-topology-and-status-events.md)). |
| Dependencies | Postgres and RabbitMQ only |

**The statement and where it runs** (as implemented in P3):

| Aspect | Rule |
|---|---|
| Statement | One statement: the `queued` rows are locked in `id` order and then updated. `status`, `queued_at` (cleared), `version` and `updated_at` change; `attempt_count`, `failures`, `due_at`, `expires_at` and the `dedupe_key` do not. |
| Lock order | `id`, the order of the batch cancel, the only other statement that waits for several rows; without it the two deadlocked in the first round of their test. Every other writer holds at most one row while it waits (claim, cancel, redrive, final writes) or never waits (lane transaction, expiry pass, lease sweeper). |
| Rows held by another transaction | Waited for, not skipped: a row skipped while a cancel holds it, whose transaction then rolls back, would stay `queued` with no copy in the broker. After the wait a row that is no longer `queued` is left alone. |
| Where it runs | In the publishing loop's own goroutine, between two ticks, never while a lane transaction is open. The statement takes the rows that are `queued` in its snapshot; a lane that committed `queued` just after it started would be missed. With one relay and reconciliation serialized with the lanes that cannot happen. |
| Timing | The publishing loop compares the connection's count of (re)connections with the count it has served, at every tick, so none is missed and several between two ticks are one trigger. A run starts at the first tick after its trigger; a deferred run at the first tick after the window ends (up to one tick, 100 ms, late). The window is measured from the start of the last successful run. A run that fails is retried at every tick and does not count as a run. |
| `requeue-stale` | The same statement with `queued_at < now() − threshold`; `--all` is reconciliation itself. The command connects to Postgres only and serves no admin port, so it runs next to a live relay. It prints `requeue-stale: N notifications returned to pending` and exits 0; a usage error exits 2, a configuration or database error 1. The relay's Compose service names the binary as its entrypoint so that `docker-compose run --rm relay requeue-stale` reaches it. |

**Measured on a large backlog** (P3; 200,000 `queued` rows next to 300,000 rows in other states, Postgres 18.7 with Compose's default settings, fresh statistics, a loaded machine):

| Statement | Rows | Plan | Time, custom plan | Time, generic plan | Row locks held | WAL | Events in memory |
|---|---|---|---|---|---|---|---|
| Reconciliation | 200,000 | Expiry index filtered on `status = 'queued'`, sort by `id` (on disk, 6 MB), row locks, hash join back to the table, update | 2.7 s (2.8 to 3.1 s from Go) | 2.6 s (the statement has no parameters: one plan) | about 2.6 s | 454 MB | 26 MB |
| `requeue-stale` (5 minutes) | about 155,000 | The same with the `queued_at` filter; identical in both modes | 2.2 s (2.5 to 3.5 s from Go) | 2.2 s | about 2 s | 410 MB | 21 MB |

The high-water marks keep the real set at about 6,000 rows, where the same statement takes well under 100 ms. At 200,000 rows it is a three-second transaction: acceptable for an event as rare as a reconnection, not cheap. If the high-water marks are ever raised by an order of magnitude, the lever is to run it in chunks in `id` order (each lock then held for about 0.1 s, memory bounded); the single statement is kept because it is simpler and the set is bounded.

The rule is deliberately coarse. After a reconnection the relay cannot know whether the broker kept its data, so it treats every `queued` row as possibly lost. The HWM keeps that set small (about 6,000 rows), and the claim guard makes the resulting duplicates harmless.

**What reconciliation does not cover.** A message that disappears while the connection stays up is not detected automatically: a message dead-lettered to a poison queue ([ADR-0001](0001-rabbitmq-as-dispatch-broker.md)), or a publish wrongly recorded as `queued`. For these there are two tools and one bound:

- the gauge `lane_oldest_queued_age_seconds{channel,priority}`, with an alert on the `high` lanes ([ADR-0009](0009-observability.md));
- a manual **`requeue-stale`** subcommand of the relay binary that applies transition 8 on demand, run as `docker-compose run --rm relay requeue-stale`. By default it takes `queued` rows older than 5 minutes (`--older-than=5m`); `--all` takes every `queued` row;
- expiry: a row that is never recovered fails as `expired` when its time runs out.

### 4. Lease sweeper (transition 7)

Recovers notifications whose worker died after claiming them.

| Aspect | Rule |
|---|---|
| Candidate rows | `status = 'processing' AND lease_expires_at < now()`, served by the partial index on `(lease_expires_at) WHERE status = 'processing'` |
| Lease length | 30 seconds, set at claim time. It covers the wait for the send after the claim, the provider HTTP timeout (10 s) and a margin for the final write ([ADR-0007](0007-retry-policy-and-circuit-breaker.md)). The wait after a claim is normally at most 250 ms, but a claim that misses its rate-limit slot waits for a new one; the worker's **send deadline** bounds it: a send starts within `lease − HTTP timeout − 5 s` (15 s) of issuing the claim call, or the attempt is closed as `aborted` ([ADR-0007](0007-retry-policy-and-circuit-breaker.md) §5). The deadline is measured from the call, not from its return, because the lease starts when the claim statement begins, including the time it waits on a row lock. |
| Action | In **one transaction**: the attempt row goes from `in_flight` to `unknown` with `finished_at = now()`; `failures` is incremented, since an unknown outcome consumes retry budget; `decideRetry` then sets the row to `retrying` (with a back-off `due_at`) or to `failed` (`max_failures_unknown_outcome` or `expired`). An event is emitted after commit. |
| Dependencies | **None on Redis or on circuit state.** A dead worker is a fact regardless of the provider's health, so this sweep always runs. |
| Late workers | A slow (not dead) worker whose lease expired cannot overwrite the new state: the fencing token rejects its write, and a late success can still move the row to `sent` through transition 12 ([ADR-0004](0004-data-model-state-machine-and-fencing.md)). |
| Cadence | Every 5 seconds; at most 500 rows per pass, selected with `SKIP LOCKED` |

Rows reach this sweeper whenever a worker stops without finishing its sends: `SIGKILL`, an out-of-memory kill, a host or container failure, a shutdown that exceeds its grace period, or a Postgres outage that outlasts the worker's final-write retries ([ADR-0004](0004-data-model-state-machine-and-fencing.md) §4). On `SIGTERM`, workers stop consuming, finish their in-flight sends and nack any prefetched messages they have not claimed ([ADR-0005](0005-service-topology-and-status-events.md)).

### 5. Where the recovery paths run

Reconciliation, the lease sweeper and the expiry pass ([ADR-0002](0002-postgres-as-outbox-scheduler-and-retry-delay.md)) run as goroutines inside the **relay** process ([ADR-0005](0005-service-topology-and-status-events.md)). They are counted in `reconciliation_runs_total`, `reconciled_rows_total`, `sweeper_recovered_total{kind}` and `expired_total{channel,priority}` ([ADR-0009](0009-observability.md)).

### 6. Plans of the statements that run all the time

Three statement classes of this system were correct and fast in `EXPLAIN` and slow as they really ran: the listing (P2), the expiry pass and the idempotency-key cleanup (P3). Each time the cause was a plan that Postgres had cached for a prepared statement and that no longer fitted ([design review log](../ai/design-review-log.md) §6). After the third case every statement that runs periodically or per message and usually finds little or nothing was **measured**, not reasoned about.

**Method.** Postgres 18.7, the exact SQL text the store runs. Dataset R: 1,000,000 `pending` rows, due, spread over the nine lanes, next to about 550,000 rows in other states. Dataset S: 200,000 `processing` rows with an expired lease, next to the same remainder. Each statement was prepared and explained (`ANALYZE`, `BUFFERS`) under `plan_cache_mode = force_custom_plan` and `force_generic_plan`, with fresh statistics, three runs each. The machine was loaded by other work, so the times are orders of magnitude.

| Statement | Runs | Data | Custom plan | Generic plan (forced) | Verdict | Action |
|---|---|---|---|---|---|---|
| `CountQueuedByLane` | every 100 ms | R | Bitmap scan of the queued-lane index, hash aggregate; 3.6 ms | The same (the statement has no parameters, so it has only one plan); 3.1 ms | same | left cached |
| `LockDueLane` | up to 9 per tick | R | Limit, row locks, ordered scan of the relay-lane index; 0.19 ms | The same; 0.19 ms | same | left cached |
| `MarkQueued` (200 ids) | per lane | R | Primary-key index scan; 2.0 ms | The same; 1.9 ms | same | left cached |
| `ClaimNotification` | per message | R | Primary-key index scan in the CTE, insert of the attempt; 0.14 ms | The same; 0.10 ms | same | left cached |
| `MarkSent` | per message | R | Primary-key index scan; 0.05 ms | The same; 0.07 ms | same | left cached |
| `LockProcessing` | per failed attempt | R | Primary-key index scan, row lock; 0.016 ms | The same | same | left cached |
| `MarkRetrying` | per failed attempt, per swept row | R, S | Primary-key index scan; 0.06 ms | The same; 0.05 ms | same | left cached |
| `MarkFailed` | per failed attempt, per swept row | R, S | Primary-key index scan; 0.05 ms | The same; 0.04 ms | same | left cached |
| `FinishAttempt` | per message | R | Index scan of the attempts' primary key; 0.03 ms | The same; 0.03 ms | same | left cached |
| `LockExpiredLeases` | every 5 s | S | Limit, row locks, ordered scan of the lease index; 0.35 to 0.6 ms | The same; 0.35 ms | same | left cached |
| `MarkAttemptUnknown` | per swept row | S | Index scan of the one-in-flight index; 0.03 ms | The same; 0.025 ms | same | left cached |
| `ExpireDue`, `DeleteExpiredIdempotencyKeys` | every second, every minute | | | Worse: the batch joined against the whole expired backlog ([ADR-0002](0002-postgres-as-outbox-scheduler-and-retry-delay.md), expiry) | generic worse | planned on every call (since `p3/classification-and-retry`) |

**Result: for these eleven statements the generic plan is the custom plan.** Their plans do not depend on the values of their parameters, which is what distinguishes them from the expiry pass (whose selectivity changes with time) and the listing (whose optional filters hide the selectivity). They stay cached prepared statements. The reconciliation and `requeue-stale` statements are measured the same way when they are written.

**A different defect, found by the same measurement and not settled here.** The comparison above uses fresh statistics. In the order production can meet them, the same statements were also run with statistics taken while the tables were small (40 rows), executed 35 times, and then the backlog of dataset R or S inserted without an `ANALYZE` in between, as when a fresh deployment receives its first burst. The cached plans are then the sequential scans that were right for 40 rows:

| What | With the stale cached plans | After the next `ANALYZE` |
|---|---|---|
| One primary-key write of the worker | 90 to 480 ms | 0.05 ms |
| Claim plus final write of one message | 240 to 390 ms | about 1 ms |
| One relay tick (count and nine lanes) | about 6 s | about 40 ms |
| One sweep of 500 expired leases | 42 to 48 s | about 0.2 s |

This is not the generic-plan defect: custom and generic plan are equal here, and both are stale. It ends when autovacuum analyses the table, which invalidates the cached plans: 22 s after the insert in the one run measured, and in general within the autovacuum launcher's cycle (60 s by default) plus the time of the `ANALYZE`. It affects every cached statement of every role, and also the foreign-key checks that Postgres runs inside the claim's insert, which the application cannot plan differently by changing how it sends a statement. Three remedies were measured, and the author chose the first (**D79**, below). Two cases stay slow under every plan until statistics are fresh, because the planner itself is misled: `CountQueuedByLane` with statistics from a steady state without a backlog (100 to 200 ms per tick), and the sweeper's `MarkRetrying` with 200,000 expired leases and statistics from a small table (about 30 ms per row).

**D79: every pool plans with the current values.** `plan_cache_mode = force_custom_plan` is set on every Postgres pool the application opens (`store.OpenPool`, used by the api, the relay and the worker; a guard test fails when non-test code opens a pool any other way, and a test per role asserts the setting on a pooled connection). It is set in code as a startup parameter, so a `DATABASE_URL` works unchanged and one that names another mode does not decide. Postgres then plans every execution with the table's current size, also the foreign-key checks, which no change to how a statement is sent can reach. A statement without parameters is never re-planned under the setting, so the relay's count runs as an unnamed statement.

**D79 is the general rule.** The listing (P2), the expiry pass and the idempotency-key cleanup (the first P3 pull request) were moved to unnamed statements one by one, each when its defect was found, before the rule existed. They stay as they are: an unnamed statement is planned with its values under any setting, so the two mechanisms do not conflict, and their tests (`TestList_RunsWithCustomPlans`, `TestExpiry_RunsWithCustomPlans`) keep guarding them.

**Measured again on the real code** (the tree with D79 against the commit before it; the same sequence: statistics from about 50 rows, the statements executed 35 times, then 1,000,000 pending rows across the lanes and 550,000 rows in other states without an `ANALYZE`; autovacuum off on both tables for the run; a loaded machine):

| | Before D79 | With D79 |
|---|---|---|
| One relay tick (count and nine lanes) | 5.8 to 6.8 s | 32 to 36 ms |
| The count alone | 104 to 227 ms (sequential scan and sort) | 0.4 to 2.0 ms (index-only scan of the queued-lane index) |
| Claim with its final write, first 10 messages | 260 to 607 ms | 1.0 to 1.4 ms |
| The foreign-key check inside the claim | up to 87 ms (sequential scan; 3 of 20 checks) | 0.002 to 0.003 ms (primary key), all 20 |
| A sweep of 500 expired leases (200,000 expired) | 41 to 44 s | 15 to 19 s |

Every statement shows `generic_plans = 0` afterwards. The cost at steady state is about 0.1 to 0.2 ms per message and 0.2 to 0.3 ms per relay tick.

**What D79 does not cover: a planner misled by stale statistics.** The sweep in the table is still slow: with statistics from a small table the planner picks the lease index for the per-row update, and that index now holds 200,000 entries (29 ms per row). The same class turned out to be broader than the two cases named above when the burst was repeated on the Compose stack. There the statistics were taken while the outbox was **empty** (everything sent), which is the normal idle state. With them, the custom plan of the claim, and of the relay's update to `queued`, looks the row up through the partial expiry index, which the statistics say is empty and which then holds every pending row:

| First minute after 1,000,000 rows arrive (Compose, autovacuum off for the run) | Arrivals at the provider, per channel | Sent after 60 s |
|---|---|---|
| Before D79 | 0 to 25 per second for 8 s, then about 32 per second | 5,846 |
| With D79, statistics from an empty outbox | about 35 per second (each claim about 290 ms) | 6,344 |
| With D79, tables never analysed | 98 per second from the first second | 18,253 |

So D79 removes the stale cached plans, and on a stack whose tables were analysed while idle the first minute of a large burst is still about a third of the configured rate, until autovacuum's `ANALYZE` (with default settings at most about a minute after the burst). Nothing is lost or repeated, and the logs show nothing: the only symptom is the slow provider. The remedy is at the level of statistics. It was open issue 11 of P3; by the author's decision it is part of the tuning of P6, where the claim's cost is measured against the size of the backlog and the remedies are compared in the burst scenario ([implementation plan](../plan/implementation-plan.md), P6).

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
- The relay must refill buffers faster than workers drain them (up to 90 msg/s per channel). Batch size and tick interval are tuned together and verified by the load tests ([ADR-0010](0010-testing-and-ci.md)).
- Reconciliation covers reconnections only. A message lost while the connection stays up waits for the alert and the manual command, or for expiry.
- Every reconnection republishes up to an HWM's worth of rows per lane. The duplicates are harmless, but they are wasted work.
- The Postgres backlog has no upper limit other than expiry; admission control is [future work](../future-work.md).

## Review history

Changes made after the design review, recorded in the [design review log](../ai/design-review-log.md):

- The periodic stale-queued sweeper was replaced by reconciliation on every relay reconnection, plus a manual `requeue-stale` command and an oldest-queued-age alert, because the sweeper's conditions failed in both directions. (D5)
- The recovery paths no longer run in a relay leader: there is one relay, restarted by the container runtime. (D4)
- The lease now accounts for the rate-limit wait after the claim, and the lease sweeper sets `finished_at` on the attempts it marks `unknown`. (D20)
- Lanes are counted in Postgres over one broker queue per channel; "about 6,000 messages" became "about 6,000 distinct notifications; duplicates possible". (D7)
- After experiment E4 (P0), the reconciliation trigger was named exactly (the connection's change from `reconnecting` to `open`), and the recovery budget of 24 attempts at 5 s was added. (D5)
- In P3 reconciliation was implemented: its statement locks in `id` order, waits for held rows, runs serialized with the lane transactions, and was measured on 200,000 queued rows (section 3). D79 put `plan_cache_mode = force_custom_plan` on every pool; the repeat of the burst measurement on the real code confirmed it and showed what it does not cover (section 6).
- In P3, after the expiry pass had been found slow under a cached generic plan, every periodic and per-message statement was measured under both plan modes (section 6). The eleven statements have the same plan either way and stay cached; the measurement also found that plans cached while a table was small stay in use after a burst until the next `ANALYZE`.
- The rejection of unbounded publishing no longer cites memory pressure, which does not apply to quorum queues; unbounded backlog in Postgres is now listed as a known limitation. (D56)
