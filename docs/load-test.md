# Load and chaos results

The measurements behind the quantitative claims of the design ([ADR-0010](adr/0010-testing-and-ci.md) §3). Every run uses the mock provider (`docker-compose.mock.yml`); nothing here was measured against webhook.site.

## How to read the numbers

**The machine** (one developer laptop; the load generator, the stack and the mock provider all run on it):

| | |
|---|---|
| Computer | Apple M4 Pro, 14 cores (10 performance, 4 efficiency), 24 GB of memory, macOS 27.0.1 |
| Docker | Docker Desktop, engine 29.9.2, Linux virtual machine with 14 processors and 7.75 GiB of memory (arm64) |
| Compose | v5.6.1 |
| Images | The versions pinned in `docker-compose.yml`: Postgres 18.7, RabbitMQ 4.4.1, Redis 8.10.3 |

**Which numbers are exact and which are orders of magnitude.**

- **Exact, and the same on any machine:** counts. How many notifications were accepted, sent and failed; how many requests the provider received; how many idempotency keys it received more than once; the largest number of arrivals in a one-second window. These are the numbers the claims rest on.
- **Orders of magnitude, for this machine only:** every rate that is not paced by the rate limit (ingest per second), every latency and every duration of a statement. They depend on the processor, on the disk behind Docker's virtual machine and on what else the machine was doing. A statement that takes 3 ms here says "a few milliseconds", not "3 ms".

## The rate limit

**The claim:** per channel, no half-open one-second window `[t, t + 1 s)` over the arrival times at the provider holds more than 100 messages (R15; [ADR-0007](adr/0007-retry-policy-and-circuit-breaker.md) §5). The mock provider records every arrival; the window is computed over the sorted arrivals (`internal/ratewindow`, with its own unit test) and cross-checked against the mock's own figure. The largest window is a count: it is exact.

### The pacing margin: why the configured rate is 97

The worker starts each send at a slot of its own, `1 / rate` apart, with no burst. Arrivals can still bunch by a few milliseconds (scheduling, the network), so the configured rate must leave slots free under the limit. The plan's rule: `load-smoke` (6,000 sms, about a minute at the full rate) is run 20 times; **if any run shows a largest window of 100, the rate is lowered** and the runs are repeated.

| Configured rate | Runs | Largest one-second window | Conditions |
|---|---|---|---|
| 98 msg/s | 18 | 99 in every run | Two series on 2026-11-20 on a quiet machine (6 runs, then 12), on one stack each. Both series were cut short, the first by the machine going to sleep, the second when the rate was decided; the runs counted here had finished before. |
| 98 msg/s | 1 | **100** | One `make test-e2e` on `main` on 2026-11-20 while ten runs of the full test suite used the same machine. |
| 98 msg/s | 8 | 99 in every run | The other `make test-e2e` runs recorded in `docs/progress.md` for P5 and P6, some of them with other work on the machine. |
| **97 msg/s** | 20 | **98 in every run** | One series on 2026-11-20, one stack kept up, nothing else running on the machine apart from short compiles. |

A window of 100 is not above the limit, but it is the limit with no margin, on a machine that was merely busy. So the rule applies and the default is **97 msg/s** (`RATE_LIMIT_PER_SECOND`), by the author's decision: three free slots under the limit instead of two, at a cost of 1 % of throughput. The 20 runs at 97:

| Run | SMS | Seconds from the first to the last arrival | Arrivals per second | Largest one-second window |
|---|---|---|---|---|
| 01 | 6,000 | 61.9 | 97.0 | **98** |
| 02 | 6,000 | 62.1 | 96.6 | **98** |
| 03 | 6,000 | 61.9 | 97.0 | **98** |
| 04 | 6,000 | 61.9 | 97.0 | **98** |
| 05 | 6,000 | 61.9 | 97.0 | **98** |
| 06 | 6,000 | 61.9 | 97.0 | **98** |
| 07 | 6,000 | 61.8 | 97.0 | **98** |
| 08 | 6,000 | 61.9 | 97.0 | **98** |
| 09 | 6,000 | 61.9 | 97.0 | **98** |
| 10 | 6,000 | 61.9 | 97.0 | **98** |
| 11 | 6,000 | 61.9 | 97.0 | **98** |
| 12 | 6,000 | 61.9 | 97.0 | **98** |
| 13 | 6,000 | 61.9 | 97.0 | **98** |
| 14 | 6,000 | 61.8 | 97.0 | **98** |
| 15 | 6,000 | 61.9 | 97.0 | **98** |
| 16 | 6,000 | 61.9 | 96.9 | **98** |
| 17 | 6,000 | 61.9 | 97.0 | **98** |
| 18 | 6,000 | 61.9 | 97.0 | **98** |
| 19 | 6,000 | 61.9 | 97.0 | **98** |
| 20 | 6,000 | 61.9 | 97.0 | **98** |

### The rate scenario: 30,000 sms

`make load`: 30,000 sms created through the batch endpoint and sent at the configured rate through a stack of its own, traced at 1 %.

| | |
|---|---|
| Run | 2026-11-20, `make load` on the commit that set the rate to 97; nothing else running on the machine |
| Created | 30,000 sms in 3.6 s through the batch endpoint with 8 clients (about 8,300 per second: an order of magnitude) |
| Arrived at the provider | 30,000, in 309.3 s from the first to the last arrival: **97.0 per second** |
| **Largest one-second window** | **98** (limit 100) |
| Result | Pass: no one-second window above 100 |

The run takes 30,000 / 97 ≈ 309 s; the measured 309.3 s means the pacer used every slot and added none.

## Strict priority and channel isolation

No load scenario was run for these two claims (see "Not measured" below). What exists is tested, not measured under load:

| Claim | Tests that exist | What they show | What they do not show |
|---|---|---|---|
| **Priority is strict:** `high` is delivered before `low` | `TestCycle_LaneOrderHighFirstWithinEachChannel` (`internal/relay`): the relay publishes the `high` lane of a channel before `normal` and `low`. `TestRelay_PrioritySetOnEveryMessage`: every published message carries its priority. Experiment E1 (`experiments/e1`, `TestLevelsFourFiveSixAndNoPriority`): the broker's quorum queue hands out all messages of a higher priority before any of a lower one, with no ratio. `TestHighestPriorityTaken` (`internal/worker`): the worker takes the highest-priority message of its prefetch buffer. | Each stage orders strictly, on its own. | The whole path under a backlog: that every `high` message submitted into a `low` backlog arrives at the provider before the `low` messages submitted with it, and what `high` latency is while `low` is saturated. |
| **Channels are isolated:** one channel's failing provider does not slow another | `TestE2E_CircuitChannelsAreIsolated` (`internal/integration`): with the sms circuit open, the other channels are sent. `TestCycle_LaneErrorIsIsolated` (`internal/relay`): a failing lane does not stop the others. `TestComposeE2E_WorkerPerChannel` (`make test-e2e`): with one channel's worker killed, the other two channels are sent, and each channel stays at or below 100 per second. | Sending continues on the other channels. | Throughput: that e-mail keeps its full rate while 100 % of sms fail. |

## Not measured

Reduced scope, decided by the author on 2026-11-20 with the submission a few hours away; it goes beyond the fallback order of the plan (§12), which would have dropped only the first two rows. Each item is in [future-work.md](future-work.md) with what is known.

| Not done | What is known instead |
|---|---|
| k6 and every k6 scenario | `make load` runs the rate scenario with the Go harness of `load-smoke`. It needs Go, where the design said Docker only. |
| Phase 1: 1,000,000 messages ingested, RabbitMQ depth flat while the Postgres backlog grows; listing and summary latency on that dataset | Ingest of 6,000 sms through the batch endpoint: 5,600 to 8,000 per second with 8 clients on this machine (an order of magnitude). The relay's lane high-water marks and the listing's and the summary's plans are tested on small tables (`TestList_ExplainUsesIndex`, `TestGauges_PlansReadOnlyUnfinishedRows`); the summary's counts grow with the backlog ([future-work.md](future-work.md)). In P3 1,000,000 rows were inserted with SQL for the plan measurements of [ADR-0003](adr/0003-bounded-relay-buffer-and-sweepers.md) §6. |
| Phase 2: 100,000 messages across three channels drained, each `Idempotency-Key` received exactly once | The chaos scenarios count keys at the provider for 3 to 990 notifications per scenario, and `load-smoke` and the rate scenario for 6,000 and 30,000 sms on one channel: no key more than once outside the `SIGKILL` scenario. |
| Saturation: `high` p95 and `low` wait under a `low` backlog | Nothing. |
| Strict priority and channel isolation under load | The tests in the table above. |
| The claim after an idle period: its cost against the backlog, a remedy, explicit pool sizes | The limitation and the measurements of P3 stay as documented in ADR-0003 §6: with statistics from an empty outbox, the first minute after 1,000,000 rows arrive runs at about 35 instead of 98 per second per channel, until autovacuum's `ANALYZE`. A small-scale exploration in P6 is recorded in [future-work.md](future-work.md); no SQL was changed. |
| Tuning of the first values (event buffer, gateway queue limits, send queue, alert thresholds; D66, D74) | The first values stand. They were not checked against load. |

## Chaos scenarios

The scenarios of `make test-e2e` ([ADR-0010](adr/0010-testing-and-ci.md) §1, "Chaos scenarios"). Each ends with one `chaos report:` line, read from Postgres and from the mock provider's report. The counts below are from six runs of the suite on 2026-11-20 (three by a delegate and two by the main loop at 98 msg/s, one by the main loop at 97); where a count differed between runs, the range is given.

| Scenario | Accepted | Sent | Failed | Requests at the provider | Keys received more than once | Most requests for one key |
|---|---|---|---|---|---|---|
| Provider down for 60 s, three `high` notifications | 3 | 3 | 0 | 3 | 0 | 1 |
| **Worker killed with `SIGKILL` mid-send** | 100 | 100 | 0 | **120 to 123** | **20 to 23** | **2** |
| RabbitMQ restarted | 164 to 182 | all | 0 | as many as accepted | 0 | 1 |
| RabbitMQ wiped with its data | 165 to 179 | all | 0 | as many as accepted | 0 | 1 |
| `requeue-stale` | 10 | 10 | 0 | 10 | 0 | 1 |
| Postgres restarted during sends | 713 to 714 | all | 0 | as many as accepted | 0 | 1 |
| Redis stopped | 7 | 7 | 0 | 7 | 0 | 1 |
| One worker per channel, one of them killed while idle | 990 | 990 | 0 | 990 | 0 | 1 |

**The duplicates of the `SIGKILL` scenario are the delivery guarantee at work, not a defect.** The mock answers sms after 3 s in that scenario, so the worker's 25 concurrency slots are in flight when it is killed. The provider had received 20 to 23 of those requests; the worker died before it recorded an answer; the rows stayed `processing` until their lease expired (30 s), the lease sweeper returned them to `retrying`, and they were sent again with the same `Idempotency-Key`. No notification reached the provider more than twice, and the scenario asserts that only a notification the kill left `processing` reached it twice. The bound is the concurrency: at most `WORKER_CONCURRENCY` (25) notifications per channel and worker are repeated by a kill. The notifications that were queued or buffered at the kill, 75 or more, were sent once.

The other scenarios produced no duplicate in these runs. A duplicate is possible in them too (an attempt that ends `unknown` is retried); they report the count and do not assert it.
