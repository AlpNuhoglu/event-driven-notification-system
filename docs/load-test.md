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

### The configured rate is 90: a margin of ten

**Decided by the author on 2026-11-20, at the end of P7** (ledger D24). The reason: the comparison below showed that a stall on the send side can add a send to a one-second window (two pairs of calls that started together, in 14 stalls over 30,000 sends), and the margin of three at 97 msg/s was exceeded once (a window of 102). A margin of ten absorbs several such stalls in one second. **The defect stays open and documented; the lower rate bounds its effect and does not fix it.** No pacer code was changed: the default, `.env.example` and the tests that pin the default.

The evidence at 90, with **fewer runs than the plan's rule asks for** (ten `load-smoke` runs where the rule says twenty, and one run of the rate scenario per condition), because the change was made in the last hour before the submission:

| Run at 90 msg/s | Conditions | Arrived | **Largest one-second window** |
|---|---|---|---|
| `load-smoke`, 10 runs of 6,000 sms on one stack | Nothing else running; the session waited | 6,000 in 66.7 s each (the tenth in 66.8 s), 89.8 to 90.0 per second | **91 in every one of the ten runs** |
| `make load`, 30,000 sms | Nothing else running; the session waited | 30,000 in 333.4 s, 90.0 per second | **91** |
| `make load`, 30,000 sms | **Four busy processes on the host for the whole run,** as in the comparison run; the session was editing documents as well | 30,000 in 333.5 s, 90.0 per second | **92** |

All twelve runs passed. On an idle machine the largest window is one above the rate, as it was at 97 (98) and at 98 (99); with the host loaded it was two above, nine below the limit.

At 90 msg/s a slot is 11.1 ms, a run of 30,000 sms takes 333 s and `load-smoke` 67 s. **Not shown at 90:** a run that shares the machine with ordinary light work of the kind that produced the 102 (the busy run above is the nearest); more than one run per condition; the send side against the receive side (the comparison below was made at 97).

**The history, next to it:**

| Rate | Largest one-second window |
|---|---|
| 98 msg/s (until P6) | 99 in 26 runs; **100** in one run on a busy machine |
| 97 msg/s (P6 until the end of P7) | 98 in 20 `load-smoke` runs and in two runs of 30,000 on an idle machine; **102** in one run of 30,000 on a machine in use (the run failed); 99 in the run of 30,000 with four busy host processes and every send traced |
| **90 msg/s** | 91 in ten `load-smoke` runs, 91 in the run of 30,000 on an idle machine, 92 with four busy host processes |

The sections below are the measurements at the two earlier rates, as they were written when the rate was 97.

### The pacing margin at 98 and at 97

The worker starts each send at a slot of its own, `1 / rate` apart, with no burst. Arrivals can still bunch by a few milliseconds (scheduling, the network), so the configured rate must leave slots free under the limit. The plan's rule: `load-smoke` (6,000 sms, about a minute at the full rate) is run 20 times; **if any run shows a largest window of 100, the rate is lowered** and the runs are repeated.

| Configured rate | Runs | Largest one-second window | Conditions |
|---|---|---|---|
| 98 msg/s | 18 | 99 in every run | Two series on 2026-11-20 on a quiet machine (6 runs, then 12), on one stack each. Both series were cut short, the first by the machine going to sleep, the second when the rate was decided; the runs counted here had finished before. |
| 98 msg/s | 1 | **100** | One `make test-e2e` on `main` on 2026-11-20 while ten runs of the full test suite used the same machine. |
| 98 msg/s | 8 | 99 in every run | The other `make test-e2e` runs recorded in `docs/progress.md` for P5 and P6, some of them with other work on the machine. |
| **97 msg/s** | 20 | **98 in every run** | One series on 2026-11-20, one stack kept up, nothing else running on the machine apart from short compiles. |

**The first pacing series was discarded, and one thing in it was not investigated.** The first series at 98 msg/s ran on battery, and the machine went to sleep in the middle of it (idle sleep after one minute). Its first six runs had finished before that and are the "6 runs" of the first row. Runs 07 to 11 of that series spanned five hours of wall-clock time and are not measurements: they are kept as files and used for nothing. **Their logs show a "largest one-second window" of 118.** A window above 100 would be a violation of the limit if it were real. It was **not investigated**: the timestamps were taken across a suspended virtual machine, where the provider's clock and the sender's timers stop and resume together with the host, and nothing can be concluded from them either way. No run on a machine that stayed awake has shown a window above 100 (the table above), but the 118 was set aside, not explained. Every long run since holds a `caffeinate` assertion, so that the machine cannot sleep during a measurement.

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

### One run of the rate scenario failed: a window of 102

The scenario was run twice more on 2026-11-20, in P7, from a fresh clone of the repository on the same machine (on battery, `caffeinate` held, the same code and the same rate):

| Run | Conditions | Arrived | **Largest one-second window** | Result |
|---|---|---|---|---|
| P6 (above) | Nothing else running | 30,000 in 309.3 s, 97.0 per second | **98** | Pass |
| **P7, first** | **The session that wrote the documents was working on the machine:** shell commands, Git, a few HTTP requests, small scripts. No build, no test run, no other container | 30,000 in 309.5 s, 96.9 per second | **102** | **Fail:** "a one-second window at the provider holds 102 sms arrivals, more than 100". The mock's own figure and the window computed from its arrival times agree |
| P7, second | Nothing else running; the session waited | 30,000 in 309.3 s, 97.0 per second | **98** | Pass |
| P7, third (the comparison below) | **Four busy processes on the host for the whole run,** every request traced instead of one in a hundred, the stack kept afterwards | 30,000 in 309.8 s, 96.8 per second | **99** | Pass |

**So the claim of this section does not hold unconditionally: at 97 msg/s, one of four runs of 30,000 sms put 102 arrivals into one second at the mock provider, on a machine that was only lightly used.** Two arrivals above the limit in the worst second of a run of 30,000.

What the failed run's logs show (they are kept outside the repository): the worker logged all 30,000 notifications as sent, no warning and no error, and its "sent" lines have three places where nothing completed for a moment and a bunch followed: two gaps of 51 and 57 ms eight seconds into the run, gaps of 53 and 99 ms at 3 min 27 s, and one of 178 ms at 4 min 22 s. Counted over the worker's own completion times, 16 one-second windows hold 100 or more (the largest 106); those are completions, not arrivals, and the arrivals are the mock's 102.

**What is not established:** where the stalls were. The stack was removed with its data when the run ended, so the attempts' start times and the mock's arrival list are gone. Two readings fit, and they differ in what they mean:

- The receiving side stalled (the mock's process, or the virtual machine's network path), while the worker went on starting sends at its slots. Requests then queue and are received together. This is the limitation the design already states: **the limit is enforced when a send starts, not when it arrives** ([future-work.md](future-work.md)). A stall of 50 ms holds about five sends; three free slots do not absorb that, and no rate close to 100 would.
- The worker stalled and then sent late claims together. The missed-slot rule (D77) exists to prevent exactly this and has its tests and its mutation; nothing in this run contradicts them, and nothing in it confirms that they held.

The failed run itself cannot be examined any more. The next section is the comparison that was made instead, on a run of its own. The plan's pacing-margin rule (lower the rate when a run shows 100 or more, then repeat the runs) was **not** applied: it is a change of the configured rate and of D24, which is the author's decision, and by the first reading a rate of 96 would not have prevented this run. The 20 `load-smoke` runs and the two runs with 98 above were on an idle machine; **an idle machine is the condition under which "largest window 98" was measured.**

### Send side against receive side (open issue 16)

One run, time-boxed to 30 minutes on 2026-11-20, to find out on which side arrivals bunch. The rate stayed 97 and no code was changed. The host was loaded on purpose (four processes that only burn a processor, for the whole run), every request was traced (`OTEL_TRACES_SAMPLER_ARG=1.0` for this run only; the scenario normally traces 1 %), and the stack was kept afterwards.

- **Receive side:** the mock provider's 30,000 arrival stamps.
- **Send side:** the start of the worker's HTTP client span (`HTTP POST`) of each of the 30,000 provider calls, read from Jaeger. This is the moment the worker starts the call. Both sides read the clock of the same virtual machine.

**The window above 100 was not reproduced:** the largest one-second window was 99, of arrivals and of starts alike.

| | Result |
|---|---|
| The n-th start against the n-th arrival, all 30,000 | The arrival follows the start by 0.10 ms (median), 1.03 ms at the 99.9th percentile, 12.4 ms at most; 2 of 30,000 by more than 5 ms |
| One-second windows of **starts** | 97 in 16,034 of them, 98 in 13,073, **99 in 2** |
| One-second windows of **arrivals** | At most 99, at the same instant as the starts' 99 |
| Gaps between consecutive starts | 10.32 ms (median). 14 gaps over 20 ms, 2 over 50 ms, one of 252 ms: the stalls are on the send side too |
| After the gap of 252 ms | No catch-up: at most 98 starts in any second of the next two |
| Calls that started less than 5 ms apart | **Two pairs, 0.22 ms and 0.07 ms apart,** both within a quarter of a second after a gap of 52 ms (which followed gaps of 42 and 33 ms). They are the two windows of 99 |
| `pacer_slots_missed_total` for the run | 28: the missed-slot rule gave up 28 slots |

**What the comparison shows, for this run:**

1. **The arrivals were not bunched on the way.** Every irregularity in the arrivals is already in the starts; the receiving side and the path between the two added about a millisecond at most. The reading "the sends were on time and a stall of the receiver bunched them" is **not** what this run shows.
2. **The stalls were on the send side, and the pacer did not catch up after them.** While the worker (or what it waits for: the claim in Postgres, the reservation in Redis) stood still, nothing was sent; afterwards the sends were one slot apart again. The longest stall, a quarter of a second, was followed by no window above 98.
3. **Twice, after a shorter stall, two calls started within a millisecond of each other.** This is on the worker's side and it is outside what the missed-slot rule (D77) allows: a send may start up to 5 ms after its slot (`PACER_SLOT_TOLERANCE`), so two consecutive sends can be 5.3 ms apart by design, not 0.1 ms. In this run it cost one message per window: 99 instead of 98.

**What is not established:** which step lets two calls start together. Read from the code, not measured: the rule is checked when the wait for the slot ends, so a goroutine that is held between that check and the start of its call starts late, next to the following one; and a reservation's wait is relative, so a reservation whose answer from Redis was delayed by the stall can land on a later slot's time. The spans do not record a call's slot, so neither reading could be checked. **Nor is it established that this effect produced the 102 of the failed run:** that run's data is gone, its stalls were longer (up to 178 ms), and it traced 1 % of the requests where this one traced all. What can be said is that the one mechanism observed is on the send side, that it appeared right after stalls, and that the failed run had stalls followed by bunches.

**Stopped here, by the author's time box.** No fix was attempted, the rate was not changed, and the effect is recorded as a worker-side defect that is open ([future-work.md](future-work.md), the limitation on the rate limit; open issue 16 in [progress.md](progress.md)). The next step would be to record each call's slot next to its start (a span attribute) and to repeat the run until a window above 100 appears with that data kept.

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

The scenarios of `make test-e2e` ([ADR-0010](adr/0010-testing-and-ci.md) §1, "Chaos scenarios"). Each ends with one `chaos report:` line, read from Postgres and from the mock provider's report. The counts below are from seven runs of the suite on 2026-11-20 (three by a delegate and two by the main loop at 98 msg/s, two by the main loop at 97); where a count differed between runs, the range is given.

| Scenario | Accepted | Sent | Failed | Requests at the provider | Keys received more than once | Most requests for one key |
|---|---|---|---|---|---|---|
| Provider down for 60 s, three `high` notifications | 3 | 3 | 0 | 3 | 0 | 1 |
| **Worker killed with `SIGKILL` mid-send** | 100 | 100 | 0 | **120 to 125** | **20 to 25** | **2** |
| RabbitMQ restarted | 164 to 182 | all | 0 | as many as accepted | 0 | 1 |
| RabbitMQ wiped with its data | 165 to 179 | all | 0 | as many as accepted | 0 | 1 |
| `requeue-stale` | 10 | 10 | 0 | 10 | 0 | 1 |
| Postgres restarted during sends | 713 to 714 | all | 0 | as many as accepted | 0 | 1 |
| Redis stopped | 7 | 7 | 0 | 7 | 0 | 1 |
| One worker per channel, one of them killed while idle | 990 | 990 | 0 | 990 | 0 | 1 |

**The duplicates of the `SIGKILL` scenario are the delivery guarantee at work, not a defect.** The mock answers sms after 3 s in that scenario, so the worker's 25 concurrency slots are in flight when it is killed. The provider had received 20 to 25 of those requests; the worker died before it recorded an answer; the rows stayed `processing` until their lease expired (30 s), the lease sweeper returned them to `retrying`, and they were sent again with the same `Idempotency-Key`. No notification reached the provider more than twice, and the scenario asserts that only a notification the kill left `processing` reached it twice. The bound is the concurrency: at most `WORKER_CONCURRENCY` (25) notifications per channel and worker are repeated by a kill. The notifications that were queued or buffered at the kill, 75 or more, were sent once.

The other scenarios produced no duplicate in these runs. A duplicate is possible in them too (an attempt that ends `unknown` is retried); they report the count and do not assert it.
