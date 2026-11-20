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

## Chaos scenarios

The scenarios of `make test-e2e` ([ADR-0010](adr/0010-testing-and-ci.md) §1, "Chaos scenarios"). Each ends with one `chaos report:` line, read from Postgres and from the mock provider's report. The counts below are from five runs of the suite on 2026-11-20 (three by a delegate, two by the main loop); where a count differed between runs, the range is given.

| Scenario | Accepted | Sent | Failed | Requests at the provider | Keys received more than once | Most requests for one key |
|---|---|---|---|---|---|---|
| Provider down for 60 s, three `high` notifications | 3 | 3 | 0 | 3 | 0 | 1 |
| **Worker killed with `SIGKILL` mid-send** | 100 | 100 | 0 | **120 to 122** | **20 to 22** | **2** |
| RabbitMQ restarted | 164 to 182 | all | 0 | as many as accepted | 0 | 1 |
| RabbitMQ wiped with its data | 165 to 179 | all | 0 | as many as accepted | 0 | 1 |
| `requeue-stale` | 10 | 10 | 0 | 10 | 0 | 1 |
| Postgres restarted during sends | 713 to 714 | all | 0 | as many as accepted | 0 | 1 |
| Redis stopped | 7 | 7 | 0 | 7 | 0 | 1 |
| One worker per channel, one of them killed while idle | 990 | 990 | 0 | 990 | 0 | 1 |

**The duplicates of the `SIGKILL` scenario are the delivery guarantee at work, not a defect.** The mock answers sms after 3 s in that scenario, so the worker's 25 concurrency slots are in flight when it is killed. The provider had received 20 to 22 of those requests; the worker died before it recorded an answer; the rows stayed `processing` until their lease expired (30 s), the lease sweeper returned them to `retrying`, and they were sent again with the same `Idempotency-Key`. No notification reached the provider more than twice, and the scenario asserts that only a notification the kill left `processing` reached it twice. The bound is the concurrency: at most `WORKER_CONCURRENCY` (25) notifications per channel and worker are repeated by a kill. The 75 notifications that were queued or buffered at the kill were sent once.

The other scenarios produced no duplicate in these runs. A duplicate is possible in them too (an attempt that ends `unknown` is retried); they report the count and do not assert it.
