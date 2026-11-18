# Implementation plan

The plan that the implementation sessions execute. Each phase is sized for one fresh Claude Code session and one or a few pull requests.

- **What to build** is decided in the [decision ledger](../ai/decision-ledger.md) (`D…`, `G…`) and the [ADRs](../adr/). This plan decides only **in which order**, and **how each step is shown to be done**.
- **Where things stand** is recorded in [`docs/progress.md`](../progress.md). A session reads that file first.
- If this plan and the ledger or an ADR disagree, the ledger and the ADRs win; fix the plan in the same pull request and note it under "Known deviations from the plan" in `progress.md`.

## Contents

1. [Critical path and day mapping](#1-critical-path-and-day-mapping)
2. [Rules for every phase](#2-rules-for-every-phase)
3. [P0: repository skeleton and first-hour experiments](#p0-repository-skeleton-and-first-hour-experiments)
4. [P1: core path](#p1-core-path)
5. [P2: API completeness](#p2-api-completeness)
6. [P3: delivery robustness](#p3-delivery-robustness)
7. [P4: templates and scheduling](#p4-templates-and-scheduling)
8. [P5: real-time and observability](#p5-real-time-and-observability)
9. [P6: quality](#p6-quality)
10. [P7: delivery](#p7-delivery)
11. [Unverified assumptions](#11-unverified-assumptions)
12. [Fallback order and the "never drop" list](#12-fallback-order-and-the-never-drop-list)
13. [Risks](#13-risks)

## 1. Critical path and day mapping

The critical path is the shortest chain of work that proves the design holds: **schema → transitions → relay → worker (pacer, rate limit, claim) → provider → one notification `sent`, with the relay-versus-worker race tested and the rate limit measured.** Everything else is built on top of a path that already works end to end.

| Day | Phases | The day ends when |
|---|---|---|
| **1** | P0, P1 | **Critical-path exit criterion:** from a clean clone, `docker-compose up` starts the stack; a single notification created with `curl` on an idle system reaches `sent` at the provider; the two relay-versus-worker race tests and the unroutable-publish test pass; `load-smoke` shows no one-second window above 100 arrivals; CI is green. The README has a working quick start. |
| **2** | P2, P3, P4, P5 | The API is complete, failures are classified and recovered, templates and scheduling work, WebSocket updates and the metrics summary work. The README has API examples and `make demo`. |
| **3** | P6, P7 | The test matrix, chaos and load scenarios and `make verify-suite` are in place; CI is hardened; the README, the regenerated architecture document and the design review log are written; the clean-clone rehearsal has passed; the webhook.site URL has been renewed. |

**If day 1 does not end with its exit criterion, nothing from day 2 starts.** The time is taken from the end of the plan, following the [fallback order](#12-fallback-order-and-the-never-drop-list).

## 2. Rules for every phase

These restate `CLAUDE.md` where a phase depends on them.

- **Branches:** `p<phase-number>/<short-topic>`. One pull request per branch, opened with `gh pr create`; never merged by the agent.
- **Exit criteria are commands.** A phase is done when every command in its exit criteria has been run and its output is pasted into the pull request description.
- **Tests come with the code they test,** in the same pull request. No `time.Sleep` in tests; durations are configuration and are set to milliseconds in tests (D52).
- **Every state transition goes through a repository function that returns its event** ([ADR-0004](../adr/0004-data-model-state-machine-and-fencing.md) §1).
- **Scope that is dropped is recorded in `docs/future-work.md` in the same commit** (D59).
- **Delegation** follows `.claude/skills/fable-orchestration/SKILL.md`. Each phase below names which parts go to which tier. In every phase the **concurrency-critical code is reviewed by the main loop line by line**, whoever wrote it: the claim statement, the fencing writes, the pacer, the GCRA script, the relay transaction and reconciliation.
- **Planned test names** are the ones used in [ADR-0010](../adr/0010-testing-and-ci.md). A session may rename a test, but it then updates the ADR's matrix and the requirements map in the same pull request.

Delegation tiers used below:

| Tier | Used for |
|---|---|
| **Main loop** | Decisions, briefs, integration, review of every diff, line-by-line review of concurrency-critical code, judging experiment and load results |
| **Opus** | Multi-file implementation, debugging, integration tests that need judgment |
| **Sonnet** | Scoped, well-specified work: boilerplate from an ADR table, validators with table tests, YAML, pattern clones |
| **Haiku** | Not expected to be needed; there is no bulk data work in this project |

---

## P0: repository skeleton and first-hour experiments

**Goal.** A repository that builds, lints and tests in CI with pinned infrastructure, and six experiments whose results confirm (or replace) the decisions that rest on documentation nobody has run.

**Implements.** D60, D59, D48, D45, D47 (the `healthcheck` subcommand), D53 (first jobs), G5; ledger sections I and J.

### Task 1: repository set-up (D60)

Done by the author in the repository settings, before any code is written. The session verifies each item and stops if one is missing.

| Item | Check |
|---|---|
| The design documents and agent configuration are on `main` (the documents pull request is merged) | `git log --oneline origin/main` shows only documentation commits |
| `main` is protected: pull requests required, no direct pushes, no force pushes | `gh api repos/{owner}/{repo}/branches/main/protection` returns the rule |
| "Rebase and merge" is the only merge method (D59) | `gh api repos/{owner}/{repo} --jq '[.allow_rebase_merge, .allow_merge_commit, .allow_squash_merge]'` prints `[true,false,false]` |
| Dependabot is off (D53) | No `.github/dependabot.yml`; security updates disabled in settings |
| `.inputs/` is not tracked (G5) | `git ls-files .inputs` prints nothing |

### Task 2: skeleton

| Area | Content |
|---|---|
| Go module | `go.mod` with Go 1.26 or later; `cmd/{api,relay,worker,gateway,migrate,mockprovider}/main.go` as stubs, each with a `healthcheck` subcommand; `internal/platform/config` reading environment variables with the names in `.env.example` |
| Tooling | `Makefile` with the targets of [ADR-0010](../adr/0010-testing-and-ci.md) §4 (`test`, `test-docker`, `test-e2e`, `load`, `demo`, `verify-suite`, `fuzz`) plus `generate` and `lint`; pinned versions of `sqlc`, `oapi-codegen`, `golang-migrate`, `golangci-lint` v2; `.golangci.yml` in the v2 format |
| Image | One `Dockerfile` for all roles |
| Compose | `docker-compose.yml` with the infrastructure services only, **every image on an exact tag** (D48): Postgres 18.x, RabbitMQ 4.4.x with a fixed hostname, a named volume and `rabbitmq_prometheus` enabled (D9), Redis, Prometheus, Grafana, Jaeger 2.x (D43). Each has a health check. |
| CI | `.github/workflows/ci.yml` with `lint`, `unit` and `codegen-drift`; actions pinned to full commit SHAs; minimal `permissions` |

Two cautions for the Compose file:

- The official Postgres 18 image stores its data under `/var/lib/postgresql` (the data directory is version-specific below it). A volume mounted at `/var/lib/postgresql/data`, which was correct up to version 17, targets the wrong path.
- RabbitMQ stores data under its node name, which defaults to the container hostname. Without a fixed hostname, a recreated container starts with an empty broker.

### Task 3: first-hour experiments (ledger section I)

Each experiment is a small program or test run against the pinned infrastructure. Its result (pass or fail, the evidence, the fallback taken) is written to `docs/progress.md`. Experiment code is throwaway unless the table names the permanent test it becomes. **If an experiment fails, its fallback becomes the design:** the affected ADR is updated in the same pull request and the change is listed under "Known deviations from the plan".

| # | Experiment | Pass check | Fallback if it fails | Becomes |
|---|---|---|---|---|
| **E1** | RabbitMQ 4.4: publish N `low` messages, then one `high`, to a quorum queue; consume | The `high` message arrives first. A message published without a priority is delivered as if it had priority 4. | A separate queue per priority with a strict local pick in the worker (no weights) | `TestRelay_PrioritySetOnEveryMessage` and the strict-priority load test (P1, P6) |
| **E2** | `oapi-codegen` strict-server with the standard-library router | The code generates and compiles; a stub endpoint returns `application/problem+json` on an error | strict-server with `chi` | The API skeleton of P1 |
| **E3** | `testcontainers-go` inside a container on macOS with the settings of [ADR-0010](../adr/0010-testing-and-ci.md) §4 | `make test-docker` runs one integration test | Document `make test` (Go and Docker) as the only path on macOS | The `test-docker` CI job (P6) |
| **E4** | `amqp091-go` recovery: stop the broker for 60 seconds | Recovery is on by default or can be enabled; the state change is observed through `NotifyStateChange`; the topology is redeclared. What happens when recovery gives up is observed and written down. | An own reconnect loop with exit-and-restart (D5) | The reconciliation tests of P3 and the broker-wipe chaos scenario (P6) |
| **E5** | Go transport with `GetBody = nil`: a mock server reads the request and closes the connection without responding | The server receives the request exactly once | Disable keep-alives for the provider client | The "sent exactly once" provider client test (P1) |
| **E6** | Quorum queue with `x-consumer-timeout` = 24 h | The argument is accepted at declaration; with a short override, an unacknowledged delivery is not cancelled before the override and is cancelled after it | Cancel and resubscribe during long open circuits | The queue declaration of P1 and the resubscribe safety net of P3 |

**Files and packages.** `go.mod`, `Makefile`, `Dockerfile`, `docker-compose.yml`, `.golangci.yml`, `.github/workflows/ci.yml`, `cmd/*`, `internal/platform/config`, `experiments/` (temporary).

**Tests that must exist.** One trivial unit test, so that the `unit` job runs something; the six experiments.

**Exit criteria.**

```
git rev-parse --show-toplevel            # prints this repository's root
go build ./... && make lint && make test # all succeed
docker-compose up -d --wait postgres rabbitmq redis   # all healthy
docker-compose config | grep 'image:'    # no "latest", no floating tag
gh pr checks                             # CI green on the pull request
grep -c '^| E[1-6] ' docs/progress.md    # 6: one result row per experiment
```

**Pull requests.** `p0/skeleton`, then `p0/experiments`.

**Delegation.** Sonnet: Makefile, Dockerfile, Compose and CI YAML, `cmd` stubs, E2, E5. Opus: E1, E3, E4, E6. Main loop: the choice of exact image tags, reading the `amqp091-go` frame-dispatch code for U8, and the verdict on each experiment.

---

## P1: core path

**Goal.** One notification created through the API reaches `sent` at the provider through the real path (outbox, relay, broker, worker, rate limiter), with the relay-versus-worker race closed and tested.

**Implements.** D1, D2, D3, D7, D8 (queue argument), D10, D17 (signature), D19, D20, D21, D22, D23, D24, D28, D29, D33 (basic health endpoints), D44, D45, D46, D47, D51 (`load-smoke`), D52.

| ADR | Sections |
|---|---|
| [0001](../adr/0001-rabbitmq-as-dispatch-broker.md) | Delivery topology, priority, unroutable publishes, consumer timeout |
| [0002](../adr/0002-postgres-as-outbox-scheduler-and-retry-delay.md) | One scheduling key, relay cycle, pacing |
| [0003](../adr/0003-bounded-relay-buffer-and-sweepers.md) | §1 (HWM), §4 (lease sweeper, minimal) |
| [0004](../adr/0004-data-model-state-machine-and-fencing.md) | §1–4, §6–8; transitions 1, 2, 3, 4, 5, 6, 7 |
| [0005](../adr/0005-service-topology-and-status-events.md) | Processes, Compose, worker internals, connections |
| [0006](../adr/0006-api-contract-and-three-layer-idempotency.md) | §1–4 (single create and get only), §9 |
| [0007](../adr/0007-retry-policy-and-circuit-breaker.md) | §5 (GCRA), §6 (provider contract), §8, §11 |

**Scope notes.**

- **Migrations** create `notifications`, `notification_attempts`, `batches` and `idempotency_keys` with all constraints and indexes of ADR-0004. Template tables come in P4.
- **`decideRetry` has its final signature** (D17) and a minimal body: retry with a fixed delay while budget remains, otherwise fail. P3 replaces the body. Classification in P1 is three-way: `202` and other `2xx` → success; anything else that got a response → `retryable`; no response → `unknown`.
- **Transitions return events** from the start. P1 hands them to a publisher interface with a no-op implementation; the real publisher comes in P5.
- **The lease sweeper is minimal:** it marks the attempt `unknown` with `finished_at`, increments `failures` and applies `decideRetry`.
- **`mockprovider`** implements the provider contract, records arrival times and `Idempotency-Key` headers, has a configurable status code, and exposes the report endpoint. Fault injection grows in P3.
- **Compose** gains the application services with `restart: unless-stopped`, the `depends_on` chain, `stop_grace_period: 20s` for the worker, and the default `PROVIDER_URL` (D44). `mockprovider` is started by the test and mock configurations only.
- **README:** the quick start (day 1 of D57), using `docker-compose up`.
- **Manual check (U2):** send a few requests to the webhook.site URL and note which status codes it returns at its cap and after expiry, if they can be provoked; record them in `progress.md`.

**Files and packages.** `migrations/`, `internal/store`, `internal/domain`, `internal/queue`, `internal/relay`, `internal/worker`, `internal/ratelimit`, `internal/provider`, `internal/api`, `api/openapi.yaml`, `cmd/{api,relay,worker,migrate,mockprovider}`, `docker-compose.yml`, `README.md`, `.github/workflows/ci.yml` (`integration`, `migrations`, `e2e` jobs).

**Tests that must exist.**

| Guarantee | Test |
|---|---|
| A worker faster than the relay's commit waits and claims (D1) | `TestClaim_WaitsForRelayCommit` |
| A single notification on an idle system is sent (D1) | `TestE2E_IdleSingleNotificationIsSent` |
| A relay rollback leaves a row that is claimed directly | `TestClaim_DirectAfterRelayRollback` |
| Two claims, one winner | `TestClaim_TwoWorkersExactlyOne` |
| Expired at claim: `failed`, no attempt row, `attempt_count` unchanged | Claim test |
| Unroutable publish leaves the row `pending` (D2, U8) | `TestRelay_UnroutablePublishStaysPending`, run with `-count=100` |
| Priority on every message (D7) | `TestRelay_PrioritySetOnEveryMessage` |
| Two relays never publish the same row | `TestRelay_TwoRelaysNeverPublishSameRow` |
| Lane HWMs hold; `high` is visited first | Relay tests |
| Fencing | `TestFencing_LateWriteAffectsZeroRows` |
| Transition invariants and database constraints (D19) | Transition tests for transitions 1–7 |
| No one-second window above 100 (D24) | GCRA integration test; deterministic pacer test (fake clock, fixed jitter); unit test of the window computation |
| Pacer rules (D23) | Highest priority taken; slot kept on a zero-row claim |
| Provider contract (D28, D29) | Body has exactly three fields; `202` parsed; other `2xx` is success with a warning; sent exactly once; connections reused; redirect not followed |
| Migrations are reversible | `up → down → up` |

**Exit criteria.**

```
make test                                              # green
docker-compose up -d --wait                            # from a clean clone, no .env file
curl -fsS localhost:8080/healthz && curl -fsS localhost:8080/readyz
curl -fsS -X POST localhost:8080/v1/notifications \
  -H 'Content-Type: application/json' \
  -d '{"channel":"sms","recipient":"+905551234567","content":"hello","priority":"high"}'
curl -fsS localhost:8080/v1/notifications/<id>         # "status":"sent" within a few seconds
go test -tags integration -race -count=20 \
  -run 'TestClaim_WaitsForRelayCommit|TestE2E_IdleSingleNotificationIsSent' ./...
go test -tags integration -race -count=100 -run TestRelay_UnroutablePublishStaysPending ./...
make test-e2e                                          # includes load-smoke: no window above 100
gh pr checks                                           # CI green
```

One manual mutation check closes the phase: revert the claim guard to `status = 'queued'` locally and confirm that the two race tests fail; restore it. (`make verify-suite` automates this in P6.)

**Pull requests.** `p1/schema-and-store`, `p1/relay-worker-provider`, `p1/compose-e2e`.

**Delegation.** Main loop: writes or reviews line by line the claim CTE, the fenced final write, the relay transaction with returns and confirms, the pacer and the GCRA script. Opus: `store` with its transition tests, the relay, the worker, `mockprovider`, the race tests. Sonnet: migrations from the ADR-0004 tables, the OpenAPI skeleton for two endpoints, Compose service entries, the README quick start.

---

## P2: API completeness

**Goal.** Every endpoint of the Notification Management API behaves as specified, with all three idempotency layers.

**Implements.** D14 (documented in the API), D15, D16, D22, D34, D35, D36, D38. [ADR-0006](../adr/0006-api-contract-and-three-layer-idempotency.md) §1–9 and §11; [ADR-0004](../adr/0004-data-model-state-machine-and-fencing.md) transitions 9 and 11.

**Scope.** Batch create with partial success; `Idempotency-Key` on both create endpoints; `dedupe_key` with the D15 predicate in the index, in every `ON CONFLICT` and in the conflict look-up; cancel (single and batch) with `may_have_been_sent`; redrive; listing with single-valued filters and a keyset cursor (two `sqlc` queries); attempts and batch endpoints; per-channel validation and SMS segment counting; the 5 MB body limit; `X-Request-ID` validation; embedded Swagger UI; contract tests for every response.

**Files and packages.** `api/openapi.yaml`, `internal/api`, `internal/store`, `internal/domain` (validators, SMS counting, cursor), `migrations/` only if an index must change.

**Tests that must exist.**

| Area | Test |
|---|---|
| Cancel | `TestCancel_Idempotent`, `TestCancel_RacesWithClaim`, `TestCancel_DuringRelayPublish`, `TestCancel_WhileProcessingReturns409`; `may_have_been_sent` after an `unknown` attempt |
| Idempotency | `TestIdempotencyKey_ConcurrentSameKey`; expired key taken over; different request → `422`; the hash covers method and path; validation failures are not stored |
| Dedupe | Single-create conflict → `409` with `existing_id`; batch conflict through the partial index; duplicates inside one batch; the key is released only when cancelled with `attempt_count = 0` |
| Batch | 1,000 items accepted; 1,001 → `422 too_many_items`; partial success; all rejected → `422` and no batch |
| Redrive | `TestRedrive_VersusExpiry`; a future `expires_at` is kept; a past one requires a new value; `attempt_count` never reset |
| Listing | Pagination walk (500 rows in the same second, `limit=7`, each row exactly once); `EXPLAIN` check for both queries |
| Validation | GSM-7 septets and UCS-2 code units; escape sequences and surrogate pairs never split; `+905551234567` accepted (U3); email and push limits; fuzz tests for the segment counter and the cursor |
| Contract | Every success and error response validated against the spec |

**Exit criteria.**

```
make test                                   # green, including contract tests
make fuzz                                   # time-boxed, no crashers
curl -fsS localhost:8080/docs               # Swagger UI loads with the network disconnected
curl -fsS localhost:8080/openapi.yaml | head
make generate && git diff --exit-code       # generated code matches the spec
gh pr checks
```

The README gains its API examples (day 2 of D57).

**Pull requests.** `p2/batch-and-idempotency`, `p2/cancel-redrive-listing`, `p2/validation`.

**Delegation.** Main loop: review of the idempotency-key statement, of every use of the dedupe predicate, and of transitions 9 and 11. Opus: the idempotency flow and the batch insert with their concurrency tests. Sonnet: validators and SMS counting with table tests, listing queries and cursor, OpenAPI paths, Swagger UI embedding, contract-test wiring.

---

## P3: delivery robustness

**Goal.** Failures are classified by scope, retried with the right budget, and every recovery path of the design exists and is tested.

**Implements.** D2 (redeclaration on reconnection), D5, D6, D8 (resubscribe safety net), D12, D14, D17 (full), D18, D20, D23 (abort path), D25, D26, D27, D30, D31, D32.

| ADR | Sections |
|---|---|
| [0002](../adr/0002-postgres-as-outbox-scheduler-and-retry-delay.md) | Expiry |
| [0003](../adr/0003-bounded-relay-buffer-and-sweepers.md) | §3 (reconciliation, `requeue-stale`), §4, §5 |
| [0004](../adr/0004-data-model-state-machine-and-fencing.md) | §4 (final-write retry, transition 12), §5; transitions 5–8, 10, 12 in full |
| [0005](../adr/0005-service-topology-and-status-events.md) | Connections to RabbitMQ (exit when recovery gives up) |
| [0007](../adr/0007-retry-policy-and-circuit-breaker.md) | §1–4, §7–10 |

**Scope.** The full classification table with `unreachable` and `aborted`; `decideRetry` in full with the priority profiles and TTLs; `Retry-After` and consecutive-`429` handling; the in-process circuit breaker; the Redis-down fallback limiter; the expiry pass; reconciliation on (re)connection with the deferred trigger; the `requeue-stale` command; transition 12; the 60-second final-write retry; the lease sweeper in full; resubscribe on a server-side consumer cancellation; `mockprovider` fault injection (error rates, `429`, connection close, timeouts, redirects, malformed bodies, per channel).

**Files and packages.** `internal/domain` (classification, `decideRetry`), `internal/breaker`, `internal/ratelimit` (fallback), `internal/relay` (expiry, reconciliation, lease sweeper), `internal/worker`, `internal/queue`, `internal/store`, `cmd/relay` (`requeue-stale`), `cmd/mockprovider`.

**Tests that must exist.**

| Area | Test |
|---|---|
| Classification | Table test over every row of ADR-0007 §2, including the `WroteRequest` rules |
| `decideRetry` | Table test: budget, expiry shortcut, `Retry-After`, scope, fixed jitter values, delay within `[1 s, cap]` |
| Breaker | State machine with a fake clock: window rule, **five consecutive failures**, doubling open duration, `provider_fatal`, `throttled`, one probe in half-open |
| Abort path | A circuit that opens after the claim yields `aborted` and `retrying` with `failures` unchanged |
| Reconciliation | All `queued` rows return to `pending` on reconnection and are republished; a trigger inside the window is deferred; an empty broker has its topology before the first publish |
| Late success | `TestLateSuccess_AfterLeaseSweep`, `_AfterReconcile`, `_AfterExpiry`, `_AfterCancel`, `_AfterRedrive` |
| Final write | Retried while Postgres is unavailable; transition 12 tried when transition 4 affects zero rows |
| Lease sweeper | `TestLeaseSweeper_VersusFinalWrite`, `TestLeaseSweeper_TwoSweepersSkipLocked`; `finished_at` set |
| Expiry | `TestExpiry_VersusClaim`; bounded batches; an immediate next batch when one comes back full |
| Fallback limiter | Sending continues with Redis stopped; `limiter_fallback_active` is set |
| Throttling | `429` with and without `Retry-After`; doubling up to 2 minutes; `failures` unchanged |
| Consumer | A server-side cancellation is followed by a resubscribe |

**Exit criteria.**

```
make test                                   # green
make test-e2e                               # fault scenarios: 5xx → retries → sent; 401 → circuit open, failures = 0
docker-compose restart rabbitmq             # then: reconciliation_runs_total increased, every row ends sent
docker-compose stop redis                   # then: sending continues, limiter_fallback_active = 1
gh pr checks
```

**Pull requests.** `p3/classification-and-retry`, `p3/breaker-and-fallback`, `p3/recovery`.

**Delegation.** Main loop: line-by-line review of reconciliation, transition 12 and the final-write retry. Opus: the recovery paths and their integration tests, the breaker, the worker's abort path. Sonnet: classification and `decideRetry` table tests, `Retry-After` parsing with its fuzz test, `mockprovider` fault options.

---

## P4: templates and scheduling

**Goal.** Messages can be created from safe, versioned templates and scheduled for later delivery.

**Implements.** D40, D22, D3 (scheduling precision). [ADR-0008](../adr/0008-templates-and-scheduling.md) in full; [ADR-0006](../adr/0006-api-contract-and-three-layer-idempotency.md) §3 (`template`, `scheduled_at`, `expires_at`).

**Scope.** Template tables and endpoints; the allow-list walker with the function-name check and the one-template rule; `default` and `truncate`; `variables` and `optional_variables`; value types; the per-request parsed-template map; render at creation, then validate; scheduling rules (tolerance stored as `NULL`, 30-day horizon, `expires_at` rule).

**Files and packages.** `migrations/`, `internal/domain` (template validation), `internal/api`, `internal/store`, `api/openapi.yaml`.

**Tests that must exist.**

| Area | Test |
|---|---|
| Allow-list | Each of the 19 builtin functions rejected; `range`, `with`, `define`, `block`, `template`, variable declarations, chained fields and bare dot rejected; a source defining two templates rejected |
| Functions | `default` and `truncate` in pipeline form |
| Variables | Missing required → `422`; missing optional → empty string; undeclared extra → `422`; declared but unused → rejected at creation; string, number and boolean accepted; object and array → `422` |
| Limits | Body over 64 KB rejected; rendered output over the channel limit rejected (SMS segments after rendering) |
| Versions | Existing name → `409`; concurrent version creation gives consecutive versions; an inactive template is refused even with a pinned version |
| Escaping | Email variables are HTML-escaped; SMS and Push are not |
| Scheduling | A time within the 30-second tolerance is stored as `NULL`; older → `422`; beyond 30 days → `422`; `expires_at` rule; an end-to-end scheduled delivery |
| Fuzz | Template validation never panics |

**Exit criteria.**

```
make test && make fuzz
make test-e2e                               # template scenario and scheduled-delivery scenario
make generate && git diff --exit-code
gh pr checks
```

**Pull requests.** `p4/templates`, `p4/scheduling`.

**Delegation.** Main loop: review of the allow-list walker, which is the security boundary of this phase. Opus: the walker and its tests. Sonnet: template endpoints and queries, scheduling validators with table tests.

---

## P5: real-time and observability

**Goal.** A client can follow a notification in real time, and an operator can see queue depth, rates, latency, traces and health without reading the database.

**Implements.** D9, D11, D13, D33, D37, D39, D41, D42, D43. [ADR-0005](../adr/0005-service-topology-and-status-events.md) (status events, gateway); [ADR-0006](../adr/0006-api-contract-and-three-layer-idempotency.md) §10; [ADR-0009](../adr/0009-observability.md) in full.

**Scope.** The lazy, bounded event publisher in api, relay and worker; the gateway with subscription-then-snapshot, origin patterns, the bounded send queue and the bounded event queue; all metrics of ADR-0009 §1; the metrics summary with its two caches; the "Overview" dashboard and the alert rules; `slog` with the `Recipient` type, key-based and value-based redaction; correlation IDs; hybrid tracing; health endpoints in full, on the public port and in the OpenAPI spec; Prometheus scraping RabbitMQ per object; `make demo`.

**Files and packages.** `internal/queue` (event publisher), `internal/gateway`, `cmd/gateway`, `internal/platform` (logging, metrics, tracing), `internal/api` (summary, health), `deploy/` (Prometheus, Grafana, Jaeger), `docker-compose.yml`, `Makefile` (`demo`).

**Tests that must exist.**

| Area | Test |
|---|---|
| Events | All 12 transitions produce an event; a full buffer drops events without blocking the transition; the api starts and is ready with RabbitMQ stopped |
| WebSocket | A client that subscribes after creation receives a snapshot; a batch subscription yields one message per notification; a full send queue closes the connection; a disallowed origin is refused |
| Summary | Values on a known dataset, including every outcome and the `success_rate` definition; `backlog_due` cached separately |
| Health | `/healthz` and `/readyz` on the public port; readiness per service as in ADR-0009 §7 |
| Tracing | In-memory exporter: one trace for a first attempt, a new linked trace for a retry and for a scheduled delivery |
| PII | `Recipient` is masked however it is logged; **canary scan** over all service logs, exported spans and `/metrics` output in the e2e run |
| Dashboard and alerts | Smoke test: every panel query returns data after the e2e run; `promtool check rules` |

**Exit criteria.**

```
make test
make test-e2e                               # includes the canary scan and the dashboard smoke test
promtool check rules deploy/prometheus/alerts.yml
curl -fsS localhost:8080/v1/metrics/summary # lanes, attempts, success_rate, latencies
make demo                                   # probes the provider URL, sends at most 10 requests, shows events
gh pr checks
```

**Pull requests.** `p5/events-and-gateway`, `p5/metrics-and-summary`, `p5/logging-tracing-health`.

**Delegation.** Main loop: review of the gateway's subscribe-then-snapshot order and of the redaction paths. Opus: the gateway, tracing, the canary scan. Sonnet: metric registration, summary queries, the dashboard JSON, alert rules, the `demo` target.

---

## P6: quality

**Goal.** The evidence for every claim exists and is itself checked.

**Implements.** D49, D50, D51, D52, D53, D54, D55, D61 (the release run). [ADR-0010](../adr/0010-testing-and-ci.md) in full.

**Scope.**

- **Actor-pair matrix:** every cell of the matrix in ADR-0010 §1 maps to an existing test or is "n/a"; missing tests are written.
- **A test for every ledger decision:** walk the ledger and name the test for each `D…`; gaps are filled or recorded.
- **Chaos scenarios** scripted on the e2e stack: RabbitMQ wiped with its data, Postgres restart, low-traffic provider outage, worker `SIGKILL`, Redis stop, and one run with the per-channel worker override.
- **Load scenarios** with k6: 30,000 SMS rate test, strict-priority ordering, saturation report, channel isolation, phase 1 (1 M ingest), phase 2 (100,000 drain), listing and summary latency on the phase-1 dataset. Results go to `docs/load-test.md`; the README gains its load results (day 3 of D57).
- **Pacing margin:** the main loop reads the jitter measured by the rate scenario and confirms 98 msg/s or adjusts the configured rate (D24).
- **`make verify-suite`:** the six mutation patches of ADR-0010 §8, each failing a named test.
- **CI hardening:** every third-party action pinned by SHA; minimal permissions; `test-docker`, `migrations` and `compose-up` jobs (the last runs `docker-compose up` with the standalone binary); the scheduled, non-blocking `security` job.
- **Release workflow,** run once with a test tag.

**Files and packages.** `test/` (e2e, chaos, load scripts), `test/mutations/`, `Makefile`, `.github/workflows/{ci,release}.yml`, `docker-compose.override.*.yml`, `docs/load-test.md`, `docs/adr/0010-testing-and-ci.md` (matrix, if test names changed).

**Tests that must exist.** All of ADR-0010 §1, §3 and §8.

**Exit criteria.**

```
make test && make test-e2e && make test-docker
make load                                   # results written to docs/load-test.md
make verify-suite                           # 6 of 6 mutations make a named test fail
gh pr checks                                # every CI job green, including compose-up and test-docker
gh run list --workflow release.yml          # one successful run for the test tag
grep -rnE 'uses: [^@]+@v?[0-9]' .github/workflows   # no output: no action pinned by tag
```

**Pull requests.** `p6/matrix-and-chaos`, `p6/load`, `p6/verify-suite`, `p6/ci-and-release`.

**Delegation.** Main loop: reading the load and chaos results, the pacing-margin decision, checking that each mutation fails for the intended reason. Opus: chaos scripts, k6 scenarios, missing matrix tests. Sonnet: mutation patch files, CI and release YAML, the load-test write-up from the recorded numbers.

---

## P7: delivery

**Goal.** A reader who clones the repository can run it, understand it and verify its claims from the README alone.

**Implements.** D31 (burst figures in the README), D44 (URL renewal), D56, D57, D58, D61 (clean-clone rehearsal).

**Scope.**

- **README** in full: quick start with `docker-compose up` and the minimum Compose version; the webhook.site URL with its creation date and how to create a new one; API examples; `make demo`; the requirements map **with the evidence column filled**; interpretations of the assessment; limitations, including the largest burst per priority; load results; **"How AI was used"** and **"How AI output was verified"**.
- **`docs/architecture.md` regenerated** from the implemented system; the "planned" markers and the top note are removed.
- **`docs/future-work.md`** checked against everything dropped during implementation.
- **`docs/ai/`:** the curated `design-review-log.md` (AI mistakes caught, including the reviewer's own; critiques rejected, with reasons; findings reached independently, in their real order), the sanitised review prompt and sample sub-agent briefs. No raw transcripts, personal data or local paths. Its source material is in `.inputs/`.
- **Markdown link check** switched on in CI. It is enabled here, not in P6, because every ADR links to `docs/ai/design-review-log.md`, which exists only from this phase.
- **Clean-clone rehearsal** on another machine or under a fresh user account, following the README from its first line.
- **webhook.site URL renewed** right before submission: the Compose default and the README date are updated in one commit.

**Files and packages.** `README.md`, `docs/architecture.md`, `docs/future-work.md`, `docs/ai/*`, `docker-compose.yml` (provider URL), `.github/workflows/ci.yml` (link check).

**Tests that must exist.** The link check. No new product tests.

**Exit criteria.**

```
# on a machine or account that has never built the project:
git clone <repository> && cd <repository> && docker-compose up -d --wait
# then every command of the README quick start, unchanged, succeeds
make demo                                   # against the renewed webhook.site URL
grep -c 'planned' docs/architecture.md      # 0
git ls-files | grep -E '\.inputs/|\.pdf$|requirements\.md|review-findings\.md'   # no output
gh pr checks                                # includes the Markdown link check
```

**Pull requests.** `p7/readme`, `p7/architecture-and-review-log`, `p7/submission` (URL renewal, last).

**Delegation.** Main loop: the README's argument sections, the design review log, the regenerated architecture document, the rehearsal. Sonnet: filling the evidence column from the list of tests, link fixes, example commands checked against the running stack.

---

## 11. Unverified assumptions

Ledger section J lists statements that the design treats as facts but that nobody has run. Each is a hypothesis until the test below passes. A failed assumption is handled like a failed experiment: the fallback becomes the design, and the change is recorded.

| # | Assumption | Decision | Confirmed in | By |
|---|---|---|---|---|
| U1 | Go's transport replays a request carrying `Idempotency-Key` only when `GetBody` is set | D29 | P0, then P1 | E5; the "sent exactly once" provider client test |
| U2 | The status codes webhook.site returns at its request cap and after expiry | D44 | P1 (day 1) | Manual check, recorded in `progress.md`; whatever is returned is classified provider-scoped, with a classification test in P3 |
| U3 | `IsPossibleNumber` accepts `+905551234567` | D36 | P2 | Phone validation unit test |
| U4 | `NotifyStateChange` exposes the reconnect event that reconciliation needs | D5 | P0, then P3 | E4; the reconciliation integration tests; the broker-wipe chaos scenario in P6 |
| U5 | RabbitMQ 4.4 strict priority behaves as documented, including the default priority of 4 | D7 | P0, then P1 | E1; `TestRelay_PrioritySetOnEveryMessage`; the strict-priority load test in P6 |
| U6 | strict-server works with the standard-library router | D48 | P0 | E2 |
| U7 | `x-consumer-timeout` behaves as documented | D8 | P0 | E6 |
| U8 | `amqp091-go` hands over a `basic.return` before the matching publisher confirm | D2 | P0 (reading), P1 (test) | Reading the library's frame-dispatch code; `TestRelay_UnroutablePublishStaysPending` with `-count=100` |

A note on U8: a repeated test can make a violation unlikely, but only the library's code can show that the ordering is guaranteed. If it is not, the residual case is a row marked `queued` with no message, which reconciliation, `requeue-stale` and the oldest-queued-age alert already cover; the README then lists it as a limitation.

## 12. Fallback order and the "never drop" list

Scope is cut only when correctness cannot be shown by tests within the time, or when a component adds risk without matching value (G2). When time runs short, items are dropped **from the top of this list downwards**, and each drop is recorded in `docs/future-work.md` in the same commit.

| Order | Dropped first | What remains |
|---|---|---|
| 1 | Phase-1 load scenario (1 M ingest) and the latency measurements on its dataset | Rate, isolation, priority and 100,000-drain scenarios |
| 2 | Saturation report; the per-channel-override chaos run; the Postgres-restart chaos scenario | Broker wipe, worker `SIGKILL`, low-traffic outage, Redis stop |
| 3 | Release workflow (CD) | CI in full |
| 4 | Dashboard panels beyond the required figures; alert rules beyond relay down and poison depth | One dashboard, smoke-tested |
| 5 | Template conveniences: preview, activate and deactivate, `optional_variables` | Create, version, render with the allow-list |
| 6 | Redrive | Failed notifications stay failed; resubmission needs a new `dedupe_key` |
| 7 | The hybrid trace rule | Linked traces for every delivery |
| 8 | WebSocket gateway (bonus, R28) | Status through `GET` |
| 9 | Templates (bonus, R27) | Direct `content` only |
| 10 | Distributed tracing (bonus, R29) | Correlation IDs in logs |

**Never dropped,** whatever the time:

- Every non-bonus requirement: R1–R24 and R31–R36.
- The claim guard of D1 and its two race tests; `mandatory` publishing and its test (D2).
- The rate-limit evidence: `load-smoke` in CI and the 30,000-message scenario (D24, D51).
- webhook.site as the default provider and the exact provider request format (D28, D44).
- `docker-compose up` from a clean clone without a `.env` file; `make test` as the single test command.
- No recipient or content in logs, spans or metrics (D42).
- The README with its requirements map, interpretations, limitations and the two sections on AI use (D57).
- A clean, linear commit history through pull requests (D59), and `future-work.md` updated with every drop.

## 13. Risks

| # | Risk | Likelihood | Effect | Mitigation |
|---|---|---|---|---|
| 1 | A RabbitMQ 4.4 behaviour differs from its documentation (strict priority, default priority 4, consumer timeout) | Medium | The dispatch design changes | E1 and E6 in the first hour; fallbacks are pre-agreed |
| 2 | `amqp091-go` recovery does not expose what reconciliation needs, or does not redeclare the topology | Medium | D5 needs an own reconnect loop | E4; the fallback is exit-and-restart, which also reconciles |
| 3 | A return can arrive after its confirm (U8) | Low to medium | A row is `queued` without a message | Code reading in P0, the repeated test in P1, and the existing recovery paths |
| 4 | Timing jitter is larger than the two-message margin | Medium | A one-second window exceeds 100 | `load-smoke` from day 1 measures it; the rate is configuration |
| 5 | The webhook.site URL reaches its cap or expires during development or evaluation | High | The default path shows failures | `make demo` sends at most 10 requests; tests use `mockprovider`; the URL is renewed right before submission; the README shows its date |
| 6 | Concurrency code written by a sub-agent is subtly wrong | Medium | The defect class the review found | Line-by-line review by the main loop; the actor-pair matrix; `make verify-suite` |
| 7 | Flaky integration tests | Medium | Lost time, a red `main` | No sleeps, millisecond durations, `-count` runs on the race tests before merging |
| 8 | The plan is too large for three days | Medium | An unfinished submission | The day-1 gate; the fallback order; bonus features are cut before evidence is |
| 9 | testcontainers inside a container does not work on macOS | Medium | `make test-docker` cannot be offered | E3; fallback is to document `make test` |
| 10 | The Postgres 18 image's data path differs from earlier versions | High if unnoticed | Data is lost on container recreation | Called out in P0; checked by recreating the container |
| 11 | Links to `docs/ai/design-review-log.md` are broken until P7 | Certain | A link check would fail | The link check is enabled in P7 |
| 12 | A session starts without the context of earlier ones | Certain | Rework or contradicting decisions | `docs/progress.md` is read first and written last in every session |
