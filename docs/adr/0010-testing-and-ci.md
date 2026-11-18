# ADR-0010: Testing strategy and CI/CD

- **Status:** Accepted
- **Date:** 2026-11-18
- **Related:** all previous ADRs

## Context

The deliverables include a **test suite runnable with a single command** (R36), **GitHub Actions CI/CD** as a bonus (R30), and a repository with a clean commit history (R31).

The design makes specific claims that ordinary unit tests cannot verify:

- a message is never dropped when a worker is faster than the relay's commit;
- concurrent claims never both succeed;
- a late worker cannot overwrite newer state;
- no one-second window exceeds 100 sends per channel;
- `high` priority is delivered first under a `low` backlog;
- a provider outage in one channel does not affect another;
- the system loses nothing when RabbitMQ loses its data or a worker dies.

Each claim needs a test at the level where it can actually fail, and the most important ones need numbers. Tests of races between two instances of the *same* actor are not enough: the most serious defect found in this design was a race between two *different* actors, the relay and the worker.

## Decision

### 1. Test layers

| Layer | Tooling | Proves | Examples |
|---|---|---|---|
| **Unit** | `go test -race`, table-driven tests | Pure rules in `domain/` and validators | Every allowed transition and the rejection of every disallowed one; `decideRetry` (budget, expiry shortcut, `Retry-After`, scope, fixed jitter values); back-off always within `[1 s, cap]`; result → (outcome, scope) classification, including the `WroteRequest` rules; the circuit breaker's state machine with a fake clock; a deterministic pacer test (fake clock, fixed jitter); the one-second window computation used by the load tests; `Retry-After` parsing; cursor round-trips; SMS encoding and segments; phone validation including `+905551234567`; template allow-list and pipeline argument order; PII masking formats |
| **Fuzz** | Go native fuzzing | No panics on hostile input | SMS segment counter, template validation, cursor decoding, `Retry-After` parsing |
| **Integration** | `testcontainers-go` with real Postgres, RabbitMQ and Redis | Concurrency and storage guarantees | The actor-pair matrix and the per-area tests below |
| **Contract** | `kin-openapi` response validation | The implementation matches `openapi.yaml` | Every API test response is validated against the spec, **including error responses** (`400`, `404`, `409`, `413`, `422`, `503`) against the `application/problem+json` schema |
| **End-to-end** | Compose stack and `mockprovider` | Whole flows | Create → subscribe → snapshot and events → `sent`; injected faults → retries and circuit opening; scheduled delivery; template rendering; batch partial success; health endpoints; trace propagation; the PII canary scan; the dashboard smoke test; `load-smoke` |
| **Chaos** | Scripted scenarios on the e2e stack | The dependency failure matrix ([ADR-0007](0007-retry-policy-and-circuit-breaker.md) §10) | Below |
| **Load** | k6 against `mockprovider` | The design's quantitative claims | Section 3 |

#### Actor-pair matrix

Integration tests are organised by **pairs of actors** acting on the same notification. Every cell names a test or says "n/a"; a cell may not be left empty. The matrix is symmetric, so only the upper half is filled and the lower half is marked "—". The actors are the api (cancel, redrive), the relay (publish, expiry, reconciliation), the worker (claim, final write), the lease sweeper, and the late worker (a worker whose lease has expired).

| | api | relay | worker | lease sweeper | late worker |
|---|---|---|---|---|---|
| **api** | `TestCancel_Idempotent`; `TestIdempotencyKey_ConcurrentSameKey` | `TestCancel_DuringRelayPublish`; `TestRedrive_VersusExpiry` | `TestCancel_RacesWithClaim` (never both) | `TestCancel_WhileProcessingReturns409` | `TestLateSuccess_AfterCancel` (`cancelled → sent`); `TestLateSuccess_AfterRedrive` |
| **relay** | — | `TestRelay_TwoRelaysNeverPublishSameRow` | **`TestClaim_WaitsForRelayCommit`**; **`TestE2E_IdleSingleNotificationIsSent`**; `TestClaim_DirectAfterRelayRollback`; `TestExpiry_VersusClaim` | n/a (disjoint states) | `TestLateSuccess_AfterReconcile`; `TestLateSuccess_AfterExpiry` (`failed → sent`) |
| **worker** | — | — | `TestClaim_TwoWorkersExactlyOne` | `TestLeaseSweeper_VersusFinalWrite` | `TestFencing_LateWriteAffectsZeroRows` |
| **lease sweeper** | — | — | — | `TestLeaseSweeper_TwoSweepersSkipLocked` | `TestLateSuccess_AfterLeaseSweep` |
| **late worker** | — | — | — | — | n/a (one late worker per attempt) |

The two tests in bold are the ones that would have caught the dropped-message defect: a single notification on an idle system must reach `sent`, and a claim issued while the relay's transaction is still open must wait and then succeed.

#### Other integration tests, by area

| Area | Test |
|---|---|
| Transitions | Every one of the 12 transitions increments `version`, sets `updated_at`, returns an event, and **leaves immutable columns untouched**; the database rejects rows that violate the status invariants ([ADR-0004](0004-data-model-state-machine-and-fencing.md) §6) |
| Claim | An expired row is failed at claim without an attempt row and without incrementing `attempt_count`; a `retrying` row that is not yet due is not claimed |
| Publish | `TestRelay_UnroutablePublishStaysPending`: with no binding, the row stays `pending`. **Repeated 100 times**, because it rests on the client's ordering of returns and confirms ([ADR-0001](0001-rabbitmq-as-dispatch-broker.md)). `TestRelay_PrioritySetOnEveryMessage`: every published message carries `6 − db_priority`. Lane HWMs hold. |
| `requeue-stale` | By default only `queued` rows older than the threshold return to `pending`; `--all` takes every `queued` row |
| Cancel | A cancel after an `unknown` attempt reports `may_have_been_sent`; a batch cancel reports the `cancelled`, `not_cancellable` and `may_have_been_sent` counts ([ADR-0006](0006-api-contract-and-three-layer-idempotency.md) §7) |
| Reconciliation | On reconnection all `queued` rows return to `pending` and are republished; a trigger inside the 60-second window is deferred, not skipped; a broker that comes back empty has its topology again before the first publish |
| Final write | With Postgres unavailable, the final write is retried and lands; transition 12 is tried when transition 4 affects zero rows |
| Redrive | Keeps a future `expires_at`; requires a new one when it has passed; never resets `attempt_count` |
| Idempotency | An expired key is taken over; the hash covers method and path; a `dedupe_key` conflict through the partial-index `ON CONFLICT`; a key is released only when cancelled without an attempt; duplicates within a batch ([ADR-0006](0006-api-contract-and-three-layer-idempotency.md)) |
| Listing | **Pagination walk:** 500 rows created in the same second, `limit=7`, every row returned exactly once. An `EXPLAIN` check asserts that both listing queries use their index. |
| Outbox | With RabbitMQ stopped, the api keeps accepting; after restart, everything is published |
| Rate limiter | GCRA keeps every one-second window at or below 100 across several concurrent clients; the script returns a relative wait ([ADR-0007](0007-retry-policy-and-circuit-breaker.md) §5) |
| Connection epoch | `TestWorker_ConnectionLossWithBufferedMessages`: the connection to RabbitMQ is broken while messages sit in the worker's local buffer. Every notification ends `sent`, and no ack from an older connection epoch reaches the broker ([ADR-0005](0005-service-topology-and-status-events.md), D76). |
| Pacer | The highest-priority buffered message is taken; the slot is kept when a claim affects zero rows; a circuit that opens after the claim yields `aborted` and `retrying` without budget |
| Provider client | The request body has exactly three fields (also asserted end to end on `mockprovider`'s report); a `202` body is parsed; another `2xx` is success and increments `provider_unexpected_2xx_total`; a redirect is not followed; the request is sent exactly once when the server closes the connection; connections are reused (`httptrace.GotConnInfo.Reused`) |
| Templates | Builtin functions and multi-template sources are rejected; concurrent version creation produces consecutive versions ([ADR-0008](0008-templates-and-scheduling.md)) |
| WebSocket | A client that subscribes after creation receives a snapshot; a full send queue closes the connection |
| Observability | Trace propagation with an in-memory exporter (one trace for a first attempt, a linked trace for a retry); metrics summary values on a known dataset; health endpoints on the public port ([ADR-0009](0009-observability.md)) |

**Every decision in the [decision ledger](../ai/decision-ledger.md) has at least one test.**

#### Chaos scenarios

| Scenario | Expected result |
|---|---|
| **RabbitMQ wiped with its data and restarted** | Every notification ends `sent`. This is the test of reconciliation and of topology redeclaration. |
| Postgres restarted during sends | Nothing is lost; duplicates at the provider are counted and **reported**, not failed |
| Low-traffic provider outage: three `high` notifications, provider down for 60 seconds | All three end `sent` with `failures = 0` |
| Worker killed with `SIGKILL` mid-send | The lease sweeper recovers its rows; duplicates are reported |
| Redis stopped | Sending continues on the fallback limiter; the fallback alert fires |
| Per-channel worker deployment (Compose override) | One scenario runs with a worker per channel, to keep that layout working |

**Determinism.**

- `domain/` takes a `Clock` interface and receives jitter as an argument.
- **All durations are configurable** (lease, tick, sweep intervals, reconciliation window, breaker durations, timeouts) and are set to milliseconds in tests. Times computed in SQL from `now()` cannot be controlled by an injected clock, so tests shorten the durations instead; rows are back-dated only for expiry and lease edge cases.
- Tests never use `time.Sleep` to wait for asynchronous effects; an `eventually(condition, timeout)` helper polls instead. Sleep-based waiting is the main source of flaky tests.

### 2. `mockprovider`

A separate binary (`cmd/mockprovider`) with an admin API for fault injection. It implements the provider contract of [ADR-0007](0007-retry-policy-and-circuit-breaker.md) §6 and is the provider for **all automated tests and load tests**. It is not the default provider of the Compose stack; that is webhook.site ([ADR-0005](0005-service-topology-and-status-events.md)).

| Capability | Purpose |
|---|---|
| Configurable error rate, latency distribution and specific status codes, per channel | Retry, circuit-breaker and channel-isolation scenarios |
| `429` with and without `Retry-After`; redirects | Throttling and redirect paths |
| Closing the connection after reading the request; timeouts; malformed `2xx` bodies | `unknown` outcomes and anomaly handling |
| **Records the arrival time and the `Idempotency-Key` header of every request** | Rate-limit verification and **duplicate detection** |
| Records the **field names** of every request body, never the values | Proof that the provider receives exactly `to`, `channel` and `content` (R23) |
| Report endpoint | Counts per key and per channel; the sorted arrival timestamps; the set of body field names seen |

### 3. Load tests: evidence for the claims

| Claim | Scenario | Pass criterion |
|---|---|---|
| The rate limit holds | **30,000 SMS** | **Per channel,** no half-open one-second window `[t, t + 1 s)` over the sorted arrival timestamps at `mockprovider` contains more than 100 arrivals. The window computation has its own unit test. |
| Priority is strict | `high` and `low` submitted together into a backlog | Every `high` message arrives before the `low` messages submitted with it, within the tolerance of the prefetch buffer |
| Priority under saturation | A `low` backlog with sustained `high` traffic | **Reported, not gated:** `high` end-to-end p95 and how long `low` waits |
| **Channels are isolated** | `mockprovider` fails 100 % of SMS | Email throughput is unaffected. This is the evidence for per-channel pacers inside one worker process. |
| Bursts are absorbed (phase 1: ingestion) | 1 M messages (1,000 batches) across the three channels | All accepted without loss; RabbitMQ depth stays flat while the Postgres backlog grows |
| Nothing is lost or duplicated (phase 2: full drain) | 100,000 messages across the three channels | Every notification `sent`, and **each `Idempotency-Key` received exactly once** at `mockprovider` |
| Reads stay fast on a large table | Listing and metrics summary on the phase-1 dataset | Latency reported |

- **`load-smoke`** is a 60-second version of the rate scenario that runs **in the end-to-end CI job from day 1**. It measures the jitter from which the pacing margin is derived ([ADR-0007](0007-retry-policy-and-circuit-breaker.md) §5).
- **Why the burst test has two phases:** fully draining 1 M messages takes about 2.8 hours per channel, which is impractical for every run. Phase 1 proves the buffering behaviour at full scale; phase 2 proves completeness on a dataset that can be drained.
- **Duplicates are measured at the provider, not in the database.** `id` is the primary key, so `COUNT(DISTINCT id) = COUNT(*)` is always true and proves nothing. Duplicate *sends* happen at the provider, which is why `mockprovider` counts idempotency keys.
- Chaos runs report their expected at-least-once duplicates separately. This is an honest measurement of the delivery semantics, not a pass/fail criterion.
- Trace sampling is set to about 1 % during load tests ([ADR-0009](0009-observability.md)).
- **Results are published in `docs/load-test.md`** with figures and Grafana screenshots. webhook.site is never used for load.

### 4. Commands

| Command | Runs | Requires |
|---|---|---|
| **`make test`** | Unit, fuzz seed corpus, integration and contract tests, all with `-race` | Go and Docker |
| `make up-mock` | Starts the stack with the mock override, `docker-compose.mock.yml`, which adds `mockprovider` and points `PROVIDER_URL` at it ([ADR-0005](0005-service-topology-and-status-events.md)) | Docker |
| `make test-e2e` | Starts the Compose stack with the mock override; runs the e2e and chaos scenarios, `load-smoke`, the canary scan and the dashboard smoke test | Docker |
| `make load` | k6 scenarios against the Compose stack | Docker |
| `make test-docker` | Same as `make test`, inside a Go container | Docker only |
| `make verify-suite` | The mutation patches of section 8 | Go and Docker |
| `make demo` | A probe of the provider URL, then at most 10 requests through the running stack | Docker |
| `make fuzz` | Extended fuzzing (optional, time-boxed) | Go |

- Integration tests use the `//go:build integration` tag, so `go test ./...` in an editor stays fast; `make test` enables the tag.
- **`make test-docker`** mounts the Docker socket and the source tree **at the same path as on the host** (`-v $PWD:$PWD -w $PWD`), and sets `--add-host=host.docker.internal:host-gateway`, `TESTCONTAINERS_HOST_OVERRIDE=host.docker.internal` and `TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock`. Inside a container, `localhost` refers to the test container itself, and the gateway address that testcontainers detects on its own is not reachable on Docker Desktop.

### 5. CI: `.github/workflows/ci.yml`

Runs on pushes and pull requests. A new push to the same branch cancels the running workflow. Go modules and Docker layers are cached.

| Job | Contents |
|---|---|
| `lint` | `golangci-lint` v2 (configuration in the v2 format) with `govet`, `staticcheck`, `errcheck`, `gosec`, `bodyclose`, `noctx`, `contextcheck`, `sqlclosecheck`, `revive`; `gofumpt` as a formatter; a Markdown link check |
| `codegen-drift` | Runs `sqlc generate` and `oapi-codegen`, then `git diff --exit-code`: generated code must match its sources. The OpenAPI spec is also linted. |
| `unit` | `go test -race` with a coverage profile, uploaded as an artifact |
| `integration` | testcontainers on the GitHub-hosted runner, which has Docker |
| `test-docker` | Runs `make test-docker`, **proving that the Go-less path works** rather than only claiming it |
| `migrations` | `up → down → up` on an empty Postgres: every migration is reversible |
| `compose-up` | Runs the assessment's command, **`docker-compose up`**, once with the standalone binary and waits for the services to be healthy |
| `e2e` | Starts the stack, then the e2e suite with `load-smoke`; **all service logs are uploaded if it fails** |
| `security` | `govulncheck`, and a `trivy` scan of the built image. **Scheduled and non-blocking:** a newly published advisory must not turn `main` red. |

- **Third-party actions are pinned to full commit SHAs,** not tags.
- Each workflow declares minimal `permissions`.
- **Dependabot is off until after the evaluation,** so that the history and the pinned versions do not change while the repository is being read.
- Coverage is reported but not gated: coverage gates tend to be met with low-value tests. The `domain/` package is expected to be covered almost completely by its table tests.

### 6. CD: `.github/workflows/release.yml`

- Triggered by a `v*` tag.
- Builds the image for **`linux/amd64` and `linux/arm64`**, so it also runs natively on Apple Silicon machines, and pushes it to GHCR, tagged with the version and the commit SHA and carrying OCI labels.
- With no deployment target in scope, a versioned, published image is the delivery artifact.
- **The workflow is run once with a test tag before submission,** so that its first execution is not after the fact.

### 7. Commit history and pull requests

- Work happens on **a branch per phase or feature**. The agent pushes the branch and opens the pull request with `gh`; the author reviews and merges. `main` is protected: no direct pushes and no force pushes.
- Pull requests are merged with **"Rebase and merge"**, the only method enabled in the repository settings, so the curated commits form a linear history.
- Commits follow **Conventional Commits** (`feat:`, `fix:`, `test:`, `docs:`, `chore:`) and carry a `Co-Authored-By` trailer for the AI.
- Each commit is one logical change that builds and passes tests: for example, the schema, then the repository with its transition tests, then the relay.
- Sub-agents never commit or push; the main loop reviews every diff before committing.
- When a feature is dropped, `future-work.md` is updated in the same commit.
- Before submission, the repository is cloned on another machine (or under a fresh user account) and the README's quick start is followed from the first line.

### 8. Verifying the suite: `make verify-suite`

A test suite that passes proves little unless it can also fail. `make verify-suite` applies **mutation patches** in a temporary worktree, one at a time, and requires that each one makes a **named test fail** (a compile error does not count):

| Mutation | Must be caught by |
|---|---|
| Claim guard reverted to `status = 'queued'` | The idle-notification and wait-for-commit tests |
| Fencing condition removed from the final write | The late-write test |
| Pacer set to exactly 100 msg/s | The deterministic pacer test (fake clock, fixed jitter) |
| Redirect following re-enabled | The redirect test |
| Builtin template functions allowed | The template allow-list test |
| `mandatory` removed from the publish | The unroutable-publish test |

## Alternatives considered

| Alternative | Why it was rejected |
|---|---|
| **Integration tests organised by component only** | Every race test then pits an actor against itself. None would have caught a worker overtaking the relay's commit. The matrix makes a missing pair visible as an empty cell. |
| **A rate criterion of "no window above 100" while pacing at exactly 100** | Unattainable: any jitter fails it. The criterion is kept and the pacing rate lowered ([ADR-0007](0007-retry-policy-and-circuit-breaker.md) §5). |
| **A priority test with `high` at a few per cent of capacity** | Passes with or without priority, because `high` never has to overtake anything. |
| **A "relay leader killed" chaos scenario** | There is no leader ([ADR-0005](0005-service-topology-and-status-events.md)). The broker-wipe scenario tests the recovery path that matters. |
| **Blocking security scans on every push** | A gate that changes with the date of the run makes `main` red for reasons unrelated to the change. |
| **Dependabot during the evaluation** | Automated pull requests and version bumps while the repository is being assessed add noise and risk for no benefit in that window. |
| **Action tags instead of commit SHAs** | Tags can be moved; widely used actions have had their tags hijacked. |
| **Mocked database in tests** (`sqlmock`) | Cannot verify `SKIP LOCKED`, row locks, partial-index `ON CONFLICT` or transaction isolation, which are exactly the guarantees that matter. |
| **A shared Compose stack for integration tests** | Shared state between tests and manual setup; testcontainers gives each suite a clean, isolated environment. |
| **`time.Sleep` in tests** | Flaky and slow. |
| **Duplicate checks in the database** | Meaningless, because `id` is a primary key (section 3). |
| **A single 1 M full-drain run** | About 2.8 hours per run. |
| **Load testing against webhook.site** | Free URLs have a request cap; results would measure webhook.site, not the system. |
| **E2E and load inside `make test`** | Would make the single test command slow and dependent on the whole stack. |
| **A hard coverage threshold** | Encourages tests written for the number. |

## Consequences

**Positive**

- Each guarantee from the earlier ADRs has a test at the level where it could fail, including races between different actors.
- The most important tests are themselves tested: a mutation that removes a guarantee must turn a named test red.
- The quantitative claims are backed by published measurements, not assertions.
- A reviewer can run everything with one command, with or without Go installed, and CI proves both paths as well as the assessment's own `docker-compose up`.
- Generated code, migrations and the API contract cannot drift silently.

**Negative / costs**

- Integration and e2e tests need Docker and take minutes, not seconds.
- `mockprovider` is additional code to build and maintain.
- The full load scenarios take longer than a CI job allows; they are run manually and only `load-smoke` runs on every push.
- The mutation patches must be kept in step with the code they patch.
- The CI pipeline is broad, and its jobs and caches need maintenance.

## Review history

Changes made after the design review, recorded in the [design review log](../ai/design-review-log.md):

- Integration tests were reorganised into an actor-pair matrix, because the earlier tests covered only same-actor races and none would have caught the relay-versus-worker defect. (D49)
- The rate scenario became 30,000 SMS with a per-channel half-open window, and `load-smoke` runs in CI from day 1; the previous criterion could not be met at exactly 100 msg/s. (D51, D24)
- Chaos scenarios gained a broker data wipe, a Postgres restart and a low-traffic outage, and lost the leader scenario. (D50)
- `make verify-suite` was added so that removing a guarantee must make a named test fail. (D54)
- CI was hardened (actions pinned by SHA, minimal permissions, non-blocking scheduled scans, Dependabot off, a `docker-compose up` job), and the Git workflow with pull requests was defined. (D53, D59, D61)
- After the first-hour experiments (P0), `TestRelay_PrioritySetOnEveryMessage` checks the mapping `6 − db_priority`, and `TestWorker_ConnectionLossWithBufferedMessages` was added for the connection epoch. (D7, D76)
