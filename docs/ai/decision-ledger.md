# Decision ledger (v3, after the second ledger review)

The authoritative list of decisions taken during the design review (12 review turns plus the final report). **Where this ledger and an ADR disagree, this ledger wins** until the ADRs have been revised. After the revision, the ADRs are authoritative and this file becomes the source for `docs/ai/design-review-log.md`.

Sources are review-turn findings: `T2 F1` means Turn 2, finding F1. "Author" means a decision taken in discussion without a specific finding.

## Guiding rules

| ID | Rule |
|---|---|
| G1 | **Comply strictly with every explicit instruction in the assessment PDF.** Where the PDF is ambiguous, document the interpretation; never contradict the text. |
| G2 | Cut scope only when correctness cannot be demonstrated by tests within 3 days, or when a component adds risk without a matching signal for the role. |
| G3 | All repository content is in English. |
| G4 | Capacity figures: 3 channels × 98 msg/s = **294 msg/s**, about **25.4 M per day**. |
| G5 | The project lives in **its own repository**, outside any other Git working tree. The assessment PDF, `requirements.md` (which quotes it) and `review-findings.md` (raw review material) are **not committed**: they live in the repository's `.inputs/` folder, which is excluded through `.git/info/exclude` (local) and `.gitignore` (committed). The README's requirements map paraphrases each requirement under its R-ID. |

## A. Messaging and dispatch

| ID | Decision | Affects | Source |
|---|---|---|---|
| D1 | **Claim guard widened:** `WHERE id = $1 AND status IN ('pending','retrying','queued') AND due_at <= now()`. A worker that arrives before the relay commits waits on the relay's row lock and claims after commit. The claim is **one CTE statement** (UPDATE … RETURNING, then INSERT of the `in_flight` attempt; the expiry branch sets `failed/expired` without incrementing `attempt_count` and without an attempt row). Zero rows → ack and drop. | 0002, 0004, 0005 | T0, T2 F1, T4 F1 |
| D2 | Publish with **`mandatory=true`**. A `basic.return` means the row is **not** marked `queued`. Returns are buffered per batch and matched by `MessageId` after confirms. Every role declares the topology idempotently **at start-up and on every (re)connection**, so a broker that comes back empty (D50) has its exchanges, queues and bindings again before the first publish. Implementation: the return channel (`NotifyReturn`) is buffered, and every confirm is processed only after a non-blocking drain of pending returns; this relies on the client enqueuing a `basic.return` before its matching ack (U8). | 0001, 0002, 0005 | T1 F4, T7, ledger reviews |
| D3 | **LISTEN/NOTIFY removed.** The relay runs on a fixed **100 ms ticker** and processes `high` first in each channel. Scheduling precision ≈ 100 ms. | 0002, 0005, 0006, 0008, architecture | T2 P1, F2 |
| D4 | **Single relay** with `restart: unless-stopped`. No leader election or advisory lock (moved to future work). Correctness never depended on leadership (`SKIP LOCKED`, conditional transitions). | 0003, 0005, 0009, architecture | T5 F2 |
| D5 | **Periodic stale-queued sweeper removed.** Replaced by **reconciliation on every relay (re)connection to RabbitMQ**: all `queued` rows return to `pending` and are republished; duplicates are dropped by the claim guard. Hooked to amqp091-go's recovery state notifications (`NotifyStateChange`); runs at most once per 60 s, and **a trigger inside that window is deferred, not skipped** (it runs once when the window ends). **If the library's connection recovery gives up, the relay, worker and gateway processes exit with a non-zero code** (the api does not: its publisher is optional, D11, so it logs, keeps serving and retries lazily) and the Compose restart policy (`restart: unless-stopped`, used by every application service) starts it again, which also runs reconciliation. Plus a manual `requeue-stale` command and the `lane_oldest_queued_age_seconds` alert. **The relay does not use Redis.** | 0003, 0005, 0009, 0010 | T3 F1–F2, T3 P2, T6, T7 |
| D6 | Custom `x-delivery-limit` removed (broker default, 20). DLX `notifications.dlx` → `dispatch.{channel}.poison` kept as a safety net. The real poison defence is the `failures` budget plus the lease sweeper (the worker acks right after claim). A dead-lettered row stays `queued` and is recovered by reconciliation or the manual command. Poison queues: classic, no consumer, depth is a metric. | 0001, 0004, 0007, architecture | T1 F1, N3, T7 |
| D7 | **Strict priority using RabbitMQ 4.3+ built-in quorum-queue priorities.** One dispatch queue per channel: `dispatch.sms`, `dispatch.email`, `dispatch.push`, bound to `notifications.dispatch` with routing key `{channel}`. AMQP priority = `2 − db_priority`, **set explicitly on every message** (a message without priority is treated as 4) and covered by a test. RabbitMQ pinned to **4.4.x**. The pacer picks the highest-priority message in its local prefetch buffer. Per-lane HWMs stay in Postgres. The weighted dispatcher and the 9 lane queues are removed. Limitations: `low` can wait during sustained `high` traffic; a message returned to the queue (nack on `SIGTERM`, connection loss) loses its priority position. | 0001, 0003, 0005, 0007, 0009, 0010, architecture | Author (G1), T1 F2, T1 P1, T12 |
| D8 | **Consumer timeout:** the dispatch queues are declared with a long `x-consumer-timeout` (24 h), so prefetched messages can wait unacked while a circuit is open without the broker cancelling the consumer. There is no cancel-and-resubscribe logic. As a safety net, a server-side consumer cancellation is handled by resubscribing. The argument's behaviour is verified in the first-hour experiments (section I). | 0001, 0007 | T1 F6, ledger review |
| D9 | RabbitMQ in Compose: **fixed hostname and a named volume**; `rabbitmq_prometheus` explicitly enabled; Prometheus scrapes the per-object/detailed endpoint, not the aggregated default. | 0005, 0009 | T3 F1, T9 F2 |
| D10 | Relay transaction: at most **200 rows per lane per cycle**, publisher-confirm timeout **5 s**; no transaction is started if the AMQP channel is down. Publishes still awaiting a confirm when the connection drops count as failed, and the transaction rolls back. Holding row locks across the publish is the documented exception to "no locks across network calls" (and it is what makes D1 work). | 0002, 0004 | T2 F4 |
| D11 | Event name **`notification.status_changed`**, routing key `status.{channel}.{status}`. All event publishers (api, relay, worker) connect lazily, use a bounded buffer, drop events when it is full, and never affect start-up or readiness. | 0001, 0002, 0005, 0006 | T2 N2, T5 F1 |
| D12 | Expiry pass (transition 10): bounded batches of 1,000 with `FOR UPDATE SKIP LOCKED`, every second, repeated immediately while a batch comes back full. Covers `pending`, `retrying` and `queued`. Workers also check expiry at claim (D1). | 0002 | T2 F3 |
| D13 | Tracing, **hybrid rule:** for the first attempt of a non-scheduled notification, the relay's publish span is a **child** of the stored `trace_parent`, so request, relay, worker and provider form one trace. For retries and scheduled deliveries, the relay starts a **new root span linked** to the stored `trace_parent`. In both cases the relay puts its own span's context in the AMQP header. | 0002, 0009 | T9 F5, P3, ledger review |

## B. Data model and state machine

| ID | Decision | Affects | Source |
|---|---|---|---|
| D14 | **Transition 12** (late success): `attempt_count = $n AND status IN ('pending','queued','retrying','failed','cancelled')`. Consequently `cancelled` and `failed` are **not strictly terminal**: they can move to `sent` through transition 12 only, when a late result proves the message was delivered. Clients may see `cancelled → sent`; this is documented. | 0004, 0006 | T4 F2, T7, ledger review |
| D15 | **Cancel** is always allowed from `pending`, `queued` and `retrying`. Cancelling an already `cancelled` notification returns `200` (idempotent). From `processing`, `sent` or `failed` it returns `409` with the current status. If the notification has an attempt with outcome `unknown`, the response includes `"may_have_been_sent": true`. Dedupe index: `UNIQUE (dedupe_key) WHERE dedupe_key IS NOT NULL AND NOT (status = 'cancelled' AND attempt_count = 0)`. Every `ON CONFLICT` repeats this predicate. | 0004, 0006 | T4 F7, T7 (revised) |
| D16 | **Redrive** only from `failed`: resets `failures` and `due_at`, **never** `attempt_count`. If the stored `expires_at <= now()`, the request must include a new `expires_at` (≥ now + 30 s), otherwise `409`. Otherwise the stored value is kept. | 0004, 0006 | T4 F6 |
| D17 | `decideRetry(outcome, failures, maxFailures, expiresAt, retryAfter, profile, now, jitter float64) → Decision{Action, Delay, Reason, ConsumesBudget}`. `due_at = now() + delay` is computed in SQL; the caller passes the database `now()` read in the same transaction. One clock. | 0002, 0004, 0007 | T2 F6, T4 F3 |
| D18 | The worker's final write is retried with back-off for up to **60 s**: transition 4 first, transition 12 if it affects zero rows. | 0004, 0005 | T5 F6 |
| D19 | Database constraints: at most one `in_flight` attempt per notification (partial unique index); `status = 'failed'` ⇔ `failure_reason IS NOT NULL`; `status = 'sent'` ⇔ `sent_at IS NOT NULL`. Partial index on `batch_id WHERE batch_id IS NOT NULL`. UUIDv7 via `google/uuid`. | 0004 | T4 P2, N2, N3 |
| D20 | Lease (30 s) covers the token wait after claim (≤ 250 ms, because D23 claims during the wait), the HTTP timeout (10 s) and the final-write margin. The lease sweeper sets `finished_at = now()` when it marks an attempt `unknown`. `queued_at` can be `NULL` for directly claimed rows; metrics skip such observations. | 0003, 0004, 0005, 0009 | T3 N1, T4 T1, T9 F8, N3 |
| D21 | One `notifications` table. Rationale: write volume is negligible at 294 msg/s (not TOAST, which only applies above 2 kB). Splitting hot columns is future work at larger scale. | 0004, future-work | T4 F4 |
| D22 | **`subject` removed entirely** (API, data model, templates, `subject_tpl`). The PDF defines a notification as recipient, channel, content and priority. Subject/title is future work for real provider integrations. | 0004, 0006, 0007, 0008, future-work | Author (G1), T12 |

## C. Worker, rate limiting, circuit breaker, retry

| ID | Decision | Affects | Source |
|---|---|---|---|
| D23 | **One pacer loop and one semaphore per channel**, inside the single worker process. Order: acquire slot → wait until the local buffer has a message (signal, not consume) → **check the circuit breaker** (open → wait) → reserve a GCRA slot → take the highest-priority buffered message and **claim it during the wait** → send at the slot time. The token is kept when a claim affects zero rows. If the circuit opens between claim and send, the row returns to `retrying` without consuming budget, with `due_at` at the circuit's next half-open time, and the open attempt row is closed with outcome **`aborted`** (nothing was sent; excluded from success/failure rates and from the breaker). Concurrency **25 per channel per replica**; prefetch = concurrency. | 0005, 0007 | T5 F3, F4, T7, T12 |
| D24 | GCRA: **98 msg/s per channel** (configurable; margin derived from the jitter measured by load-smoke), zero burst, Lua using Redis `TIME`, returns a **relative wait**, reservation horizon 250 ms (re-ask after 50 ms). This is how "maximum 100 per second" is guaranteed, not a deviation. | 0007, 0010 | T7 F1, N2, T12 |
| D25 | Redis unavailable: in-process limiter at `98 / WORKER_REPLICAS`, documented as **approximate**; `limiter_fallback_active` alert. Sending does not stop. | 0007, 0009 | T7 F8 |
| D26 | **Circuit breaker in-process**, per `(channel, provider)`, a pure state machine with an injected clock. Opens at ≥ 20 requests and ≥ 50 % failures in 30 s, **or after 5 consecutive failures**. Open duration 5 s doubling to 2 min; `provider_fatal` 1 min; `throttled` = `Retry-After`. Redis is used **only** for GCRA. | 0005, 0007, 0009, architecture | T7 F5, F2 |
| D27 | Classification changes: network errors before the request was written get a new attempt outcome, **`unreachable`**: provider-scoped, consumes no budget, counts toward the breaker. Together with `aborted` (D23) it is added to the attempts `CHECK` constraint, the metric label set and the summary; `408`, `425` and `409` are retryable; `410` is `provider_fatal`; redirects are **not followed** (`CheckRedirect` returns `http.ErrUseLastResponse`) and `3xx` is `provider_fatal`. A request counts as sent only if `WroteRequest` fired with `Err == nil`. | 0007 | T7 F2, F3, F6 |
| D28 | **Provider contract:** request body exactly `{"to", "channel", "content"}`. The `202` response `{"messageId", "status", "timestamp"}` is parsed; `messageId` → `provider_message_id`, which is **not** assumed unique. Any other `2xx` is success plus a warning metric. Headers: `Idempotency-Key: <notification_id>` (configurable per provider; webhook.site ignores it, mockprovider uses it to count duplicates) and `traceparent`. | 0007, architecture | T0, T6 F4, T7 F4, T12 |
| D29 | HTTP client: `req.GetBody = nil` (disables transport replays); `MaxIdleConnsPerHost` = concurrency; body drained (≤ 64 KB) then closed; 2 s connect, 10 s total. | 0007 | T7 F7 |
| D30 | `429` without a usable `Retry-After`: 5 s default, doubling on consecutive 429s up to 2 min. | 0007 | T7 |
| D31 | Default TTLs kept (high 10 min, normal 6 h, low 24 h), configurable. The README documents the largest absorbable burst per priority (TTL × rate). | 0007, README | T7 F10 |
| D32 | Provider failover moved to future work; breaker keys remain provider-aware. | 0007, future-work | T7 F11 |

## D. API

| ID | Decision | Affects | Source |
|---|---|---|---|
| D33 | `/healthz` and `/readyz` are also served on the **api's public port** and are in OpenAPI. `/metrics` and `pprof` stay on the internal admin port. | 0006, 0009 | T6, T9 F1 |
| D34 | List filters are **single-valued** (as R13 asks). Two sqlc queries (with and without status filter); an `EXPLAIN` check is tied to a test; pagination walk test (500 rows in the same second, `limit=7`, every row exactly once). | 0006, 0010 | T6 F2, P1 |
| D35 | Single-create `dedupe_key` conflict → `409`, `code: duplicate`, `existing_id`. Batch over 1,000 items → `422`, `code: too_many_items`. `Idempotency-Key` applies only to the two create endpoints; the hash includes method and path. `X-Request-ID`: at most 128 characters of `[A-Za-z0-9._-]`, otherwise ignored and regenerated. 5 MB body limit on all endpoints. | 0006, 0009 | T6 F3, N2, N3, T9 F3 |
| D36 | SMS counting: GSM-7 in septets, UCS-2 in UTF-16 code units; escape sequences and surrogate pairs are never split across segments. Phone numbers: `IsPossibleNumber` by default, strict validation by configuration; a test covers `+905551234567` from the PDF. | 0006, 0010 | T6 F5, F6 |
| D37 | WebSocket: the gateway **registers the subscription first, then reads Postgres and sends a snapshot** (`status_changed` with current `version`); for a batch subscription, one event per notification (≤ 1,000). Gateway has read-only Postgres access. `OriginPatterns` from configuration; bounded per-connection send queue, connection closed when full. Event queue has `x-max-length` and a short `x-message-ttl`. The `last_version` map is removed; the client rule (ignore `version ≤ known`) remains. | 0005, 0006, 0009 | T5 F7, N3, T6 F1, F7, T7 |
| D38 | Swagger UI assets embedded (`go:embed`); works offline. | 0006 | T6 N1 |
| D39 | Metrics summary: `circuits` field removed (circuit state comes from the worker's `circuit_state` metric); `backlog_due` cached separately for 10 s; all outcomes listed and the `success_rate` denominator defined. **Postgres is the api's only required dependency**; its event publisher (D11) is optional and never affects readiness. | 0009, 0005, architecture | T9 F8, N1, T12, ledger review |
| D40 | Templates: variables accept string, number and boolean (converted to string via `json.Number`); objects and arrays → `422`. Per-version `optional_variables` (missing → empty string). FuncMap: **`default` and `truncate` only** (`upper`/`lower` removed). Allow-list checks **function names** (all 19 builtins rejected) and rejects templates that define more than one template. Body ≤ 64 KB. Declared but unused variable → rejected at creation. Existing name → `409`. Parsed templates are cached **per request**, not process-wide. A past `scheduled_at` within the 30 s tolerance is stored as `NULL`. | 0008, 0006, 0010 | T8 F1–F6, N3, P1 |

## E. Observability

| ID | Decision | Affects | Source |
|---|---|---|---|
| D41 | **One "Overview" Grafana dashboard** plus a smoke test (every panel query must return data after the e2e run) and `promtool check rules`. Alerts are Prometheus rules only (no Alertmanager): relay down (`up{job="relay"} == 0`), `high` lane oldest queued age, poison DLQ depth > 0, expiry spike, circuit open, limiter fallback. Metric changes: `relay_is_leader` and `ws_stale_events_dropped_total` removed; `reconciliation_runs_total` and `reconciled_rows_total` added; `lane_oldest_queued_age_seconds` added; `notifications_created_total{source}` takes `single` or `batch`; the gateway's readiness includes Postgres (D37). | 0009, 0010 | T9 F6, F7, N2, T12, ledger review |
| D42 | PII: a `Recipient` type implementing `slog.LogValuer`; a **canary scan** in e2e searches all service logs, exported spans and `/metrics` output for a unique recipient and content. | 0009, 0010 | T9 F4, P1 |
| D43 | Jaeger v2 image pinned (not the v1 all-in-one). | 0005, 0009 | T9 |

## F. Compose and operations

| ID | Decision | Affects | Source |
|---|---|---|---|
| D44 | **Default provider is webhook.site** (the author's free URL, configured to return `202` as in the PDF). The default lives **in `docker-compose.yml`** as `${PROVIDER_URL:-https://webhook.site/<uuid>}`, so a clean clone works without a `.env` file (which is git-ignored); `.env.example` only documents how to override it. The README shows the URL's creation date on its first screen; the URL is renewed right before submission. `make demo` probes the URL first and sends at most 10 requests. Batch and load examples use the mock profile. webhook.site's cap/expiry responses are classified provider-scoped. `mockprovider` is used only by automated tests and load tests. | 0005, 0007, 0010, README | Author (G1), T12 |
| D45 | Compose file is `docker-compose.yml`; the README uses the PDF's command `docker-compose up`; CI runs it once with the standalone binary; the README states the minimum Compose version. | 0005, 0010, README | T5 N1, T12 |
| D46 | Default Compose runs **one worker container** (`WORKER_CHANNELS=sms,email,push`). Per-channel deployments live in an override file used by one chaos scenario. | 0005 | T5 P2 |
| D47 | `stop_grace_period: 20s` for workers; binaries have a `healthcheck` subcommand (no `curl` in minimal images); the `depends_on` chain rests on Postgres, RabbitMQ, Redis and `migrate`. | 0005 | T5 F5, F9 |
| D48 | Versions pinned: Go ≥ 1.26 (oapi-codegen requirement), RabbitMQ 4.4.x, `rabbitmq/amqp091-go` ≥ v1.13.0 (automatic connection recovery since v1.12.0, topology recovery since v1.13.0), Jaeger 2.x, golangci-lint v2. **Every container image is pinned to an exact tag** in `docker-compose.yml`, including Postgres (18.x, which also provides `uuidv7()`) and Redis; the exact tags are chosen in P0. | architecture, 0001, 0010 | T1 F11, T6, T12 N2 |

## G. Testing and CI

| ID | Decision | Affects | Source |
|---|---|---|---|
| D49 | Integration tests organised as an **actor-pair matrix** (api, relay, worker, lease sweeper, late worker); every cell names a test or "n/a". Tests for every decision in this ledger. | 0010 | T10 F2, P1 |
| D50 | Chaos: Postgres restart during sends (report duplicates); **RabbitMQ wiped with its data and restarted** (all rows end `sent`; tests D5); low-traffic provider outage (three `high` notifications, provider down 60 s, all `sent`, `failures = 0`); worker `SIGKILL`; Redis stop. Leader scenario removed. | 0010, 0007 | T10 F4, T7 P1 |
| D51 | Load: rate scenario **30,000 SMS**, criterion per channel, half-open 1 s window over sorted arrival timestamps at mockprovider (the window computation has its own unit test); strict-priority ordering test plus a saturation scenario reporting `high` p95 and `low` wait; **channel isolation** (mockprovider fails 100 % of SMS; Email throughput is unaffected; this is the evidence for per-channel pacers inside one worker process, D23 and D46); phase 1 = 1 M ingest with flat broker depth; phase 2 = 100 k full drain with each `Idempotency-Key` received exactly once; **load-smoke (60 s) in the e2e job from day 1**. | 0010 | T10 F1, F3, F8, P2 |
| D52 | All durations configurable and millisecond-scale in tests; back-dating rows only for expiry and lease edge cases. | 0010 | T10 F5 |
| D53 | CI: third-party actions pinned to full commit SHAs; minimal `permissions`; security scan (`govulncheck`, `trivy`) non-blocking and scheduled; **Dependabot off until after the evaluation**; golangci-lint v2 config format; Markdown link check; `make test-docker` adds `-v $PWD:$PWD -w $PWD`; a job runs `docker-compose up`. | 0010 | T10 F7, N2, N3, T11 P2, final report |
| D54 | **`make verify-suite`:** mutation patches applied in a temporary worktree; each must make a **named** test fail (not a compile error): claim guard reverted to `status = 'queued'`, fencing condition removed, pacer at exactly 100/s (checked by a deterministic pacer test with fake clock and fixed jitter), redirect following re-enabled, builtin template functions allowed, `mandatory` removed. Written on day 3. | 0010, README | Author, final report |
| D55 | Additional tests: trace propagation (in-memory exporter), metrics summary values on a known dataset, health endpoints, listing and summary latency on the phase-1 dataset. | 0010 | T10 F6 |

## H. Documentation and process

| ID | Decision | Affects | Source |
|---|---|---|---|
| D56 | ADRs keep a **clean final text** plus a short "Review history" section (3–5 bullets) linking to `docs/ai/design-review-log.md`. `architecture.md` gets a consistency update now and is **regenerated after implementation**. `future-work.md`: no deviations section needed after G1; an "Interpretations" section instead; the limitations table lists at least: largest burst per priority bounded by TTL (D31), approximate rate limit in Redis fallback (D25), unbounded backlog with expiry as the only bound, `low` waiting under strict priority (D7), single relay with restart (D4), reconciliation covering reconnections only (D5), webhook.site URL lifetime and request cap (D44), breaker state per replica (D26), no authentication; claims softened ("some providers" honour idempotency keys; no "nothing here is an accident"); multi-tenancy sketch uses per-tenant `LATERAL`. | all docs | T11, T12, final report |
| D57 | README written in stages: day 1 quick start, day 2 API examples and `make demo`, day 3 load results. Sections: requirements map **with an evidence column**, interpretations of the PDF, limitations, **"How AI was used"** and **"How AI output was verified"**. | README | T10 F9, T12 P1, final report |
| D58 | `docs/ai/`: curated `design-review-log.md` (AI mistakes caught, including the reviewer's own; critiques rejected with reasons; findings reached by both models, stated with their real order), the sanitised review prompt, the orchestration skill, sample sub-agent briefs. No raw transcripts, personal data or local paths. | docs/ai | Author, final report |
| D59 | **Git workflow:** a branch per phase or feature; the agent pushes the branch and **opens the PR with `gh`**; the author merges manually; `main` is protected (no direct pushes). Conventional Commits; every commit carries a `Co-Authored-By` trailer for the AI; one logical change per commit; sub-agents never commit or push; the main loop reviews each diff before committing. When a feature is dropped, `future-work.md` is updated in the same commit. PRs are merged with **"Rebase and merge"** (the only method enabled in the repository settings), so the curated commits form a linear history. | CLAUDE.md, 0010 | Author, ledger review |
| D60 | Repository set-up before P0: create the repository; move the design documents into `docs/` and `docs/adr/`; protect `main` (PRs required, no force pushes); the first commit contains only the documents and agent configuration. | CLAUDE.md, plan | T0, T12 N3, ledger review |
| D61 | Before submission: a clean-clone rehearsal on another machine (or a fresh user account), and one run of the release workflow with a test tag. | plan, 0010 | Final report, ledger review |

## I. First-hour experiments (P0, before any feature work)

Each one confirms a decision that rests on documentation nobody has run. Each has a fallback.

| # | Experiment | Pass | Fallback if it fails |
|---|---|---|---|
| E1 | RabbitMQ 4.4: publish N `low`, then 1 `high` to a quorum queue; consume | `high` arrives first; a message without priority is treated as 4 | Separate queue per priority with a strict local pick (the pre-D7 design without weights) |
| E2 | `oapi-codegen` strict-server with the standard-library router | Generates and compiles; a stub endpoint returns `application/problem+json` on error | strict-server with `chi` |
| E3 | testcontainers-go inside a container on macOS with the D53 settings | `make test-docker` runs one integration test | Document `make test` (Go + Docker) as the only path on macOS |
| E4 | amqp091-go recovery: kill the broker for 60 s | Recovery is on by default (or enabled), the state change is observed, topology is redeclared; behaviour when recovery gives up is observed | Own reconnect loop with exit-and-restart (D5) |
| E5 | Go transport with `GetBody = nil`: mock closes the connection after reading the request without responding | The request is sent exactly once | Disable keep-alives for the provider client |
| E6 | Quorum queue `x-consumer-timeout` = 24 h | Accepted at declaration; an unacked message is not cancelled after the default 30 min (tested with a short override) | Cancel-and-resubscribe on long open circuits (the v1 D8) |

## J. Unverified assumptions

Stated as facts elsewhere but not yet run. Treat them as hypotheses until a test or experiment confirms them.

| # | Assumption | Confirmed by |
|---|---|---|
| U1 | Go's transport replays a request carrying `Idempotency-Key` only when `GetBody` is set (D29) | E5 |
| U2 | The status codes webhook.site returns when the free URL's cap is reached or the URL has expired (D44) | Manual check on day 1; classify whatever is returned as provider-scoped |
| U3 | `IsPossibleNumber` accepts the PDF's `+905551234567` (D36) | Unit test in P2 |
| U4 | `NotifyStateChange` exposes the reconnect event needed by D5 | E4 |
| U5 | RabbitMQ 4.4 strict priority behaves as documented, including the default priority 4 (D7) | E1 |
| U6 | strict-server works with the standard-library router (D48) | E2 |
| U7 | `x-consumer-timeout` behaves as documented (D8) | E6 |
| U8 | amqp091-go enqueues a `basic.return` on the return channel before delivering the matching publisher confirm (D2); reported as a non-goal of the library | The D2 integration test in P1 (publish without a binding; the row must stay `pending`), repeated 100 times |

## Statements to correct in the documents

- "The only remaining duplicate window" → every attempt that ends `unknown` can produce a duplicate with a provider that ignores idempotency keys; webhook.site does. (architecture, T12 F2)
- Principle 3 → "every race on a notification is decided by a conditional `UPDATE` on `notifications`". (architecture, T12 F4)
- "RabbitMQ never holds more than about 6,000 messages" → "about 6,000 distinct notifications; duplicates possible". (0003, T3 F5)
- Outdated technology claims: two quorum-queue priority levels; Kafka has no per-message nack (share groups, Kafka 4.2); RabbitMQ has no per-message delay (4.4 has); quorum-queue memory pressure. (0001, 0002, 0003)
- "Each row is exercised by a chaos test" only after D50 is in place. (0007, architecture)
- ADR-0006 "there is no transition out of `cancelled`" → see D14; the batch conflict look-up uses the D15 predicate instead of `status <> 'cancelled'`. (0006, ledger review)
- ADR-0007 "holds exactly" → the GCRA guarantee as stated in D24; approximate in fallback (D25). (0007)
- future-work "153 characters per concatenated segment" with shift tables → "somewhat fewer than 153". (future-work, T11 N1)
- ADR-0005 title and file name mention leadership → rename to `0005-service-topology-and-status-events.md` and update every link. (0005, ledger review)
- ADR-0001 rejection of the Postgres-only queue → rewrite honestly: at this volume a Postgres-only queue would suffice; the broker is chosen because the PDF frames the system as event-driven with queue workers, for standard consumer semantics, and as the extension point for status events. (0001, T1 F5)
