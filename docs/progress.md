# Progress

The first file every session reads and the last one it updates. The phases and their exit criteria are in the [implementation plan](plan/implementation-plan.md).

## Current phase

**P0: repository skeleton and first-hour experiments** — in review (task 3, the experiments).

**Next step:** the author merges the `p0/experiments` pull request and answers open issues 2, 3 and 4. Then P1 on `p1/core-path`. The three issues block parts of P1 (the relay's publish, the RabbitMQ connection set-up, the worker's acknowledgements), not its start.

**Minimum Compose version:** v2.1.1, from the release notes (`up --wait`); tested with v5.6.1 (D45, D70). `up --wait` is the newest feature the P0 file and the exit criteria use; everything else in the file (`healthcheck` with `start_period`, named volumes, short-syntax ports and mounts, `${VAR:-default}` interpolation) is older. The number is not run against a v2.1.1 binary. P1 re-derives it from the release notes when the one-shot `migrate` service lands (probably v2.7.0, whose notes list the fix that makes `up --wait` handle one-shot containers). The practical check is the clean-clone rehearsal in P7.

**Decision ledger:** v4 (`D1`–`D75`). Section K records the points settled during the preparation session. The ADRs are the primary design documents; the ledger is the decision record.

## Phase status

Status is one of: `not started`, `in progress`, `in review` (pull requests open), `done` (all exit criteria met and pull requests merged).

| Phase | Title | Status | Pull requests | Exit criteria met on |
|---|---|---|---|---|
| P0 | Repository skeleton and first-hour experiments | in review | [#2](https://github.com/AlpNuhoglu/event-driven-notification-system/pull/2) (`p0/skeleton`, merged), [#3](https://github.com/AlpNuhoglu/event-driven-notification-system/pull/3) (`p0/experiments`) | |
| P1 | Core path | not started | | |
| P2 | API completeness | not started | | |
| P3 | Delivery robustness | not started | | |
| P4 | Templates and scheduling | not started | | |
| P5 | Real-time and observability | not started | | |
| P6 | Quality | not started | | |
| P7 | Delivery | not started | | |

## First-hour experiments

Filled in during P0. One row per experiment; the row prefix `| E<n> |` is checked by the P0 exit criteria.

All six ran on 2026-11-18 on macOS with Docker Desktop 29.9.2 and Go 1.27, against `rabbitmq:4.4.1-alpine` started by `testcontainers-go` v0.45.0, with `amqp091-go` v1.17.0. Each experiment is its own Go module in `experiments/e<n>/`; unless a row says otherwise, its command is run in that directory. No experiment failed, so no fallback was taken and no ADR changed. Three results raise questions that only the author can settle (open issues 2, 3 and 4). What the later phases must take from the results is listed in the session log.

| # | Result (pass / fail) | Evidence (command and what was observed) | Fallback taken | Documents updated |
|---|---|---|---|---|
| E1 | pass | `go test -tags=integration -count=1 -v ./...` (8 s). 200 `low` messages (priority 0), then one `high` (2): the `high` message is consumed first. 100 messages at each of 0, 1 and 2, published lows first and round-robin, consumed with `basic.get` and with prefetch 1: all 2s, then all 1s, then all 0s, in publishing order within a level, with no ratio; the same for 1, 2 and 3. A message without a priority is consumed among the 4s (`p5, p4, none, p4, p3, p2, p0`). A quorum queue needs no argument for this; `x-max-priority` is rejected with 406. **The client cannot send an explicit 0:** a message published with `Priority: 0` is consumed as a 4 (open issue 2). The explicit 0s of this experiment were published through the management HTTP API. | None | This file (open issue 2) |
| E2 | pass | `go test -count=1 -v ./...`. `oapi-codegen` v2.8.2 with `models`, `std-http-server` and `strict-server` generates code that compiles (build constraint `go1.22`; one direct dependency, `oapi-codegen/runtime` v1.9.0), and generating again gives an identical file. Served through `http.ServeMux`, a typed 404 from the specification, a path parameter that is not a UUID (400), a malformed JSON body (400) and an error returned by a handler (500) all answer `application/problem+json`. | None | This file |
| E3 | pass | `make -C experiments/e3 -f ../../Makefile test-docker`, run at the repository root: the unmodified `test-docker` recipe applied to the experiment's module. Inside `golang:1.27.4-trixie`, with `-race` and with Ryuk enabled, the test starts RabbitMQ through testcontainers, dials `host.docker.internal:<mapped port>`, publishes a message and reads it back (`ok`, 3.3 s). The same test passes on the host, and `make test-docker` at the root passes with today's unit tests. | None | This file |
| E4 | pass | `go test -tags=integration -count=1 -v -timeout 20m ./...` (130 s). Recovery is not on by default; it is enabled with `Config.Recovery`. With it enabled and a budget of 40 attempts at 5 s: the broker is stopped for 60 s; the connection's `NotifyStateChange` reports `open → reconnecting` at the stop and `reconnecting → open` 3 s after the start; confirm mode, the `NotifyReturn` and `NotifyPublish` listeners, the consumer and its prefetch work again without application code. With the broker replaced by an empty one at the same address, the exchange, the quorum queue, the binding and the consumer exist again after recovery. Giving up: with the library's default budget (5 attempts at 5 s) recovery gave up 26 s into the outage; the connection reports `reconnecting → closed` with a non-nil `Err`, every listener channel is closed, and `Publish` returns `ErrClosed` (open issues 3 and 4). | None | This file (open issues 3 and 4) |
| E5 | pass | `go test -race -count=20 -v ./...`. On a reused keep-alive connection the server reads the `POST` and closes the connection without answering. Control, `Idempotency-Key` with `GetBody` set: the server receives the request twice. With `GetBody = nil`: exactly once, also with `X-Idempotency-Key`. The counts are the same in 20 of 20 runs; the client gets `EOF`. The deciding code is `Request.isReplayable` in `net/http/request.go` (Go 1.27.1). | None | This file |
| E6 | pass | `go test -tags=integration -count=1 -v ./...` (44 s). `x-consumer-timeout` = 86400000 is accepted on a durable quorum queue. With the broker default lowered to 30 s, an unacknowledged delivery is cancelled at 30.0 s on a queue without the argument, is still held at 40 s on the queue with 24 h, and is cancelled at 10.0 s on a queue with a 10 s argument (twice in a row). The timeout arrives as `basic.cancel` (`NotifyCancel`): the channel stays open, the message is redelivered with `redelivered=true`, the same channel can subscribe again, and the timeout does not count toward the delivery limit. | None | This file |

## Unverified assumptions

Filled in as each assumption is confirmed or refuted (ledger section J; mapping in the plan, section 11).

| # | Status (open / confirmed / refuted) | Confirmed by | Date |
|---|---|---|---|
| U1 | confirmed | E5. The "sent exactly once" provider client test of P1 keeps it confirmed. | 2026-11-18 |
| U2 | open | | |
| U3 | open | | |
| U4 | confirmed | E4: the connection's `StateChanged{From: reconnecting, To: open}` is the reconnect event. The reconciliation tests of P3 build on it. | 2026-11-18 |
| U5 | confirmed | E1, for the broker: strict order and default priority 4. The client cannot send priority 0 (open issue 2). | 2026-11-18 |
| U6 | confirmed | E2 | 2026-11-18 |
| U7 | confirmed | E6 | 2026-11-18 |
| U8 | confirmed | Reading the frame dispatch of `amqp091-go` v1.17.0 (session log, 2026-11-18): the order holds inside the library, under two conditions. `TestRelay_UnroutablePublishStaysPending` with `-count=100` follows in P1. | 2026-11-18 |

## Open issues

Questions that the ledger and the ADRs do not answer, and problems that are not yet solved. An entry is removed only when it is answered, and the answer is recorded under "Decisions taken" in the session log.

| # | Raised on | Issue | Blocks |
|---|---|---|---|
| 2 | 2026-11-18 | **The client cannot send the priority that D7 gives to `low` (E1).** D7, ADR-0001 and ADR-0002 set the AMQP priority to `2 − db_priority`, so `low` is 0. `amqp091-go` v1.17.0 writes the priority property only when it is greater than 0 (`write.go`: `if f.Properties.Priority > 0`). A `low` message therefore leaves without a priority, RabbitMQ sorts it as 4, and `low` overtakes `high`. Proposal: AMQP priority = `3 − db_priority` (`high` 3, `normal` 2, `low` 1). E1 shows that 1, 2 and 3 are strictly ordered, and all three stay below 4, so a message published without a priority still stands out in the test that D7 requires. The author decides; D7, ADR-0001 and ADR-0002 change with the decision. | P1: the relay's publish and `TestRelay_PrioritySetOnEveryMessage` |
| 3 | 2026-11-18 | **The library's recovery is opt-in, marked experimental, and has a retry budget that the design does not set (E4).** D5, D48 and ADR-0005 rely on the automatic recovery of `amqp091-go`. It works, but it must be enabled with `Config.Recovery`; the library documents it as "Experimental … may be subject to API or behavioral changes"; and its default budget (5 attempts at a fixed 5 s interval) gave up 26 s into a 60 s outage, after which D5's exit-and-restart path takes over. The ledger and the ADRs do not say whether the experimental status is accepted, what the retry interval and the number of attempts are, or which configuration variables carry them (durations are configuration, D52). Proposal: accept it, and add `AMQP_RECOVERY_RETRY_INTERVAL=5s` and `AMQP_RECOVERY_MAX_RETRIES=24` to `.env.example`, about two minutes before a process exits and is restarted. | P1: the RabbitMQ connection set-up of the relay and the worker; `.env.example` |
| 4 | 2026-11-18 | **After a recovery, acknowledging a delivery received before the outage acknowledges a different message (E4).** The library keeps the same `Channel` object and delivery tags restart at 1. `Ack` on a delivery from before the outage returns nil and acknowledges whichever message has that tag on the recovered channel (observed: acknowledging the old `m2` removed `m3`); a tag the broker does not know closes the channel with 406, and the library reopens it. A worker whose send finishes after a reconnect would do exactly this. The ADRs do not say how the worker prevents it. Options: (a) the worker stamps every delivery with a connection epoch that advances on `open → reconnecting` and never acknowledges or rejects a delivery of an older epoch; the broker redelivers it and the claim guard (D1) drops the duplicate. (b) The worker does not use the library's recovery and opens a new connection and channel after a loss, so that an old delivery's acknowledgement fails with `ErrClosed`. Proposal: (a), which keeps D5 as written. | P1: the worker's acknowledgement path |

## Session log

Newest entry first. One entry per session, in this form:

```
### YYYY-MM-DD — P<n>: <topic>

- **Done:** what was implemented, with the tests that prove it.
- **Pull requests opened:** links.
- **Exit criteria:** which commands were run and passed; which remain.
- **Decisions taken:** choices made within the latitude of the ledger and the ADRs, with the reason.
- **Open issues:** new entries added to the table above.
- **Next step:** the first thing the next session does.
```

### 2026-11-18 — P0: first-hour experiments (task 3)

- **Done (task 1, repeated):** the five repository checks were run again with the author's changed token (Administration: read-only). None failed for permissions. `rules/branches/main` lists `deletion`, `non_fast_forward` and `pull_request`, the ruleset's enforcement is `active`, and `branches/main` reports `protected: true`. The classic endpoint `branches/main/protection` answers 404 "Branch not protected": the protection is a ruleset, not a classic rule, and the answer is not a permission error.
- **Done (task 3):** the six experiments, all `pass`; the rows are in "First-hour experiments". The code is in `experiments/e1` … `experiments/e6`.
- **Done (host ports):** Grafana, Prometheus and the Jaeger UI publish `${GRAFANA_PORT:-3000}`, `${PROMETHEUS_PORT:-9090}` and `${JAEGER_UI_PORT:-16686}`; `.env.example` documents the three variables. The full six-service stack was then started from the repository file itself: `GRAFANA_PORT=3001 PROMETHEUS_PORT=9091 JAEGER_UI_PORT=16687 docker-compose up -d --wait` ended with all six services healthy, the three user interfaces answered 200 on the remapped ports, and Prometheus reported both of its targets `up`. The variables were needed because another project held the default ports at that moment; without them the same command failed with "port is already allocated". The scratch override of the previous session is no longer used.
- **U8, read in `amqp091-go` v1.17.0.** A connection has one reader goroutine (`Connection.reader`, `connection.go`). It reads frames in wire order and dispatches each one synchronously (`demux` → `dispatchN` → `Channel.recv` → `Channel.dispatch`, `channel.go`). For `basic.return`, `dispatch` sends the return to the `NotifyReturn` listeners; for `basic.ack` it calls the confirm sequencer, which resolves the deferred confirmation and sends to the `NotifyPublish` listeners. Both happen on that goroutine, and the next frame is not read before the current one is handed over. So if the broker writes the return before the ack, the application has the return before the confirm. Two conditions:
  1. The return listener must have room. `notifyAll` waits at most 5 s (`notifyTimeout`) for a full listener and then **drops** the notification. The relay's return channel therefore needs a capacity of at least one batch, or a goroutine that always drains it.
  2. The order on the wire is the broker's. RabbitMQ documents that it sends `basic.return` before `basic.ack`; the P1 test, repeated 100 times, checks it.
- **What later phases must take from the experiments** (inside the design; no decision needed):
  1. **Topology (P1).** Quorum queues are declared with `x-queue-type: quorum` and no priority argument; `x-max-priority` fails the declaration. `x-consumer-timeout` is sent as an `int64`: a Go `int` is encoded as 32 bits and wraps above 2³¹ − 1. Declaring an existing queue with another value of the argument, or without it, fails with 406, so changing the value later needs a policy or a new queue. RabbitMQ 4.4.1 refuses a transient, non-exclusive queue (error 541); the queues of ADR-0001 are durable or exclusive, so the design is not affected.
  2. **Relay after a reconnect (P1, P3).** Publish sequence numbers restart at 1. Confirmations that were outstanding when the connection dropped stay pending for the whole outage, resolve as not acknowledged at the reconnect, and `NotifyPublish` receives nothing for them; in the verification run all 62 such messages were in the queue. They are "unknown", not rejected: the row stays `pending`, is published again, and the claim guard drops the duplicate. A `Publish` during the outage returns `ErrClosed` at once.
  3. **Signals (P3).** The reconnect hook is the connection's `reconnecting → open`; giving up is `→ closed` with a non-nil `Err`. `NotifyClose` is not a reliable signal: it can receive extra errors during recovery, and it is closed without a value both when recovery gives up and on a normal `Close`. A channel's own state stays `open` during the outage.
  4. **Recorded topology (P1).** The library records declarations per channel and forgets them when the application closes that channel. Declaring on every (re)connection, as D2 requires, covers this.
  5. **Consumer timeout (P3).** After the broker's `basic.cancel`, acknowledging the held delivery returns nil and is ignored; the message is delivered again. The resubscribe safety net drops the deliveries of a cancelled consumer, and the claim guard covers one that was already being sent.
  6. **Requeue (P1).** A message returned with `nack` is delivered before higher-priority messages that are already waiting. This is the limitation D7 names, observed.
  7. **API (P1, P2).** `http.ServeMux` answers an unknown path or method itself with `text/plain` (404, 405), and the generated wrapper's default error handler also writes `text/plain`: the mux is wrapped and `ErrorHandlerFunc`, `RequestErrorHandlerFunc` and `ResponseErrorHandlerFunc` are set. The generated code does not validate a body against the schema (`{}` is accepted where `name` is required); required fields are checked by the validation of ADR-0006 §5.
  8. **Provider client test (P1).** With `httptrace`, only the first `GotConn` says whether the pooled connection was used; a replay calls it again on a new connection.
- **Pull requests opened:** [#3](https://github.com/AlpNuhoglu/event-driven-notification-system/pull/3) from `p0/experiments`.
- **Exit criteria:** run and passed: `git rev-parse --show-toplevel`; `go build ./... && make lint && make test`; `docker-compose up -d --wait postgres rabbitmq redis`; `docker-compose config | grep 'image:'` (six exact tags); `grep 'Minimum Compose version' docs/progress.md`; `grep -c '^| E[1-6] ' docs/progress.md` prints 6. `gh pr checks` is reported on the pull request.
- **Decisions taken (by the author):**
  1. Open issue 1 is closed without testing an old binary. The minimum Compose version is recorded as "v2.1.1, from the release notes (`up --wait`); tested with v5.6.1". P1 re-derives the minimum from the release notes when the one-shot `migrate` service lands (probably v2.7.0), and the clean-clone rehearsal in P7 is the practical check.
  2. The host ports of the three user interfaces are configurable, and the full stack is started from the repository file, not from a scratch override.
- **Decisions taken** (within the latitude of the ledger and the ADRs):
  1. Each experiment is a separate Go module under `experiments/`. The root module gains no throwaway dependency, and `go build ./...`, `make lint`, `make test` and CI do not include the experiments: they need Docker, and E4 and E6 wait for real broker timers. An experiment is deleted when the permanent test it becomes exists (the plan's "Becomes" column).
  2. E3 runs the `test-docker` recipe through `make -C experiments/e3 -f ../../Makefile`, because the root module has no integration test yet. The recipe is the unmodified one.
  3. The experiments wait by polling with a deadline, with a short pause between polls. The rule against `time.Sleep` applies to the permanent tests, which will use the `eventually` helper.
  4. The three port variables are ordinary lines of `.env.example`, like every other variable. The configuration test lists them as Compose-only, because no service reads them.
- **Not verified:** E3 on Linux (the CI runner); there `host-gateway` is the bridge gateway and files written into the mounted tree would belong to root. The broker's order of `basic.return` and `basic.ack` is documented, not observed (P1). `Nack` on a delivery from before a reconnect, and a publish made while the library is reconnecting, were not exercised. The three testcontainers settings of the recipe turned out to be unnecessary on Docker Desktop (the test also passes without them); they are kept because they make the address explicit.
- **Delegation:** Opus for E1, E3, E4 and E6; Sonnet for E2 and E5. The main loop wrote the briefs, read each test, ran every experiment again itself, checked the cited library lines in the module cache, did the U8 reading and gave the verdicts.
- **Open issues:** 2 (priority mapping), 3 (recovery set-up) and 4 (acknowledgements after a reconnect).
- **Next step:** after the merge and the author's answers, P1.

### 2026-11-18 — P0: repository checks and skeleton (tasks 1 and 2)

- **Done (task 1):** the five repository checks. Four passed at once. The protection check failed on the first run: the `main` ruleset existed with the right rules (no deletion, no force push, pull request required) but its enforcement was `disabled`. The session stopped; the author set it to `active`; the check then passed (`rules/branches/main` lists `deletion`, `non_fast_forward`, `pull_request`; `branches/main` reports `protected: true`). Merge methods print `[true,false,false]`; Dependabot is off (no `dependabot.yml`, security updates `disabled`); `git ls-files .inputs` prints nothing.
- **Done (task 2):** the Go module (Go 1.27) with the six role stubs, each with a `healthcheck` subcommand; `internal/platform/config`, which reads every variable of `.env.example` (two tests fail if the file and the loader disagree on a name or a default); `health`, `shutdown` and a temporary `bootstrap` runner; the `Makefile`, `.golangci.yml` (v2 format), one `Dockerfile` for all roles (a `scratch` runtime stage, non-root, 37 MB); `docker-compose.yml` with the six infrastructure services; the CI workflow with `lint`, `unit` and `codegen-drift`.
- **Pull requests opened:** [#2](https://github.com/AlpNuhoglu/event-driven-notification-system/pull/2) from `p0/skeleton`.
- **Exit criteria:** run and passed: `git rev-parse --show-toplevel`; `go build ./... && make lint && make test`; `docker-compose up -d --wait postgres rabbitmq redis` (all healthy); `docker-compose config | grep 'image:'` (six exact tags); `grep 'Minimum Compose version' docs/progress.md`. `gh pr checks` is reported on the pull request. Remaining: the six experiment rows (task 3).
- **Image tags (chosen by the main loop; D48).** Each is the newest patch release of its line on Docker Hub on 2026-11-18, and that version had the same digest as `latest` that day (for the two Alpine variants, the non-Alpine tag of the same version did).

  | Service | Tag | Why this tag |
  |---|---|---|
  | Postgres | `postgres:18.7` | Newest 18.x. The Debian variant, not Alpine: glibc collations, and it is the image's default variant. |
  | RabbitMQ | `rabbitmq:4.4.1-alpine` | Newest 4.4.x, published 2026-11-17. Without the management plugin: the design reads broker metrics from `rabbitmq_prometheus` and never from the management API (ADR-0003), so the UI would be an addition of our own. |
  | Redis | `redis:8.10.3-alpine` | Newest stable release; 8.12 exists only as a release candidate. |
  | Prometheus | `prom/prometheus:v3.16.0` | Newest stable release. |
  | Grafana | `grafana/grafana:13.3.1` | Newest stable release. |
  | Jaeger | `jaegertracing/jaeger:2.23.0` | Newest 2.x; the v2 image, not the v1 all-in-one (D43). |

- **Tool and action versions:** Go 1.27.4 in CI and in the images (`golang:1.27.4-alpine3.24` to build, `golang:1.27.4-trixie` for `make test-docker`, because the race detector needs cgo); `go 1.27.0` in `go.mod`; golangci-lint v2.15.0, sqlc v1.31.1, oapi-codegen v2.8.2, golang-migrate v4.20.1, each run with `go run <package>@<version>` from one variable in the `Makefile`. Actions by commit SHA: `actions/checkout` v7.0.1, `actions/setup-go` v7.1.0, `actions/upload-artifact` v7.0.2.
- **The two Compose cautions, checked by recreating the containers:** a table row in Postgres and a vhost in RabbitMQ both survived `up -d --force-recreate`. Postgres reports `data_directory = /var/lib/postgresql/18/docker` on the named volume mounted at `/var/lib/postgresql`, and no anonymous volume is created. RabbitMQ reports node `rabbit@rabbitmq` and stores under `/var/lib/rabbitmq/mnesia/rabbit@rabbitmq`.
- **Decisions taken** (within the latitude of the ledger and the ADRs):
  1. Postgres, RabbitMQ and Redis publish **no host ports**; only the three user interfaces do (Grafana `3000`, Prometheus `9090`, Jaeger `16686`). A clean clone must not fail because a local Postgres or Redis already holds a port (plan risk 13). The experiments and tests reach the infrastructure through testcontainers or the Compose network.
  2. The infrastructure services also have `restart: unless-stopped`; ADR-0005 requires it only for application services.
  3. Health checks: Postgres is probed over TCP (`pg_isready -h 127.0.0.1`), because the image's first initialisation runs a temporary server on the Unix socket only; Jaeger is probed on its health-check extension (`:13133/status`).
  4. `migrate healthcheck` exits 1 with a message: `migrate` is one-shot and has no health endpoint. The other five stubs serve `/healthz` and `/readyz` on the admin port.
  5. `PROVIDER_URL` has no default in the binary; the default lives in the Compose file (D44), which gains it in P1. Which roles require it is decided where each role is wired (P1).
  6. `make generate` skips `sqlc` and `oapi-codegen` with a message while `sqlc.yaml` and `api/openapi.yaml` do not exist; P1 adds both files and removes the conditions. The `Makefile` targets that later phases implement (`test-e2e`, `load`, `demo`, `up-mock`, `verify-suite`, `fuzz`) exist and fail with "not implemented yet".
  7. The `Dockerfile` has no `ENTRYPOINT` and no `CMD`: Compose names the role, and a missing command fails instead of starting an arbitrary role. It has no `# syntax=` line either, because that would be a floating image reference.
  8. Configuration files for the infrastructure are under `deploy/` (`deploy/prometheus/prometheus.yml`, `deploy/rabbitmq/enabled_plugins`), the directory the plan and `architecture.md` name. Prometheus scrapes RabbitMQ on `/metrics/detailed` (D9); application targets and alert rules arrive in P5.
- **Not verified:** the workflow's Go cache (there is no `go.sum` yet; the key hashes `go.mod` and the `Makefile`); the `sqlc` and `oapi-codegen` branches of `make generate`; the Compose file with a Compose release older than v5.6.1 (open issue 1). On the development machine, ports `3000`, `9090` and `16686` were held by another project, so the full six-service start was checked with a scratch override that remaps those three ports; the repository file itself was started for Postgres, RabbitMQ and Redis.
- **Delegation:** three Sonnet delegates (Go module and stubs; Compose; Makefile, lint, Dockerfile and CI). The main loop chose the tags and versions, read every file, re-ran the checks and changed three things: the `# syntax=` line removed, `math.IsNaN` in place of `f != f`, and a doc comment corrected.
- **Open issues:** 1 (minimum Compose version not run against an old binary).
- **Next step:** after the merge, `p0/experiments` from the updated `main`: E1–E6 and the U8 code reading.

### 2026-11-18 — Preparation: ledger v4 and design review log draft

- **Done:** the decision ledger replaced by v4 (new header; section K with D62–D74, and D75 added afterwards) and the first draft of `docs/ai/design-review-log.md` added. The log's turn and finding references in section 2 were checked against the raw review material, and section 4 gained the three reviewer proposals that the material records as not adopted. On the author's instruction, section 2 was then corrected to 15 rows: the latency row moved to section 4 as a decision that was attacked and held, the pacer row was rewritten to its finding, and the container count moved to section 3 as the reviewer's own miscount. `CLAUDE.md`, the plan, ADR-0005, ADR-0009 and `.env.example` aligned with v4.
- **Pull requests opened:** none new; the changes are in the documents pull request.
- **Exit criteria:** not applicable.
- **Decisions taken (by the author):** the circuit alert fires when a circuit is **not closed** (open or half-open) for more than 5 minutes; ADR-0009 §3 said 1 minute and D74 said "open", and both were corrected. For D74's expiry rule, `expired_total` gains a `priority` label and ADR-0009 adds `notifications_finished_total{channel,priority,status}`.
- **Open issues:** none. One was raised in this session (three points in the documents but not in the ledger) and resolved by the author the same day: D75 records the `priority` label on `expired_total` and `notifications_finished_total`, and D66 now names the marker that `.env.example` uses and points to D74 for the alert thresholds.
- **Next step:** P0, task 1.

### 2026-11-18 — Preparation: starting documents

- **Done:** the ten ADRs, `architecture.md` and `future-work.md` revised to the decision ledger (v3 at the time); the implementation plan, `CLAUDE.md`, this file and the repository files written. No application code.
- **Pull requests opened:** the documents pull request from `docs/initial-design`.
- **Exit criteria:** not applicable.
- **Decisions taken:** the author settled ten points that ledger v3 left open. They are recorded in ledger v4, section K (D62–D70), and applied in the ADRs named here.
  1. `success_rate` = `success / (all finished attempts except aborted)` (ADR-0009 §2).
  2. A `2xx` other than `202` is counted in its own metric, `provider_unexpected_2xx_total{channel,provider,code}`; `provider_malformed_response_total` remains for an unparseable `202` body (ADR-0007 §2, ADR-0009 §1).
  3. `requeue-stale` is a relay subcommand: `--older-than=5m` by default, `--all` for every `queued` row; run with `docker-compose run --rm relay requeue-stale` (ADR-0003 §3).
  4. Mock profile: `docker-compose.mock.yml` adds `mockprovider` and points `PROVIDER_URL` at it; `make up-mock` (ADR-0005, ADR-0010 §4).
  5. The values in `.env.example` for the event buffer, the gateway queue limits and the send queue are kept as initial values and tuned in P6. The first alert thresholds are given by D74 and are tuned in P6.
  6. Ports: public API `:8080`, admin `:8081`, gateway `:8082` (ADR-0009 §8).
  7. High-water marks are counted per lane and configured per priority, each value applying to every channel (ADR-0003 §1). The variable names of `.env.example` are the ones P0 implements.
  8. The 1-second back-off floor stays: full jitter can produce near-zero delays, and the floor protects the provider (ADR-0007 §3).
  9. Batch cancel reports a `may_have_been_sent` count (ADR-0006 §7).
  10. The minimum Compose version is determined in P0 from the features used.
- **Also decided:** the fallback order drops our own additions before any feature the assessment names (D73, plan §12); `make verify-suite` with at least three mutations is never dropped; demo and test data use only reserved fake values, because requests to the default webhook.site URL are publicly visible (D71); `mockprovider` records body field names only (D72).
- **Open issues:** none.
- **Next step:** P0, task 1.

## Known deviations from the plan

Anything done differently from the implementation plan, the ledger or an ADR: a failed experiment whose fallback became the design, a phase split differently, scope dropped under the fallback order. Each entry names the documents that were updated with it.

| Date | Deviation | Reason | Documents updated |
|---|---|---|---|
| 2026-11-18 | `main` starts with one **empty root commit** (`chore: initialise repository`), pushed directly to `main`. D60 says the first commit contains only the documents and agent configuration, and D59 says nothing is pushed to `main` directly. | The remote repository was completely empty, so the documents pull request had no base branch to target. The author approved a one-time push; the documents themselves arrived through the pull request. | This table |
