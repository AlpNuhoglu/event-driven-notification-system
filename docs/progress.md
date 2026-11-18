# Progress

The first file every session reads and the last one it updates. The phases and their exit criteria are in the [implementation plan](plan/implementation-plan.md).

## Current phase

**P0: repository skeleton and first-hour experiments** — in review (task 2, the skeleton).

**Next step:** after the author merges the `p0/skeleton` pull request, create `p0/experiments` from the updated `main` and run experiments E1–E6 (P0 task 3).

**Minimum Compose version:** v2.1.1, from the release notes (`up --wait`); tested with v5.6.1 (D45, D70). `up --wait` is the newest feature the P0 file and the exit criteria use; everything else in the file (`healthcheck` with `start_period`, named volumes, short-syntax ports and mounts, `${VAR:-default}` interpolation) is older. The number is not run against a v2.1.1 binary. P1 re-derives it from the release notes when the one-shot `migrate` service lands (probably v2.7.0, whose notes list the fix that makes `up --wait` handle one-shot containers). The practical check is the clean-clone rehearsal in P7.

**Decision ledger:** v4 (`D1`–`D75`). Section K records the points settled during the preparation session. The ADRs are the primary design documents; the ledger is the decision record.

## Phase status

Status is one of: `not started`, `in progress`, `in review` (pull requests open), `done` (all exit criteria met and pull requests merged).

| Phase | Title | Status | Pull requests | Exit criteria met on |
|---|---|---|---|---|
| P0 | Repository skeleton and first-hour experiments | in review | [#2](https://github.com/AlpNuhoglu/event-driven-notification-system/pull/2) (`p0/skeleton`) | |
| P1 | Core path | not started | | |
| P2 | API completeness | not started | | |
| P3 | Delivery robustness | not started | | |
| P4 | Templates and scheduling | not started | | |
| P5 | Real-time and observability | not started | | |
| P6 | Quality | not started | | |
| P7 | Delivery | not started | | |

## First-hour experiments

Filled in during P0. One row per experiment; the row prefix `| E<n> |` is checked by the P0 exit criteria.

| # | Result (pass / fail) | Evidence (command and what was observed) | Fallback taken | Documents updated |
|---|---|---|---|---|

## Unverified assumptions

Filled in as each assumption is confirmed or refuted (ledger section J; mapping in the plan, section 11).

| # | Status (open / confirmed / refuted) | Confirmed by | Date |
|---|---|---|---|
| U1 | open | | |
| U2 | open | | |
| U3 | open | | |
| U4 | open | | |
| U5 | open | | |
| U6 | open | | |
| U7 | open | | |
| U8 | open | | |

## Open issues

Questions that the ledger and the ADRs do not answer, and problems that are not yet solved. An entry is removed only when it is answered, and the answer is recorded under "Decisions taken" in the session log.

| # | Raised on | Issue | Blocks |
|---|---|---|---|

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

- **Decisions taken (by the author):**
  1. Open issue 1 is closed without testing an old binary. The minimum Compose version is recorded as "v2.1.1, from the release notes (`up --wait`); tested with v5.6.1". P1 re-derives the minimum from the release notes when the one-shot `migrate` service lands (probably v2.7.0), and the clean-clone rehearsal in P7 is the practical check.

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
