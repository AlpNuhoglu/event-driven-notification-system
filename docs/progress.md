# Progress

The first file every session reads and the last one it updates. The phases and their exit criteria are in the [implementation plan](plan/implementation-plan.md).

## Current phase

**P0: repository skeleton and first-hour experiments** — not started.

**Next step:** complete the repository set-up checks of P0 task 1, then start the skeleton on branch `p0/skeleton`.

**Minimum Compose version:** to be determined in P0 from the features the Compose file uses (D45).

## Phase status

Status is one of: `not started`, `in progress`, `in review` (pull requests open), `done` (all exit criteria met and pull requests merged).

| Phase | Title | Status | Pull requests | Exit criteria met on |
|---|---|---|---|---|
| P0 | Repository skeleton and first-hour experiments | not started | | |
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

### 2026-11-18 — Preparation: starting documents

- **Done:** the ten ADRs, `architecture.md` and `future-work.md` revised to the decision ledger (v3); the implementation plan, `CLAUDE.md`, this file and the repository files written. No application code.
- **Pull requests opened:** the documents pull request from `docs/initial-design`.
- **Exit criteria:** not applicable.
- **Decisions taken:** the author settled ten points that the ledger (v3) leaves open. The ledger copy is unchanged; the ADRs named here carry the decisions.
  1. `success_rate` = `success / (all finished attempts except aborted)` (ADR-0009 §2).
  2. A `2xx` other than `202` is counted in its own metric, `provider_unexpected_2xx_total{channel,provider,code}`; `provider_malformed_response_total` remains for an unparseable `202` body (ADR-0007 §2, ADR-0009 §1).
  3. `requeue-stale` is a relay subcommand: `--older-than=5m` by default, `--all` for every `queued` row; run with `docker-compose run --rm relay requeue-stale` (ADR-0003 §3).
  4. Mock profile: `docker-compose.mock.yml` adds `mockprovider` and points `PROVIDER_URL` at it; `make up-mock` (ADR-0005, ADR-0010 §4).
  5. The values in `.env.example` for the event buffer, the gateway queue limits and the send queue are kept as initial values and tuned in P6. The two alert thresholds (oldest queued age, expiry rate) had no proposed value: they get first values in P5 and are tuned in P6.
  6. Ports: public API `:8080`, admin `:8081`, gateway `:8082` (ADR-0009 §8).
  7. High-water marks are counted per lane and configured per priority, each value applying to every channel (ADR-0003 §1). The variable names of `.env.example` are the ones P0 implements.
  8. The 1-second back-off floor stays: full jitter can produce near-zero delays, and the floor protects the provider (ADR-0007 §3).
  9. Batch cancel reports a `may_have_been_sent` count (ADR-0006 §7).
  10. The minimum Compose version is determined in P0 from the features used.
- **Also decided:** the fallback order drops our own additions before any feature the assessment names (plan §12); `make verify-suite` with at least three mutations is never dropped; demo and test data use only reserved fake values, because requests to the default webhook.site URL are publicly visible.
- **Open issues:** none.
- **Next step:** P0, task 1.

## Known deviations from the plan

Anything done differently from the implementation plan, the ledger or an ADR: a failed experiment whose fallback became the design, a phase split differently, scope dropped under the fallback order. Each entry names the documents that were updated with it.

| Date | Deviation | Reason | Documents updated |
|---|---|---|---|
| 2026-11-18 | `main` starts with one **empty root commit** (`chore: initialise repository`), pushed directly to `main`. D60 says the first commit contains only the documents and agent configuration, and D59 says nothing is pushed to `main` directly. | The remote repository was completely empty, so the documents pull request had no base branch to target. The author approved a one-time push; the documents themselves arrived through the pull request. | This table |
