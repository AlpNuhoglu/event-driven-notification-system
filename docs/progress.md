# Progress

The first file every session reads and the last one it updates. The phases and their exit criteria are in the [implementation plan](plan/implementation-plan.md).

## Current phase

**P0: repository skeleton and first-hour experiments** — not started.

**Next step:** complete the repository set-up checks of P0 task 1, then start the skeleton on branch `p0/skeleton`.

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
| 1 | 2026-11-18 | `success_rate` in the metrics summary: the ledger says the denominator is defined (D39) but not how. ADR-0009 §2 uses "all attempts finished in the window except `aborted`". Confirm. | P5 |
| 2 | 2026-11-18 | The warning metric for a `2xx` other than `202` (D28) has no name in the ledger. ADR-0009 reuses `provider_malformed_response_total`. Confirm or name another. | P1 |
| 3 | 2026-11-18 | `requeue-stale` (D5): which binary carries it, and whether it takes all `queued` rows or only those older than a given age. | P3 |
| 4 | 2026-11-18 | "Mock profile" (D44): a Compose profile, an override file or an environment switch; how the batch and load examples select it. | P1 |
| 5 | 2026-11-18 | No values are fixed for: the event publisher buffer, the gateway queue's `x-max-length` and `x-message-ttl`, the per-connection send queue (D11, D37); the alert thresholds for oldest queued age and expiry rate (D41). `.env.example` marks its values "(proposed)". | P5 |
| 6 | 2026-11-18 | Port numbers for the public API and the gateway are not in the ADRs (the admin port is `:8081`). `.env.example` proposes `:8080` and `:8082`. | P0 |
| 7 | 2026-11-18 | Environment variable names in `.env.example` are proposals; P0 fixes them in `internal/platform/config`. High-water marks are offered per priority for all channels, although ADR-0003 says "per lane". | P0 |
| 8 | 2026-11-18 | The 1-second floor of the retry back-off was justified by a 1-second relay precision that is now 100 ms. The floor is kept in ADR-0007 §3 without that reason. Confirm the floor. | P3 |
| 9 | 2026-11-18 | Whether the batch-cancel response also reports `may_have_been_sent` (D15 defines it for single cancel only). | P2 |
| 10 | 2026-11-18 | The minimum Compose version to state in the README (D45) is determined by the features the Compose file ends up using. | P0 |

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
- **Decisions taken:** none.
- **Open issues:** 1–10 in the table above: points the documents needed that the ledger and the ADRs do not settle.
- **Next step:** P0, task 1.

## Known deviations from the plan

Anything done differently from the implementation plan, the ledger or an ADR: a failed experiment whose fallback became the design, a phase split differently, scope dropped under the fallback order. Each entry names the documents that were updated with it.

| Date | Deviation | Reason | Documents updated |
|---|---|---|---|
