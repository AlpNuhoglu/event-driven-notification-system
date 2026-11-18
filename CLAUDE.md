# CLAUDE.md

Loaded into every session. An event-driven notification service in Go: REST API → Postgres outbox → relay → RabbitMQ → worker → provider, with strict priority, a per-channel rate limit, retries and real-time status. The design is fixed in the ADRs; the sessions implement it phase by phase.

## Where things are

| What | Where |
|---|---|
| Progress and next step | `docs/progress.md` |
| The plan (phases P0–P7, exit criteria) | `docs/plan/implementation-plan.md` |
| Design, by topic (the primary design documents) | `docs/adr/0001` … `0010`, `docs/architecture.md` |
| Decision record: why the design is as it is (`D1`–`D76`, `G1`–`G5`, experiments, assumptions) | `docs/ai/decision-ledger.md` |
| How the design was reviewed | `docs/ai/design-review-log.md` |
| Limitations, interpretations, deferred items | `docs/future-work.md` |
| Delegation policy | `.claude/skills/fable-orchestration/SKILL.md` |

**Local inputs.** The assessment PDF and `requirements.md` (the R1–R36 checklist) are in `.inputs/` at the repository root. That folder is **never committed** (G5). Read them there when a requirement must be checked against the original text. In tracked files, refer to a requirement by its R-ID and a short paraphrase; do not quote the PDF.

## Precedence

When sources disagree, the higher one wins:

1. Explicit instructions in the assessment PDF.
2. `docs/ai/decision-ledger.md` and the ADRs.
3. The orchestration skill.
4. General best practice and other skills.

The ADRs have been revised to the ledger and are the primary design documents; the ledger records the decisions behind them. **If an ADR and the ledger disagree, do not pick one silently:** write it under "Open issues" in `docs/progress.md` and ask.

Do not invent a decision. If the ledger and the ADRs do not answer a question, write it under "Open issues" in `docs/progress.md` and ask; do not settle it silently in code. Do not change a ledger decision; if one looks wrong, say so and keep the work consistent with it. Only the author changes the ledger, or approves a change to it explicitly.

## Session start protocol

1. Run `git rev-parse --show-toplevel` and confirm it prints this repository's root. If it prints anything else, stop.
2. Read `docs/progress.md`.
3. Read the current phase in `docs/plan/implementation-plan.md`.
4. Read **only** the ledger entries and ADR sections that the phase names.
5. Create the phase branch from an up-to-date `main`.

## Session end protocol

1. Update `docs/progress.md`: what was done, open issues, decisions taken, the next step.
2. Commit.
3. Push the branch.
4. Open the pull request with `gh pr create`.

## Git workflow (D59)

- **Check the repository first.** Before the first commit of every session, `git rev-parse --show-toplevel` must print this project's own root, and `git status` must list nothing under `.inputs/`. Never use `git add -f` on `.inputs/`.
- **Branches:** `p<phase-number>/<short-topic>`, for example `p1/core-path`. Branches are kept after merge.
- **Commits:** Conventional Commits (`feat:`, `fix:`, `test:`, `docs:`, `chore:`, `ci:`, `refactor:`). One logical change per commit; each commit builds and passes its tests.
- **Trailer:** every commit message ends with

  ```
  Co-Authored-By: Claude <noreply@anthropic.com>
  ```

- **Sub-agents never commit or push.** The main loop reviews every diff before committing it.
- **Pull requests:** `git push` the branch, then `gh pr create` with a title, a summary, the ledger IDs implemented, test evidence (commands and output) and the checklist from `.github/pull_request_template.md`.
- **Never** push to `main`, never merge a pull request, never force-push. `main` is protected (D60); the author merges with "Rebase and merge".

## Engineering rules

- **Strict compliance with the assessment (G1).** Never contradict an explicit instruction of the PDF. Where it is ambiguous, follow the interpretation recorded in `docs/future-work.md`.
- **English only** in code, comments, commits and documents (G3).
- **No `time.Sleep` in tests.** Wait with the `eventually(condition, timeout)` helper.
- **Durations are configuration.** Every interval, timeout, lease and window comes from configuration and is set to milliseconds in tests (D52).
- **Every state transition goes through a repository function** in `internal/store`, and that function returns its `StatusChanged` event. No other code updates `notifications.status`.
- **`internal/domain` performs no I/O.**
- **Scope dropped → `docs/future-work.md` updated in the same commit.**
- **No secrets in the repository.** `.env` is git-ignored; `.env.example` holds names and local defaults only.
- **No personal data, local paths or recruiting details** in any tracked file.
- **Never log a recipient or message content.** Use the `Recipient` type for recipients (D42).
- **Demo and test data use only reserved fake values** (`+905551234567`, `example.com` addresses); requests to the default webhook.site URL are publicly visible, so real recipients never go through it (D71).
- **Generated code is never edited by hand.** Change `api/openapi.yaml` or the SQL, then run `make generate`.
- **Pin versions.** Container images use exact tags; GitHub Actions use full commit SHAs (D48, D53).

## Delegation

Follow `.claude/skills/fable-orchestration/SKILL.md`: the main loop plans, briefs, reviews and integrates; work is routed to the cheapest tier that can carry it; every `Agent` call names its `model`.

The concurrency-critical code is **reviewed by the main loop line by line**, whoever wrote it:

- the claim statement (transition 3);
- the fenced final writes and the late-success transition (4, 12);
- the pacer loop;
- the GCRA Lua script;
- the relay transaction (select, publish, confirms and returns, commit);
- reconciliation.

A delegate's report is a claim: check the diff and run the tests before reporting work as done.

## Commands

Defined in P0; until then this table is the target.

| Command | Purpose |
|---|---|
| `make generate` | `sqlc` and `oapi-codegen` |
| `make lint` | `golangci-lint` v2 |
| `make test` | Unit, integration and contract tests with `-race` (needs Docker) |
| `make test-docker` | The same inside a container (Docker only) |
| `make test-e2e` | Compose stack, end-to-end and chaos scenarios, `load-smoke` |
| `make load` | k6 load scenarios |
| `make verify-suite` | Mutation patches; each must fail a named test |
| `make demo` | At most 10 requests through the running stack |
| `make up-mock` | The stack with `docker-compose.mock.yml`: `mockprovider` as the provider |
| `docker-compose run --rm relay requeue-stale` | Return stale `queued` rows to `pending` (`--older-than=5m` by default, `--all`) |
| `docker-compose up` | The whole stack, as the assessment specifies |
