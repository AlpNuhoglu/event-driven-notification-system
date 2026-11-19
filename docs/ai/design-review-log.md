# Design review log

How the design was reviewed before any code was written, which AI mistakes the review caught, which critiques were kept or rejected, and which claims were turned into experiments instead of being trusted.

The decisions themselves are in the [decision ledger](decision-ledger.md) (`D1`–`D76`); the final design is in the [ADRs](../adr/). This log records the *path* to them. It is curated: no raw transcripts.

> **Status:** first draft, written at the end of the design phase while both sides of the review were still available. P7 completes section 4 from the raw review material and adds the implementation-phase entries (section 6).

## 1. How the review ran

| Step | Who | Output |
|---|---|---|
| Design discussion | The author with a Claude model as design partner | Ten ADRs, `architecture.md`, `future-work.md` |
| Checklist | Reviewer (Claude Fable 5.2), Turn 0 | Every requirement of the assessment as an ID (R1–R36), with interpretations |
| Adversarial review | Reviewer, Turns 1–12, one document per turn | Findings (`F`), proposals (`P`), notes (`N`); each claim labelled *Verified*, *Reasoned* or *Unverified*; rejected alternatives steelmanned; at most three improvement proposals per document |
| Final report | Reviewer | Cross-document findings, the fallback principle, the day plan |
| Triage | Author with the design partner | Each finding accepted, revised or rejected, recorded in the ledger with its source (`T2 F1` = Turn 2, finding 1) |
| Ledger reviews (two) | Reviewer | Inconsistencies inside the ledger itself |
| Preparation session | Implementation agent | ADRs revised to the ledger; plan, `CLAUDE.md`, progress log; ten open questions settled (D62–D73) |

The review prompt asked for judgement, not agreement: the reviewer had to say when a document was wrong, when a fix was disproportionate, and which of its own claims it had not verified. The sanitised prompt is added to this folder in P7.

## 2. Mistakes in the design that the review caught

The design partner produced every item below with confidence. None was caught by the partner itself. The turn and finding references were checked against the raw review material.

| # | Mistake | Consequence if shipped | Caught in | Fix |
|---|---|---|---|---|
| 1 | **Claim guard accepted only `status = 'queued'`.** The relay publishes before its transaction commits, so a fast worker sees `pending`, claims nothing and acks. | The message is dropped. During triage, the partner first estimated the impact as a five-minute delay until the stale sweeper runs; that estimate was also wrong, because the sweeper's progress signal was tied to claims, so on an idle lane the row stayed `queued` indefinitely. The review's only blocker. | T2 F1, T4 F1 | D1, D10: widened guard; the relay's row locks across the publish are now part of the design |
| 2 | Stale-queued sweeper conditions | Failed in both directions: missed stuck rows on idle lanes, republished healthy rows under load | T3 F1, F2 | D5: reconciliation on every reconnection, `requeue-stale`, oldest-queued-age alert |
| 3 | Outdated RabbitMQ and Kafka facts: quorum queues with two priority levels, Kafka without per-message nack, RabbitMQ without per-message delay, memory pressure as an argument for quorum queues | The broker choice and its rejected alternatives were argued from facts that were no longer true | T1 F2, F8, F9; T2 F5; T3 F4 | D7, D48; ADR-0001 alternatives rewritten |
| 4 | Weighted 6:3:1 dispatcher over nine lane queues | Contradicted the stated goal that urgent messages go first | T1 F3 | D7: built-in strict priority, one queue per channel |
| 5 | Connection errors consumed the failure budget | A provider outage would exhaust the budget of healthy notifications | T7 F2 | D27: `unreachable` outcome, provider-scoped, no budget |
| 6 | Go's HTTP client follows redirects by default | One attempt could become a request to a different URL | T7 F3 | D27: `CheckRedirect` returns `ErrUseLastResponse`; `3xx` is `provider_fatal` |
| 7 | A request with an `Idempotency-Key` header can be replayed by the transport | One attempt could become two sends | T7 F7 | D29: `GetBody = nil`; experiment E5 |
| 8 | Template allow-list checked node types only | Builtin functions and `define` bypassed it | T8 F1, F3 | D40: function names checked, multiple templates rejected |
| 9 | Lease did not include the rate-limit wait after the claim | Healthy attempts could be marked `unknown` | T3 N1 | D20 |
| 10 | One table justified by TOAST | TOAST applies above 2 kB; the argument did not hold (the decision did) | T4 F4 | D21: rationale replaced |
| 11 | "Pace at exactly 100 msg/s" as the guarantee | Jitter can put 101 requests in one second | T7 F1 | D24: 98 msg/s with zero burst; D51 measures it |
| 12 | The worker loop was described ambiguously, allowing a reading in which idle goroutines send in a burst | A burst would break the limit of 100 messages per second per channel (R15) | T5 F3 | D23: one pacer per channel with a fixed order of operations; the channel-isolation load test (D51) is supporting evidence |
| 13 | Gateway kept a `last_version` map | A client learns the `id` after the first events may have passed | T5 F7, T6 F1 | D37: subscribe, then snapshot |
| 14 | Health endpoints only on the admin port | The required endpoint was unreachable from the host | T9 F1 | D33 |
| 15 | Provider contract defined nowhere | Body, response and headers were assumptions | T6 F4, T7 F4 | D28 |

## 3. Mistakes in the review process

- **Proposal reversed after objection.** For cancel after an attempt with outcome `unknown`, the author's first answer, drafted with the design partner, was `409`. The reviewer objected that the client cannot act on it; the final rule allows the cancel and returns `may_have_been_sent: true` (D15, T7, revised).
- **A ledger decision replaced in the ledger review.** The first version of D8 handled long open circuits with cancel-and-resubscribe; the ledger review replaced it with a 24-hour `x-consumer-timeout`, kept as a fallback in E6.
- **Plan reconstructed instead of read.** The reviewer's final report was not among the preparation session's inputs, so the session reconstructed the plan from the ledger. A comparison with the real report found differences in the fallback order and in what is never dropped; they were corrected before merge (D73).
- **The reviewer's own miscount.** At Turn 0 the reviewer counted 12 containers from the overview, while ADR-0005 stated 15. The documents were consistent; the count was the reviewer's mistake.
- **Ledger reviews found contradictions inside the ledger** (trace rule, transition 12 preconditions, the api's required dependencies), each now resolved (D13, D14, D39).

## 4. Choices questioned and kept

| Choice questioned | Decision | Reason |
|---|---|---|
| Default TTLs (high 10 min, normal 6 h, low 24 h) | Kept (D31, T7 F10) | The TTL bounds the largest burst each priority can absorb; the README states that bound |
| One `notifications` table | Kept; rationale replaced (D21, T4 F4) | Write volume at 294 msg/s is negligible; splitting hot columns is future work |
| The 1 s back-off floor | Kept (D68, preparation session) | Full jitter can produce near-zero delays; the floor protects the provider |
| Dead-letter queues after the custom delivery limit was removed | Kept as a safety net (D6) | The real defence is the failure budget, but a poison queue makes a broker-side failure visible |
| End-to-end latency measured from `COALESCE(scheduled_at, created_at)`, not from `due_at` | Kept; the reviewer attacked it in Turn 9 and could not break it | `due_at` is overwritten on every retry, so a message retried for hours would look instant; `created_at` alone would make scheduled notifications look late (ADR-0009 §1) |

Critiques and proposals of the reviewer that were not adopted:

| Critique or proposal | Outcome | Reason |
|---|---|---|
| Repair the stale-queued sweeper with a worker heartbeat and a "healthy since" window (the reviewer's own fix for T3 F1 and F2) | Not adopted; the sweeper was replaced by reconciliation on reconnection (D5) | Compared side by side, reconciliation is simpler, deterministic and easier to test; the reviewer agreed |
| Cut transition 12, the late-success rescue (T4 P1) | Withdrawn by the reviewer in Turn 6; kept (D14) | Once the cut criterion became whether correctness can be shown by tests, two deterministic tests were enough |
| Keep the mock provider as the default target, disclosed as a deviation (T5 F8) | Rejected; webhook.site is the default (D44) | The author chose strict compliance with the assessment's explicit instruction (G1). The reviewer then challenged the form of that decision, because free URLs expire and are capped, and three safeguards were added |

*P7: complete from the raw review material, including the critiques rejected outright and the reasons.*

## 5. Claims turned into experiments

The review labelled several facts *Unverified*: they came from documentation or memory and nobody had run them. They were not argued further; each became a first-hour experiment in P0 with a pass criterion and a fallback (ledger section I) or an assumption with a named confirming test (section J).

| Claim | Check |
|---|---|
| Quorum-queue strict priority, default priority 4 | E1 |
| strict-server with the standard-library router | E2 |
| testcontainers inside a container on macOS | E3 |
| amqp091-go connection and topology recovery | E4 |
| No transport replay with `GetBody = nil` | E5 |
| `x-consumer-timeout` on quorum queues | E6 |
| `basic.return` arrives before its confirm | U8, repeated integration test |

## 6. Implementation phase

*Added from P0 onwards: experiment results, AI mistakes caught in code review or by `make verify-suite`, and any decision changed by evidence.*

| Phase | What was assumed | What the evidence showed | What changed |
|---|---|---|---|
| P1 | The plan and ADR-0010 named two tests that "would have caught the dropped-message defect" (a claim guard of `status = 'queued'` only): the store-level wait-for-commit test and the end-to-end test of a single notification on an idle system. The closing mutation check of P1 was to show both failing. | With the guard narrowed, the store-level test failed every time, and the idle end-to-end test **did not fail once in 220 runs**. With one message on an idle system the relay commits about 1 ms after its publish returns, and the worker's claim (delivery, a Redis reservation, then the claim) arrives later, so the narrowed guard still finds a `queued` row. Which side wins depends on the machine: the test could not be relied on to catch the defect. | A forced-race test, `TestE2E_WorkerFasterThanRelayCommit`, holds the relay's transaction open until the worker's claim is seen waiting on the row lock, through the real broker; it fails every time under the mutation and replaced the idle test as the mutation target. The idle test stays as the plain proof that one notification is sent. ADR-0010, the plan and D54 were corrected. The claim about what a test catches was only settled by removing the guarantee and running it. |
| P2 | ADR-0006 §6 specified the batch insert as one `INSERT … SELECT FROM unnest(…) ON CONFLICT … DO NOTHING` and discussed conflicts only against rows that already exist. Twelve review turns and two ledger reviews did not ask what two batches do to each other while both are still open. | Two concurrent batches with the same `dedupe_key` values in opposite orders **deadlocked in 3 of 3 runs** (`40P01`): each insert waits on the other transaction's uncommitted entry in the unique index. The scenario is the one `dedupe_key` exists for, a campaign batch that is re-split and resubmitted. **It was found by the concurrency test written with the store layer, not by review.** | The batch insert orders its rows by `dedupe_key`, so every batch takes its index entries in one global order; `TestCreateBatch_ConcurrentOverlappingKeysNoDeadlock` fails without the `ORDER BY` and passes with it. ADR-0006 §6 states the rule. |
| P2 | D34 and ADR-0006 §8: two `sqlc` queries, with and without the status filter, are each "served in order by its index", and an `EXPLAIN` check tied to a test guards it. The design assumed that the plan seen in `EXPLAIN` is the plan that runs. | The check was run under both plan modes. With a **generic plan** (what a prepared statement switches to after a few executions) the status query was a bitmap scan of every row of the status followed by a sort: the optional filters (`$n IS NULL OR col = $n`) leave the planner with an estimate of one row, so the sort looks free. With a custom plan the same text is an ordered index scan with the keyset as index condition. An `EXPLAIN` with literal values would have passed while production sorted. | Both listing statements always run with a custom plan (the unnamed statement), the `EXPLAIN` test asserts the custom plans for a frequent and a rare status and logs the generic plan as the reason, and `TestList_RunsWithCustomPlans` checks that the listing never enters the prepared-statement cache. ADR-0006 §8 states the rule. Found by the test, not by review. |

## 7. What this shows about using AI here

- The partner's mistakes were not random: most were confident statements of outdated or unchecked facts (rows 3, 6, 7, 10) and races between two actors (rows 1, 2, 13). The test matrix is organised by actor pairs for this reason (D49).
- A second model with an explicit mandate to disagree found the blocker; agreement was never the goal.
- Unverified facts were converted into experiments and tests rather than settled by argument.
- `make verify-suite` (D54) applies the same idea to the code: removing a guarantee must make a named test fail.
