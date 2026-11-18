# Design review log

How the design was reviewed before any code was written, which AI mistakes the review caught, which critiques were kept or rejected, and which claims were turned into experiments instead of being trusted.

The decisions themselves are in the [decision ledger](decision-ledger.md) (`D1`–`D74`); the final design is in the [ADRs](../adr/). This log records the *path* to them. It is curated: no raw transcripts.

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

The design partner produced every item below with confidence. None was caught by the partner itself. The turn and finding references were checked against the raw review material. Three rows are not confirmed by it as written and are re-checked in P7: rows 10 and 13 have no matching finding there, and row 16 is recorded there as a miscount by the reviewer at Turn 0, not as a mistake of the design partner.

| # | Mistake | Consequence if shipped | Caught in | Fix |
|---|---|---|---|---|
| 1 | **Claim guard accepted only `status = 'queued'`.** The relay publishes before its transaction commits, so a fast worker sees `pending`, claims nothing and acks. | The message is dropped. The partner first estimated the impact as "a five-minute delay until the stale sweeper runs"; that estimate was also wrong, because the sweeper's progress signal was tied to claims, so on an idle lane the row stayed `queued` indefinitely. The review's only blocker. | T2 F1, T4 F1 | D1, D10: widened guard; the relay's row locks across the publish are now part of the design |
| 2 | Stale-queued sweeper conditions | Failed in both directions: missed stuck rows on idle lanes, republished healthy rows under load | T3 F1, F2 | D5: reconciliation on every reconnection, `requeue-stale`, oldest-queued-age alert |
| 3 | Outdated RabbitMQ and Kafka facts: quorum queues with two priority levels, Kafka without per-message nack, RabbitMQ without per-message delay, memory pressure as an argument for quorum queues | The broker choice and its rejected alternatives were argued from facts that were no longer true | T1 F2, F8, F9; T2 F5; T3 F4 | D7, D48; ADR-0001 alternatives rewritten |
| 4 | Weighted 6:3:1 dispatcher over nine lane queues | Contradicted the stated goal that urgent messages go first | T1 F3 | D7: built-in strict priority, one queue per channel |
| 5 | Connection errors consumed the failure budget | A provider outage would exhaust the budget of healthy notifications | T7 F2 | D27: `unreachable` outcome, provider-scoped, no budget |
| 6 | Go's HTTP client follows redirects by default | One attempt could become a request to a different URL | T7 F3 | D27: `CheckRedirect` returns `ErrUseLastResponse`; `3xx` is `provider_fatal` |
| 7 | A request with an `Idempotency-Key` header can be replayed by the transport | One attempt could become two sends | T7 F7 | D29: `GetBody = nil`; experiment E5 |
| 8 | Template allow-list checked node types only | Builtin functions and `define` bypassed it | T8 F1, F3 | D40: function names checked, multiple templates rejected |
| 9 | Lease did not include the rate-limit wait after the claim | Healthy attempts could be marked `unknown` | T3 N1 | D20 |
| 10 | `due_at` used as the start of the latency measurement | Retries and scheduled sends would inflate latency | T9 | D20, ADR-0009 |
| 11 | One table justified by TOAST | TOAST applies above 2 kB; the argument did not hold (the decision did) | T4 F4 | D21: rationale replaced |
| 12 | "Pace at exactly 100 msg/s" as the guarantee | Jitter can put 101 requests in one second | T7 F1 | D24: 98 msg/s with zero burst; D51 measures it |
| 13 | One pacer per process instead of per channel | One failing channel could slow the others | T5, T7 | D23; channel-isolation load test (D51) |
| 14 | Gateway kept a `last_version` map | A client learns the `id` after the first events may have passed | T5 F7, T6 F1 | D37: subscribe, then snapshot |
| 15 | Health endpoints only on the admin port | The required endpoint was unreachable from the host | T9 F1 | D33 |
| 16 | Container count in `architecture.md` (12 vs 15) | Inconsistent documents | T0 | Corrected |
| 17 | Provider contract defined nowhere | Body, response and headers were assumptions | T6 F4, T7 F4 | D28 |

## 3. Mistakes in the review process

- **Proposal reversed after objection.** For cancel after an attempt with outcome `unknown`, the partner proposed `409`. The reviewer objected that the client cannot act on it; the final rule allows the cancel and returns `may_have_been_sent: true` (D15, T7, revised).
- **A ledger decision replaced in the ledger review.** The first version of D8 handled long open circuits with cancel-and-resubscribe; the ledger review replaced it with a 24-hour `x-consumer-timeout`, kept as a fallback in E6.
- **Plan reconstructed instead of read.** The reviewer's final report was not among the preparation session's inputs, so the session reconstructed the plan from the ledger. A comparison with the real report found differences in the fallback order and in what is never dropped; they were corrected before merge (D73).
- **Ledger reviews found contradictions inside the ledger** (trace rule, transition 12 preconditions, the api's required dependencies), each now resolved (D13, D14, D39).

## 4. Choices questioned and kept

| Choice questioned | Decision | Reason |
|---|---|---|
| Default TTLs (high 10 min, normal 6 h, low 24 h) | Kept (D31, T7 F10) | The TTL bounds the largest burst each priority can absorb; the README states that bound |
| One `notifications` table | Kept; rationale replaced (D21, T4 F4) | Write volume at 294 msg/s is negligible; splitting hot columns is future work |
| The 1 s back-off floor | Kept (D68, preparation session) | Full jitter can produce near-zero delays; the floor protects the provider |
| Dead-letter queues after the custom delivery limit was removed | Kept as a safety net (D6) | The real defence is the failure budget, but a poison queue makes a broker-side failure visible |

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

## 7. What this shows about using AI here

- The partner's mistakes were not random: most were confident statements of outdated or unchecked facts (rows 3, 6, 7, 11) and races between two actors (rows 1, 2, 14). The test matrix is organised by actor pairs for this reason (D49).
- A second model with an explicit mandate to disagree found the blocker; agreement was never the goal.
- Unverified facts were converted into experiments and tests rather than settled by argument.
- `make verify-suite` (D54) applies the same idea to the code: removing a guarantee must make a named test fail.
