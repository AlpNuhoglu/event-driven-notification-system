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
| **api** | `TestCancel_Idempotent`; `TestCancel_ConcurrentSameNotification` (of several concurrent cancels exactly one applies); `TestIdempotencyKey_ConcurrentSameKey` | `TestCancel_DuringRelayPublish`; `TestRedrive_VersusExpiry` | `TestCancel_RacesWithClaim` (never both) | `TestCancel_VersusLeaseSweep` (a cancel never applies to a row the sweeper holds, and applies after the sweep with `may_have_been_sent`); `TestCancel_WhileProcessingReturns409` | `TestLateSuccess_AfterCancel` (`cancelled → sent`); `TestLateSuccess_AfterRedrive` |
| **relay** | — | `TestRelay_TwoRelaysNeverPublishSameRow` | **`TestClaim_WaitsForRelayCommit`**; **`TestE2E_WorkerFasterThanRelayCommit`** (the same race forced through the real broker: the relay's transaction is held open until the claim is seen waiting); `TestE2E_IdleSingleNotificationIsSent`; `TestClaim_DirectAfterRelayRollback`; `TestExpiry_VersusClaim` | n/a (disjoint states) | `TestLateSuccess_AfterReconcile`; `TestLateSuccess_AfterExpiry` (`failed → sent`) |
| **worker** | — | — | `TestClaim_TwoWorkersExactlyOne` | `TestLeaseSweeper_VersusFinalWrite` | `TestFencing_LateWriteAffectsZeroRows` |
| **lease sweeper** | — | — | — | `TestLeaseSweeper_TwoSweepersSkipLocked` | `TestLateSuccess_AfterLeaseSweep` |
| **late worker** | — | — | — | — | n/a (one late worker per attempt) |

The two tests in bold are the ones that catch the dropped-message defect: a claim issued while the relay's transaction is still open must wait and then succeed, at the store and through the real broker. `TestE2E_IdleSingleNotificationIsSent` is the plain proof that a single notification on an idle system reaches `sent`; it does **not** catch a narrowed claim guard, because without a forced order the relay's commit can win the race (on the development machine it always did: the relay commits about 1 ms after its publish returns). The mutation check in P1 showed this, and the forced-race test replaced it as the mutation target ([design review log](../ai/design-review-log.md) §6).

#### Other integration tests, by area

| Area | Test |
|---|---|
| Transitions | Every one of the 12 transitions increments `version`, sets `updated_at`, returns an event, and **leaves immutable columns untouched**; the database rejects rows that violate the status invariants ([ADR-0004](0004-data-model-state-machine-and-fencing.md) §6) |
| Claim | An expired row is failed at claim without an attempt row and without incrementing `attempt_count`; a `retrying` row that is not yet due is not claimed |
| Publish | `TestRelay_UnroutablePublishStaysPending`: with no binding, the row stays `pending`. **Repeated 100 times**, because it rests on the client's ordering of returns and confirms ([ADR-0001](0001-rabbitmq-as-dispatch-broker.md)). `TestRelay_PrioritySetOnEveryMessage`: every published message carries `6 − db_priority`. Lane HWMs hold. |
| `requeue-stale` | By default only `queued` rows older than the threshold return to `pending`; `--all` takes every `queued` row (`TestRequeueStale_OlderThan` in the store; `TestRequeueStaleCommand` through the binary's entry point, with RabbitMQ unreachable and the admin port taken; `TestParseRequeueStale`) |
| Cancel | A cancel after an `unknown` attempt reports `may_have_been_sent`; a batch cancel reports the `cancelled`, `not_cancellable` and `may_have_been_sent` counts ([ADR-0006](0006-api-contract-and-three-layer-idempotency.md) §7) |
| Reconciliation | On reconnection all `queued` rows return to `pending` and are republished; a trigger inside the 60-second window is deferred, not skipped; a broker that comes back empty has its topology again before the first publish. Store: **`TestReconcile_EveryQueuedRowReturnsToPending`** (360 rows over the nine lanes, the exact set), `TestReconcile_WaitsForClaim`, `TestReconcile_WaitsForCancel` (a cancel that rolls back: the row is reconciled, not skipped), `TestReconcile_VersusBatchCancel` (no deadlock: both lock in `id` order). Relay, on an injected clock: the first connection, a reconnection, `TestReconcile_TriggerInsideWindowIsDeferred`, three triggers in one window, `TestReconcile_NeverOverlapsALaneTransaction`, a failed run retried and not counted. Wired roles: `TestE2E_ReconciliationAfterReconnection` (the broker's vhost wiped while disconnected), `TestE2E_ReconciliationDeferredInsideWindow`, `TestE2E_ReconciliationAtStartup`, `TestE2E_BrokerComesBackEmpty`, `TestConnection_TopologyDeclaredAgainOnEmptyBroker` |
| Final write | With Postgres unavailable, the final write is retried and lands; transition 12 is tried when transition 4 affects zero rows. Worker, fake clock: `TestFinalWrite_RetriedUntilItLands` (one send, one record, one event), `_GivesUpAfterTheWindow`, `_DrainEndStopsTheRetry`, `_NotAppliedIsNotRetried`, `_LateSuccessIsLogged`, `TestAbort_RetriedUntilItLands`. Wired roles: `TestE2E_FinalWriteRetriedThroughOutage`, `TestE2E_FinalWriteOutageOutlastsLease` (the write lands through transition 12; one request at the provider), `TestE2E_LateSuccessAfterLeaseSweep`, `TestE2E_LateResultAfterNewerAttempt` |
| Late success (transition 12) | Beside the matrix tests: **`TestLateSuccess_NewerAttemptWins`** (after a newer claim the late result never applies, whatever the status: the fence), `TestLateSuccess_AlreadySentChangesNothing`, transition 12 in the invariants test from each of its five source states |
| Redrive | Keeps a future `expires_at`; requires a new one when it has passed; never resets `attempt_count` |
| Idempotency | An expired key is taken over; the hash covers method and path; a `dedupe_key` conflict through the partial-index `ON CONFLICT`; a key is released only when cancelled without an attempt; duplicates within a batch ([ADR-0006](0006-api-contract-and-three-layer-idempotency.md)) |
| Listing | **Pagination walk:** 500 rows created in the same second, `limit=7`, every row returned exactly once. An `EXPLAIN` check asserts, **under a custom plan**, that both listing queries are an ordered index scan with the keyset comparison as index condition and no sort, for a frequent and a rare status; it logs the generic plan of the status query, which sorts. A further test asserts that the listing never enters the prepared-statement cache, so the plan checked is the plan that runs ([ADR-0006](0006-api-contract-and-three-layer-idempotency.md) §8). |
| Outbox | With RabbitMQ stopped, the api keeps accepting; after restart, everything is published |
| Rate limiter | GCRA keeps every one-second window at or below 100 across several concurrent clients; the script returns a relative wait ([ADR-0007](0007-retry-policy-and-circuit-breaker.md) §5) |
| Fallback limiter (D25) | The in-process limiter keeps the interval under concurrent callers, grants no burst after idle time and divides the rate by the replicas. The wrapper switches on an error and answers the same call locally, does not switch on the caller's own cancellation, asks Redis at most once per probe interval by one caller, and returns on the first answer; the gauge follows. With real Redis behind a TCP proxy: reservations go on when Redis is unreachable, and a hanging Redis costs one call at most the timeout. Wired roles: `TestE2E_RedisDownSendingContinues` (no one-second window above the fallback rate), `TestE2E_WorkerStartsWithRedisUnreachable`. Compose: `TestComposeE2E_RedisDownSendingContinues` reads `limiter_fallback_active` from the worker's `/metrics` (1 with Redis stopped, 0 after it is started again) and the state of the alert `LimiterFallbackActive` from the stack's Prometheus (firing with Redis stopped, inactive again after it is back) |
| Circuit breaker (D26, D30) | The state machine on a fake clock (`internal/breaker`): **`TestBreaker_FiveConsecutiveFailuresOpen`**, the window rule at its thresholds and its expiry, the lazy half-open, one probe under 100 concurrent admissions, the doubling and its reset, `provider_fatal`, `throttled` with and without a hint, uncounted outcomes, late results, a concurrency test of the invariants. Wired roles: `TestE2E_CircuitProviderFatalOpensAtOnce` (`401`: far fewer requests than without a breaker, `failures = 0`, one error-level log line per opening), **`TestE2E_CircuitOpensAfterFiveConsecutiveFailures`** (exactly five requests before the circuit opens, later notifications keep their budgets), `TestE2E_CircuitOpenedAfterClaimAbortsTheAttempt` (D23), `TestE2E_CircuitThrottlingDoublesOnConsecutiveProbes`, `TestE2E_CircuitHalfOpenSendsOneProbe`, `TestE2E_CircuitChannelsAreIsolated`. Compose: `TestComposeE2E_CircuitOpensOn401WithoutFailures` |
| Claim during a Postgres outage | `TestClaimRetry_…` on the fake clock: a claim that fails n times is sent once, with one slot for the first try and one after the late claim; the back-off sequence; with every concurrency slot retrying the pacer takes and reserves nothing; shutdown nacks exactly once without waiting for the drain; the send deadline counts from the successful call. `TestE2E_WorkerDatabaseOutage`: the worker's database connection is cut and restored; every notification ends `sent` once, the limiter is not called during the outage, the poison queue is empty. On RabbitMQ 4.4.1: `TestConsumer_NackWithRequeueDoesNotCountTowardDeliveryLimit` (25 nacks, not dead-lettered) with the control `TestConsumer_RejectWithRequeueCountsTowardDeliveryLimit` (dead-lettered after 21 deliveries) ([ADR-0001](0001-rabbitmq-as-dispatch-broker.md)) |
| Connection epoch | `TestWorker_ConnectionLossWithBufferedMessages`: the connection to RabbitMQ is broken while messages sit in the worker's local buffer. Every notification ends `sent`, and no ack from an older connection epoch reaches the broker ([ADR-0005](0005-service-topology-and-status-events.md), D76). **Checked on the wire:** a tap on the test proxy records every delivery and every answer per channel session and the broker's close frames; an answer that names no unanswered delivery of its own session fails the test. Also `TestWorker_ClaimHeldAcrossReconnect` (a delivery held through a long claim retry: the late claim succeeds, nothing is acked for the old delivery, one send), `TestWorker_ShutdownWithStaleDeliveryInDispatch` (no nack), `TestConsumer_ChannelReopenAdvancesEpoch` (a channel error without a connection loss), and the three unit tests of the ack rule on the fake clock (stale at the ack, at the nack, in the buffer) |
| Consumer | `TestConsumer_ServerCancelResubscribes` and `TestWorker_ServerCancelResubscribes`: a `basic.cancel` from the broker is followed by a new subscription; the held message is delivered again and sent once (D8). `TestConsumer_ChannelClosedForGoodIsError`: a stream that ends for good still ends the role |
| Plan cache (D79) | `TestService_PoolPlansWithCurrentStatistics` in the api, the relay and the worker: `SHOW plan_cache_mode` answers `force_custom_plan` on a pooled connection, inside a transaction, on a connection opened after a reset, and with a URL that asks for another mode. `TestNoPoolIsOpenedOutsideOpenPool` fails when non-test code opens a pool any other way. `TestExpiry_RunsWithCustomPlans` also covers the relay's count |
| Pacer | The highest-priority buffered message is taken; the slot is kept when a claim affects zero rows; a circuit that opens after the claim yields `aborted` and `retrying` without budget. **Missed slot (D77):** a claim that returns later than slot + tolerance does not send then and sends on a new slot (deterministic, fake clock); a batch that arrives on an idle channel while the relay commits 30 ms later shows no one-second window above 100; a claim that returns after the send deadline ends `aborted` with `failures` unchanged |
| Classification | Through the store: every failure outcome with its budget accounting, the three reasons of an exhausted budget, the `Retry-After` hint with its cap, the throttle default, the expiry shortcut (`TestCompleteFailure_EveryOutcome` and its neighbours). Through the wired roles with `mockprovider` (`TestE2E_Fault…`): `500` twice then `sent`; `429` with and without `Retry-After`; `404` more often than `max_failures` with `failures = 0`; `400` rejected after one attempt; a closed connection as `unknown`; an unreachable provider that never exhausts a budget and expires; a redirect that is not followed; a malformed `2xx` body ([ADR-0007](0007-retry-policy-and-circuit-breaker.md) §2–4) |
| Expiry | `TestExpiry_VersusClaim` (both orders, concurrently, and each side holding the row lock: exactly one of them fails the row, one event); batches are bounded and oldest first; a backlog larger than one batch is failed within one tick; the pass runs while the broker connection is down ([ADR-0002](0002-postgres-as-outbox-scheduler-and-retry-delay.md)) |
| Idempotency-key cleanup | An expired key is deleted and a live one kept; `TestDeleteExpiredIdempotencyKeys_VersusTakeOver`: a take-over succeeds whether the delete runs before it, after it, or while its transaction is open (D78) |
| Provider client | The request body has exactly three fields (also asserted end to end on `mockprovider`'s report); a `202` body is parsed; another `2xx` is success and increments `provider_unexpected_2xx_total`; a redirect is not followed; the request is sent exactly once when the server closes the connection; connections are reused (`httptrace.GotConnInfo.Reused`) |
| Templates | Builtin functions and multi-template sources are rejected (`TestTemplateFunctionNameCheck_…`, `TestTemplateOneTemplateRule_…`, `TestTemplateWalker_RejectsConstructs`); an accepted source always renders and agrees with an independent reference (`FuzzCompileTemplate`); concurrent version creation produces consecutive versions (`TestCreateTemplateVersion_ConcurrentVersionsAreConsecutive`), concurrent creates of one name have one winner; the template stage of a create through HTTP on a real database (`templates_create_integration_test.go`: every error code, e-mail escaped and SMS not, an inactive template refused with a pinned version, partial success with the `dedupe_key` claim after the stage, the replay of a keyed request after a deactivation); one load and one compilation per template and request (`render_test.go`); the rendered text reaches the provider (`internal/integration/template_test.go`); the list's pagination walk (`TestListTemplatesAPI_PaginationWalk`); a boolean variable in an `if` (`TestTemplateRender_BooleanInIf`, `TestTemplate_BooleanVariables`); `TestComposeE2E_Templates` on the Compose stack ([ADR-0008](0008-templates-and-scheduling.md)) |
| Scheduling | The rules of `scheduled_at` and `expires_at` at their exact boundaries on an injected clock (`TestValidateCreate_Schedule…`, `TestCreate_ScheduleBoundariesThroughHTTP`); the stored rows (`TestCreate_Scheduled`, `TestBatch_Scheduled`); a keyed retry is replayed after the clock has moved past the rules (`TestCreate_ClockRulesDoNotBreakTheReplay`); through the wired roles a scheduled notification is not sent before its time and a cancelled one never (`internal/integration/schedule_test.go`); `TestComposeE2E_Scheduling` on the Compose stack: `sent_at` is not earlier than `scheduled_at`, the tolerance, a cancelled scheduled notification ([ADR-0008](0008-templates-and-scheduling.md)) |
| Status events | Every one of the 12 transitions reaches the events exchange through the wired roles, as an unbroken chain of versions per notification (`TestEvents_AllTwelveTransitionsReachTheExchange`); a full buffer drops events without holding the caller (`TestEventPublisher_FullBufferDropsWithoutBlocking`, and a cancel through the API in `TestEvents_FullBufferDoesNotBlockACancel`); the api starts and serves with RabbitMQ stopped (`TestService_ServesWithRabbitMQStopped`); a broker that is down is one dial per interval and one log line (`TestEventPublisher_BrokerDownDropsLogsOnceAndPacesDials`); publishing resumes after a lost connection, and the exchange exists again on an empty broker and after it was deleted; a role's stop sends what is buffered and is not held by a broker that does not answer (`TestEventPublisher_Close…`, `TestEvents_WorkerShutdown…`, `TestService_ShutdownDoesNotHangOnStalledBroker`, `TestRequeueStaleCommand_StalledBrokerDoesNotHoldIt`) |
| WebSocket | A client that subscribes after creation receives a snapshot with the row's state and version (`TestGateway_SnapshotAfterCreationHasTheRowsStateAndVersion`); **the subscription is registered before the snapshot is read** (`TestGateway_SubscriptionIsRegisteredBeforeTheSnapshotIsRead`: a transition commits after the read and its event is dispatched before the request has finished; the client must receive it); a batch subscription yields one message per notification; a full send queue closes the connection with 1013 and does not hold the other connections (`TestGateway_FullSendQueueClosesTheConnectionAndDoesNotBlockTheHub`); a client that does not read its batch snapshot is disconnected when the snapshot's deadline passes and delays nobody (`TestGateway_ClientThatDoesNotReadItsSnapshotIsDisconnected`, `…DelaysNobody`); a disallowed origin is refused before the upgrade; the limit is all or nothing; unsubscribe; every error code, and no error message repeats what the client sent; an event reaches a connection once through id and batch; shutdown sends 1001; a client that does not answer pings is disconnected. The gateway's queue is exclusive, auto-delete and bounded, and exists again after a reconnection and on an empty broker (`TestEventConsumer_…`); a write through the gateway's pool is refused (`TestOpenPoolWith_ReadOnlyRefusesWrites`). Compose: `TestComposeE2E_GatewayFollowsNotificationsToSent` (create → subscribe → snapshot → live events → `sent`, for one notification and for a batch) |
| Ids | One spelling everywhere (`TestParseID_…`); the four refused spellings on every operation with a path id and on `batch_id` (`TestPathID_OtherSpellingsAreRefused`, `TestListBatchID_OtherSpellingsAreRefused`), at the gateway, in the worker's message body (`TestConsumer_IdInAnotherSpellingGoesToPoisonQueue`) and in a status event; **the guard** `TestLenientUUIDParserIsCalledOnlyByParseID` type-checks every non-test package and names the file and line of a direct use of the library's parser or of a JSON, XML or parameter binding into a value that holds a UUID |
| Metrics | Every metric moves on its event with its labels, per role on a registry of its own (`TestMetrics_…` in `internal/api`, `relay`, `worker`, `gateway`); every role serves every metric name and every series of its fixed label sets before any traffic; the `route` label is the template and an unknown path creates no series (`TestMetrics_RouteLabelIsTheTemplateNeverThePath`); through the wired roles each final transition is counted once in the role that performed it, with `expired_total` by priority (`TestMetrics_FinishedCountedOnceByTheRoleThatFinished`); a direct claim observes no queue wait and a scheduled notification's latency counts from `scheduled_at` (`TestMetrics_DirectClaimAndScheduledLatency`); an expiry found at claim time is counted (`TestMetrics_ClaimOfAnExpiredNotificationCountsAnExpiry`); the gauge statements read only rows that are not finished (`TestGauges_PlansReadOnlyUnfinishedRows`) |
| Summary | Every number on a known dataset, with every outcome and the `success_rate` definition (`TestSummary_ValuesOnAKnownDataset`); nulls when there is nothing to measure; `backlog_due` cached separately (`TestSummary_BacklogDueIsCachedSeparatelyFromTheAnswer`); concurrent requests compute once (`TestSummaryService_ConcurrentRequestsOnColdCacheComputeOnce`); an error is not cached |
| Dashboard and alerts | `make check-rules`: `promtool check rules` and the rule unit tests of `alerts_test.yml` (the expiry rule by priority, the circuit rule on "not closed"); `TestDashboardSmoke` after the end-to-end run: the four role targets are up, the six rules are loaded, every panel query returns data |
| Readiness | The registry (`TestReadiness_…` in `internal/platform/health`: a failed check is a `503`, one that does not answer fails and does not hold the answer, the error text is not in the body); the gateway's three checks through a broken and restored broker connection and a closed pool (`TestReadiness_FollowsPostgresRabbitMQAndTheConsumer`) |
| Logging and redaction | The handler's fields, from the options and from the context (`TestHandler_…`); key-based redaction at every depth, through `With`, groups and maps, with valid JSON as the output (`TestRedact_DropsContentAndVariables`, `TestRedact_MasksRecipient`); the recipient masked in every formatting and encoding path, and real, byte for byte, on the three paths that must carry it (`TestRecipient_MaskedInEveryPath`, `TestSend_BodyCarriesTheRealRecipientByteForByte`, the store's and the API's tests); value-based redaction of a provider's error, also across the cut, without the plus sign and broken off at the read limit (`TestRedactProviderError_…`); the client hands on the body of an answer that is not a `2xx` up to its limit (`TestSend_ErrorBodyIsKeptUpToItsLimit`). **The roles' real paths into capturing loggers at `debug`,** against a provider that echoes the recipient and the content: no line holds a recipient, a content or a variable value, and every line of a notification carries its correlation id (`TestLogs_NoPersonalDataAndCorrelationIDsInEveryRole`, `TestLogs_FailureWithAnEchoingProviderError`); at `info` a sent notification is one line (`TestLogs_InfoLevelShowsOnlyTheFinalOutcome`); the api's request line is `debug`, `error` for a `5xx`, and holds no query, id or header value (`TestRequestLog_LevelsAndContent`) |
| PII canary scan | `TestCanaryScan` in the end-to-end run ([ADR-0009](0009-observability.md) §4): 63 canaries searched in 22 sources (the logs of every service, the exported spans, every `/metrics`, the Prometheus series, the mock provider's report, every answer of a failed request, every gateway message), each source with a control that must be found. **Shown to fail** in P5 with five planted leaks, one per kind: the content in a worker log line, the recipient as a span attribute, a metric label, a problem detail that echoes a value (also a truncated copy), a gateway error that echoes an id. A refused handshake repeats nothing (`TestGateway_RefusedHandshakeRepeatsNothingOfTheRequest`); a received request's span keeps only its list (`TestExporter_ServerSpanKeepsOnlyItsList`) |
| Tracing | In-memory exporter through the wired roles: one trace for a first attempt, with the chain request → publish → process → provider call, and the provider receives that call's `traceparent` (`TestTracing_FirstAttemptIsOneTrace`); a retry is a new trace linked to the request (`TestTracing_RetryIsNewTraceLinkedToTheRequest`); a scheduled delivery is a new linked trace on its first attempt (`TestTracing_ScheduledDeliveryIsNewLinkedTrace`); a client's `traceparent` is continued (`TestTracing_ClientTraceParentIsContinued`); no span holds the recipient or the content, and no span name an id; third-party spans leave cleaned (`TestExporter_SanitizesThirdPartySpans`); a role starts and stops with the collector unreachable (`TestService_StartsAndStopsWithUnreachableTracing`); the sampler variables are honoured. Checked by hand in P5: with the rule always choosing "child", the retry and the scheduled test fail |
| Health | `/healthz` and `/readyz` on the public port (the contract test); on the admin port the registry and each role's checks: the api and the relay `postgres`; the worker `postgres`, `rabbitmq` and `consumer` through a broken and restored broker connection, with Redis unreachable (`TestService_ReadinessFollowsPostgresBrokerAndConsumers`, `TestConsumer_ActiveFollowsTheSubscription`); the gateway's three |
| Demo | The provider probe against test servers: reachable, unreachable, a webhook.site URL with room, with too few requests left, expired, with an unreadable count; the provider URL itself never receives a request (`TestProbe…`). The run against fakes: only the reserved fake values, one request through the batch endpoint, an `Idempotency-Key` on each, events printed with the version rule, a replay said so, a count above 10 refused before anything is sent (`TestRun…`) |
| Observability | Metrics summary values on a known dataset; the rows above ([ADR-0009](0009-observability.md)) |

**Every decision in the [decision ledger](../ai/decision-ledger.md) has at least one test.** [Decision tests](../decision-tests.md) names them decision by decision, with the evidence for the decisions a Go test cannot check and the gaps that remain.

#### Chaos scenarios

| Scenario | Expected result |
|---|---|
| **RabbitMQ wiped with its data and restarted** | Every notification ends `sent`. This is the test of reconciliation and of topology redeclaration. |
| Postgres restarted during sends | Nothing is lost; duplicates at the provider are counted and **reported**, not failed; the poison queues stay empty (a claim that fails during the outage must not push its message toward the delivery limit, [ADR-0001](0001-rabbitmq-as-dispatch-broker.md)) |
| Low-traffic provider outage: three `high` notifications, provider down for 60 seconds | All three end `sent` with `failures = 0` |
| Worker killed with `SIGKILL` mid-send | The lease sweeper recovers its rows; duplicates are reported |
| Redis stopped | Sending continues on the fallback limiter; the fallback alert fires |
| Per-channel worker deployment (Compose override) | One scenario runs with a worker per channel, to keep that layout working |

**Every scenario runs on the Compose stack as a test of `make test-e2e`,** against the real containers. The broker, Postgres and `requeue-stale` scenarios have run since P3 (`test/e2e/chaos_test.go`), because they are the exit criteria of that phase, and the Redis scenario since the fallback limiter (`fallback_test.go`); P6 added the `SIGKILL` scenario, the 60-second provider outage, the fallback alert and the per-channel deployment. The per-channel scenario needs a stack of its own: `make test-e2e` has a second, short pass that starts the stack with `docker-compose.override.worker-per-channel.yml` under its own project, runs only that scenario (on the first stack it is skipped), and removes the stack again; like the first pass, a failed pass leaves every service's log in `e2e-logs/` (`e2e-logs/worker-per-channel/`).

| Test | What it does to the real containers | Asserted |
|---|---|---|
| `TestComposeE2E_RabbitMQRestartEveryNotificationSent` | `docker-compose restart rabbitmq` with 125 notifications queued and 25 in flight | `reconciliation_runs_total` grows, every notification ends `sent`, no service exited, the poison queues are empty; duplicates at the provider are counted |
| `TestComposeE2E_RabbitMQWipedEveryNotificationSent` | The broker stopped, its data volume removed, started again | The topology exists before the first publish (no channel error in the relay's or worker's log), reconciliation runs, every notification ends `sent` |
| `TestComposeE2E_RequeueStale` | `docker-compose run --rm relay requeue-stale`, then `--all`, with the worker and relay stopped | Exit code 0; exactly the rows queued longer than five minutes are `pending` again; after `--all` the rest; everything ends `sent` once |
| `TestComposeE2E_PostgresRestartDuringSends` | `docker-compose restart postgres` while 700 notifications are being delivered | Every accepted notification ends `sent`, none `failed`, the poison queues stay empty, every service healthy; duplicates are reported, not failed |
| `TestComposeE2E_ProviderOutageLowTraffic` | Three `high` notifications created with the `mockprovider` container stopped; it stays stopped for 60 real seconds, then is started again | During the outage no notification ends, every attempt is `unreachable` or `aborted` and `failures` is 0 (read in Postgres; the API does not expose it); afterwards all three end `sent`, still with `failures = 0`, success being each one's last attempt |
| `TestComposeE2E_WorkerSIGKILLMidSend` | `docker-compose kill worker` (`SIGKILL`) with 100 `high` sms created, the worker's 25 concurrency slots claimed and at least 10 requests at the mock, none answered yet (it answers after 3 s), then `docker-compose start worker` | The container exited with 137; `sweeper_recovered_total{kind="retrying"}` grows by at least the number of rows the kill left `processing`, each of which has an attempt closed as `unknown`; every notification ends `sent`, none `failed`, the dispatch and poison queues are empty; a notification reached the provider more than once only if the kill left it `processing`; duplicates are reported |
| `TestComposeE2E_RedisDownSendingContinues` | `docker-compose stop redis`, then `start` | Sending continues; `limiter_fallback_active` is 1 and the alert `LimiterFallbackActive` is firing in the stack's Prometheus; after Redis is back the gauge is 0 and the alert inactive; no duplicate |
| `TestComposeE2E_WorkerPerChannel` (second pass) | The per-channel layout; then `docker-compose kill worker-email` and `start` | No single worker, one worker per channel with its `WORKER_CHANNELS` and `WORKER_REPLICAS` 1, the three scraped as the job `worker`; 300 notifications per channel end `sent`, each channel's attempts made by its own worker only, no one-second window above 100 per channel; with the email worker killed, sms and push are sent and every email waits unsent, and after its start the emails are sent by it |

The scenarios of `chaos_*_test.go` run first in the first pass (file-name order), and each scenario leaves the stack clean, so the fault scenarios, the canary scan and `load-smoke` after them check the stack they leave. **Every chaos scenario ends with one line of the same shape,** read from the mock provider's report and written also when the scenario fails: `chaos report: <scenario>: accepted=<n> sent=<n> failed=<n> provider_requests=<n> duplicate_keys=<n> max_per_key=<n>`; it goes to the job summary in CI as well. Duplicates are reported there, never asserted on a fixed number. The e2e override sets `RECONCILE_MIN_INTERVAL` to one second for the e2e stack only, because the scenarios follow each other within seconds; the AMQP recovery interval stays at its default of 5 s, and the lease (30 s) and the lease sweeper's interval (5 s) stay at theirs, which the `SIGKILL` scenario waits for. A quorum queue's depth as `rabbitmqctl` reports it is refreshed about every 5 s, so a test waits until a depth has read 0 for 6 s before it calls a queue empty. Docker does not restart a container killed through its API (`docker-compose kill`), whatever its restart policy: it treats the kill as a manual stop. The `SIGKILL` scenarios therefore check the exit code 137 and start the container themselves, as the restart policy does after a crash.

**Determinism.**

- `domain/` takes a `Clock` interface and receives jitter as an argument.
- **All durations are configurable** (lease, tick, sweep intervals, reconciliation window, breaker durations, timeouts) and are set to milliseconds in tests. Times computed in SQL from `now()` cannot be controlled by an injected clock, so tests shorten the durations instead; rows are back-dated only for expiry and lease edge cases.
- Tests never use `time.Sleep` to wait for asynchronous effects; an `eventually(condition, timeout)` helper polls instead. Sleep-based waiting is the main source of flaky tests.
- **A test container's start is repeated when it fails** (up to six attempts with a pause that doubles, in `internal/testutil`; each failed attempt is written to the output). Only the start is repeated, never anything a test asserts. Two failures of the start were seen in ten shuffled runs of `make test` in P6 and reproduced with 14 test binaries starting containers at once. **The reaper race:** testcontainers-go v0.45.0 shares one reaper container between the test binaries of one `go test` invocation, and a binary that finds it between another binary's create and start reports the status `created` as a permanent error (16 of 40 rounds); the next attempt finds it running. **The missing port:** Docker Engine 29.9.2 on Docker Desktop now and then starts a container without the host port it was asked for (2 of 40 rounds). It comes in bursts for one test binary: 60 seconds and more in one run, three containers in a row over 35 seconds in another, where the fourth attempt started normally. Why Docker leaves the port out is not known (a host process holding the next ports does not cause it; that was tried), so the retry makes this failure rarer and does not exclude it.
- **A test does not assume how fast its own loop runs.** The same runs found two assertions that did: one required that no event found the publisher's buffer full while the test published on every poll, one required every rate-limit slot to be exactly one interval after the previous. Both now state the property without the assumption (`TestEventPublisher_ReconnectsAfterConnectionLoss`, `TestZeroBurstSlotsAreOneIntervalApart`). The first CI run of that pull request, on a slower runner, found two more: a test that read the relay's other gauges as soon as the first was set (`TestMetrics_GaugeLoopSetsEveryLaneFromEachMeasurement`), and one whose forced order needed a dispatch to wake within the 5 ms slot tolerance (`TestE2E_CircuitOpenedAfterClaimAbortsTheAttempt`, which now sets its own tolerance). A third failure of that run was not a test's assumption but a defect of the provider client ([ADR-0007](0007-retry-policy-and-circuit-breaker.md) §2, "Why not the hook alone").

### 2. `mockprovider`

A separate binary (`cmd/mockprovider`) with an admin API for fault injection. It implements the provider contract of [ADR-0007](0007-retry-policy-and-circuit-breaker.md) §6 and is the provider for **all automated tests and load tests**. It is not the default provider of the Compose stack; that is webhook.site ([ADR-0005](0005-service-topology-and-status-events.md)).

| Capability | Purpose |
|---|---|
| Configurable error rate, latency distribution and specific status codes, per channel | Retry, circuit-breaker and channel-isolation scenarios |
| Failing the first N requests of each `Idempotency-Key` | A deterministic "fails N times, then succeeds" for one notification, without changing the configuration mid-test |
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
| `make test-e2e` | Starts the Compose stack with the mock override, under its own Compose project so that it never touches a developer's running stack; runs the e2e and chaos scenarios, `load-smoke`, the canary scan and the dashboard smoke test; always stops the stack afterwards. Then a second pass on a stack of its own with the per-channel worker override, for the one scenario of that layout (section 1). `load-smoke` logs the largest one-second window it saw, and every chaos scenario its `chaos report:` line. | Go and Docker (the tests run on the host against the stack) |
| `make load` | k6 scenarios against the Compose stack | Docker |
| `make test-docker` | Same as `make test`, inside a Go container | Docker only |
| `make verify-suite` | The mutation patches of section 8 | Go and Docker |
| `make demo` | A probe of the provider URL, then at most 10 requests through the running stack | Docker |
| `make fuzz` | Extended fuzzing (optional, time-boxed) | Go |

- Integration tests use the `//go:build integration` tag, so `go test ./...` in an editor stays fast; `make test` enables the tag.
- **`make test-e2e` adds a third Compose file,** `test/e2e/docker-compose.e2e.yml`, for the e2e stack only: it shortens the durations the scenarios wait for (the circuit's open period after a `provider_fatal` answer, 2 s there). `make up-mock` and `docker-compose up` never use it.
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
| Claim guard reverted to `status = 'queued'` | `TestClaim_WaitsForRelayCommit` and `TestE2E_WorkerFasterThanRelayCommit` |
| Fencing condition removed from the final write | The late-write test |
| Pacer set to exactly 100 msg/s | The deterministic pacer test (fake clock, fixed jitter) |
| Redirect following re-enabled | The redirect test |
| Builtin template functions allowed (checked by hand in P4: the function-name check deleted from the walker; a builtin is then still refused by the argument check, but for another reason, which the tests assert; and the stronger form in which a builtin with two string arguments is really accepted) | `TestTemplateFunctionNameCheck_RejectsEveryBuiltinInCallPosition` (`printf`, `call` and the 17 others), `TestTemplateFunctionNameCheck_RejectsPrintfAndCallElsewhere`; in the stronger form also `FuzzCompileTemplate`'s seeds |
| The one-template rule removed from the template walker (candidate, checked by hand in P4) | `TestTemplateOneTemplateRule_RejectsHarmlessSecondTemplate`, `TestTemplateOneTemplateRule_RejectsSelfNamedDefine` |
| `mandatory` removed from the publish | The unroutable-publish test |
| The five-consecutive-failures rule removed from the breaker (candidate, checked by hand in P3) | `TestBreaker_FiveConsecutiveFailuresOpen` and `TestE2E_CircuitOpensAfterFiveConsecutiveFailures` |
| The claim's retry replaced by the immediate nack (candidate, checked by hand in P3) | The `TestClaimRetry_…` tests and `TestE2E_WorkerDatabaseOutage` |
| The `attempt_count` fence removed from transition 12 (candidate, checked by hand in P3) | `TestLateSuccess_NewerAttemptWins` |
| One status removed from transition 12's list (candidate, checked by hand in P3) | For `cancelled`: `TestLateSuccess_AfterCancel`; each of the five has its own test |
| Reconciliation made to skip one queued row (candidate, checked by hand in P3) | `TestReconcile_EveryQueuedRowReturnsToPending` |
| The gateway's snapshot read moved before the registration of the subscription (candidate, checked by hand in P5) | `TestGateway_SubscriptionIsRegisteredBeforeTheSnapshotIsRead` |
| The epoch check removed from the ack (candidate, checked by hand in P3) | `TestWorker_ConnectionLossWithBufferedMessages` (on the wire: the broker closes the channel with `406 unknown delivery tag`) |

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
