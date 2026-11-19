# ADR-0006: API contract and three-layer idempotency

- **Status:** Accepted
- **Date:** 2026-11-18
- **Related:** [ADR-0002](0002-postgres-as-outbox-scheduler-and-retry-delay.md), [ADR-0004](0004-data-model-state-machine-and-fencing.md), [ADR-0005](0005-service-topology-and-status-events.md), [ADR-0007](0007-retry-policy-and-circuit-breaker.md), [ADR-0008](0008-templates-and-scheduling.md), [ADR-0009](0009-observability.md)

## Context

The API must support creating single and batch notifications (up to 1,000 per request), querying status by ID or batch ID, cancelling pending notifications, and listing with filters (status, channel, date range) and pagination (R8–R13). The processing engine must validate content (character limits, required fields) and support **idempotency to prevent duplicate sends** (R17, R18).

Delivery is asynchronous, so the API acknowledges acceptance rather than delivery. Clients retry on network failures, and campaign batches can be re-split and resubmitted. Duplicates can therefore enter the system at several points, and each one needs its own defence.

## Decision

### 1. Contract-first OpenAPI

- `api/openapi.yaml` is **the single source of truth**. Go types and the server interface are generated with `oapi-codegen` in strict-server mode for the standard-library router.
- The spec is served at `/openapi.yaml`, with Swagger UI at `/docs`. The Swagger UI assets are **embedded in the binary** (`go:embed`), so the page works offline. The page is `swagger-ui-dist` 5.35.0, vendored unchanged with its checksums in a test; `/docs` and its assets are served next to the contract, not described by it, and the page's Content-Security-Policy allows nothing from another origin, which is what makes "works offline" checkable.
- CI fails if the generated code drifts from the spec ([ADR-0010](0010-testing-and-ci.md)). Contract tests validate every response, including errors, against the spec with `kin-openapi`.
- All business paths are versioned under `/v1`.

### 2. Endpoints

| Method | Path | Success | Notes |
|---|---|---|---|
| `POST` | `/v1/notifications` | `202` | Single create; `Location` header |
| `POST` | `/v1/notifications/batch` | `202` / `422` | Up to 1,000 items; partial success (section 6) |
| `GET` | `/v1/notifications/{id}` | `200` | Includes `version` ([ADR-0005](0005-service-topology-and-status-events.md)) |
| `GET` | `/v1/notifications/{id}/attempts` | `200` | Delivery attempt history ([ADR-0004](0004-data-model-state-machine-and-fencing.md)) |
| `GET` | `/v1/notifications` | `200` | Filters and keyset pagination (section 8) |
| `POST` | `/v1/notifications/{id}/cancel` | `200` / `409` | A state transition, so `POST` rather than `DELETE` |
| `POST` | `/v1/notifications/{id}/redrive` | `200` / `409` | `failed → pending` (transition 11) |
| `GET` | `/v1/batches/{id}` | `200` | Batch summary and per-status counts |
| `POST` | `/v1/batches/{id}/cancel` | `200` | Returns the counts `{cancelled, not_cancellable, may_have_been_sent}` |
| `*` | `/v1/templates/...` | | See [ADR-0008](0008-templates-and-scheduling.md) |
| `GET` | `/v1/metrics/summary` | `200` | Real-time metrics as JSON ([ADR-0009](0009-observability.md)) |
| `GET` | `/healthz`, `/readyz` | `200` / `503` | Health checks on the **public port**, part of the OpenAPI spec ([ADR-0009](0009-observability.md) §7) |
| `GET` | `/openapi.yaml`, `/docs` | `200` | Spec and Swagger UI |
| `WS` | `/v1/ws` (gateway) | | Real-time status updates (section 10) |

Prometheus metrics and `pprof` are served on each service's separate admin port ([ADR-0009](0009-observability.md) §8).

**Request headers:**

| Header | Rule |
|---|---|
| `Idempotency-Key` | Optional. Applies **only to the two create endpoints** (section 11); other endpoints ignore it. A key is 1 to 255 visible ASCII characters (U+0021 to U+007E), sent once; anything else, an empty value included, is a `400` (`invalid_idempotency_key`) rather than a request that silently runs without idempotency |
| `X-Request-ID` | Optional. Accepted if it is at most 128 characters of `[A-Za-z0-9._-]`; otherwise it is ignored and a new one is generated. Echoed in the response ([ADR-0009](0009-observability.md) §5) |
| `traceparent` | W3C Trace Context |

**Request size.** Every endpoint limits the body to 5 MB (`http.MaxBytesReader`); a larger body returns `413`.

### 3. Request model

```json
{
  "channel": "sms",
  "recipient": "+905551234567",
  "priority": "high",
  "content": "Your verification code is 482913",
  "template": null,
  "scheduled_at": null,
  "expires_at": "2026-11-18T10:15:00Z",
  "dedupe_key": "otp-user42-login-8812"
}
```

| Field | Rule |
|---|---|
| `channel` | Required: `sms`, `email` or `push` |
| `recipient` | Required; validated per channel (section 5) |
| `priority` | `high`, `normal` (default) or `low` |
| `content` / `template` | **Exactly one is required.** `template` is `{"name": "...", "version": 3, "variables": {...}}`, where `version` is optional and defaults to the latest one, and `variables` is optional. Templates are rendered at creation time ([ADR-0008](0008-templates-and-scheduling.md) §5). `"template": null` means that the member is absent, as in the example above; every other member keeps its rule that `null` is not a value. Neither of the two is `content` / `required`; both is `template` / `conflicts_with_content`. The object is as strict as the body: an unknown member is `template.<member>` / `unknown_field`. |
| `scheduled_at` | Optional, RFC 3339 with an offset; `null` means absent. A value up to 30 seconds in the past means "now" (clock-skew tolerance) and is **stored as `NULL`**; further in the past is rejected (`in_the_past`). At most 30 days ahead (`too_far_ahead`) ([ADR-0008](0008-templates-and-scheduling.md)). |
| `expires_at` | Optional. Must satisfy `expires_at >= COALESCE(scheduled_at, now()) + 30s` (`too_soon` otherwise). If omitted, it defaults to `due time + TTL` from the priority profile ([ADR-0007](0007-retry-policy-and-circuit-breaker.md) §3). Both times are judged by the API's clock, read once per request. |
| `dedupe_key` | Optional, 1–255 characters. Record-level idempotency (section 11) |

The request has the four fields the assessment names (recipient, channel, content, priority) plus the optional ones above. There is no `subject` field.

**A field the API does not know is rejected** (`422`, code `unknown_field`), never ignored: a client that sends a field of a later version, or misspells one, must not get a different behaviour silently. The optional fields above arrived with their phases, the last ones (`template`, `scheduled_at`, `expires_at`) in P4. A repeated key is rejected as well (`duplicate_field`).

### 4. Response model

```json
{
  "id": "0193f0d2-7c1e-7b3a-9d41-5be0a1c2e3f4",
  "batch_id": null,
  "channel": "sms",
  "recipient": "+905551234567",
  "priority": "high",
  "status": "pending",
  "version": 1,
  "sms": { "encoding": "GSM-7", "segments": 1 },
  "template": null,
  "scheduled_at": null,
  "expires_at": "2026-11-18T10:15:00Z",
  "attempt_count": 0,
  "failure_reason": null,
  "provider_message_id": null,
  "created_at": "2026-11-18T10:05:00Z",
  "updated_at": "2026-11-18T10:05:00Z",
  "sent_at": null
}
```

All timestamps are RFC 3339 in UTC. `version` lets clients order status events (section 10). `template` is `{"name", "version"}` for a notification created from a template (the version that produced the content), `null` otherwise; the variables are not returned, as the content is not.

### 5. Content validation

Validation runs **after** template rendering, because variables can push content over a limit.

| Channel | `recipient` | Content limits |
|---|---|---|
| **SMS** | E.164, checked with `nyaruka/phonenumbers`. The default check is `IsPossibleNumber` (length and structure for the country code); strict validation against national numbering plans (`IsValidNumber`) can be enabled by configuration. The input must already be in E.164 form: `05551234567` is rejected with an explanatory error rather than silently normalised, because normalising would mean guessing a country. A test covers the assessment's example number, `+905551234567`. | At most **10 segments** (below) |
| **Email** | `net/mail.ParseAddress`, **and** the parsed `Address` must equal the input. This rejects display-name forms such as `"Ali <ali@x.com>"`. At most 254 characters. | Body ≤ 100 KB (102,400 bytes of UTF-8) |
| **Push** | Opaque device token, 1–4,096 characters, no control characters | Total payload ≤ 4 KB (the APNs and FCM limit), applied to the content: at most 4,096 bytes of UTF-8 |

`content` is required on every channel, directly or through a template.

The error codes are `invalid_e164`, `invalid_email` and `invalid_token` for the recipient, `too_many_segments` for an SMS and `too_long` for an email or push content over its limit. Each field has at most one error, and a channel-specific check runs only when the field passed the general ones.

**SMS encoding and segments.** The encoding is detected automatically:

| Encoding | Unit counted | Single segment | Per segment when concatenated |
|---|---|---|---|
| GSM-7 | Septets | 160 | 153 |
| UCS-2 | UTF-16 code units | 70 | 67 |

- Characters from the GSM-7 extension table (`€ [ ] { } ^ ~ | \`) cost **two septets**: an escape and the character. The table of the standard also holds the form feed character, which is counted the same way.
- **An escape sequence is never split across two segments, and neither is a surrogate pair** (a character outside the Basic Multilingual Plane, such as most emoji, takes two UTF-16 code units). When one would straddle a boundary, it moves to the next segment, so a message can need more segments than its length divided by the segment size suggests.
- Turkish characters such as `ş`, `ğ` and `ı` are not in the GSM-7 basic set. A single one forces the whole message into UCS-2, which more than doubles the segment count and therefore the cost.
- The response reports `{"encoding", "segments"}` so this cost is visible to the caller.
- Support for the GSM national language shift tables is [future work](../future-work.md).

### 6. Batch semantics: partial success

The request body is `{"notifications": [ ... ]}`: an object with that one field, whose items have the fields of the single create (section 3). It is as strict as the single create: another top-level field, or an unknown field inside an item, is rejected. A top-level error (`notifications` missing, not an array, empty, or over the limit) is one `422` and no item is checked; otherwise every item is validated on its own.

| Outcome | Status | Body |
|---|---|---|
| At least one item accepted | `202` | `{"batch_id", "accepted": [{"index", "id"}], "rejected": [{"index", "errors": [...]}]}`, with `Location: /v1/batches/{batch_id}` |
| Every item rejected | `422` | Problem Details listing the per-index errors in a `rejected` member of the same shape as in the `202`, complete and never truncated; no batch is created |
| More than 1,000 items | `422` | `code: too_many_items`; nothing is processed |

- One invalid phone number must not block the other 999 messages in a flash sale.
- `207 Multi-Status` is not used, because it is WebDAV-specific. There is no atomic mode (YAGNI).
- **Duplicate `dedupe_key` values inside the same batch** are rejected in the application, before the database, with `code: duplicate_in_batch`. Otherwise `ON CONFLICT DO NOTHING` would silently skip the second item. Among the items that pass validation, the first one with a key keeps it and every later one is rejected; an invalid item never claims its key.
- **Insert:** one statement, `INSERT ... SELECT FROM unnest($1::uuid[], ...) ON CONFLICT (dedupe_key) WHERE dedupe_key IS NOT NULL AND NOT (status = 'cancelled' AND attempt_count = 0) DO NOTHING RETURNING id, dedupe_key`. The `WHERE` clause in the `ON CONFLICT` target is required: Postgres can only infer a **partial** unique index if the predicate is repeated. `COPY` is not used, because a single conflict would abort the whole operation. **The rows are inserted in `dedupe_key` order** (`ORDER BY` in the `SELECT`): an insert that meets another transaction's uncommitted entry for the same key waits for it, so two concurrent batches whose keys overlap in different orders would otherwise deadlock (`40P01`); a test runs such batches against each other.
- **Conflicting items:** `RETURNING` lists only inserted rows, so the keys that were not inserted are looked up with one more query that uses the same predicate (`WHERE dedupe_key = ANY($1) AND NOT (status = 'cancelled' AND attempt_count = 0)`). They are reported as `{code: "duplicate", existing_id}`. If the conflicting row is cancelled between the two statements, the error has no `existing_id` member. This race is harmless. The single create reports its conflict the same way.
- The `batches` row records `total` (items in the request), `accepted` (items stored) and `rejected` (items rejected in validation plus duplicates). Per-status progress is computed with `GROUP BY status`; `GET /v1/batches/{id}` returns the three counts and `status_counts`, with every status present.
- Templates are recommended for large email batches, because of the 5 MB body limit.

### 7. Cancel and redrive

| Operation | Behaviour |
|---|---|
| Cancel a notification | "Pending" in the requirement is read as *not yet held by a worker*: cancel is allowed from `pending`, `queued` and `retrying` (transition 9). If the notification is already `cancelled`, the call returns `200` (idempotent). From `processing`, `sent` or `failed` it returns `409` with the current status (`current_status` in the problem document). The answer is `{"notification": {...}, "may_have_been_sent": true or false}`. The request has no body; one that is not empty is a `400`. |
| `may_have_been_sent` | If the notification has an attempt whose outcome is `unknown`, the cancel response includes `"may_have_been_sent": true`. The cancel still succeeds, so a client can always stop further retries; the flag says that one delivery may already have reached the recipient. The member is always present, `false` otherwise. **It is not stored:** cancel changes only the status, and the flag is read from the attempts each time, so a late result (transition 12) has nothing to undo and the repeated cancel answers with the flag as it is then. |
| `cancelled → sent` | A cancelled notification can later become `sent` if a late result proves that it was delivered ([ADR-0004](0004-data-model-state-machine-and-fencing.md), transition 12). The same applies to `failed`. The OpenAPI description of both states says so. |
| Cancel a batch | A single statement: it selects the cancellable rows of the batch `ORDER BY id FOR UPDATE` and updates them (`UPDATE ... WHERE batch_id = $1 AND status IN (...)` with the rows locked in a fixed order first). **Why the order:** a plain `UPDATE` locks rows in the order its scan meets them. Two cancels of one batch deadlocked (`40P01`) when their sessions scanned in different orders (a sequential scan against an index scan over rows that updates had moved inside their pages: 43 deadlocks in 600 rounds); when both sessions met the rows in the same order they never did (720 rounds), and the ordered statement never did in any setting (1,080 rounds). A batch holds at most 1,000 rows, so this takes milliseconds. The response gives three counts: `cancelled`, `not_cancellable`, and `may_have_been_sent`, the number of cancelled notifications that have an attempt with outcome `unknown`. **The counts describe the batch after the call:** `cancelled` includes notifications cancelled earlier and `not_cancellable` is every other notification of the batch, so the call can be repeated like the single cancel: a second call publishes events only for what it cancels itself, which is nothing unless a notification that a worker held has returned to `retrying` in between (in a manual run against a failing provider, a second call cancelled two more). Up to 1,000 events are published asynchronously after commit and the response returns immediately. Cancelling a whole campaign that spans many batches is [future work](../future-work.md). |
| Redrive | Allowed only from `failed`; otherwise `409`. Resets the retry budget and `due_at`, never `attempt_count`. The stored `expires_at` is kept if it is still in the future. If it has passed, the request must include a new `expires_at` at least 30 seconds ahead (`EXPIRES_AT_MIN_LEAD`); without one the call returns `409` (`expires_at_required`), and one that is closer is a `422` (`too_soon`) ([ADR-0004](0004-data-model-state-machine-and-fencing.md), transition 11). **A request that gives `expires_at` while the stored one is still in the future is refused** with `409` (`expires_at_not_allowed`): the stored value is kept, and a request field is never silently ignored. That `409` carries the stored `expires_at` (D16 as amended). The body is optional and strict: its only field is `expires_at`. The rule is decided by the database clock inside the transition's statement, so a redriven notification is never already expired. |

### 8. Listing and pagination

- **Filters:** `status`, `channel`, `priority` and `batch_id`, **each a single value**, and `created_from` / `created_to` (RFC 3339, half-open interval `[from, to)`). The requirement asks for filtering by status, channel and date range (R13).
- **Query parameters are as strict as bodies.** A parameter the operation does not define (`unknown_parameter`), a parameter given twice (`duplicate_parameter`), an invalid value or a `limit` outside 1 to 200 (`invalid_parameter`) and a cursor that does not decode (`invalid_cursor`) are each a `400`; nothing is clamped or ignored, so a misspelled filter never returns an unfiltered list. Only the two listings define query parameters (this one, and the list of templates with `limit` and `cursor`, [ADR-0008](0008-templates-and-scheduling.md) §9): every other operation under `/v1/` rejects any.
- **Ordering:** `created_at DESC, id DESC`.
- **Keyset cursor:** an opaque, base64-encoded `(created_at, id)` (24 bytes: the time in microseconds and the id). `limit` defaults to 50, with a maximum of 200. A row created while a client pages never appears in a later page, and no row is skipped or repeated. A cursor has exactly one accepted spelling; anything else is `invalid_cursor`.
- **Response:** `{"data": [...], "next_cursor": "..." | null}`.
- **No `total_count`, deliberately.** `COUNT(*)` over millions of rows is expensive and would cancel out the benefit of keyset pagination.
- **Implementation:** two `sqlc` queries, one with and one without the status filter, so that each is served in order by an index ([ADR-0004](0004-data-model-state-machine-and-fencing.md) §7). An `EXPLAIN` check and a pagination walk test guard this ([ADR-0010](0010-testing-and-ci.md)). The cursor and the date bounds are written with `COALESCE` against the largest and smallest possible values, never as `$n IS NULL OR …`, so the keyset comparison is always an index condition.
- **Both queries always run with a custom plan** (the unnamed statement, planned with the bound values; they never enter the driver's statement cache). The optional filters (`$n IS NULL OR col = $n`) make the row estimate of a generic plan meaningless: in the `EXPLAIN` check the generic plan of the status query was a bitmap scan of the whole status followed by a sort, which a prepared statement switches to after a few executions. With custom plans the planner chooses by the data: the status index for a rare status, and for a frequent one either listing index, in both cases in order and without a sort. The cost is one planning per listing request. A test checks that the listing statements are not in `pg_prepared_statements`.
- **Attempts:** `GET /v1/notifications/{id}/attempts` returns `{"data": [...]}` in attempt order; the history is bounded by the retry budget, so it is not paginated.

### 9. Errors: RFC 9457 Problem Details

```json
{
  "type": "https://github.com/<owner>/<repo>/blob/main/docs/errors.md#validation",
  "title": "Validation failed",
  "status": 422,
  "detail": "2 field(s) are invalid",
  "correlation_id": "0193f0d2-...",
  "errors": [
    { "field": "recipient", "code": "invalid_e164", "message": "recipient must be an E.164 number such as +905551234567" }
  ]
}
```

`Content-Type: application/problem+json`. `code` values are stable and machine-readable; `message` is for humans.

| Status | Used for |
|---|---|
| `400` | Malformed JSON or invalid query parameters |
| `404` | Unknown notification, batch or template |
| `409` | A transition that is not allowed (cancel, redrive; problem type `conflict`, with `current_status`); a `dedupe_key` conflict on single create (`code: duplicate`, with `existing_id`); a template name that already exists (problem type `template-exists`) |
| `413` | Request body over 5 MB |
| `422` | Validation failure; more than 1,000 batch items (`code: too_many_items`); an `Idempotency-Key` reused with a different request |
| `503` | Postgres unavailable |
| `405`, `415`, `500` | A known path with another method (with `Allow`); a create whose `Content-Type` is not `application/json`; an unexpected error, whose cause is never in the answer |

Every non-2xx answer the server can produce is a problem document, including the ones the router writes for an unknown path or method. The problem types and the `code` values are listed in [`docs/errors.md`](../errors.md). Timestamps carry the database's microsecond precision.

### 10. WebSocket protocol (gateway)

| Direction | Message |
|---|---|
| Client → server | `{"action": "subscribe", "notification_ids": [...], "batch_id": "..."}` and `{"action": "unsubscribe", ...}` |
| Server → client | `{"type": "status_changed", "id", "batch_id", "status", "version", "failure_reason", "updated_at"}` |

- The gateway builds these messages from the `notification.status_changed` events that api, relay and worker publish after each transition ([ADR-0005](0005-service-topology-and-status-events.md)).
- **Snapshot on subscribe.** After registering a subscription, the gateway sends the current state of each subscribed notification as a `status_changed` message with its current `version`; for a batch, one message per notification ([ADR-0005](0005-service-topology-and-status-events.md)). A client can therefore create a notification, read its `id` from the `202` response and subscribe afterwards without missing anything.
- **Client rule:** ignore any message whose `version` is less than or equal to the version already known.
- At most 1,000 subscriptions per connection, counted as `len(notification_ids) + len(batch_ids) <= 1000`. A batch subscription counts as one, however many notifications the batch contains. Ping/pong every 30 seconds.
- **Origin check:** cross-origin connections are accepted only from the origins listed in configuration.
- **Slow clients:** each connection has a bounded send queue; when it is full, the gateway closes the connection.
- Delivery is best-effort; `GET` remains authoritative.

### 11. Idempotency in three layers

The requirement places idempotency under the *processing engine*, with the goal of **preventing duplicate sends** (R18). Duplicates originate in three distinct places, and each layer closes one of them:

| Layer | Duplicate it prevents | Mechanism |
|---|---|---|
| **Request** | A client retrying the same HTTP request after a network failure | `Idempotency-Key` header |
| **Record** | The same business event submitted in different requests (e.g. a batch re-split and resubmitted) | `dedupe_key` field |
| **Send** | At-least-once publishing, reconciliation, lease expiry | Claim guard, fencing token, late-success transition, provider `Idempotency-Key` ([ADR-0004](0004-data-model-state-machine-and-fencing.md), [ADR-0007](0007-retry-policy-and-circuit-breaker.md) §6) |

#### Request layer: `Idempotency-Key`

The semantics follow the IETF *Idempotency-Key HTTP Header Field* draft and Stripe's model. The header is honoured on `POST /v1/notifications` and `POST /v1/notifications/batch` only.

| Situation | Response |
|---|---|
| Same key, same request | The **original response** (same status and body), plus `Idempotent-Replayed: true` |
| Same key, different request | `422`: key reused with a different request |
| Same key, concurrent requests | The second request **waits** for the first to finish, then replays its response |

The keys live in Postgres, in an `idempotency_keys` table, **written in the same transaction as the notifications**. A Redis-based store could end up with a notification created but no key recorded, or the reverse.

The key is claimed first, before any other work:

```sql
INSERT INTO idempotency_keys (key, request_hash, created_at, expires_at)
VALUES ($1, $2, now(), now() + interval '24 hours')
ON CONFLICT (key) DO UPDATE
  SET request_hash = EXCLUDED.request_hash,
      created_at = EXCLUDED.created_at,
      expires_at = EXCLUDED.expires_at,
      response_status = NULL,
      response_body = NULL
  WHERE idempotency_keys.expires_at <= now()
RETURNING key;
```

| Result | Meaning | Action |
|---|---|---|
| A row is returned | The key is new, or had expired and is taken over atomically | Create the notifications in the same transaction, then store `response_status` and `response_body` |
| No row is returned | A live key exists | Read it. The same hash replays the stored response; a different hash returns `422` |

- **Concurrency needs no extra lock.** `INSERT ... ON CONFLICT` waits for a concurrent uncommitted insert of the same key. If that transaction rolls back, the waiting request proceeds as new. The draft's `409 Conflict` for in-progress requests is therefore never needed.
- **Canonical hashing.** `request_hash` covers the **method, the path** and the parsed and re-serialised request body, not the raw bytes. JSON key order and whitespace do not matter, and the same key used on the other create endpoint counts as a different request.
- **Validation failures are not stored.** As in Stripe's model, a request rejected before any work is done (malformed, or a `422` batch with no accepted items) does not record its key, so the client can fix the request and retry with the same key. Requests that created anything, including partially successful batches, are stored. **The rule is "stored if and only if something was created":** a single create refused with `409` for its `dedupe_key`, and a batch whose remaining items were all duplicates, roll back and leave no key either.
- **What a replay returns.** The stored status and body, with `Idempotent-Replayed: true`. The body is stored as `jsonb`, so it is the same JSON value, not necessarily the same bytes (key order and whitespace can differ). It is the answer of the first request, so a notification that has been sent since still shows `pending` in it. `Location` is rebuilt from the stored id, and `X-Request-ID` is the correlation id of the replaying request, not of the first one.
- **A reused key** is its own problem type (`idempotency-key-reused`, `422`), so a client can tell it from a validation failure.
- **Retention:** 24 hours (`IDEMPOTENCY_KEY_TTL`). Expired keys are taken over on write, so correctness never depends on the cleanup. **The relay deletes expired keys** next to the expiry pass: batches of 1,000, ordered by `expires_at`, selected with `FOR UPDATE SKIP LOCKED`, every `IDEMPOTENCY_CLEANUP_INTERVAL` (1 minute by default), and again at once while a batch comes back full (D78). The stored response body contains the recipient, so the retention must be real: an expired key is removed, not merely no longer honoured. The table also stops growing. The delete never waits for a request: a key that a request holds (a take-over in an open transaction, or a replay being read) is skipped, a key taken over meanwhile is live again and kept, and a take-over that meets a running delete waits for it and then inserts the key as new.
- **Scope:** global, because there are no tenants. A future multi-tenant version would scope keys by `(tenant_id, key)`.

**A request that renders a template looks its key up first.** The claim runs inside the create transaction, after the request was validated; for a request that uses a template, validation includes rendering, and that can give a different result on the retry (the template gained a version or was deactivated). Such a request therefore reads its key before it renders, with the liveness rule of the claim and without a lock: the same hash replays the stored response, another hash is the `422` of a reused key, and no live key lets the request go on to the claim, which still decides between concurrent requests ([ADR-0008](0008-templates-and-scheduling.md) §5). **So does a request that fails only the rules judged against the clock** (`in_the_past`, `too_far_ahead`, `too_soon`): it may be the retry of a request that was valid when it was first sent, so with a key it is answered from the key if the key is live for the same request, and with its `422` otherwise. Every other validation error is the same for a request and its retry, so such a request was never accepted and no key is read. All other requests are not changed.

#### Record layer: `dedupe_key`

- Enforced by `UNIQUE (dedupe_key) WHERE dedupe_key IS NOT NULL AND NOT (status = 'cancelled' AND attempt_count = 0)` ([ADR-0004](0004-data-model-state-machine-and-fencing.md) §7). Every `ON CONFLICT` and every conflict look-up repeats this predicate.
- **Single create:** a conflict returns `409` with `code: duplicate` and the `existing_id`. **Batch create:** the item is rejected with the same code (section 6).
- **A `dedupe_key` is permanent, with one exception.**
  - A notification that was **cancelled before any attempt** leaves the index, so the client can resubmit. Such a row has no attempt, so no late result can ever move it to `sent`, and it can never re-enter the index and conflict.
  - A notification that was **cancelled after an attempt** keeps its key. It may have been delivered, and it can still become `sent` through a late result.
  - A **failed** notification keeps its key. The way to retry it is redrive. A notification that failed with `max_failures_unknown_outcome` may in fact have been delivered, so a new record with the same key would risk a duplicate.
- The tension between this unique index and future table partitioning is recorded in [future work](../future-work.md).

## Alternatives considered

| Alternative | Why it was rejected |
|---|---|
| **A `subject` field (email subject, push title)** | The assessment defines a notification as recipient, channel, content and priority, and its provider request has no field for a subject. Accepting a field that is stored and never sent would mislead callers. Recorded in [future work](../future-work.md) for real provider integrations. |
| **Multi-valued list filters** (`status=a,b`) | The requirement asks for single values. Optional multi-value filters cannot be expressed cleanly in `sqlc`, and an `IN` list is not served in order by the listing indexes. |
| **`409` when cancelling a notification that has an `unknown` attempt** | It would also prevent a client from stopping further retries. The cancel succeeds and reports `may_have_been_sent` instead. |
| **Releasing the `dedupe_key` on every cancellation** | A notification cancelled after an ambiguous attempt may already have been delivered; a resubmission with the same key would then be a duplicate send. |
| **Strict phone validation (`IsValidNumber`) by default** | Stricter than "content validation" requires, and it rejects valid numbers when numbering-plan metadata lags, as well as common test numbers. Available by configuration. |
| **Health checks on the admin port only** | The admin port is not published to the host, so the health check endpoint the requirements ask for (R22) would be unreachable for an API consumer. |
| **Swagger UI loaded from a CDN** | The documentation page would not work offline. |
| **Code-first OpenAPI (`swaggo` annotations)** | Annotations drift from the code over time. Generating from the spec makes drift a CI failure. |
| **All-or-nothing batches**, or an opt-in `?mode=atomic` | Forces clients to clean lists themselves; the extra mode adds untested branches (YAGNI). |
| **`207 Multi-Status`** | WebDAV-specific. |
| **`DELETE` for cancel** | Cancel is a state transition; the resource remains queryable. |
| **Offset pagination, or a `total_count` field** | Both degrade on large tables. |
| **Idempotency store in Redis** | Not transactional with the notification insert. |
| **Hashing the raw request body** | Semantically identical requests with different key order or whitespace would be rejected as reuse. |
| **`409` for concurrent requests with the same key** | Unnecessary when the second request can wait on the unique index. |
| **Regex-only phone validation** | Checks the format only; `IsPossibleNumber` also checks the length for the country code. |
| **Automatic normalisation of national phone formats** | Requires guessing a country; an explicit error is safer. |
| **`COPY` for batch inserts** | A single conflict aborts the whole copy. |

## Consequences

**Positive**

- The contract is explicit, versioned and enforced by CI and contract tests.
- Duplicates are handled at each point where they can originate, and each layer has a clear owner.
- Partial batch success matches campaign workloads.
- A client that subscribes over WebSocket after creating a notification still sees its state.
- SMS cost is visible to callers, which is particularly relevant for Turkish-language traffic.

**Negative / costs**

- There is no authentication. It is out of scope for this assessment and recorded in [future work](../future-work.md) (API keys, which would also introduce tenants).
- `cancelled` and `failed` can change to `sent`. Clients that treat them as final must be told; the OpenAPI spec and the README say so.
- `dedupe_key` uniqueness is permanent, which grows the index and complicates future partitioning.
- Batch requests need two statements in the conflict case (insert, then look up existing IDs).
- Phone validation depends on an external library (`nyaruka/phonenumbers`) and its metadata updates.

## Review history

Changes made after the design review, recorded in the [design review log](../ai/design-review-log.md):

- The WebSocket protocol gained a snapshot on subscribe, an origin check and a slow-client rule, because a client learns the notification `id` only after the first events may have passed. (D37)
- `subject` was removed from the request, the response and validation, to match the notification the assessment defines. (D22)
- Cancel and the `dedupe_key` index were redefined around ambiguous attempts (`may_have_been_sent`; the key is released only when no attempt was made), and "no transition out of `cancelled`" was corrected. (D15, D14)
- Health endpoints moved onto the public port; list filters became single-valued; redrive keeps a client's `expires_at`. (D33, D34, D16)
- Previously undefined responses and limits were fixed: `dedupe_key` conflict, more than 1,000 items, `Idempotency-Key` scope and hash, `X-Request-ID` format, SMS counting units, phone validation level, embedded Swagger UI. (D35, D36, D38)
- In P4 the `template` member was built: `null` means absent, the two exclusion errors and the strict member object were specified, the response gained `template`, and a request that renders a template looks its `Idempotency-Key` up before rendering; `scheduled_at` and `expires_at` were built with their codes, and a request that fails only their clock rules is answered from its key when it is a retry. (D40)
