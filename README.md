# Event-Driven Notification System

> **Work in progress.** The core path is implemented: a notification created through the API is stored, published, rate-limited and sent to the provider, notifications can be created in batches with two layers of duplicate prevention (`Idempotency-Key`, `dedupe_key`), and they can be listed, cancelled and redriven. Per-channel content validation, templates, scheduling, the WebSocket gateway and the dashboards follow in the phases of the [implementation plan](docs/plan/implementation-plan.md); [progress](docs/progress.md) shows where the work stands. This README grows with it.

A notification service written in Go that accepts SMS, Email and Push notifications through a REST API and delivers them asynchronously through an external provider. It is built around a Postgres outbox, a RabbitMQ dispatch broker with strict priority, a per-channel rate limit of at most 100 messages per second, retries classified by failure scope, and real-time status tracking.

## Quick start

**Requirements:** Docker with Docker Compose v2.24.0 or later (`docker-compose version`). Nothing else; no `.env` file is needed.

```
docker-compose up
```

For a shell you get back, use `docker-compose up -d --wait`. This starts four application containers (`migrate` runs once and exits, then `api`, `relay` and `worker` keep running) and six infrastructure containers (Postgres, RabbitMQ, Redis, Prometheus, Grafana, Jaeger). If a host port is taken on your machine, set `API_PORT`, `GRAFANA_PORT`, `PROMETHEUS_PORT` or `JAEGER_UI_PORT`, for example `API_PORT=18080 docker-compose up`.

Create a notification and read it back:

```
curl -fsS localhost:8080/healthz && curl -fsS localhost:8080/readyz
curl -fsS -X POST localhost:8080/v1/notifications \
  -H 'Content-Type: application/json' \
  -d '{"channel":"sms","recipient":"+905551234567","content":"hello","priority":"high"}'
curl -fsS localhost:8080/v1/notifications/<id>
```

Within a few seconds the notification shows `"status":"sent"`; `sent` means the provider accepted the message.

**The default provider is a public webhook.site URL.** Every request sent to it is publicly visible to anyone who has the URL. Use only the fake values shown above (`+905551234567`, `example.com` addresses) and never a real recipient. A free URL accepts 50 requests in total (observed on 2026-11-18: the 51st is answered with `429`) and expires after seven days (a URL that no longer exists answers `404`), so send only a few requests to it. The URL was created on 2026-11-18. To use your own URL: `PROVIDER_URL=https://webhook.site/<your-uuid> docker-compose up`.

**The mock profile** is for anything beyond a few requests. `make up-mock` starts the same stack plus `mockprovider`, a local fake provider, and points the worker at it, so nothing leaves your machine. `curl -fsS localhost:8090/admin/report` shows what the provider received (field names only, never values). `make down` stops the stack and removes its volumes.

`make test` runs the tests (needs Go and Docker). `make test-e2e` starts the stack with the mock profile under its own Compose project, runs the end-to-end tests and `load-smoke` (a minute of SMS at the full rate, checked against the limit of 100 per second at the provider) against it, and removes it again (needs Go and Docker).

## API examples

**Run these against the mock profile (`make up-mock`), not the default stack.** A batch sends one provider request per accepted item, and the default provider is a public URL that accepts 50 requests in total. With the mock profile nothing leaves your machine; `make down` removes the stack again. The full contract is served at `localhost:8080/openapi.yaml`, and every error code is listed in [`docs/errors.md`](docs/errors.md).

**Create a batch** of up to 1,000 notifications. Every item is checked on its own: the valid ones are stored, the others are listed with their position in the request.

```
curl -sS -X POST localhost:8080/v1/notifications/batch \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: flash-sale-42-part-1' \
  -d '{"notifications":[
        {"channel":"sms","recipient":"+905551234567","content":"Flash sale starts now","priority":"low","dedupe_key":"sale-42-user-1"},
        {"channel":"email","recipient":"user@example.com","content":"Flash sale starts now"},
        {"channel":"fax","recipient":"+905551234567","content":"x"}]}'
```

The answer is `202` with `Location: /v1/batches/<batch_id>`:

```json
{
  "batch_id": "01a1f112-75f0-7619-966c-fe88d0c89a27",
  "accepted": [
    {"index": 0, "id": "01a1f112-75f0-761e-a851-0ab87f732194"},
    {"index": 1, "id": "01a1f112-75f0-7620-9c52-79c2f55f5b21"}
  ],
  "rejected": [
    {"index": 2, "errors": [{"field": "channel", "code": "invalid_value", "message": "channel must be one of: sms, email, push"}]}
  ]
}
```

If no item can be stored, the answer is `422` with the same `rejected` list and no batch is created. More than 1,000 items is `422` with the code `too_many_items`, and nothing is processed. Request bodies are strict: a field the API does not define is rejected, never ignored.

**Read the batch:** the counts of the request and the number of its notifications in each status.

```
curl -sS localhost:8080/v1/batches/<batch_id>
```

**Retry safely with `Idempotency-Key`** (both create endpoints). Sending the request above again with the same key creates nothing: the answer is the stored answer of the first request, with the header `Idempotent-Replayed: true`. Two requests with the same key that arrive at the same time are handled the same way: the second waits for the first. The same key with a different request is refused with `422` (`idempotency_key_reused`). A key is kept for 24 hours (`IDEMPOTENCY_KEY_TTL`), and only for requests that created something, so a request that failed validation can be corrected and sent again with its key.

**Prevent a duplicate of the same business event with `dedupe_key`**, also across different requests. While a notification holds a key, a second one with the same key is refused: a single create answers `409` with the id of the existing notification, and a batch rejects that item and stores the others.

```
curl -sS -X POST localhost:8080/v1/notifications \
  -H 'Content-Type: application/json' \
  -d '{"channel":"sms","recipient":"+905551234567","content":"Flash sale starts now","dedupe_key":"sale-42-user-1"}'
```

```json
{
  "type": "https://github.com/AlpNuhoglu/event-driven-notification-system/blob/main/docs/errors.md#duplicate",
  "title": "Duplicate notification",
  "status": 409,
  "detail": "A notification with this dedupe_key already exists.",
  "correlation_id": "01a1f112-7611-7bba-94b7-2ff1e3099424",
  "errors": [
    {"field": "dedupe_key", "code": "duplicate", "message": "a notification with this dedupe_key already exists", "existing_id": "01a1f112-75f0-761e-a851-0ab87f732194"}
  ]
}
```

**List notifications**, newest first, with single-valued filters (`status`, `channel`, `priority`, `batch_id`, `created_from`, `created_to`) and a cursor. `limit` is 1 to 200 (50 by default); pass `next_cursor` back as `cursor` until it is `null`. There is no total count. A parameter the API does not define, or an invalid value, is a `400`: a misspelled filter never returns an unfiltered list.

```
curl -sS 'localhost:8080/v1/notifications?status=sent&channel=sms&created_from=2026-11-19T00:00:00Z&limit=2'
curl -sS 'localhost:8080/v1/notifications?status=sent&channel=sms&created_from=2026-11-19T00:00:00Z&limit=2&cursor=<next_cursor>'
```

```json
{"data": [{"id": "...", "status": "sent", "...": "..."}, {"id": "...", "status": "sent", "...": "..."}], "next_cursor": "AAZgllqIilIBofE-Fqp437HYv1fJakgk"}
```

**Read the delivery attempts** of a notification (one entry per attempt, with its outcome and the provider's status code):

```
curl -sS localhost:8080/v1/notifications/<id>/attempts
```

**Cancel** a notification that no worker holds yet (`pending`, `queued`, `retrying`), or every such notification of a batch. Cancelling again answers `200` again. A notification that is `processing`, `sent` or `failed` answers `409` with `current_status`.

```
curl -sS -X POST localhost:8080/v1/notifications/<id>/cancel
curl -sS -X POST localhost:8080/v1/batches/<batch_id>/cancel
```

```json
{"notification": {"id": "...", "status": "cancelled", "...": "..."}, "may_have_been_sent": false}
```

```json
{"cancelled": 580, "not_cancellable": 20, "may_have_been_sent": 0}
```

`may_have_been_sent` is `true` when an earlier attempt ended without a known result: the cancel stops further attempts, but one delivery may already have reached the recipient. **`cancelled` and `failed` are not strictly final:** if the provider's result arrives late and shows that the message was accepted, the notification becomes `sent`.

**Redrive** a `failed` notification: it becomes `pending` with a new retry budget, and its attempt history continues. If its `expires_at` has passed, the request must give a new one, at least 30 seconds ahead; otherwise the stored one is kept and the request must not give one.

```
curl -sS -X POST localhost:8080/v1/notifications/<id>/redrive
curl -sS -X POST localhost:8080/v1/notifications/<id>/redrive \
  -H 'Content-Type: application/json' -d '{"expires_at":"2026-11-20T12:00:00Z"}'
```

To try cancel and redrive on the mock profile, make the mock provider fail first with `curl -X PUT localhost:8090/admin/config -H 'Content-Type: application/json' -d '{"status":500}'` (a notification then fails after its retries), and set it back with `{"status":202}`.

## Documentation

| Document | Content |
|---|---|
| [Architecture overview](docs/architecture.md) | The system in one document: principles, requirements map, diagrams, failure handling |
| [Architecture decision records](docs/adr/) | Ten ADRs, each with the decision, the alternatives rejected and the consequences |
| [Known limitations and future work](docs/future-work.md) | Limitations, the interpretations adopted where the requirements are open, deferred items |
| [Implementation plan](docs/plan/implementation-plan.md) | Phases, exit criteria, experiments, fallback order |
| [Progress](docs/progress.md) | Current phase and session log |

## Planned sections

Added in the phases named in the implementation plan:

- How to create and configure your own webhook.site URL
- API examples for templates and scheduling, and `make demo`
- Requirements map with evidence
- Interpretations of the assessment and known limitations
- Load-test results
- How AI was used, and how AI output was verified
