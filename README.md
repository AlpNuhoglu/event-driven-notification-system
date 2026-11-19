# Event-Driven Notification System

> **Work in progress.** The core path is implemented: a notification created through the API is stored, published, rate-limited and sent to the provider, notifications can be created in batches with two layers of duplicate prevention (`Idempotency-Key`, `dedupe_key`), they can be listed, cancelled and redriven, they can be created from versioned templates and scheduled for later delivery, and every request is validated for its channel, with the SMS segment count reported. The WebSocket gateway and the dashboards follow in the phases of the [implementation plan](docs/plan/implementation-plan.md); [progress](docs/progress.md) shows where the work stands. This README grows with it.

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

`make test` runs the tests (needs Go and Docker); `make fuzz` runs the fuzz targets for 30 seconds each. `make test-e2e` starts the stack with the mock profile under its own Compose project, runs the end-to-end tests and `load-smoke` (a minute of SMS at the full rate, checked against the limit of 100 per second at the provider) against it, and removes it again (needs Go and Docker).

## API examples

**Run these against the mock profile (`make up-mock`), not the default stack.** A batch sends one provider request per accepted item, and the default provider is a public URL that accepts 50 requests in total. With the mock profile nothing leaves your machine; `make down` removes the stack again. **Interactive documentation is at `localhost:8080/docs`** (Swagger UI, embedded in the binary, so it works without an internet connection); the contract itself is at `localhost:8080/openapi.yaml`, and every error code is listed in [`docs/errors.md`](docs/errors.md).

**Validation per channel.** A request is checked before anything is stored, and every invalid field is reported in one `422` answer:

| Channel | `recipient` | `content` |
|---|---|---|
| `sms` | An E.164 number such as `+905551234567`. A national form such as `05551234567` is rejected, not normalised. | At most 10 SMS segments |
| `email` | A single address such as `user@example.com`, without a display name | At most 100 KB |
| `push` | A device token of 1 to 4,096 characters | At most 4 KB |

```
curl -sS -X POST localhost:8080/v1/notifications \
  -H 'Content-Type: application/json' \
  -d '{"channel":"sms","recipient":"05551234567","content":"hello"}'
```

```json
{
  "type": "https://github.com/AlpNuhoglu/event-driven-notification-system/blob/main/docs/errors.md#validation",
  "title": "Validation failed",
  "status": 422,
  "detail": "1 field(s) are invalid.",
  "correlation_id": "...",
  "errors": [
    {"field": "recipient", "code": "invalid_e164", "message": "recipient must be an E.164 number such as +905551234567"}
  ]
}
```

**An SMS reports what it costs.** The answer of an SMS carries its encoding and its number of segments. A message in the GSM-7 alphabet has 160 characters in one segment (153 per segment when it is longer). One character outside that alphabet, such as the Turkish `ş`, `ğ` or `ı`, puts the whole message into UCS-2, with 70 characters in one segment (67 per segment when longer):

```
curl -sS -X POST localhost:8080/v1/notifications \
  -H 'Content-Type: application/json' \
  -d '{"channel":"sms","recipient":"+905551234567","content":"Siparişiniz yola çıktı"}'
```

```json
{"id": "...", "channel": "sms", "status": "pending", "sms": {"encoding": "UCS-2", "segments": 1}, "...": "..."}
```

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

**Templates.** A template is created once, for one channel, and notifications are then created from it with their own variables. It is rendered when the notification is created: the rendered text is checked like a `content` (an SMS template can render to more than 10 segments, and is refused then) and stored, and the notification records which version produced it. An e-mail template escapes its variables as HTML; SMS and push templates do not. A template may use `{{ .name }}`, `if` / `else` and the functions `default` and `truncate`; the full rules are in the description of `POST /v1/templates` at `localhost:8080/docs`.

```
curl -sS -X POST localhost:8080/v1/templates \
  -H 'Content-Type: application/json' \
  -d '{"name":"otp","channel":"sms","body":"Your code is {{ .code }}. It expires in {{ .minutes | default \"5\" }} minutes.","variables":["code"],"optional_variables":["minutes"]}'
```

Versions are immutable: new content is a new version, and a notification uses the latest version unless it pins one with `"version"`.

```
curl -sS -X POST localhost:8080/v1/templates/otp/versions \
  -H 'Content-Type: application/json' \
  -d '{"body":"Doğrulama kodunuz {{ .code }}","variables":["code"]}'
```

**Preview** a template: it is rendered with the given variables and nothing is stored. For an SMS template the answer shows what the text costs.

```
curl -sS -X POST localhost:8080/v1/templates/otp/preview \
  -H 'Content-Type: application/json' -d '{"variables":{"code":482913}}'
```

```json
{"content": "Doğrulama kodunuz 482913", "encoding": "UCS-2", "segments": 1}
```

**Create a notification from a template** with `template` in place of `content` (also as an item of a batch). Variable values are strings, numbers or booleans; a number keeps its digits as sent, and `false` is the empty string, so `{{ if .flag }}` takes its `else` branch for it. A missing or misspelt variable is a `422` that names the variable (`template.variables.<name>`).

```
curl -sS -X POST localhost:8080/v1/notifications \
  -H 'Content-Type: application/json' \
  -d '{"channel":"sms","recipient":"+905551234567","template":{"name":"otp","variables":{"code":482913}}}'
curl -sS -X POST localhost:8080/v1/notifications \
  -H 'Content-Type: application/json' \
  -d '{"channel":"sms","recipient":"+905551234567","template":{"name":"otp","version":1,"variables":{"code":482913,"minutes":10}}}'
```

```json
{"id": "...", "channel": "sms", "status": "pending", "sms": {"encoding": "UCS-2", "segments": 1}, "template": {"name": "otp", "version": 2}, "...": "..."}
```

**Deactivate** a template: no new notification can use it, also with a pinned version (`422`, `template_inactive`); notifications already created are not affected. `activate` reverses it. A request with an `Idempotency-Key` that was answered before the deactivation still gets its stored answer when it is retried.

```
curl -sS -X POST localhost:8080/v1/templates/otp/deactivate
curl -sS -X POST localhost:8080/v1/templates/otp/activate
```

**Schedule a notification** with `scheduled_at` (single create and batch items): it stays `pending` and is delivered at that time, not before (the relay checks every 100 ms). The time is RFC 3339 with an explicit offset (`Z` or `+03:00`), at most 30 days ahead; the answer gives it back in UTC. Replace the example time with one in the next 30 days.

```
curl -sS -X POST localhost:8080/v1/notifications \
  -H 'Content-Type: application/json' \
  -d '{"channel":"email","recipient":"user@example.com","content":"Your appointment is tomorrow","scheduled_at":"2026-11-20T09:00:00+03:00"}'
```

```json
{"id": "...", "status": "pending", "scheduled_at": "2026-11-20T06:00:00Z", "expires_at": "2026-11-20T12:00:00Z", "...": "..."}
```

A `scheduled_at` up to 30 seconds in the past means "now", to absorb clock differences between the client and the server: the notification is sent at once and the answer shows `"scheduled_at": null`. Further in the past is a `422` (`in_the_past`), more than 30 days ahead too (`too_far_ahead`). `"scheduled_at": null` is the same as leaving it out.

**Expiry.** A notification that is not sent by its `expires_at` fails as `expired`. By default `expires_at` is the due time (`scheduled_at`, or the time of creation) plus the TTL of the priority: 10 minutes for `high`, 6 hours for `normal`, 24 hours for `low`. A request may give its own `expires_at`, at least 30 seconds after the due time, so that a notification scheduled for later cannot expire before it was ever attempted; an earlier one is a `422` (`too_soon`).

```
curl -sS -X POST localhost:8080/v1/notifications \
  -H 'Content-Type: application/json' \
  -d '{"channel":"sms","recipient":"+905551234567","content":"Doors open at 09:00","scheduled_at":"2026-11-20T09:00:00+03:00","expires_at":"2026-11-20T06:00:10Z"}'
```

```json
{
  "type": "https://github.com/AlpNuhoglu/event-driven-notification-system/blob/main/docs/errors.md#validation",
  "title": "Validation failed",
  "status": 422,
  "detail": "1 field(s) are invalid.",
  "correlation_id": "...",
  "errors": [
    {"field": "expires_at", "code": "too_soon", "message": "expires_at must be at least 30s after the due time (scheduled_at, or now)"}
  ]
}
```

**Cancel a scheduled notification** like any other (below): it is `pending` until its time, so it can be cancelled until a worker takes it. A scheduled notification cannot be moved to another time; cancel it and create a new one.

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

`PUT /admin/config` replaces the whole configuration and is strict (an unknown field or an out-of-range value is `400`). Besides the `{"status":N}` shorthand it takes `{"default": {rule}, "channels": {"sms": {rule}, "email": {rule}, "push": {rule}}}`; a channel without a rule uses `default`. A rule may set `status`, `fail_status`, `fail_rate`, `fail_first` (the first N requests of each `Idempotency-Key` fail), `retry_after` (sent on `429` and `503`), `location` (sent on `3xx`), `latency_ms`, `latency_jitter_ms`, `close_connection`, `hang` and `malformed_body`. For example, `{"default":{"fail_first":2,"fail_status":503,"retry_after":"1"}}` makes every notification fail twice and then succeed. `POST /admin/reset` restores the default (`202`, no faults). `GET /admin/report` adds a `statuses` count per channel (`"closed"` and `"hung"` for requests that got no answer).

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
- `make demo`
- Requirements map with evidence
- Interpretations of the assessment and known limitations
- Load-test results
- How AI was used, and how AI output was verified
