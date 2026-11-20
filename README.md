# Event-Driven Notification System

A notification service written in Go that accepts SMS, Email and Push notifications through a REST API and delivers them asynchronously through an external provider. It is built around a Postgres outbox, a RabbitMQ dispatch broker with strict priority, a per-channel rate limit of at most 100 messages per second, retries classified by failure scope, and real-time status tracking.

## Quick start

**Requirements:** Git, and Docker with Docker Compose **v2.24.0 or later** (`docker-compose version`; tested with v5.6.1). Nothing else: no Go, no `.env` file. If your installation has only the `docker compose` plugin form, use it wherever this README says `docker-compose`.

```
git clone https://github.com/AlpNuhoglu/event-driven-notification-system.git
cd event-driven-notification-system
docker-compose up
```

The first run builds the application image (a few minutes; later runs start in about half a minute). The command stays in the foreground and prints the log lines of every container. The stack is up when the `migrate` container has exited with code 0 and the log lines stop scrolling; it consists of four application services (`api`, `relay`, `worker`, `gateway`) and six infrastructure containers (Postgres, RabbitMQ, Redis, Prometheus, Grafana, Jaeger).

In a second terminal, check the API, create one notification and read it back:

```
curl -fsS localhost:8080/healthz && curl -fsS localhost:8080/readyz
curl -fsS -X POST localhost:8080/v1/notifications \
  -H 'Content-Type: application/json' \
  -d '{"channel":"sms","recipient":"+905551234567","content":"hello","priority":"high"}'
curl -fsS 'localhost:8080/v1/notifications?limit=1'
```

The first line prints `{"status":"ok"}{"status":"ready"}`. The create answers with the notification, `"status":"pending"`, and its `id`. The last command lists the newest notification: within a few seconds it shows `"status":"sent"`, which means the provider accepted the message (`curl -fsS localhost:8080/v1/notifications/<id>` reads one notification by its id). **This create sends one real request to the public provider URL below; please read the next section before sending more.**

Stop the stack with Ctrl-C in the first terminal; `docker-compose down -v` removes the containers and the data.

For a shell you get back, use `docker-compose up -d --wait` instead. If a host port is taken on your machine, set `API_PORT`, `GATEWAY_PORT`, `GRAFANA_PORT`, `PROMETHEUS_PORT` or `JAEGER_UI_PORT`, for example `API_PORT=18080 docker-compose up`.

| What | Where |
|---|---|
| REST API, Swagger UI | http://localhost:8080, http://localhost:8080/docs |
| WebSocket gateway | `ws://localhost:8082/v1/ws` |
| Grafana (login `admin` / `admin`) | http://localhost:3000 |
| Prometheus | http://localhost:9090 |
| Jaeger | http://localhost:16686 |

### The provider URL

**The default provider is a public webhook.site URL,** as the assessment asks (R23):

```
https://webhook.site/7940ee46-a3b1-48b7-a29a-3056e46df5fb
```

- **Created on 2026-11-20. It expires on 2026-11-27,** seven days later; a URL that no longer exists answers `404`.
- **It accepts 50 requests in total. It had received none when it was put here.** Every notification you create on the default stack uses one, and so does every retry. After the 50th request the URL answers `429` (observed on a throwaway URL on 2026-11-18).
- **Every request sent to it is publicly visible** to anyone who has the URL, at https://webhook.site/#!/view/7940ee46-a3b1-48b7-a29a-3056e46df5fb. Use only the fake values shown above (`+905551234567`, `example.com` addresses), never a real recipient.
- It is configured to answer `202` with a JSON body of `messageId`, `status` and `timestamp` (R24).
- How many requests it has received (this call does not count as one): `curl -sS -H 'Accept: application/json' 'https://webhook.site/token/7940ee46-a3b1-48b7-a29a-3056e46df5fb/requests?per_page=1'`, member `total`. `make demo` reads the same number before it sends anything.
- **When the URL is used up or has expired, nothing is lost and nothing fails at once:** the worker treats `429` and `404` as the provider's problem, opens the circuit for that channel and retries; notifications stay `retrying` until they are sent or expire. Use your own URL or the mock profile then.

**Your own URL.** Create one that answers like the assessment's provider (the answer's `uuid` is your URL, `https://webhook.site/<uuid>`):

```
curl -sS -X POST https://webhook.site/token -H 'Content-Type: application/json' \
  -d '{"default_status":202,"default_content_type":"application/json","default_content":"{\"messageId\":\"00000000-0000-0000-0000-000000000000\",\"status\":\"accepted\",\"timestamp\":\"2026-01-01T00:00:00Z\"}"}'
```

Or open https://webhook.site in a browser, which creates a URL, and set its response status to `202` there. A URL left at webhook.site's defaults works too: any `2xx` answer counts as accepted. Then start the stack with it:

```
PROVIDER_URL=https://webhook.site/<your-uuid> docker-compose up
```

or put the line `PROVIDER_URL=https://webhook.site/<your-uuid>` into a file named `.env` next to `docker-compose.yml`. `.env.example` lists every setting with its default.

**The mock profile** is for anything beyond a few requests: `make up-mock` (needs `make`; or `docker-compose -f docker-compose.yml -f docker-compose.mock.yml up -d --build --wait`) starts the same stack plus `mockprovider`, a local fake provider, and points the worker at it, so nothing leaves your machine. `curl -fsS localhost:8090/admin/report` shows what the provider received (field names only, never values). `make down` stops the stack and removes its volumes.

**One worker per channel** instead of the one worker for all three: add the override `docker-compose.override.worker-per-channel.yml` to the mock profile, `docker-compose -f docker-compose.yml -f docker-compose.mock.yml -f docker-compose.override.worker-per-channel.yml up -d --build --wait` (and the same three `-f` options for `ps`, `logs` and `down -v`). It runs the services `worker-sms`, `worker-email` and `worker-push`, so a crash of one stops only its channel; it works with the mock profile only.

## Architecture in brief

```
client ──REST──▶ api ──INSERT──▶ Postgres ◀──poll every 100 ms── relay ──publish──▶ RabbitMQ ──▶ worker ──HTTP──▶ provider
                                    ▲                                                (one queue      │
                                    └───────────── claim, result, attempt history ── per channel) ───┘
client ◀─WebSocket─ gateway ◀── status events (RabbitMQ) ◀── api, relay, worker
```

- **`api`** validates a request and stores the notification in Postgres as `pending`; it needs nothing but Postgres. **Postgres is the source of truth** and holds the backlog.
- **`relay`** (one instance) publishes due rows to RabbitMQ: new, scheduled and retried work alike. It keeps only a small, bounded buffer in the broker, expires notifications and recovers stuck ones.
- **`worker`** consumes one queue per channel in strict priority order, paces each channel at 97 sends per second (a shared limiter in Redis), claims the row with a conditional `UPDATE`, calls the provider and records the result. A circuit breaker per channel stops sending while the provider fails.
- **`gateway`** pushes status changes to WebSocket clients, with a snapshot on subscribe.
- Every race on a notification (relay against worker, cancel against claim, a late result against a newer attempt) is decided by a conditional `UPDATE` on its row; there is no leader election and no distributed lock.

The overview with diagrams, the state machine and the failure table is in [docs/architecture.md](docs/architecture.md); each decision, with the alternatives rejected, is in the ten [ADRs](docs/adr/).

## Running the tests

| Command | What it does | Needs |
|---|---|---|
| `make test` | Unit, integration and contract tests with the race detector; databases and brokers are started as containers | Go 1.27, Docker |
| `make test-docker` | The same inside a Go container | Docker |
| `make test-e2e` | Starts the stack with the mock profile under its own Compose project and runs the end-to-end tests, the chaos scenarios and `load-smoke` (a minute of SMS at the full rate, checked at the provider against the limit of 100 per second); then the same for the per-channel worker layout. About six minutes. Every chaos scenario logs one `chaos report:` line with its counts and the duplicates the provider received | Go, Docker |
| `make load` | The rate scenario: 30,000 SMS at the configured rate on a stack of its own, about five minutes, with the same check | Go, Docker |
| `make verify-suite` | Applies each mutation patch of `test/mutations` to a temporary copy of the code and requires that the tests it names fail | Go, Docker |
| `make fuzz` | The five fuzz targets, 30 seconds each | Go |
| `make lint`, `make check-rules` | `golangci-lint`; the Prometheus alert rules and their unit tests | Go; Docker |

None of them sends anything to the provider URL: they use `mockprovider` or test servers.

## API examples

**Run these against the mock profile (`make up-mock`), not the default stack.** A batch sends one provider request per accepted item, and the default provider is a public URL that accepts 50 requests in total. With the mock profile nothing leaves your machine; `make down` removes the stack again. In the commands, `<id>`, `<batch_id>` and `<next_cursor>` stand for values from an earlier answer. **Interactive documentation is at `localhost:8080/docs`** (Swagger UI, embedded in the binary, so it works without an internet connection); the contract itself is at `localhost:8080/openapi.yaml`, and every error code is listed in [`docs/errors.md`](docs/errors.md).

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

**List templates**, by name, with `limit` (1 to 200, 50 by default) and a `cursor` (pass `next_cursor` back until it is `null`). Each item is a summary (`name`, `channel`, `latest_version`, `is_active`); inactive templates are listed too.

```
curl -sS 'localhost:8080/v1/templates?limit=50'
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

**Schedule a notification** with `scheduled_at` (single create and batch items): it stays `pending` and is delivered at that time, not before (the relay checks every 100 ms). The time is RFC 3339 with an explicit offset (`Z` or `+03:00`), at most 30 days ahead; the answer gives it back in UTC. The example time is 2026-12-01; once that has passed, replace it with a time in the next 30 days (a time in the past is answered `422`, `in_the_past`).

```
curl -sS -X POST localhost:8080/v1/notifications \
  -H 'Content-Type: application/json' \
  -d '{"channel":"email","recipient":"user@example.com","content":"Your appointment is tomorrow","scheduled_at":"2026-12-01T09:00:00+03:00"}'
```

```json
{"id": "...", "status": "pending", "scheduled_at": "2026-12-01T06:00:00Z", "expires_at": "2026-12-01T12:00:00Z", "...": "..."}
```

A `scheduled_at` up to 30 seconds in the past means "now", to absorb clock differences between the client and the server: the notification is sent at once and the answer shows `"scheduled_at": null`. Further in the past is a `422` (`in_the_past`), more than 30 days ahead too (`too_far_ahead`). `"scheduled_at": null` is the same as leaving it out.

**Expiry.** A notification that is not sent by its `expires_at` fails as `expired`. By default `expires_at` is the due time (`scheduled_at`, or the time of creation) plus the TTL of the priority: 10 minutes for `high`, 6 hours for `normal`, 24 hours for `low`. A request may give its own `expires_at`, at least 30 seconds after the due time, so that a notification scheduled for later cannot expire before it was ever attempted; an earlier one is a `422` (`too_soon`).

```
curl -sS -X POST localhost:8080/v1/notifications \
  -H 'Content-Type: application/json' \
  -d '{"channel":"sms","recipient":"+905551234567","content":"Doors open at 09:00","scheduled_at":"2026-12-01T09:00:00+03:00","expires_at":"2026-12-01T06:00:10Z"}'
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
  -H 'Content-Type: application/json' -d '{"expires_at":"2026-12-01T12:00:00Z"}'
```

To try cancel and redrive on the mock profile, make the mock provider fail first with `curl -X PUT localhost:8090/admin/config -H 'Content-Type: application/json' -d '{"status":500}'` (a notification then fails after its retries), and set it back with `{"status":202}`.

`PUT /admin/config` replaces the whole configuration and is strict (an unknown field or an out-of-range value is `400`). Besides the `{"status":N}` shorthand it takes `{"default": {rule}, "channels": {"sms": {rule}, "email": {rule}, "push": {rule}}}`; a channel without a rule uses `default`. A rule may set `status`, `fail_status`, `fail_rate`, `fail_first` (the first N requests of each `Idempotency-Key` fail), `retry_after` (sent on `429` and `503`), `location` (sent on `3xx`), `latency_ms`, `latency_jitter_ms`, `close_connection`, `hang` and `malformed_body`. For example, `{"default":{"fail_first":2,"fail_status":503,"retry_after":"1"}}` makes every notification fail twice and then succeed. `POST /admin/reset` restores the default (`202`, no faults). `GET /admin/report` adds a `statuses` count per channel (`"closed"` and `"hung"` for requests that got no answer).

### Metrics, dashboard and alerts

The figures as JSON, computed from the database (cached for 2 seconds):

```
curl -fsS localhost:8080/v1/metrics/summary
```

It lists, per lane (channel and priority), the queued count against its high-water mark and the due backlog; and per channel the attempts of the last 5 minutes by outcome, the `success_rate` (successes over all finished attempts except `aborted`; `null` without any), and the median and 95th percentile of the provider latency and of the end-to-end latency in milliseconds.

- **Grafana:** http://localhost:3000, dashboard "Overview" (login `admin` / `admin`, Grafana's default; a local development stack only). Ingest, lane depth, backlog, attempts by outcome, latencies, circuit state, rate-limit wait, RabbitMQ queues, API requests.
- **Prometheus:** http://localhost:9090. The six alert rules are under "Alerts" (there is no Alertmanager): relay down, `high` lane stalled, poison messages, expiry spike, circuit not closed, limiter fallback. `make check-rules` validates the rule file and runs its unit tests.
- Each service serves `/metrics`, `/healthz` and `/readyz` on its admin port (8081), which is not published to the host; Prometheus scrapes it inside the Compose network.

### Logs and traces

Every service writes JSON lines to its container's output (`docker-compose logs -f worker`). A line has `service`, `version`, and, when it belongs to a request or a notification, `correlation_id`, `trace_id` and `span_id`; a notification's lines also have `notification_id`, `channel`, `priority` and `status`. At the default level (`LOG_LEVEL=info`) a notification produces one line, its final outcome; `LOG_LEVEL=debug` shows every transition and the API's line per request. A recipient is always masked (`+90555***4567`, `u***@example.com`) and message content is never logged.

`X-Request-ID` on a request (up to 128 characters of letters, digits, `.`, `_`, `-`) becomes the `correlation_id` of everything that request causes; without one the API generates it and returns it in the response header.

**Jaeger:** http://localhost:16686. A request is one trace from the API through the relay and the worker to the provider call. A retry, and every delivery of a scheduled notification, is a new trace that links back to the request. Search by service `api` and open a `POST /v1/notifications` trace, or search by the tag `notification.id=<id>`.

### make demo

`make demo` shows the running stack working, with at most 10 notifications. Start the stack first (`docker-compose up -d --wait`, or `make up-mock` for the mock provider), then run it. It runs inside the worker container, so it uses the worker's own `PROVIDER_URL`:

1. It checks that the provider URL can take the notifications: it connects to the host, and for a webhook.site URL it also asks the webhook.site API whether the URL still exists and how many of its 50 requests are left. It never sends a request to the provider URL itself, and sends nothing if the check fails.
2. It creates 5 notifications (`--count`, at most 10) through the API: a mix of sms, email and push and of the three priorities, one of them through the batch endpoint, with only the reserved fake values. Each request has an `Idempotency-Key`, so a second run within the key's lifetime replays the stored answers and sends nothing again; the output says so.
3. It follows them over the WebSocket and prints each status change (time, short id, channel, priority, status, version) until all are `sent` or `failed`, or until `--timeout` (30s).
4. It prints each notification's final status and attempts, the per-channel attempts and success rate of `/v1/metrics/summary`, and where to look next (Grafana, Prometheus, Jaeger).

It exits 0 only when every notification ended `sent`. Pass options with `make demo DEMO_ARGS="--count 3"`. With the default webhook.site URL the requests are publicly visible and count against its 50: use the mock profile for anything beyond a few requests.

### Real-time status updates

The gateway serves a WebSocket at `ws://localhost:8082/v1/ws`. A client subscribes to notification ids, to a batch id, or to both, and receives the current state of each notification at once (a snapshot) and then every status change as it happens. Because of the snapshot, a client can create a notification first and subscribe afterwards without missing anything.

With any WebSocket client, for example [`websocat`](https://github.com/vi/websocat) (not part of this repository):

```
websocat ws://localhost:8082/v1/ws
{"action":"subscribe","notification_ids":["<id>"]}
{"action":"subscribe","batch_id":"<batch_id>"}
{"action":"unsubscribe","notification_ids":["<id>"]}
```

Every message from the server about a notification has this form, for the snapshot and for live events alike:

```json
{"type":"status_changed","id":"…","batch_id":null,"status":"sent","version":4,"failure_reason":null,"updated_at":"2026-11-19T20:00:00.123456Z"}
```

- **Apply the version rule:** ignore a message whose `version` is not greater than the one you already know for that notification. Events come from several services and can arrive out of order, and the snapshot can arrive after a newer event.
- A batch subscription sends one message per notification of the batch and counts as one subscription; a connection can hold 1,000 subscriptions.
- A request the gateway cannot serve is answered with `{"type":"error","code":"…","message":"…"}` and the connection stays open. The codes are `invalid_message`, `invalid_id`, `too_many_subscriptions` and `snapshot_unavailable`.
- Updates are best-effort: an event can be lost, for example while RabbitMQ restarts. `GET /v1/notifications/{id}` is always authoritative.
- A client that reads too slowly is disconnected with close status 1013; it reconnects, subscribes again and gets a fresh snapshot.
- A browser page from another origin is refused unless its host matches `WS_ORIGIN_PATTERNS` (see `.env.example`).

## Requirements map

The assessment's requirements, numbered R1 to R36 and paraphrased. "Evidence" names the tests and commands that show each one; a test name can be run with `go test -tags integration -run '<name>' ./...`, the `TestComposeE2E_…` ones and `TestLoadSmoke` run in `make test-e2e`. Where the evidence is thinner than the claim, the row says so.

| ID | Requirement | Where | Evidence |
|---|---|---|---|
| R1 | Three channels: SMS, Email, Push | [ADR-0005](docs/adr/0005-service-topology-and-status-events.md), [ADR-0006](docs/adr/0006-api-contract-and-three-layer-idempotency.md) §5 | `TestComposeE2E_CreatedNotificationIsSent`: one notification per channel reaches `sent` on the Compose stack. `TestValidateCreate_PerChannel` |
| R2 | High volume, millions per day | [ADR-0001](docs/adr/0001-rabbitmq-as-dispatch-broker.md): 3 × 97 = 291 msg/s, about 25.1 M per day | `make load`: 30,000 SMS arrive at 97.0 per second on one channel ([results](docs/load-test.md)). The three channels together at that rate were **not** measured |
| R3 | Burst traffic | [ADR-0003](docs/adr/0003-bounded-relay-buffer-and-sweepers.md): the backlog waits in Postgres, the broker holds a bounded buffer | `TestRelay_LaneHWMHoldsOnRealRows`; `TestBatch_Thousand`; `make load` creates 30,000 in 3.6 s and drains them at the rate. The 1,000,000-message ingest was **not** run |
| R4 | Reliable delivery | [ADR-0002](docs/adr/0002-postgres-as-outbox-scheduler-and-retry-delay.md), [ADR-0003](docs/adr/0003-bounded-relay-buffer-and-sweepers.md), [ADR-0004](docs/adr/0004-data-model-state-machine-and-fencing.md) | `TestClaim_WaitsForRelayCommit`, `TestE2E_WorkerFasterThanRelayCommit`, `TestRelay_UnroutablePublishStaysPending`; chaos: `TestComposeE2E_RabbitMQWipedEveryNotificationSent`, `TestComposeE2E_PostgresRestartDuringSends`, `TestComposeE2E_WorkerSIGKILLMidSend` end with every accepted notification `sent` |
| R5 | Intelligent retries | [ADR-0007](docs/adr/0007-retry-policy-and-circuit-breaker.md) §1–4, §7 | `TestClassify`, `TestDecideRetry`, `TestDecideRetryBudget`; `TestComposeE2E_5xxRetriedUntilSent`; `TestComposeE2E_ProviderOutageLowTraffic`: a 60-second outage ends with no retry budget consumed |
| R6 | Delivery status visible; real-time tracking | [ADR-0004](docs/adr/0004-data-model-state-machine-and-fencing.md) §6, [ADR-0005](docs/adr/0005-service-topology-and-status-events.md), [ADR-0009](docs/adr/0009-observability.md) | `TestListAPI_AttemptsRead` (the attempt history); `TestComposeE2E_GatewayFollowsNotificationsToSent` |
| R7 | Implemented in Go 1.2x | Go 1.27 | `go.mod`; the image and CI build with Go 1.27.4 |
| R8 | Create a notification with recipient, channel, content, priority | [ADR-0006](docs/adr/0006-api-contract-and-three-layer-idempotency.md) §2–5 | `TestContract_EveryDocumentedResponse`, `TestValidateCreate`, `TestCreate_EachPriority`; the quick start |
| R9 | Batch creation, up to 1,000 per request | [ADR-0006](docs/adr/0006-api-contract-and-three-layer-idempotency.md) §6 | `TestBatch_Thousand`, `TestBatch_PartialSuccess`, `TestValidateBatch_Size` (1,001 is refused) |
| R10 | Status by notification id | [ADR-0006](docs/adr/0006-api-contract-and-three-layer-idempotency.md) §2 | `TestContract_EveryDocumentedResponse`; `TestComposeE2E_CreatedNotificationIsSent` reads it until `sent` |
| R11 | Status by batch id | [ADR-0006](docs/adr/0006-api-contract-and-three-layer-idempotency.md) §2 | `TestGetBatch_StatusCounts`, `TestBatch_StatusCountsMove` |
| R12 | Cancel pending notifications | [ADR-0004](docs/adr/0004-data-model-state-machine-and-fencing.md) transition 9; [ADR-0006](docs/adr/0006-api-contract-and-three-layer-idempotency.md) §7 | `TestCancel_FromEachState`, `TestCancel_RacesWithClaim`, `TestCancel_DuringRelayPublish`, `TestCancel_Idempotent`, `TestCancelBatch` |
| R13 | List with filters (status, channel, date range) and pagination | [ADR-0006](docs/adr/0006-api-contract-and-three-layer-idempotency.md) §8 | `TestList_Filters`, `TestList_DateRangeIsHalfOpen`, `TestList_PaginationWalk`, `TestList_ExplainUsesIndex` |
| R14 | Asynchronous processing by queue workers | [ADR-0001](docs/adr/0001-rabbitmq-as-dispatch-broker.md), [ADR-0005](docs/adr/0005-service-topology-and-status-events.md) | `TestE2E_IdleSingleNotificationIsSent`, `TestClaim_TwoWorkersExactlyOne`; the create answers `202` before anything is sent |
| R15 | At most 100 messages per second per channel | [ADR-0007](docs/adr/0007-retry-policy-and-circuit-breaker.md) §5: paced at 97 msg/s, no burst | `TestLoadSmoke` on every CI run and `make load`: the largest one-second window at the provider was 98 ([Load results](#load-results)). `TestPacingAtFullRate`, `TestMissedSlot`, `TestZeroBurstSlotsAreOneIntervalApart` |
| R16 | Priority queue (high, normal, low) | [ADR-0001](docs/adr/0001-rabbitmq-as-dispatch-broker.md): strict priority; [ADR-0003](docs/adr/0003-bounded-relay-buffer-and-sweepers.md) | `TestRelay_PrioritySetOnEveryMessage`, `TestCycle_LaneOrderHighFirstWithinEachChannel`, `TestHighestPriorityTaken`: each stage orders strictly. The whole path under a backlog was **not** measured |
| R17 | Content validation | [ADR-0006](docs/adr/0006-api-contract-and-three-layer-idempotency.md) §5 | `TestValidateCreate_PerChannel`, `TestValidateCreate_Bounds`, `TestSMSInfo_StoredAndReturned`, `FuzzCountSMS` |
| R18 | Idempotency against duplicate sends | [ADR-0006](docs/adr/0006-api-contract-and-three-layer-idempotency.md) §11: at the request, the record and the send | `TestIdempotencyKey_ConcurrentSameKey`, `TestIdempotencyKey_Replay`, `TestCreateBatch_Conflicts`, `TestE2E_DuplicateMessageIsSentOnce`, `TestSend_SentExactlyOnce`; every chaos and load scenario counts the keys the provider received twice |
| R19 | Delivery and retry logic | [ADR-0007](docs/adr/0007-retry-policy-and-circuit-breaker.md), [ADR-0004](docs/adr/0004-data-model-state-machine-and-fencing.md) | `TestClassify`, `TestDecideRetryScope`, `TestBreaker_FiveConsecutiveFailuresOpen`, `TestBreaker_WindowRule`, `TestLateSuccess_NewerAttemptWins` |
| R20 | Real-time metrics: queue depth, success and failure rates, latency | [ADR-0009](docs/adr/0009-observability.md) §1–2 | `curl localhost:8080/v1/metrics/summary`; `TestSummary_ValuesOnAKnownDataset`; `TestDashboardSmoke` |
| R21 | Structured logging with correlation ids | [ADR-0009](docs/adr/0009-observability.md) §4–5 | `TestLogs_NoPersonalDataAndCorrelationIDsInEveryRole`, `TestRequestID_EveryOperation`; `TestCanaryScan`: no recipient or content in any log, span, metric or error of the running stack |
| R22 | Health check endpoint | [ADR-0009](docs/adr/0009-observability.md) §7 | `curl localhost:8080/healthz`; `TestComposeE2E_Health`, `TestHealthz_IgnoresTheChecks`; the CI job `compose-up` |
| R23 | webhook.site as the provider, in the given request format | [ADR-0007](docs/adr/0007-retry-policy-and-circuit-breaker.md) §6; `docker-compose.yml` | `TestSend_BodyHasExactlyThreeFields`, `TestSend_Headers`; the quick start's create against the default URL, done once on 2026-11-20 ([rehearsal](docs/progress.md)) |
| R24 | Expected answer `202` with `messageId`, `status`, `timestamp` | [ADR-0007](docs/adr/0007-retry-policy-and-circuit-breaker.md) §2, §6 | `TestSend_AcceptedParsed`, `TestSend_Other2xx`, `TestSend_MalformedAccepted` |
| R25 | Bonus: failure handling | [ADR-0007](docs/adr/0007-retry-policy-and-circuit-breaker.md) §7, §9–10; [ADR-0003](docs/adr/0003-bounded-relay-buffer-and-sweepers.md) | Eight chaos scenarios in `make test-e2e`, with their counts in [the load-test results](docs/load-test.md#chaos-scenarios) |
| R26 | Bonus: scheduled notifications | [ADR-0008](docs/adr/0008-templates-and-scheduling.md) | `TestComposeE2E_Scheduling`, `TestE2E_ScheduledIsSentAtItsTimeNotBefore`, `TestValidateCreate_Schedule` |
| R27 | Bonus: templates with variable substitution | [ADR-0008](docs/adr/0008-templates-and-scheduling.md) | `TestComposeE2E_Templates`, `TestTemplateRender_Variables`, `TestTemplateFunctionNameCheck_RejectsEveryBuiltinInCallPosition`, `FuzzCompileTemplate` |
| R28 | Bonus: WebSocket updates | [ADR-0005](docs/adr/0005-service-topology-and-status-events.md); [ADR-0006](docs/adr/0006-api-contract-and-three-layer-idempotency.md) §10 | `TestComposeE2E_GatewayFollowsNotificationsToSent`, `TestGateway_SubscriptionIsRegisteredBeforeTheSnapshotIsRead` |
| R29 | Bonus: distributed tracing | [ADR-0009](docs/adr/0009-observability.md) §6 | `TestTracing_FirstAttemptIsOneTrace`, `TestTracing_RetryIsNewTraceLinkedToTheRequest`; Jaeger on port 16686 |
| R30 | Bonus: GitHub Actions CI/CD | [ADR-0010](docs/adr/0010-testing-and-ci.md) §5 | `.github/workflows/ci.yml`: seven jobs on every push and pull request (`lint`, `unit`, `codegen-drift`, `migrations`, `integration`, `compose-up`, `e2e`). **No release workflow was built:** it is CI without CD |
| R31 | Repository with a clean commit history | [ADR-0010](docs/adr/0010-testing-and-ci.md) §7 | `git log --oneline`: linear, Conventional Commits, one pull request per branch |
| R32 | README: setup, architecture overview, API examples | This file; [docs/architecture.md](docs/architecture.md) | The clean-clone rehearsal of 2026-11-20, recorded in [progress](docs/progress.md) |
| R33 | Docker Compose, one-command setup | [ADR-0005](docs/adr/0005-service-topology-and-status-events.md) | `docker-compose up`; the CI job `compose-up` runs it with the standalone binary and no `.env` |
| R34 | Swagger/OpenAPI documentation | [ADR-0006](docs/adr/0006-api-contract-and-three-layer-idempotency.md) §1 | `api/openapi.yaml`, served at `/docs` and `/openapi.yaml`; `TestDocsRoutes`, `TestContract_EveryDocumentedResponse`; the CI job `codegen-drift` |
| R35 | Versioned database migrations | [ADR-0004](docs/adr/0004-data-model-state-machine-and-fencing.md) §8 | `migrations/` (three versions, each with its `down`); the `migrate` service; the CI job `migrations` runs up, down, up |
| R36 | Test suite runnable with a single command | [ADR-0010](docs/adr/0010-testing-and-ci.md) §4 | `make test`; `make test-docker` without Go installed |

[docs/decision-tests.md](docs/decision-tests.md) does the same for every design decision, and a test fails when it names a test that does not exist.

## Interpretations

Where the assessment leaves a point open, one reading was adopted and is stated here. None contradicts its text.

| Requirement | What is left open | Reading adopted |
|---|---|---|
| R15, rate limit | Scope and window of "100 per second" | One limit per channel across all workers: no one-second window at the provider holds more than 100 messages. Kept by pacing at 97 msg/s |
| R16, priority queue | Strict or weighted | Strict: `high` before `normal` before `low`, within a channel |
| R12, cancel "pending" | One state, or "not yet sent" | Cancel works from `pending`, `queued` and `retrying`: any state in which no worker holds the notification. `processing`, `sent` and `failed` answer `409` |
| R9, batch | All-or-nothing or not | Partial success: valid items are stored, invalid ones are reported by index; no valid item is `422` |
| R6, R24, status | What the success state means | `sent` means the provider accepted the message, not that it reached the user |
| R24, answer `202` | Another `2xx` | Success, with a warning: the provider accepted the message, and sending again would duplicate it |
| R18, idempotency | Where duplicates are prevented | At the request (`Idempotency-Key`), at the record (`dedupe_key`) and at the send (conditional claim, fencing, the key sent to the provider) |
| R5, R19, retries | Whether retries go on for ever | Every notification has an `expires_at`, by default per priority; after it the notification fails as `expired` instead of being sent late |
| R28, WebSocket | Whether updates are guaranteed | Best-effort events plus a snapshot on subscribe; `GET` is authoritative |
| R8, fields | An e-mail subject or push title | None: a notification has recipient, channel, content and priority, as the provider request has |
| R20, metrics endpoint | One endpoint, which format | `GET /v1/metrics/summary` as JSON; Prometheus `/metrics` in addition |
| R22, health check | Which port | The API's public port |
| R23, provider request | Headers; the default URL | The body is exactly the three fields; headers carry the idempotency key and the trace context; the default is a webhook.site URL |
| R7, Go version | Which 1.2x | Go 1.26 or later (the code generator needs it); built with 1.27 |
| R33, one command | File name and command | `docker-compose.yml` and `docker-compose up`, as written |

## Limitations

The ones a reader should know first; [docs/future-work.md](docs/future-work.md) has all of them with their reasons, and the future work.

- **The largest burst each priority can absorb is bounded by its TTL.** A notification that is not sent by its `expires_at` fails as `expired`. One channel sends 97 per second, so within the default TTLs it can send at most:

  | Priority | Default TTL | Largest burst one channel can send in time |
  |---|---|---|
  | `high` | 10 minutes | about 58,200 |
  | `normal` | 6 hours | about 2.1 million |
  | `low` | 24 hours | about 8.4 million |

  The rest of a larger burst expires unsent. The TTLs are configuration (`TTL_HIGH`, `TTL_NORMAL`, `TTL_LOW`), and a request can set its own `expires_at`. The API does not refuse work it cannot deliver in time.
- **Delivery is at-least-once, so a notification can reach the provider twice** ([Delivery guarantee](#delivery-guarantee)). webhook.site does not deduplicate.
- **Strict priority can make `low` wait:** while `high` traffic fills a channel's rate, `normal` and `low` of that channel are not sent.
- **The rate limit is approximate while Redis is down:** each worker then limits itself in-process, and an alert fires.
- **After an idle period, the first minute of a very large burst runs at about a third of the rate,** until Postgres analyses its tables again. Measured in P3 with 1,000,000 rows; the remedy was planned and not built.
- **One relay.** While it restarts, publishing, expiry and recovery pause; nothing is lost.
- **A message lost while the relay stays connected is not republished automatically:** an alert, the command `docker-compose run --rm relay requeue-stale`, and expiry as the final bound.
- **The default provider URL is public, takes 50 requests and expires** ([The provider URL](#the-provider-url)).
- **No authentication.** Anyone who can reach the API can use it.
- **`cancelled` and `failed` can still become `sent`** when a late result proves the message was accepted.
- **Evidence that was planned and not produced:** the load scenarios beyond the rate (1,000,000 ingest, 100,000 drain across three channels, priority and isolation under load, saturation), the tuning of buffer sizes and alert thresholds against load, a release workflow, and the CI jobs for `make test-docker`, a security scan and the Markdown link check ([future work, item 21](docs/future-work.md#21-load-evidence-and-tuning-that-p6-planned-and-did-not-do)).

## Load results

The limit is 100 messages per second and channel, read as: no one-second window at the provider holds more than 100 messages of one channel. The worker paces each channel at **97 messages per second** (`RATE_LIMIT_PER_SECOND`), with no burst, so that three slots stay free under the limit.

| | Result |
|---|---|
| Largest one-second window, 20 runs of `load-smoke` (6,000 SMS each) at 97 msg/s | **98** in every one of the 20 runs |
| Largest one-second window, `make load` (30,000 SMS) at 97 msg/s | **98**; 30,000 SMS arrived in 309.3 s, 97.0 per second |
| The same at 98 msg/s, the rate until P6 | 99 in 26 runs, 100 in one run on a busy machine, which is why the rate was lowered |

Counts like these are exact. The machine, every run, the chaos scenarios' counts and what was **not** measured (the 1,000,000-message ingest, the 100,000-message drain, the saturation report, priority and isolation under load) are in [docs/load-test.md](docs/load-test.md).

## Delivery guarantee

Delivery is **at-least-once**: an accepted notification is sent, or ends `failed` or `cancelled` with its reason, and it can reach the provider more than once.

- **Every request to the provider carries the notification's id in the `Idempotency-Key` header,** the same value on every attempt. A provider that honours the key can discard a repeat; webhook.site does not.
- **A second send happens when the outcome of the first is not known:** the provider did not answer in time, the connection broke after the request was written, or the worker stopped before it recorded the answer. Such an attempt is recorded as `unknown`, and a cancel of that notification reports `may_have_been_sent`.
- **A worker that is killed while it sends re-sends the notifications that were in flight.** Their rows stay `processing` until the lease expires (`LEASE_DURATION`, 30 s); the relay's lease sweeper then returns them to `retrying` and they are sent again, with the same `Idempotency-Key`. That is at most `WORKER_CONCURRENCY` notifications per channel and worker (25 by default). Notifications the worker had not started to send are not repeated. The chaos scenario that kills the worker with `SIGKILL` measures it; its numbers are in [the load-test results](docs/load-test.md#chaos-scenarios).
- A worker that stops normally (`SIGTERM`) finishes the sends in flight within its 15-second drain and repeats nothing.

Duplicates are counted at the provider, not in the database, in every chaos and load scenario ([limitations](docs/future-work.md)).

## How AI was used

Almost all of the code and the documents were written by AI models; the author directed the work, took the decisions and merged every pull request. This section says who did what; the next says why the result can be trusted, and where it could not be.

- **Design, before any code.** The author worked out the design with a Claude model as design partner: ten ADRs, the architecture overview and the list of limitations.
- **Adversarial review.** A second model (Claude Fable 5.2) reviewed those documents in twelve turns, with the instruction to disagree: to say when a document was wrong, when a fix was out of proportion, and which of its own claims it had not checked. The author triaged every finding; the results are the 79 decisions of the [decision ledger](docs/ai/decision-ledger.md), each with its source.
- **Implementation in phases.** Claude Code sessions built the system phase by phase (P0 to P7 of the [implementation plan](docs/plan/implementation-plan.md)), one branch and one pull request at a time, 21 in all. A session's main loop planned, wrote the concurrency-critical code or reviewed it line by line, and delegated bounded work to sub-agents in their own working copies; sub-agents never committed. Each pull request ended with green CI and a report to the author, who merged it or asked for more.
- **The author's part.** Questions the documents did not answer went to the author as open issues instead of being settled in code (numbered 1 to 15 in the progress log). The author amended the ledger on evidence the sessions brought back (the amendments are dated at its top), asked for checks that found defects, among them the third example below, and reduced the scope of the last two phases when time ran short.
- **Records.** [docs/progress.md](docs/progress.md) is the log of every session: what was done, which runs failed, what was decided, and a "not verified" list. [docs/ai/design-review-log.md](docs/ai/design-review-log.md) is the curated account of the review and of what the implementation overturned. The sessions' standing instructions are in `CLAUDE.md` and `.claude/skills/`.

## How AI output was verified

The working rule was that **a statement by a model is a claim until something was run.** The review itself ran no code, so its "unverified" facts became experiments first (six in the first hour of P0, [progress](docs/progress.md)), and during implementation a guarantee counted only when removing it made a named test fail. Three examples of what that caught:

1. **The claim-guard race: the design lost messages on its error-free path.** The relay publishes a message and then commits the row as `queued`; the design partner's worker claimed a row only if it was already `queued`. A worker that received the message before the commit found nothing to claim, acknowledged the message and dropped it, most likely on an idle system, which is the demo. Earlier review rounds had passed the document; the adversarial review found it, as its only blocker. The claim now accepts the row in any state before `processing` and waits for the relay's commit on the row lock (`TestClaim_WaitsForRelayCommit`).
2. **The test that was supposed to catch that defect did not.** The plan named two tests that "would have caught" the dropped message. In P1 the guard was narrowed back on purpose: the store-level test failed every time, and the end-to-end test of one notification on an idle system **passed 220 times out of 220**, because on that machine the relay's commit always won. A forced-race test that holds the relay's transaction open until the worker's claim is waiting replaced it (`TestE2E_WorkerFasterThanRelayCommit`). `make verify-suite` now repeats this check for seven guarantees: each mutation must fail the tests named for it, and all seven do.
3. **An argument that held only for small bodies.** The provider client must know whether a request had left when a call failed: "not sent" is retried freely, "maybe sent" counts as a possible duplicate. The first rule read a hook of Go's HTTP transport, and when CI showed it firing late, the session's fix was argued from the transport's write buffer and tested with an SMS. The author asked for the same test with the largest body the API accepts, a 100 KB e-mail. It failed: for any request of 4,096 bytes or more the provider could have the request before the hook ran, in the unsafe direction. The rule was replaced by one that does not depend on the buffer and errs towards "maybe sent" (`TestSend_WrittenWhenTheBodyWasReadToTheEnd`).

The mechanisms behind these:

- **Tests are organised by pairs of actors** (relay against worker, cancel against claim, sweeper against a late result), because the blocker was a race between two different actors that same-actor tests could not see. The matrix names 21 tests ([ADR-0010](docs/adr/0010-testing-and-ci.md) §1).
- **Mutation checks:** `make verify-suite` (7 of 7), and nine more mutations run by hand once, in the phase that built the code.
- **Every design decision is tied to tests** in [docs/decision-tests.md](docs/decision-tests.md), which says "partly" or names the gap where a test does not cover it.
- **Measurements in the order production meets them.** Three defects were SQL statements whose plan under `EXPLAIN` was not the plan that runs (a cached plan built before the table filled); they were found by running the statements in the sequence production meets, not by review.
- **Duplicates are counted at the provider,** not inferred from the database, in every chaos and load scenario.
- **CI on every push** (seven jobs), and every failing run kept and explained before the next change. Two defects were found only by the CI runner's scheduling, and one of the session's first explanations of a failure was wrong and is recorded as such.
- **A sub-agent's report is treated as a claim:** the main loop read every diff and ran the tests itself before a commit.

What this did not cover is listed, not hidden: each session entry in [progress](docs/progress.md) ends with "Not verified", the [load-test results](docs/load-test.md) say what was not measured, and the [review log](docs/ai/design-review-log.md) records the cases above in full, together with the reviewer's own mistakes.

## Documentation

| Document | Content |
|---|---|
| [Architecture overview](docs/architecture.md) | The system in one document: principles, diagrams, the state machine, failure handling |
| [Architecture decision records](docs/adr/) | Ten ADRs, each with the decision, the alternatives rejected and the consequences |
| [Error codes](docs/errors.md) | Every problem type and field error code of the API |
| [Load and chaos results](docs/load-test.md) | The measurements, the machine, and what was not measured |
| [Decision tests](docs/decision-tests.md) | Every decision of the ledger with the tests that would fail without it, or the evidence instead, and the gaps |
| [Known limitations and future work](docs/future-work.md) | Limitations, interpretations, deferred items |
| [Decision ledger](docs/ai/decision-ledger.md), [design review log](docs/ai/design-review-log.md) | Why the design is as it is; how it was reviewed and what the implementation overturned |
| [Implementation plan](docs/plan/implementation-plan.md), [progress](docs/progress.md) | Phases and exit criteria; the log of every session |
