# Event-Driven Notification System

> **Work in progress.** The core path is implemented: a notification created through the API is stored, published, rate-limited and sent to the provider. Batch creation, cancel, listing, templates, scheduling, the WebSocket gateway and the dashboards follow in the phases of the [implementation plan](docs/plan/implementation-plan.md); [progress](docs/progress.md) shows where the work stands. This README grows with it.

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
- API examples and `make demo`
- Requirements map with evidence
- Interpretations of the assessment and known limitations
- Load-test results
- How AI was used, and how AI output was verified
