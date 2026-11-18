# Event-Driven Notification System

> **Work in progress.** This repository currently contains the design documents and the implementation plan only. There is no runnable code yet. This README is a skeleton and is completed as the implementation proceeds.

A notification service written in Go that accepts SMS, Email and Push notifications through a REST API and delivers them asynchronously through an external provider. It is built around a Postgres outbox, a RabbitMQ dispatch broker with strict priority, a per-channel rate limit of at most 100 messages per second, retries classified by failure scope, and real-time status tracking.

## Quick start

Coming in P1.

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

- Quick start with `docker-compose up`, including how to create a webhook.site URL
- API examples and `make demo`
- Requirements map with evidence
- Interpretations of the assessment and known limitations
- Load-test results
- How AI was used, and how AI output was verified
