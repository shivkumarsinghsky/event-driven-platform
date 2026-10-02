# ADR-002: Transactional Outbox for Publishing Events

- **Status:** Accepted
- **Date:** 2026-10-01

## Context

Services change their database and must publish an event about the change. Writing to PostgreSQL and publishing
to RabbitMQ are two operations against two systems; doing them independently (dual write) loses events or
publishes events for rolled-back changes when a process crashes between them.

## Decision

Every service that publishes writes events to its own `outbox` table **in the same transaction** as the business
change. A relay (`OutboxRelay`) polls unpublished rows with `FOR UPDATE SKIP LOCKED`, publishes with publisher
confirms, and marks rows published only after the broker confirms.

## Alternatives Considered

- **Publish after commit** — simple, but an event is lost if the process dies after commit.
- **Publish before commit** — phantom events if the transaction then fails.
- **Change data capture (Debezium) on the outbox table** — lower latency and no polling load; requires Kafka
  Connect infrastructure. A good evolution path, unnecessary for this reference.

## Trade-offs

- At-least-once publication: a crash after confirm but before the update republishes the event.
- Publication latency equals the poll interval when idle (default 500 ms); the relay drains continuously while
  there is a backlog.
- The outbox table grows and needs a retention job.

## Consequences

- Consumers must be idempotent ([ADR-003](ADR-003-at-least-once-with-idempotent-consumers.md)).
- `outbox_pending` is exported as a metric to detect a stuck relay.
