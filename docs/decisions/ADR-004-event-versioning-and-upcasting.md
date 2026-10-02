# ADR-004: Event Versioning and Upcasting

- **Status:** Accepted
- **Date:** 2026-10-01

## Context

Event schemas change. Producers and consumers deploy independently, and old messages may sit in queues, retry
queues or DLQs for a long time. A breaking change must not break consumers that still receive old messages.

## Decision

- Every envelope carries an integer `version`; the routing key includes it (`order.placed.v2`).
- Additive optional fields do not change the version; breaking changes create a new version.
- Producers publish only the latest version. Consumers bind with a version wildcard (`order.placed.*`) and
  **upcast** older versions to the latest shape in one function (`readOrderPlaced`), so handlers only see one
  shape.
- Unknown versions are permanent errors and go to the DLQ, where they can be replayed after the consumer is
  upgraded.

## Alternatives Considered

- **Schema registry with Avro/Protobuf compatibility checks** — stronger guarantees and tooling, natural with
  Kafka; more infrastructure than this reference needs.
- **Publishing both versions during a migration** — avoids consumer changes but doubles traffic and complicates
  the producer.
- **Content-type versioning only (no routing key version)** — prevents routing different versions to
  different queues.

## Trade-offs

Upcasters accumulate and must be maintained until old versions are drained everywhere.

## Consequences

`order.placed` demonstrates the approach: v1 (float amount, implicit EUR) is upcast to v2 (integer minor units +
currency), covered by unit tests.
