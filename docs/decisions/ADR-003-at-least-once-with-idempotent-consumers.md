# ADR-003: At-Least-Once Delivery With Idempotent Consumers (Inbox)

- **Status:** Accepted
- **Date:** 2026-10-01

## Context

Both the outbox relay and the broker redeliver messages in failure scenarios. Exactly-once delivery across a
network is not achievable in general; duplicates must be expected.

## Decision

Consumers acknowledge only after their work is committed and deduplicate with an **inbox** table
`(consumer, message_id)`. The inbox row is inserted with `ON CONFLICT DO NOTHING` in the **same transaction** as
the handler's database changes; if the insert affects no rows, the message was already processed and is acked
without running the handler.

Handlers are additionally written to be naturally idempotent where cheap (status transitions only from
`PENDING`; existing reservation check), so correctness does not rest on a single mechanism.

## Alternatives Considered

- **Deduplicate in Redis with a TTL** — fast, but not atomic with the database changes; a crash between the two
  causes either a lost update or a duplicate.
- **Rely only on natural idempotency** — works for some handlers but not for inserts with side effects.

## Trade-offs

- One extra insert per message and a table needing retention.
- External side effects (sending email) cannot join the transaction: the provider is called before commit, so a
  crash after sending but before commit causes a resend. This is mitigated by passing the event id as the
  provider's idempotency key, and accepted as at-least-once for providers without that feature.

## Consequences

- Replaying messages (e.g. from a DLQ) is safe.
- Integration tests verify that three deliveries of the same message invoke the handler once.
