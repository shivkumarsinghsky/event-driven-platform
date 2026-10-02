# ADR-001: RabbitMQ as the Message Broker

- **Status:** Accepted
- **Date:** 2026-10-01

## Context

Services exchange domain events. The workload is business events (orders, reservations, notifications) at
moderate volume, where per-message retry, dead-lettering and flexible routing matter more than replaying long
event history or very high throughput.

## Decision

Use **RabbitMQ** with one durable topic exchange, one durable queue per consumer, publisher confirms and manual
consumer acknowledgements.

## Alternatives Considered

| Option | Strengths | Why not chosen here |
|---|---|---|
| **Apache Kafka** | Very high throughput, partition-ordered log, replay from offsets, long retention | Per-message retry and DLQ must be built (retry topics); one slow message blocks its partition; heavier to operate locally |
| **Cloud queues (SQS/SNS, Azure Service Bus)** | Managed, built-in DLQ | Ties the reference implementation to one cloud; not runnable fully offline |
| **Redis Streams** | Simple, already common in stacks | Weaker durability guarantees and tooling for DLQ/inspection |

## Trade-offs

- No replay of historical events: a new consumer only receives events published after its queue exists. Event
  history, if needed, must come from the producers' outbox tables or a separate event store.
- Ordering is per queue and weakened by competing consumers and retries; the domain logic is designed to be
  order-insensitive.

## Consequences

- Retry with delay and DLQ are expressed as broker topology ([ADR-005](ADR-005-retry-topology-with-ttl-queues.md)).
- The messaging layer is isolated in `src/platform/messaging`, so moving to Kafka would change the topology and
  consumer loop but not the outbox, inbox, envelope or handlers.
