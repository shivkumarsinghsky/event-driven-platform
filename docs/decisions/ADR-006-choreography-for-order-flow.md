# ADR-006: Choreography for the Order Flow

- **Status:** Accepted
- **Date:** 2026-10-01

## Context

Placing an order involves three services. The flow could be coordinated by an orchestrator that sends commands
or emerge from services reacting to each other's events.

## Decision

Use **choreography**: order-service publishes `order.placed`; inventory reacts and publishes its outcome;
order-service reacts and publishes `order.status-changed`; notification reacts.

## Alternatives Considered

- **Orchestration (saga orchestrator)** — explicit process state in one place, easier to answer "where is this
  order stuck?", natural compensation handling. Preferred for longer flows (payment, shipping, cancellation) —
  see the orchestrated saga in [microservices-patterns](https://github.com/shivkumarsinghsky/microservices-patterns).

## Trade-offs

- The end-to-end flow is implicit; it is documented in [architecture.md](../architecture.md) and traceable through
  `correlationId`/`causationId`, but no single component owns it.
- Adding steps (payment) increases the risk of cyclic dependencies between services' events.

## Consequences

If the flow grows beyond three or four steps or needs compensations across several services, migrate the
coordination to an orchestrator; the outbox, inbox and messaging layer remain unchanged.
