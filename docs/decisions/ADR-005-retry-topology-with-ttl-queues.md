# ADR-005: Delayed Retries With TTL Queues, Then Dead-Lettering

- **Status:** Accepted
- **Date:** 2026-10-01

## Context

Handlers fail for transient reasons (database failover, provider outage). Immediate redelivery (`nack` with
requeue) creates a hot loop that hammers the failing dependency and blocks the queue; never giving up keeps poison
messages cycling forever.

## Decision

On failure the consumer classifies the error:

- **Permanent** (invalid envelope/payload, unsupported version, `PermanentError`) → publish to `<queue>.dlq`.
- **Transient** → publish to `<queue>.retry.<delay>ms`, a queue with no consumers whose `x-message-ttl` is the
  delay and whose dead-letter target is the original queue. After the last configured delay → `<queue>.dlq`.

The attempt count is carried in an `x-attempts` header. The original message is acked only after the broker
confirms the retry/DLQ publish; if that publish fails, the original is nacked with requeue.

## Alternatives Considered

- **`nack` with requeue** — no delay, hot loop, no attempt counting.
- **RabbitMQ delayed-message plugin** — single exchange for all delays, but a non-default plugin and its delayed
  messages are not replicated.
- **Quorum queue delivery limits** — gives a max attempt count and DLX but no backoff delay.
- **Retry inside the consumer with sleep** — holds the prefetch slot and the message, reducing throughput.

## Trade-offs

- One retry queue per distinct delay per consumer queue (three by default).
- Retried messages go to the back of the work queue, so ordering is not preserved for retried messages.

## Consequences

Delays are configurable (`RETRY_DELAYS_MS`); the decision logic is a pure function (`decideOnFailure`) with unit
tests, and the topology is verified by integration tests against a real broker.
