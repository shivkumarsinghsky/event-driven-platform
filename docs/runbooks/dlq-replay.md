# Runbook: Messages in a Dead-Letter Queue

## Symptoms

- Alert: depth of a `*.dlq` queue > 0.
- `messages_consumed_total{outcome="dead_lettered"}` increasing.

## 1. Inspect

Open the RabbitMQ management UI (<http://localhost:15672> locally) → Queues → `<queue>.dlq` → *Get messages*
(choose *Nack message requeue true* to leave them in place). Headers explain the failure:

| Header | Meaning |
|---|---|
| `x-attempts` | Attempts made, including retries |
| `x-last-error` | Error message of the last attempt |
| `x-failed-at` | Time of the last failure |

Search logs by `messageId` or `correlationId` for the full context.

## 2. Classify

- **Permanent** (`x-attempts: 1`, validation or business errors): the message itself is wrong or the consumer
  does not support it. Fix the producer or deploy consumer support (e.g. a new version upcaster). Replaying
  without a fix will dead-letter again.
- **Transient that outlasted the retries** (dependency outage): verify the dependency is healthy.

## 3. Replay

```bash
npm run dlq:replay -- inventory.order-placed --limit 100
```

Replay keeps the original message id. Consumers that already processed a message (inbox) skip it, so replaying
is safe even if some messages were partially handled.

## 4. Follow up

If messages were dead-lettered because of a code defect, add a test reproducing the payload before closing the
incident.
