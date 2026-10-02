import type { Channel } from "amqplib";
import { deadLetterQueueName, retryQueueName } from "./retry-policy.js";

/** Single durable topic exchange for all domain events. Routing key = `<type>.v<version>`. */
export const EVENTS_EXCHANGE = "domain.events";

export interface QueueBinding {
  /** Queue name, conventionally `<service>.<purpose>`, e.g. `inventory.order-placed`. */
  queue: string;
  /** Routing-key patterns, e.g. `order.placed.*` to receive every version. */
  bindings: string[];
}

/**
 * Declares (idempotently) the queue topology for one consumer:
 *
 *   domain.events --(binding)--> <queue>
 *   <queue>.retry.<delay>ms  (TTL = delay, dead-letters back to <queue> via the default exchange)
 *   <queue>.dlq              (parking lot for manual inspection and replay)
 *
 * Retry queues have no consumers: messages sit there until their TTL expires, then RabbitMQ routes them
 * back to the work queue. This gives delayed retries without a delayed-message plugin.
 */
export async function declareConsumerTopology(ch: Channel, binding: QueueBinding, retryDelaysMs: number[]) {
  await ch.assertExchange(EVENTS_EXCHANGE, "topic", { durable: true });
  await ch.assertQueue(binding.queue, { durable: true, arguments: { "x-queue-type": "classic" } });
  for (const pattern of binding.bindings) await ch.bindQueue(binding.queue, EVENTS_EXCHANGE, pattern);
  for (const delay of new Set(retryDelaysMs)) {
    await ch.assertQueue(retryQueueName(binding.queue, delay), {
      durable: true,
      arguments: {
        "x-message-ttl": delay,
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": binding.queue,
      },
    });
  }
  await ch.assertQueue(deadLetterQueueName(binding.queue), { durable: true });
}

export async function declareExchange(ch: Channel) {
  await ch.assertExchange(EVENTS_EXCHANGE, "topic", { durable: true });
}
