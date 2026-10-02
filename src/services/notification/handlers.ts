import { randomUUID } from "node:crypto";
import { orderStatusChangedV1 } from "../../platform/contracts/events.js";
import type { Handler } from "../../platform/messaging/consumer.js";

/** Abstraction over an email/SMS/push provider. */
export interface NotificationProvider {
  send(message: { to: string; template: string; data: unknown; idempotencyKey: string }): Promise<void>;
}

export class ProviderUnavailableError extends Error {
  constructor() {
    super("notification provider unavailable (simulated)");
    this.name = "ProviderUnavailableError";
  }
}

/**
 * Simulated provider that fails with a configurable probability, used to demonstrate retries and
 * dead-lettering. `random` is injectable for deterministic tests.
 */
export function simulatedProvider(
  failureRate: number,
  random: () => number = Math.random,
): NotificationProvider {
  return {
    async send() {
      if (random() < failureRate) throw new ProviderUnavailableError();
    },
  };
}

/**
 * External side effects cannot join the database transaction. The provider call happens first, then the
 * notification row and inbox record commit together. If the commit fails after a successful send, the retry
 * sends again — so the source message id is passed as the provider's idempotency key to suppress duplicates
 * on the provider side (ADR-003).
 */
export function notificationHandlers(provider: NotificationProvider): Record<string, Handler> {
  return {
    "order.status-changed": async ({ envelope, tx, log }) => {
      const event = orderStatusChangedV1.parse(envelope.data);
      const template = event.status === "CONFIRMED" ? "order-confirmed" : "order-rejected";
      await provider.send({ to: event.customerId, template, data: event, idempotencyKey: envelope.id });
      await tx.query(
        `INSERT INTO notification.notifications (id, order_id, customer_id, channel, template, source_message_id)
         VALUES ($1, $2, $3, 'email', $4, $5)`,
        [randomUUID(), event.orderId, event.customerId, template, envelope.id],
      );
      log.info({ orderId: event.orderId, template }, "notification sent");
    },
  };
}
