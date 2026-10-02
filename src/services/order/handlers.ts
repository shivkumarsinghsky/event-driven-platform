import { createEnvelope } from "../../platform/contracts/envelope.js";
import {
  inventoryRejectedV1,
  inventoryReservedV1,
  type OrderStatusChanged,
} from "../../platform/contracts/events.js";
import type { Handler, HandlerContext } from "../../platform/messaging/consumer.js";
import { enqueue } from "../../platform/outbox/outbox.js";
import { nextStatus, type OrderStatus, SERVICE } from "./domain.js";

async function applyOutcome(
  { envelope, tx, log }: HandlerContext,
  orderId: string,
  outcome: "reserved" | "rejected",
  reason?: string,
) {
  const { rows } = await tx.query<{ status: OrderStatus; customer_id: string }>(
    "SELECT status, customer_id FROM orders.orders WHERE id = $1 FOR UPDATE",
    [orderId],
  );
  const order = rows[0];
  if (!order) {
    log.warn({ orderId }, "inventory result for unknown order; ignoring");
    return;
  }
  const status = nextStatus(order.status, outcome);
  if (!status) {
    log.info({ orderId, current: order.status }, "order already final; ignoring stale or duplicate result");
    return;
  }
  await tx.query(
    "UPDATE orders.orders SET status = $2, reject_reason = $3, updated_at = now() WHERE id = $1",
    [orderId, status, reason ?? null],
  );
  await enqueue(
    tx,
    "orders",
    createEnvelope<OrderStatusChanged>({
      type: "order.status-changed",
      version: 1,
      source: SERVICE,
      subject: orderId,
      causedBy: envelope,
      data: { orderId, customerId: order.customer_id, status, reason },
    }),
  );
  log.info({ orderId, status }, "order status updated");
}

export const orderHandlers: Record<string, Handler> = {
  "inventory.reserved": async (ctx) => {
    const data = inventoryReservedV1.parse(ctx.envelope.data);
    await applyOutcome(ctx, data.orderId, "reserved");
  },
  "inventory.rejected": async (ctx) => {
    const data = inventoryRejectedV1.parse(ctx.envelope.data);
    await applyOutcome(ctx, data.orderId, "rejected", `${data.reason}: ${data.sku}`);
  },
};
