import { randomUUID } from "node:crypto";
import { createEnvelope } from "../../platform/contracts/envelope.js";
import {
  type InventoryRejected,
  type InventoryReserved,
  readOrderPlaced,
} from "../../platform/contracts/events.js";
import type { Handler } from "../../platform/messaging/consumer.js";
import { enqueue } from "../../platform/outbox/outbox.js";

export const SERVICE = "inventory-service";

/**
 * Reserve stock for every line of an order, all-or-nothing, inside the consumer's transaction.
 * A SAVEPOINT lets us undo partial reservations while still committing the inbox record and the
 * `inventory.rejected` event.
 */
export const reserveStock: Handler = async ({ envelope, tx, log }) => {
  const order = readOrderPlaced(envelope); // upcasts v1 → v2; throws (permanent) on unknown versions

  const existing = await tx.query("SELECT 1 FROM inventory.reservations WHERE order_id = $1", [
    order.orderId,
  ]);
  if (existing.rowCount) {
    log.info({ orderId: order.orderId }, "reservation already exists; nothing to do");
    return;
  }

  await tx.query("SAVEPOINT reserve_lines");
  // Lock rows in a deterministic order (by SKU) to avoid deadlocks between concurrent orders.
  const lines = [...order.lines].sort((a, b) => a.sku.localeCompare(b.sku));
  for (const line of lines) {
    const updated = await tx.query(
      `UPDATE inventory.stock SET reserved = reserved + $2
        WHERE sku = $1 AND on_hand - reserved >= $2`,
      [line.sku, line.quantity],
    );
    if (updated.rowCount === 0) {
      await tx.query("ROLLBACK TO SAVEPOINT reserve_lines");
      const known = await tx.query("SELECT 1 FROM inventory.stock WHERE sku = $1", [line.sku]);
      const reason = known.rowCount ? "OUT_OF_STOCK" : "UNKNOWN_SKU";
      await enqueue(
        tx,
        "inventory",
        createEnvelope<InventoryRejected>({
          type: "inventory.rejected",
          version: 1,
          source: SERVICE,
          subject: order.orderId,
          causedBy: envelope,
          data: { orderId: order.orderId, reason, sku: line.sku },
        }),
      );
      log.info({ orderId: order.orderId, sku: line.sku, reason }, "reservation rejected");
      return;
    }
  }

  const reservationId = randomUUID();
  await tx.query("INSERT INTO inventory.reservations (id, order_id, lines) VALUES ($1, $2, $3)", [
    reservationId,
    order.orderId,
    JSON.stringify(order.lines),
  ]);
  await enqueue(
    tx,
    "inventory",
    createEnvelope<InventoryReserved>({
      type: "inventory.reserved",
      version: 1,
      source: SERVICE,
      subject: order.orderId,
      causedBy: envelope,
      data: { orderId: order.orderId, reservationId },
    }),
  );
  log.info({ orderId: order.orderId, reservationId }, "stock reserved");
};

export const inventoryHandlers: Record<string, Handler> = { "order.placed": reserveStock };
