import { z } from "zod";
import type { Envelope } from "./envelope.js";

/**
 * Event catalogue. Schemas are the contract between services; see docs/event-catalog.md.
 *
 * Versioning rules (ADR-004):
 *  - Additive, optional changes keep the version.
 *  - Breaking changes publish a new version; consumers upcast older versions to the latest they understand.
 */

const orderLine = z.object({
  sku: z.string().min(1),
  quantity: z.number().int().positive(),
});

export const orderPlacedV1 = z.object({
  orderId: z.uuid(),
  customerId: z.string().min(1),
  lines: z.array(orderLine).min(1),
  totalAmount: z.number().nonnegative(),
});

/** v2: amounts are integer minor units with an explicit currency (v1 used a float in an implicit currency). */
export const orderPlacedV2 = z.object({
  orderId: z.uuid(),
  customerId: z.string().min(1),
  lines: z.array(orderLine).min(1),
  total: z.object({ amountMinor: z.number().int().nonnegative(), currency: z.string().length(3) }),
});
export type OrderPlaced = z.infer<typeof orderPlacedV2>;

export const inventoryReservedV1 = z.object({ orderId: z.uuid(), reservationId: z.uuid() });
export const inventoryRejectedV1 = z.object({
  orderId: z.uuid(),
  reason: z.enum(["OUT_OF_STOCK", "UNKNOWN_SKU"]),
  sku: z.string(),
});
export const orderStatusChangedV1 = z.object({
  orderId: z.uuid(),
  customerId: z.string().min(1),
  status: z.enum(["CONFIRMED", "REJECTED"]),
  reason: z.string().optional(),
});

export type InventoryReserved = z.infer<typeof inventoryReservedV1>;
export type InventoryRejected = z.infer<typeof inventoryRejectedV1>;
export type OrderStatusChanged = z.infer<typeof orderStatusChangedV1>;

/** Upcast older versions to the latest shape so handlers only deal with one version. */
export function readOrderPlaced(envelope: Envelope): OrderPlaced {
  if (envelope.version === 2) return orderPlacedV2.parse(envelope.data);
  if (envelope.version === 1) {
    const v1 = orderPlacedV1.parse(envelope.data);
    return {
      orderId: v1.orderId,
      customerId: v1.customerId,
      lines: v1.lines,
      total: { amountMinor: Math.round(v1.totalAmount * 100), currency: "EUR" }, // v1 was implicitly EUR
    };
  }
  throw new UnsupportedVersionError(envelope.type, envelope.version);
}

export class UnsupportedVersionError extends Error {
  constructor(type: string, version: number) {
    super(`unsupported version ${version} of ${type}`);
    this.name = "UnsupportedVersionError";
  }
}
