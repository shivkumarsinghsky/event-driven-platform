import { createHash } from "node:crypto";
import { z } from "zod";

export const SERVICE = "order-service";

export const placeOrderRequest = z.object({
  customerId: z.string().min(1).max(64),
  currency: z.string().regex(/^[A-Z]{3}$/),
  lines: z
    .array(
      z.object({
        sku: z.string().min(1).max(64),
        quantity: z.number().int().positive().max(1_000),
        unitPriceMinor: z.number().int().nonnegative(),
      }),
    )
    .min(1)
    .max(100),
});
export type PlaceOrderRequest = z.infer<typeof placeOrderRequest>;

export const totalMinor = (req: PlaceOrderRequest) =>
  req.lines.reduce((sum, l) => sum + l.quantity * l.unitPriceMinor, 0);

/** Hash of the canonical request; same idempotency key with a different body is rejected (422). */
export function requestHash(req: PlaceOrderRequest): string {
  const canonical = JSON.stringify({
    customerId: req.customerId,
    currency: req.currency,
    lines: req.lines.map((l) => [l.sku, l.quantity, l.unitPriceMinor]),
  });
  return createHash("sha256").update(canonical).digest("hex");
}

export type OrderStatus = "PENDING" | "CONFIRMED" | "REJECTED";

/** Only PENDING orders transition; replays and out-of-order events are no-ops. */
export function nextStatus(
  current: OrderStatus,
  outcome: "reserved" | "rejected",
): "CONFIRMED" | "REJECTED" | null {
  if (current !== "PENDING") return null;
  return outcome === "reserved" ? "CONFIRMED" : "REJECTED";
}
