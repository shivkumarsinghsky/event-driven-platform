import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import { createEnvelope, envelopeSchema, routingKey } from "../../src/platform/contracts/envelope.js";
import { readOrderPlaced, UnsupportedVersionError } from "../../src/platform/contracts/events.js";
import { isPermanent } from "../../src/platform/messaging/errors.js";

describe("envelope", () => {
  it("creates a valid envelope and propagates correlation and causation", () => {
    const first = createEnvelope({
      type: "order.placed",
      version: 2,
      source: "test",
      subject: "o1",
      data: {},
    });
    const second = createEnvelope({
      type: "inventory.reserved",
      version: 1,
      source: "test",
      subject: "o1",
      data: {},
      causedBy: first,
    });
    expect(envelopeSchema.safeParse(first).success).toBe(true);
    expect(second.correlationId).toBe(first.correlationId);
    expect(second.causationId).toBe(first.id);
    expect(first.causationId).toBeNull();
    expect(routingKey(first)).toBe("order.placed.v2");
  });

  it("rejects malformed types", () => {
    const bad = {
      ...createEnvelope({ type: "x.y", version: 1, source: "s", subject: "s", data: {} }),
      type: "Bad Type",
    };
    expect(envelopeSchema.safeParse(bad).success).toBe(false);
  });
});

describe("order.placed versioning", () => {
  const orderId = randomUUID();
  const lines = [{ sku: "SKU-1", quantity: 2 }];

  it("reads v2 as-is", () => {
    const e = createEnvelope({
      type: "order.placed",
      version: 2,
      source: "s",
      subject: orderId,
      data: { orderId, customerId: "c1", lines, total: { amountMinor: 1999, currency: "USD" } },
    });
    expect(readOrderPlaced(e).total).toEqual({ amountMinor: 1999, currency: "USD" });
  });

  it("upcasts v1 (float amount, implicit EUR) to the v2 shape", () => {
    const e = createEnvelope({
      type: "order.placed",
      version: 1,
      source: "s",
      subject: orderId,
      data: { orderId, customerId: "c1", lines, totalAmount: 19.99 },
    });
    expect(readOrderPlaced(e)).toEqual({
      orderId,
      customerId: "c1",
      lines,
      total: { amountMinor: 1999, currency: "EUR" },
    });
  });

  it("treats unknown versions and invalid payloads as permanent errors", () => {
    const v9 = createEnvelope({ type: "order.placed", version: 9, source: "s", subject: orderId, data: {} });
    expect(() => readOrderPlaced(v9)).toThrow(UnsupportedVersionError);
    let caught: unknown;
    try {
      readOrderPlaced({ ...v9, version: 2, data: { orderId: "not-a-uuid" } });
    } catch (e) {
      caught = e;
    }
    expect(isPermanent(caught)).toBe(true);
    expect(isPermanent(new UnsupportedVersionError("x", 9))).toBe(true);
    expect(isPermanent(new Error("connection reset"))).toBe(false);
  });
});
