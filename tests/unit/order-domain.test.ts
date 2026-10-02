import { describe, expect, it } from "vitest";
import { nextStatus, placeOrderRequest, requestHash, totalMinor } from "../../src/services/order/domain.js";

const request = {
  customerId: "c-1",
  currency: "EUR",
  lines: [
    { sku: "SKU-A", quantity: 2, unitPriceMinor: 1_250 },
    { sku: "SKU-B", quantity: 1, unitPriceMinor: 500 },
  ],
};

describe("order domain", () => {
  it("validates requests", () => {
    expect(placeOrderRequest.safeParse(request).success).toBe(true);
    expect(placeOrderRequest.safeParse({ ...request, lines: [] }).success).toBe(false);
    expect(placeOrderRequest.safeParse({ ...request, currency: "eur" }).success).toBe(false);
    expect(
      placeOrderRequest.safeParse({ ...request, lines: [{ sku: "A", quantity: 0, unitPriceMinor: 1 }] })
        .success,
    ).toBe(false);
  });

  it("computes the total server-side in minor units", () => {
    expect(totalMinor(request)).toBe(3_000);
  });

  it("hashes equal requests equally and different requests differently", () => {
    expect(requestHash(request)).toBe(requestHash(structuredClone(request)));
    expect(requestHash({ ...request, customerId: "c-2" })).not.toBe(requestHash(request));
  });

  it("only transitions PENDING orders, making replays and late events no-ops", () => {
    expect(nextStatus("PENDING", "reserved")).toBe("CONFIRMED");
    expect(nextStatus("PENDING", "rejected")).toBe("REJECTED");
    expect(nextStatus("CONFIRMED", "rejected")).toBeNull();
    expect(nextStatus("REJECTED", "reserved")).toBeNull();
  });
});
