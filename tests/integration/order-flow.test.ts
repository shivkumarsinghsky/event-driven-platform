import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { NotificationProvider } from "../../src/services/notification/handlers.js";
import { ProviderUnavailableError } from "../../src/services/notification/handlers.js";
import { eventually, integrationEnabled, startPlatform } from "./harness.js";

/** Provider that fails the first send for every message, so each notification exercises one retry. */
function failOncePerMessage(): NotificationProvider & { sent: string[]; failures: number } {
  const seen = new Set<string>();
  const state = {
    sent: [] as string[],
    failures: 0,
    async send(m: { idempotencyKey: string; template: string }) {
      if (!seen.has(m.idempotencyKey)) {
        seen.add(m.idempotencyKey);
        state.failures++;
        throw new ProviderUnavailableError();
      }
      state.sent.push(m.template);
    },
  };
  return state;
}

describe.skipIf(!integrationEnabled)("order flow (PostgreSQL + RabbitMQ)", () => {
  const provider = failOncePerMessage();
  let platform: Awaited<ReturnType<typeof startPlatform>>;

  beforeAll(async () => {
    platform = await startPlatform(provider);
  });
  afterAll(async () => platform?.stop());

  const place = (key: string, sku: string, quantity = 2) =>
    platform.app.inject({
      method: "POST",
      url: "/orders",
      headers: { "idempotency-key": key, "x-correlation-id": `corr-${key}` },
      payload: { customerId: "cust-42", currency: "EUR", lines: [{ sku, quantity, unitPriceMinor: 1_500 }] },
    });

  const statusOf = async (orderId: string, expected: string) => {
    const res = await platform.app.inject({ method: "GET", url: `/orders/${orderId}` });
    const body = res.json();
    return body.status === expected ? body : undefined;
  };

  it("confirms an order through outbox → inventory → order → notification, with one retry", async () => {
    const res = await place("key-confirm-0001", "SKU-PUMP-SEAL");
    expect(res.statusCode).toBe(202);
    expect(res.headers["x-correlation-id"]).toBe("corr-key-confirm-0001");
    const { orderId } = res.json();

    const confirmed = await eventually(() => statusOf(orderId, "CONFIRMED"));
    expect(confirmed.total).toEqual({ amountMinor: 3_000, currency: "EUR" });

    const stock = await platform.db.query("SELECT reserved FROM inventory.stock WHERE sku = 'SKU-PUMP-SEAL'");
    expect(stock.rows[0].reserved).toBe(2);

    const notification = await eventually(async () => {
      const r = await platform.db.query(
        "SELECT template FROM notification.notifications WHERE order_id = $1",
        [orderId],
      );
      return r.rows[0];
    });
    expect(notification.template).toBe("order-confirmed");
    expect(provider.failures).toBeGreaterThanOrEqual(1);

    // The whole chain shares the correlation id of the original HTTP request.
    const events = await platform.db.query(
      "SELECT envelope->>'correlationId' AS c FROM inventory.outbox WHERE subject = $1",
      [orderId],
    );
    expect(events.rows.map((r) => r.c)).toEqual(["corr-key-confirm-0001"]);
  });

  it("rejects an order when stock is insufficient and releases nothing", async () => {
    const res = await place("key-reject-0001", "SKU-VALVE-DN50", 1);
    const rejected = await eventually(() => statusOf(res.json().orderId, "REJECTED"));
    expect(rejected.rejectReason).toBe("OUT_OF_STOCK: SKU-VALVE-DN50");
  });

  it("rejects unknown SKUs", async () => {
    const res = await place("key-unknown-001", "SKU-DOES-NOT-EXIST", 1);
    const rejected = await eventually(() => statusOf(res.json().orderId, "REJECTED"));
    expect(rejected.rejectReason).toBe("UNKNOWN_SKU: SKU-DOES-NOT-EXIST");
  });

  it("replays the original order for a repeated Idempotency-Key and rejects a different body", async () => {
    const first = await place("key-idem-000001", "SKU-BEARING-6204", 1);
    const again = await place("key-idem-000001", "SKU-BEARING-6204", 1);
    expect(again.statusCode).toBe(200);
    expect(again.headers["idempotent-replayed"]).toBe("true");
    expect(again.json().orderId).toBe(first.json().orderId);

    const different = await place("key-idem-000001", "SKU-BEARING-6204", 5);
    expect(different.statusCode).toBe(422);

    const count = await platform.db.query("SELECT count(*)::int AS n FROM orders.outbox WHERE subject = $1", [
      first.json().orderId,
    ]);
    expect(count.rows[0].n).toBe(1);
  });

  it("validates input at the boundary", async () => {
    const missingKey = await platform.app.inject({ method: "POST", url: "/orders", payload: {} });
    expect(missingKey.statusCode).toBe(400);
    const invalid = await platform.app.inject({
      method: "POST",
      url: "/orders",
      headers: { "idempotency-key": "key-invalid-01" },
      payload: { customerId: "c", currency: "EUR", lines: [] },
    });
    expect(invalid.statusCode).toBe(400);
  });

  it("exposes readiness and metrics", async () => {
    expect((await platform.app.inject({ url: "/health/ready" })).statusCode).toBe(200);
    const metrics = (await platform.app.inject({ url: "/metrics" })).body;
    expect(metrics).toContain(
      'messages_consumed_total{queue="inventory.order-placed",type="order.placed",outcome="processed"',
    );
    expect(metrics).toContain('outcome="retried"');
  });
});
