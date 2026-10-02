import { randomUUID } from "node:crypto";
import type { FastifyInstance } from "fastify";
import { createEnvelope } from "../../platform/contracts/envelope.js";
import type { OrderPlaced } from "../../platform/contracts/events.js";
import { withTransaction } from "../../platform/db.js";
import { enqueue } from "../../platform/outbox/outbox.js";
import type { ServiceContext } from "../../platform/runtime.js";
import { placeOrderRequest, requestHash, SERVICE, totalMinor } from "./domain.js";

interface OrderRow {
  id: string;
  customer_id: string;
  status: string;
  lines: unknown;
  total_minor: string;
  currency: string;
  reject_reason: string | null;
  created_at: Date;
  updated_at: Date;
}

const toDto = (r: OrderRow) => ({
  orderId: r.id,
  customerId: r.customer_id,
  status: r.status,
  lines: r.lines,
  total: { amountMinor: Number(r.total_minor), currency: r.currency },
  rejectReason: r.reject_reason ?? undefined,
  createdAt: r.created_at.toISOString(),
  updatedAt: r.updated_at.toISOString(),
});

export function registerOrderApi(app: FastifyInstance, { db }: Pick<ServiceContext, "db">): void {
  /**
   * POST /orders — accepts the order and returns 202. Confirmation happens asynchronously once inventory
   * responds; clients poll GET /orders/:id (or subscribe to notifications).
   */
  app.post("/orders", async (req, reply) => {
    const key = req.headers["idempotency-key"];
    if (typeof key !== "string" || key.length < 8 || key.length > 128) {
      return reply.code(400).send({ error: "Idempotency-Key header (8-128 chars) is required" });
    }
    const parsed = placeOrderRequest.safeParse(req.body);
    if (!parsed.success) {
      return reply.code(400).send({ error: "invalid request", issues: parsed.error.issues });
    }
    const body = parsed.data;
    const hash = requestHash(body);

    const result = await withTransaction(db, async (tx) => {
      const orderId = randomUUID();
      // Insert the order, then claim the key. A concurrent request with the same key blocks on the key insert
      // until this transaction commits, then sees the conflict and replays the original order.
      await tx.query(
        "INSERT INTO orders.orders (id, customer_id, status, lines, total_minor, currency) VALUES ($1,$2,'PENDING',$3,$4,$5)",
        [orderId, body.customerId, JSON.stringify(body.lines), totalMinor(body), body.currency],
      );
      const claimed = await tx.query(
        `INSERT INTO orders.idempotency_keys (key, request_hash, order_id) VALUES ($1, $2, $3)
         ON CONFLICT (key) DO NOTHING`,
        [key, hash, orderId],
      );
      if (claimed.rowCount === 0) {
        await tx.query("DELETE FROM orders.orders WHERE id = $1", [orderId]);
        const existing = await tx.query<{ request_hash: string; order_id: string }>(
          "SELECT request_hash, order_id FROM orders.idempotency_keys WHERE key = $1",
          [key],
        );
        const row = existing.rows[0]!;
        return row.request_hash === hash
          ? { kind: "replay" as const, orderId: row.order_id }
          : { kind: "mismatch" as const };
      }
      const event = createEnvelope<OrderPlaced>({
        type: "order.placed",
        version: 2,
        source: SERVICE,
        subject: orderId,
        correlationId: req.id,
        data: {
          orderId,
          customerId: body.customerId,
          lines: body.lines.map(({ sku, quantity }) => ({ sku, quantity })),
          total: { amountMinor: totalMinor(body), currency: body.currency },
        },
      });
      await enqueue(tx, "orders", event);
      return { kind: "created" as const, orderId };
    });

    if (result.kind === "mismatch") {
      return reply
        .code(422)
        .send({ error: "Idempotency-Key was already used with a different request body" });
    }
    const { rows } = await db.query<OrderRow>("SELECT * FROM orders.orders WHERE id = $1", [result.orderId]);
    reply.header("location", `/orders/${result.orderId}`);
    if (result.kind === "replay") reply.header("idempotent-replayed", "true");
    return reply.code(result.kind === "created" ? 202 : 200).send(toDto(rows[0]!));
  });

  app.get<{ Params: { id: string } }>("/orders/:id", async (req, reply) => {
    if (!/^[0-9a-f-]{36}$/i.test(req.params.id)) return reply.code(400).send({ error: "invalid order id" });
    const { rows } = await db.query<OrderRow>("SELECT * FROM orders.orders WHERE id = $1", [req.params.id]);
    if (!rows[0]) return reply.code(404).send({ error: "order not found" });
    return toDto(rows[0]);
  });
}
