import { runService } from "../../platform/runtime.js";
import { inventoryHandlers } from "./handlers.js";

await runService({
  schema: "inventory",
  hasOutbox: true,
  api: (ctx) => {
    ctx.http.get<{ Params: { sku: string } }>("/stock/:sku", async (req, reply) => {
      const { rows } = await ctx.db.query(
        'SELECT sku, on_hand AS "onHand", reserved, on_hand - reserved AS available FROM inventory.stock WHERE sku = $1',
        [req.params.sku],
      );
      return rows[0] ?? reply.code(404).send({ error: "unknown sku" });
    });
  },
  consumers: async (ctx) => {
    const consumer = ctx.consumer();
    // Binding with a wildcard version receives v1 and v2; the handler upcasts.
    await consumer.start(
      { queue: "inventory.order-placed", bindings: ["order.placed.*"] },
      inventoryHandlers,
    );
    return [consumer];
  },
});
