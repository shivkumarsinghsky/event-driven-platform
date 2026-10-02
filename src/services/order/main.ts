import { runService } from "../../platform/runtime.js";
import { registerOrderApi } from "./api.js";
import { orderHandlers } from "./handlers.js";

await runService({
  schema: "orders",
  hasOutbox: true,
  api: (ctx) => registerOrderApi(ctx.http, ctx),
  consumers: async (ctx) => {
    const consumer = ctx.consumer();
    await consumer.start(
      { queue: "order.inventory-results", bindings: ["inventory.reserved.*", "inventory.rejected.*"] },
      orderHandlers,
    );
    return [consumer];
  },
});
