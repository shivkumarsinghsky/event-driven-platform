import { runService } from "../../platform/runtime.js";
import { notificationHandlers, simulatedProvider } from "./handlers.js";

await runService({
  schema: "notification",
  hasOutbox: false,
  consumers: async (ctx) => {
    const consumer = ctx.consumer();
    const provider = simulatedProvider(ctx.config.NOTIFICATION_FAILURE_RATE);
    await consumer.start(
      { queue: "notification.order-status", bindings: ["order.status-changed.*"] },
      notificationHandlers(provider),
    );
    return [consumer];
  },
});
