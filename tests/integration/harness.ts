import { join } from "node:path";
import type { ChannelModel } from "amqplib";
import { createPool, type Db, migrate } from "../../src/platform/db.js";
import { createHttpServer } from "../../src/platform/http/server.js";
import { createLogger } from "../../src/platform/logger.js";
import { connectBroker, Publisher } from "../../src/platform/messaging/broker.js";
import { Consumer, type Handler } from "../../src/platform/messaging/consumer.js";
import { deadLetterQueueName, retryQueueName } from "../../src/platform/messaging/retry-policy.js";
import { createMetrics } from "../../src/platform/metrics.js";
import { OutboxRelay } from "../../src/platform/outbox/outbox.js";
import { inventoryHandlers } from "../../src/services/inventory/handlers.js";
import { type NotificationProvider, notificationHandlers } from "../../src/services/notification/handlers.js";
import { registerOrderApi } from "../../src/services/order/api.js";
import { orderHandlers } from "../../src/services/order/handlers.js";

export const DATABASE_URL = process.env.DATABASE_URL;
export const RABBITMQ_URL = process.env.RABBITMQ_URL;
export const integrationEnabled = Boolean(DATABASE_URL && RABBITMQ_URL);

export const log = createLogger("integration-test", process.env.TEST_LOG_LEVEL ?? "silent");

export async function resetBroker(connection: ChannelModel, queues: string[], retryDelaysMs: number[]) {
  const ch = await connection.createChannel();
  for (const q of queues) {
    await ch.deleteQueue(q);
    await ch.deleteQueue(deadLetterQueueName(q));
    for (const d of retryDelaysMs) await ch.deleteQueue(retryQueueName(q, d));
  }
  await ch.close();
}

export async function resetDb(db: Db, schemas: string[]) {
  for (const s of schemas) await db.query(`DROP SCHEMA IF EXISTS ${s} CASCADE`);
}

export const eventually = async <T>(probe: () => Promise<T | undefined>, timeoutMs = 15_000): Promise<T> => {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = await probe();
    if (value !== undefined) return value;
    if (Date.now() > deadline) throw new Error("condition not met before timeout");
    await new Promise((r) => setTimeout(r, 50));
  }
};

const QUEUES = ["order.inventory-results", "inventory.order-placed", "notification.order-status"];

/** Wires all three services in one process against real PostgreSQL and RabbitMQ. */
export async function startPlatform(provider: NotificationProvider, retryDelaysMs = [100, 200]) {
  const db = createPool(DATABASE_URL!);
  const connection = await connectBroker(RABBITMQ_URL!, log, { exitOnClose: false });
  await resetDb(db, ["orders", "inventory", "notification"]);
  await resetBroker(connection, QUEUES, retryDelaysMs);
  for (const schema of ["orders", "inventory", "notification"]) {
    await migrate(db, join("migrations", schema), schema, log);
  }
  const metrics = createMetrics("integration");
  const publisher = await Publisher.create(connection);
  const app = createHttpServer(log, { db, metrics, brokerConnected: () => true });
  registerOrderApi(app, { db });
  await app.ready();

  const consumers: Consumer[] = [];
  const consume = async (
    schema: string,
    queue: string,
    bindings: string[],
    handlers: Record<string, Handler>,
  ) => {
    const c = new Consumer(connection, db, log, metrics, { schema, prefetch: 10, retryDelaysMs });
    await c.start({ queue, bindings }, handlers);
    consumers.push(c);
  };
  await consume("orders", QUEUES[0]!, ["inventory.reserved.*", "inventory.rejected.*"], orderHandlers);
  await consume("inventory", QUEUES[1]!, ["order.placed.*"], inventoryHandlers);
  await consume("notification", QUEUES[2]!, ["order.status-changed.*"], notificationHandlers(provider));

  const relays = ["orders", "inventory"].map(
    (schema) => new OutboxRelay(db, schema, publisher, log, metrics, { batchSize: 50, pollIntervalMs: 50 }),
  );
  relays.forEach((r) => r.start());

  return {
    app,
    db,
    metrics,
    connection,
    async stop() {
      await Promise.all(consumers.map((c) => c.stop()));
      await Promise.all(relays.map((r) => r.stop()));
      await app.close();
      await publisher.close();
      await connection.close();
      await db.end();
    },
  };
}
