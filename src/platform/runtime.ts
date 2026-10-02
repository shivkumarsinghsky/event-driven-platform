import { join, resolve } from "node:path";
import type { ChannelModel } from "amqplib";
import type { FastifyInstance } from "fastify";
import { type Config, loadConfig } from "../config.js";
import { createPool, type Db, migrate } from "./db.js";
import { createHttpServer } from "./http/server.js";
import { createLogger, type Logger } from "./logger.js";
import { connectBroker, Publisher } from "./messaging/broker.js";
import { Consumer } from "./messaging/consumer.js";
import { createMetrics, type Metrics } from "./metrics.js";
import { OutboxRelay } from "./outbox/outbox.js";

export interface ServiceContext {
  config: Config;
  log: Logger;
  db: Db;
  connection: ChannelModel;
  publisher: Publisher;
  metrics: Metrics;
  http: FastifyInstance;
  schema: string;
  /** Create a consumer bound to this service's inbox. */
  consumer(): Consumer;
}

export interface ServiceDefinition {
  /** PostgreSQL schema owned by the service; also the migrations sub-directory name. */
  schema: string;
  /** Register HTTP routes (role "api"). */
  api?: (ctx: ServiceContext) => Promise<void> | void;
  /** Start consumers (role "consumer"); return them so shutdown can drain them. */
  consumers?: (ctx: ServiceContext) => Promise<Consumer[]>;
  /** Whether this service publishes through an outbox (role "relay"). */
  hasOutbox: boolean;
}

/**
 * Shared bootstrap: configuration → logging → migrations → broker → roles → graceful shutdown.
 * Health and metrics endpoints are always served, so workers are observable and probe-able too.
 */
export async function runService(def: ServiceDefinition): Promise<void> {
  const config = loadConfig();
  const log = createLogger(config.SERVICE_NAME, config.LOG_LEVEL);
  const metrics = createMetrics(config.SERVICE_NAME);
  const db = createPool(config.DATABASE_URL);
  await migrate(db, join(resolve(config.MIGRATIONS_DIR), def.schema), def.schema, log);

  const connection = await connectBroker(config.RABBITMQ_URL, log);
  let brokerConnected = true;
  connection.on("close", () => (brokerConnected = false));
  const publisher = await Publisher.create(connection);
  const http = createHttpServer(log, { db, metrics, brokerConnected: () => brokerConnected });

  const ctx: ServiceContext = {
    config,
    log,
    db,
    connection,
    publisher,
    metrics,
    http,
    schema: def.schema,
    consumer: () =>
      new Consumer(connection, db, log, metrics, {
        schema: def.schema,
        prefetch: config.PREFETCH,
        retryDelaysMs: config.RETRY_DELAYS_MS,
      }),
  };

  if (config.roles.has("api") && def.api) await def.api(ctx);
  const consumers = config.roles.has("consumer") && def.consumers ? await def.consumers(ctx) : [];
  let relay: OutboxRelay | undefined;
  if (config.roles.has("relay") && def.hasOutbox) {
    relay = new OutboxRelay(db, def.schema, publisher, log, metrics, {
      batchSize: config.OUTBOX_BATCH_SIZE,
      pollIntervalMs: config.OUTBOX_POLL_INTERVAL_MS,
    });
    relay.start();
  }
  await http.listen({ host: "0.0.0.0", port: config.HTTP_PORT });
  log.info({ roles: [...config.roles], port: config.HTTP_PORT }, "service started");

  const shutdown = async (signal: string) => {
    log.info({ signal }, "shutting down: draining consumers and relay");
    connection.removeAllListeners("close");
    await http.close();
    await Promise.all(consumers.map((c) => c.stop()));
    await relay?.stop();
    await publisher.close().catch(() => {});
    await connection.close().catch(() => {});
    await db.end();
    log.info("shutdown complete");
    process.exit(0);
  };
  process.once("SIGTERM", (s) => void shutdown(s));
  process.once("SIGINT", (s) => void shutdown(s));
}
