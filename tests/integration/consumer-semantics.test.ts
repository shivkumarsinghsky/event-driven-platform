import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { ChannelModel, ConfirmChannel } from "amqplib";
import { createEnvelope, routingKey } from "../../src/platform/contracts/envelope.js";
import { createPool, type Db } from "../../src/platform/db.js";
import { connectBroker } from "../../src/platform/messaging/broker.js";
import { Consumer } from "../../src/platform/messaging/consumer.js";
import { PermanentError } from "../../src/platform/messaging/errors.js";
import { EVENTS_EXCHANGE } from "../../src/platform/messaging/topology.js";
import { createMetrics } from "../../src/platform/metrics.js";
import {
  DATABASE_URL,
  eventually,
  integrationEnabled,
  log,
  RABBITMQ_URL,
  resetBroker,
  resetDb,
} from "./harness.js";

const QUEUE = "test.semantics";
const DELAYS = [100, 100];

describe.skipIf(!integrationEnabled)("consumer semantics (RabbitMQ)", () => {
  let db: Db;
  let connection: ChannelModel;
  let channel: ConfirmChannel;
  let consumer: Consumer;
  const calls: Record<string, number> = {};

  beforeAll(async () => {
    db = createPool(DATABASE_URL!);
    connection = await connectBroker(RABBITMQ_URL!, log, { exitOnClose: false });
    await resetDb(db, ["edp_test"]);
    await resetBroker(connection, [QUEUE], DELAYS);
    await db.query("CREATE SCHEMA edp_test");
    await db.query(
      "CREATE TABLE edp_test.inbox (consumer text, message_id uuid, processed_at timestamptz DEFAULT now(), PRIMARY KEY (consumer, message_id))",
    );
    consumer = new Consumer(connection, db, log, createMetrics("t"), {
      schema: "edp_test",
      prefetch: 5,
      retryDelaysMs: DELAYS,
    });
    const count = (id: string) => (calls[id] = (calls[id] ?? 0) + 1);
    await consumer.start(
      { queue: QUEUE, bindings: ["test.*.*"] },
      {
        "test.ok": async ({ envelope }) => void count(envelope.subject),
        "test.transient": async ({ envelope }) => {
          count(envelope.subject);
          throw new Error("database timeout");
        },
        "test.permanent": async ({ envelope }) => {
          count(envelope.subject);
          throw new PermanentError("business rule violated");
        },
        "test.flaky": async ({ envelope }) => {
          if (count(envelope.subject) < 2) throw new Error("transient");
        },
      },
    );
    channel = await connection.createConfirmChannel();
  });

  afterAll(async () => {
    await consumer?.stop();
    await connection?.close();
    await db?.end();
  });

  const publish = async (type: string, subject: string, override?: Buffer) => {
    const e = createEnvelope({ type, version: 1, source: "test", subject, data: {} });
    channel.publish(EVENTS_EXCHANGE, routingKey(e), override ?? Buffer.from(JSON.stringify(e)), {
      messageId: e.id,
    });
    await channel.waitForConfirms();
    return e;
  };

  const dlqMessage = async () => {
    const msg = await channel.get(`${QUEUE}.dlq`, { noAck: true });
    return msg || undefined;
  };

  it("processes a duplicated delivery only once (inbox)", async () => {
    const e = createEnvelope({ type: "test.ok", version: 1, source: "test", subject: "dup", data: {} });
    for (let i = 0; i < 3; i++)
      channel.publish(EVENTS_EXCHANGE, routingKey(e), Buffer.from(JSON.stringify(e)));
    await channel.waitForConfirms();
    await eventually(async () => {
      const r = await db.query("SELECT count(*)::int AS n FROM edp_test.inbox WHERE message_id = $1", [e.id]);
      return r.rows[0].n === 1 ? true : undefined;
    });
    await new Promise((r) => setTimeout(r, 300));
    expect(calls.dup).toBe(1);
  });

  it("retries transient failures with delays, then dead-letters with diagnostic headers", async () => {
    await publish("test.transient", "transient");
    const msg = await eventually(dlqMessage);
    expect(calls.transient).toBe(3); // first attempt + 2 retries
    expect(msg.properties.headers?.["x-attempts"]).toBe(3);
    expect(msg.properties.headers?.["x-last-error"]).toBe("database timeout");
  });

  it("dead-letters permanent errors without retrying", async () => {
    await publish("test.permanent", "permanent");
    const msg = await eventually(dlqMessage);
    expect(calls.permanent).toBe(1);
    expect(msg.properties.headers?.["x-attempts"]).toBe(1);
  });

  it("dead-letters malformed messages", async () => {
    await publish("test.ok", "malformed", Buffer.from("{not json"));
    const msg = await eventually(dlqMessage);
    expect(msg.content.toString()).toBe("{not json");
  });

  it("recovers when a retry succeeds and rolls back the failed attempt's inbox record", async () => {
    const e = await publish("test.flaky", "flaky");
    await eventually(async () => (calls.flaky === 2 ? true : undefined));
    await new Promise((r) => setTimeout(r, 100));
    const r = await db.query("SELECT count(*)::int AS n FROM edp_test.inbox WHERE message_id = $1", [e.id]);
    expect(r.rows[0].n).toBe(1);
    expect(await dlqMessage()).toBeUndefined();
  });
});
