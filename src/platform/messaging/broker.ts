import amqp, { type ChannelModel, type ConfirmChannel } from "amqplib";
import type { Envelope } from "../contracts/envelope.js";
import { routingKey } from "../contracts/envelope.js";
import type { Logger } from "../logger.js";
import { EVENTS_EXCHANGE, declareExchange } from "./topology.js";

/**
 * Connect with bounded retries at startup. After startup, losing the connection terminates the process
 * (see docs/architecture.md#failure-handling): the orchestrator restarts it and in-flight messages are redelivered, which the inbox makes safe.
 */
export async function connectBroker(
  url: string,
  log: Logger,
  opts: { attempts?: number; exitOnClose?: boolean } = {},
): Promise<ChannelModel> {
  const { attempts = 20, exitOnClose = true } = opts;
  for (let attempt = 1; ; attempt++) {
    try {
      const connection = await amqp.connect(url);
      connection.on("error", (err) => log.error({ err }, "broker connection error"));
      if (exitOnClose) {
        connection.on("close", () => {
          log.fatal("broker connection closed; exiting so the orchestrator restarts the process");
          process.exit(1);
        });
      }
      return connection;
    } catch (err) {
      if (attempt >= attempts) throw err;
      log.warn({ attempt, err: (err as Error).message }, "broker not reachable yet, retrying");
      await new Promise((r) => setTimeout(r, Math.min(1_000 * attempt, 5_000)));
    }
  }
}

/** Publishes envelopes with publisher confirms: `publish` resolves only once the broker has persisted the message. */
export class Publisher {
  private constructor(private readonly channel: ConfirmChannel) {}

  static async create(connection: ChannelModel): Promise<Publisher> {
    const channel = await connection.createConfirmChannel();
    await declareExchange(channel);
    return new Publisher(channel);
  }

  async publish(envelope: Envelope): Promise<void> {
    await this.publishRaw(EVENTS_EXCHANGE, routingKey(envelope), envelope);
  }

  /** Low-level publish, also used to move messages to retry and dead-letter queues. */
  publishRaw(
    exchange: string,
    key: string,
    envelope: Envelope,
    headers: Record<string, unknown> = {},
  ): Promise<void> {
    return new Promise((resolve, reject) => {
      this.channel.publish(
        exchange,
        key,
        Buffer.from(JSON.stringify(envelope)),
        {
          persistent: true,
          contentType: "application/json",
          messageId: envelope.id,
          type: envelope.type,
          timestamp: Math.floor(Date.parse(envelope.occurredAt) / 1000),
          correlationId: envelope.correlationId,
          headers: { "x-event-version": envelope.version, ...headers },
        },
        (err) => (err ? reject(err) : resolve()),
      );
    });
  }

  async close(): Promise<void> {
    await this.channel.close();
  }
}
