import type { ChannelModel, ConfirmChannel, ConsumeMessage } from "amqplib";
import pg from "pg";
import { type Envelope, envelopeSchema } from "../contracts/envelope.js";
import { type Db, type Tx, withTransaction } from "../db.js";
import type { Logger } from "../logger.js";
import type { Metrics } from "../metrics.js";
import { PermanentError } from "./errors.js";
import { decideOnFailure, deadLetterQueueName } from "./retry-policy.js";
import { type QueueBinding, declareConsumerTopology } from "./topology.js";

export interface HandlerContext {
  envelope: Envelope;
  /** Transaction shared with the inbox insert: handler writes and the dedup record commit together. */
  tx: Tx;
  log: Logger;
}

export type Handler = (ctx: HandlerContext) => Promise<void>;

export interface ConsumerOptions {
  /** Schema that owns the `inbox` table for this service. */
  schema: string;
  prefetch: number;
  retryDelaysMs: number[];
}

const ATTEMPTS_HEADER = "x-attempts";

/**
 * Consumes a queue with:
 *  - envelope validation (invalid → dead-letter, never retried),
 *  - idempotency via an inbox table written in the same transaction as the handler's changes,
 *  - delayed retries through TTL retry queues, then dead-lettering,
 *  - manual acks only after the outcome is durable (processed, or re-published to retry/DLQ).
 */
export class Consumer {
  private channel: ConfirmChannel | undefined;
  private consumerTag: string | undefined;
  private inFlight = 0;

  constructor(
    private readonly connection: ChannelModel,
    private readonly db: Db,
    private readonly log: Logger,
    private readonly metrics: Metrics,
    private readonly opts: ConsumerOptions,
  ) {}

  async start(binding: QueueBinding, handlers: Record<string, Handler>): Promise<void> {
    // Confirm channel: moving a message to a retry/DLQ queue is confirmed by the broker before the original is acked.
    const channel = await this.connection.createConfirmChannel();
    this.channel = channel;
    await declareConsumerTopology(channel, binding, this.opts.retryDelaysMs);
    await channel.prefetch(this.opts.prefetch);
    const { consumerTag } = await channel.consume(binding.queue, (msg) => {
      if (msg) void this.onMessage(channel, binding.queue, msg, handlers);
    });
    this.consumerTag = consumerTag;
    this.log.info({ queue: binding.queue, bindings: binding.bindings }, "consumer started");
  }

  /** Stop receiving new messages and wait for in-flight ones (graceful shutdown). */
  async stop(): Promise<void> {
    if (this.channel && this.consumerTag) await this.channel.cancel(this.consumerTag);
    while (this.inFlight > 0) await new Promise((r) => setTimeout(r, 20));
    await this.channel?.close().catch(() => {});
  }

  private async onMessage(
    channel: ConfirmChannel,
    queue: string,
    msg: ConsumeMessage,
    handlers: Record<string, Handler>,
  ) {
    this.inFlight++;
    const started = process.hrtime.bigint();
    let type = "unknown";
    try {
      const envelope = parseEnvelope(msg);
      type = envelope.type;
      const log = this.log.child({
        queue,
        messageId: envelope.id,
        type,
        correlationId: envelope.correlationId,
      });
      const handler = handlers[envelope.type];
      if (!handler) {
        log.debug("no handler for event type; acknowledging");
        this.metrics.consumed.inc({ queue, type, outcome: "ignored" });
        channel.ack(msg);
        return;
      }
      const processed = await withTransaction(this.db, async (tx) => {
        const inserted = await tx.query(
          `INSERT INTO ${pg.escapeIdentifier(this.opts.schema)}.inbox (consumer, message_id) VALUES ($1, $2)
           ON CONFLICT DO NOTHING`,
          [queue, envelope.id],
        );
        if (inserted.rowCount === 0) return false; // duplicate delivery
        await handler({ envelope, tx, log });
        return true;
      });
      this.metrics.consumed.inc({ queue, type, outcome: processed ? "processed" : "duplicate" });
      if (!processed) log.info("duplicate message skipped");
      channel.ack(msg);
    } catch (error) {
      await this.onFailure(channel, queue, msg, type, error);
    } finally {
      this.metrics.processingSeconds.observe(
        { queue, type },
        Number(process.hrtime.bigint() - started) / 1e9,
      );
      this.inFlight--;
    }
  }

  private async onFailure(
    channel: ConfirmChannel,
    queue: string,
    msg: ConsumeMessage,
    type: string,
    error: unknown,
  ) {
    const attempts = Number(msg.properties.headers?.[ATTEMPTS_HEADER] ?? 0) + 1;
    const decision = decideOnFailure(queue, attempts, error, this.opts.retryDelaysMs);
    const log = this.log.child({ queue, messageId: msg.properties.messageId, type, attempts });
    const headers = {
      ...msg.properties.headers,
      [ATTEMPTS_HEADER]: attempts,
      "x-last-error": error instanceof Error ? error.message.slice(0, 500) : String(error),
      "x-failed-at": new Date().toISOString(),
    };
    const target = decision.action === "retry" ? decision.queue : deadLetterQueueName(queue);
    try {
      // Publish to the retry/dead-letter queue through the default exchange, then ack the original.
      // If the publish fails the original is nacked with requeue, so the message is never lost.
      await new Promise<void>((resolve, reject) =>
        channel.sendToQueue(target, msg.content, { ...msg.properties, headers, persistent: true }, (err) =>
          err ? reject(err) : resolve(),
        ),
      );
      channel.ack(msg);
    } catch (publishError) {
      log.error({ err: publishError }, "could not move message to retry/DLQ; requeueing");
      channel.nack(msg, false, true);
      return;
    }
    if (decision.action === "retry") {
      log.warn({ err: error, delayMs: decision.delayMs }, "handler failed; scheduled retry");
      this.metrics.consumed.inc({ queue, type, outcome: "retried" });
    } else {
      log.error({ err: error, reason: decision.reason }, "message dead-lettered");
      this.metrics.consumed.inc({ queue, type, outcome: "dead_lettered" });
    }
  }
}

function parseEnvelope(msg: ConsumeMessage): Envelope {
  let json: unknown;
  try {
    json = JSON.parse(msg.content.toString("utf8"));
  } catch (cause) {
    throw new PermanentError("message body is not valid JSON", { cause });
  }
  const parsed = envelopeSchema.safeParse(json);
  if (!parsed.success)
    throw new PermanentError(`invalid envelope: ${parsed.error.issues[0]?.message ?? "unknown"}`);
  return parsed.data as Envelope;
}
