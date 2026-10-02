import pg from "pg";
import type { Envelope } from "../contracts/envelope.js";
import { type Db, type Tx, withTransaction } from "../db.js";
import type { Logger } from "../logger.js";
import type { Publisher } from "../messaging/broker.js";
import type { Metrics } from "../metrics.js";

const table = (schema: string) => `${pg.escapeIdentifier(schema)}.outbox`;

/** Stage an event in the caller's transaction. It is published only if the transaction commits. */
export async function enqueue(tx: Tx, schema: string, envelope: Envelope): Promise<void> {
  await tx.query(`INSERT INTO ${table(schema)} (id, subject, envelope) VALUES ($1, $2, $3)`, [
    envelope.id,
    envelope.subject,
    JSON.stringify(envelope),
  ]);
}

/**
 * Polls the outbox and publishes with publisher confirms.
 *
 * - `FOR UPDATE SKIP LOCKED` lets several relay replicas run without publishing the same row concurrently.
 *   With more than one replica, global order is no longer guaranteed; consumers rely on per-entity state checks
 *   rather than arrival order (ADR-003).
 * - A row is marked published only after the broker confirmed it. A crash between confirm and commit causes a
 *   duplicate publish, which consumers absorb through their inbox (at-least-once + idempotent consumers).
 */
export class OutboxRelay {
  private timer: NodeJS.Timeout | undefined;
  private running = false;
  private stopped = false;

  constructor(
    private readonly db: Db,
    private readonly schema: string,
    private readonly publisher: Publisher,
    private readonly log: Logger,
    private readonly metrics: Metrics,
    private readonly opts: { batchSize: number; pollIntervalMs: number },
  ) {}

  start(): void {
    const tick = async () => {
      if (this.stopped) return;
      try {
        // Drain quickly while there is a backlog, then fall back to polling.
        while (!this.stopped && (await this.publishBatch()) === this.opts.batchSize) {
          /* keep draining */
        }
      } catch (err) {
        this.log.error({ err }, "outbox relay batch failed; will retry");
      }
      if (!this.stopped) this.timer = setTimeout(tick, this.opts.pollIntervalMs);
    };
    this.timer = setTimeout(tick, 0);
  }

  async stop(): Promise<void> {
    this.stopped = true;
    clearTimeout(this.timer);
    while (this.running) await new Promise((r) => setTimeout(r, 20));
  }

  /** Publishes one batch; returns the number of rows published. Exposed for tests. */
  async publishBatch(): Promise<number> {
    this.running = true;
    try {
      return await withTransaction(this.db, async (tx) => {
        const { rows } = await tx.query<{ id: string; envelope: Envelope }>(
          `SELECT id, envelope FROM ${table(this.schema)}
            WHERE published_at IS NULL
            ORDER BY seq
            LIMIT $1
            FOR UPDATE SKIP LOCKED`,
          [this.opts.batchSize],
        );
        const published: string[] = [];
        try {
          for (const row of rows) {
            await this.publisher.publish(row.envelope);
            published.push(row.id);
          }
        } catch (err) {
          this.log.warn(
            { err, published: published.length, pending: rows.length },
            "publish failed mid-batch",
          );
        }
        if (published.length) {
          await tx.query(`UPDATE ${table(this.schema)} SET published_at = now() WHERE id = ANY($1::uuid[])`, [
            published,
          ]);
          this.metrics.outboxPublished.inc(published.length);
        }
        const pending = await tx.query<{ n: string }>(
          `SELECT count(*) AS n FROM ${table(this.schema)} WHERE published_at IS NULL`,
        );
        this.metrics.outboxPending.set(Number(pending.rows[0]?.n ?? 0) - published.length);
        return published.length;
      });
    } finally {
      this.running = false;
    }
  }
}
