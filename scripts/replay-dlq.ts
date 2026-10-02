/**
 * Move messages from a dead-letter queue back to its work queue after the root cause is fixed.
 *
 *   npx tsx scripts/replay-dlq.ts <queue> [--limit 100] [--dry-run]
 *
 * The attempts counter is reset; the original message id is kept, so consumers that already processed the
 * message (inbox) will skip it rather than apply it twice.
 */
import amqp from "amqplib";

const [queue, ...rest] = process.argv.slice(2);
if (!queue) {
  console.error("usage: replay-dlq.ts <queue> [--limit N] [--dry-run]");
  process.exit(2);
}
const limitIdx = rest.indexOf("--limit");
const limit = limitIdx >= 0 ? Number(rest[limitIdx + 1]) : 100;
const dryRun = rest.includes("--dry-run");
const url = process.env.RABBITMQ_URL ?? "amqp://guest:guest@localhost:5672";

const connection = await amqp.connect(url);
const channel = await connection.createConfirmChannel();
const dlq = `${queue}.dlq`;
let moved = 0;
while (moved < limit) {
  const msg = await channel.get(dlq, { noAck: false });
  if (!msg) break;
  const headers = { ...msg.properties.headers };
  console.log(
    `${dryRun ? "[dry-run] " : ""}${msg.properties.messageId} ${msg.properties.type} last error: ${headers["x-last-error"]}`,
  );
  if (dryRun) {
    channel.nack(msg, false, true);
    break; // a requeued message would be fetched again; inspect one batch with the management UI instead
  }
  delete headers["x-attempts"];
  headers["x-replayed-at"] = new Date().toISOString();
  channel.sendToQueue(queue, msg.content, { ...msg.properties, headers });
  await channel.waitForConfirms();
  channel.ack(msg);
  moved++;
}
console.log(`${dryRun ? "inspected" : "replayed"} ${moved} message(s) from ${dlq} to ${queue}`);
await channel.close();
await connection.close();
