import { z } from "zod";

const schema = z.object({
  SERVICE_NAME: z.string().min(1),
  /** Comma-separated roles this process runs: api, relay, consumer. Lets one image scale roles independently. */
  ROLES: z.string().default("api,relay,consumer"),
  HTTP_PORT: z.coerce.number().int().positive().default(3000),
  DATABASE_URL: z.url(),
  RABBITMQ_URL: z.url(),
  LOG_LEVEL: z.enum(["fatal", "error", "warn", "info", "debug", "trace"]).default("info"),
  PREFETCH: z.coerce.number().int().positive().default(10),
  OUTBOX_POLL_INTERVAL_MS: z.coerce.number().int().positive().default(500),
  OUTBOX_BATCH_SIZE: z.coerce.number().int().positive().default(100),
  /** Retry delays for failed messages, e.g. "1000,5000,30000" → 3 retries then dead-letter. */
  RETRY_DELAYS_MS: z
    .string()
    .default("1000,5000,30000")
    .transform((s) => s.split(",").map((n) => Number.parseInt(n.trim(), 10)))
    .pipe(z.array(z.number().int().positive()).min(1)),
  /** Notification service only: probability (0..1) that the simulated provider fails. */
  NOTIFICATION_FAILURE_RATE: z.coerce.number().min(0).max(1).default(0),
  MIGRATIONS_DIR: z.string().default("migrations"),
});

export type Config = z.infer<typeof schema> & { roles: Set<string> };

export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const parsed = schema.safeParse(env);
  if (!parsed.success) {
    const issues = parsed.error.issues.map((i) => `${i.path.join(".")}: ${i.message}`).join("; ");
    throw new Error(`invalid configuration: ${issues}`);
  }
  return { ...parsed.data, roles: new Set(parsed.data.ROLES.split(",").map((r) => r.trim())) };
}
