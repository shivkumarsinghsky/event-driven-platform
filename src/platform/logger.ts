import { pino, type Logger } from "pino";

export type { Logger };

export function createLogger(service: string, level = "info"): Logger {
  return pino({
    level,
    base: { service },
    timestamp: pino.stdTimeFunctions.isoTime,
    // Never log credentials that may appear in connection strings or headers.
    redact: ["databaseUrl", "rabbitmqUrl", "headers.authorization", "req.headers.authorization"],
  });
}
