import { randomUUID } from "node:crypto";
import Fastify, { type FastifyBaseLogger, type FastifyInstance } from "fastify";
import type { Db } from "../db.js";
import type { Logger } from "../logger.js";
import type { Metrics } from "../metrics.js";

export interface HealthDeps {
  db: Db;
  brokerConnected: () => boolean;
  metrics: Metrics;
}

/**
 * Every process (API or worker) exposes the same operational endpoints:
 *   GET /health/live   – the process is running (no dependency checks; used for restarts)
 *   GET /health/ready  – database and broker reachable (used to route traffic / roll deployments)
 *   GET /metrics       – Prometheus metrics
 * Requests carry X-Correlation-Id (generated if absent); it becomes the request id (`reqId` in logs) and is echoed back.
 */
export function createHttpServer(log: Logger, deps: HealthDeps): FastifyInstance {
  const app = Fastify({
    loggerInstance: log as FastifyBaseLogger,
    genReqId: (req) => (req.headers["x-correlation-id"] as string | undefined) ?? randomUUID(),
  });

  app.addHook("onSend", async (req, reply) => {
    reply.header("x-correlation-id", req.id);
  });

  app.get("/health/live", async () => ({ status: "ok" }));

  app.get("/health/ready", async (_req, reply) => {
    const checks = {
      database: await deps.db
        .query("SELECT 1")
        .then(() => true)
        .catch(() => false),
      broker: deps.brokerConnected(),
    };
    const ready = Object.values(checks).every(Boolean);
    return reply.code(ready ? 200 : 503).send({ status: ready ? "ready" : "not-ready", checks });
  });

  app.get("/metrics", async (_req, reply) => {
    reply.header("content-type", deps.metrics.registry.contentType);
    return deps.metrics.registry.metrics();
  });

  return app;
}
