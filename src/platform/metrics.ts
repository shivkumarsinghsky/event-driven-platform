import { Counter, Gauge, Histogram, Registry, collectDefaultMetrics } from "prom-client";

export function createMetrics(service: string) {
  const registry = new Registry();
  registry.setDefaultLabels({ service });
  collectDefaultMetrics({ register: registry });
  return {
    registry,
    consumed: new Counter({
      name: "messages_consumed_total",
      help: "Messages handled by outcome",
      labelNames: ["queue", "type", "outcome"] as const,
      registers: [registry],
    }),
    processingSeconds: new Histogram({
      name: "message_processing_seconds",
      help: "Handler duration including the database transaction",
      labelNames: ["queue", "type"] as const,
      buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5],
      registers: [registry],
    }),
    outboxPublished: new Counter({
      name: "outbox_published_total",
      help: "Events published from the outbox",
      registers: [registry],
    }),
    outboxPending: new Gauge({
      name: "outbox_pending",
      help: "Unpublished outbox rows observed by the last relay poll",
      registers: [registry],
    }),
  };
}

export type Metrics = ReturnType<typeof createMetrics>;
