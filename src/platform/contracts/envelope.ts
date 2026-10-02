import { randomUUID } from "node:crypto";
import { z } from "zod";

/**
 * Every message on the bus uses this envelope. Metadata is separate from the payload so that routing,
 * tracing, deduplication and versioning never depend on the business payload.
 */
export const envelopeSchema = z.object({
  id: z.uuid(),
  type: z.string().regex(/^[a-z]+(\.[a-z-]+)+$/, "type must look like 'order.placed'"),
  version: z.number().int().positive(),
  source: z.string().min(1),
  occurredAt: z.iso.datetime(),
  correlationId: z.string().min(1),
  /** Id of the message that caused this one; forms the causal chain across services. */
  causationId: z.string().nullable(),
  /** Partition/ordering key for the entity this event is about. */
  subject: z.string().min(1),
  data: z.unknown(),
});

export type Envelope<T = unknown> = Omit<z.infer<typeof envelopeSchema>, "data"> & { data: T };

export function createEnvelope<T>(args: {
  type: string;
  version: number;
  source: string;
  subject: string;
  data: T;
  correlationId?: string;
  causedBy?: Pick<Envelope, "id" | "correlationId">;
}): Envelope<T> {
  return {
    id: randomUUID(),
    type: args.type,
    version: args.version,
    source: args.source,
    occurredAt: new Date().toISOString(),
    correlationId: args.causedBy?.correlationId ?? args.correlationId ?? randomUUID(),
    causationId: args.causedBy?.id ?? null,
    subject: args.subject,
    data: args.data,
  };
}

/** Routing key on the topic exchange: `<type>.v<version>`, e.g. `order.placed.v2`. */
export const routingKey = (e: Pick<Envelope, "type" | "version">) => `${e.type}.v${e.version}`;
