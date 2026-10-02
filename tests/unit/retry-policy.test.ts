import { describe, expect, it } from "vitest";
import { PermanentError } from "../../src/platform/messaging/errors.js";
import {
  deadLetterQueueName,
  decideOnFailure,
  retryQueueName,
} from "../../src/platform/messaging/retry-policy.js";

const delays = [1_000, 5_000, 30_000];

describe("decideOnFailure", () => {
  it("schedules retries with increasing delays", () => {
    expect(decideOnFailure("q", 1, new Error("timeout"), delays)).toEqual({
      action: "retry",
      attempt: 1,
      delayMs: 1_000,
      queue: "q.retry.1000ms",
    });
    expect(decideOnFailure("q", 3, new Error("timeout"), delays)).toMatchObject({ delayMs: 30_000 });
  });

  it("dead-letters after the last configured delay", () => {
    expect(decideOnFailure("q", 4, new Error("timeout"), delays)).toEqual({
      action: "dead-letter",
      reason: "retries-exhausted",
    });
  });

  it("dead-letters permanent errors immediately", () => {
    expect(decideOnFailure("q", 1, new PermanentError("bad payload"), delays)).toEqual({
      action: "dead-letter",
      reason: "permanent-error",
    });
    const zodLike = Object.assign(new Error("invalid"), { name: "ZodError" });
    expect(decideOnFailure("q", 1, zodLike, delays).action).toBe("dead-letter");
  });

  it("names queues consistently", () => {
    expect(retryQueueName("inventory.order-placed", 5_000)).toBe("inventory.order-placed.retry.5000ms");
    expect(deadLetterQueueName("inventory.order-placed")).toBe("inventory.order-placed.dlq");
  });
});
