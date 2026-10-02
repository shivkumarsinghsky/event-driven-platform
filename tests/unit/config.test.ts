import { describe, expect, it } from "vitest";
import { loadConfig } from "../../src/config.js";
import { simulatedProvider } from "../../src/services/notification/handlers.js";

const base = {
  SERVICE_NAME: "svc",
  DATABASE_URL: "postgres://u:p@localhost:5432/db",
  RABBITMQ_URL: "amqp://guest:guest@localhost:5672",
};

describe("configuration", () => {
  it("applies defaults and parses lists", () => {
    const c = loadConfig({ ...base, RETRY_DELAYS_MS: "200, 1000", ROLES: "consumer" });
    expect(c.RETRY_DELAYS_MS).toEqual([200, 1_000]);
    expect([...c.roles]).toEqual(["consumer"]);
    expect(c.PREFETCH).toBe(10);
  });

  it("fails fast with a readable message", () => {
    expect(() => loadConfig({ ...base, DATABASE_URL: "not a url" })).toThrow(/DATABASE_URL/);
    expect(() => loadConfig({ ...base, RETRY_DELAYS_MS: "abc" })).toThrow(/RETRY_DELAYS_MS/);
  });
});

describe("simulated notification provider", () => {
  it("fails according to the configured rate", async () => {
    const msg = { to: "c", template: "t", data: {}, idempotencyKey: "k" };
    await expect(simulatedProvider(0.5, () => 0.4).send(msg)).rejects.toThrow("unavailable");
    await expect(simulatedProvider(0.5, () => 0.6).send(msg)).resolves.toBeUndefined();
  });
});
