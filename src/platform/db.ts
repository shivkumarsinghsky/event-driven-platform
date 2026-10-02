import { readdir, readFile } from "node:fs/promises";
import { join } from "node:path";
import pg from "pg";
import type { Logger } from "./logger.js";

export type Db = pg.Pool;
export type Tx = pg.PoolClient;

export function createPool(connectionString: string): Db {
  return new pg.Pool({
    connectionString,
    max: 10,
    idleTimeoutMillis: 30_000,
    connectionTimeoutMillis: 5_000,
  });
}

export async function withTransaction<T>(db: Db, work: (tx: Tx) => Promise<T>): Promise<T> {
  const client = await db.connect();
  try {
    await client.query("BEGIN");
    const result = await work(client);
    await client.query("COMMIT");
    return result;
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

/**
 * Minimal forward-only migrator. Each service owns a schema and its own migration directory
 * (database-per-service, realised as schema-per-service on one PostgreSQL instance for local use).
 * An advisory lock makes concurrent startups of several replicas safe.
 */
export async function migrate(db: Db, dir: string, schema: string, log: Logger): Promise<void> {
  const files = (await readdir(dir)).filter((f) => f.endsWith(".sql")).sort();
  await withTransaction(db, async (tx) => {
    await tx.query("SELECT pg_advisory_xact_lock(hashtext($1))", [`migrate:${schema}`]);
    await tx.query(`CREATE SCHEMA IF NOT EXISTS ${pg.escapeIdentifier(schema)}`);
    await tx.query(
      `CREATE TABLE IF NOT EXISTS ${pg.escapeIdentifier(schema)}.schema_migrations (
         name text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`,
    );
    const applied = new Set(
      (
        await tx.query<{ name: string }>(`SELECT name FROM ${pg.escapeIdentifier(schema)}.schema_migrations`)
      ).rows.map((r) => r.name),
    );
    for (const file of files) {
      if (applied.has(file)) continue;
      await tx.query(await readFile(join(dir, file), "utf8"));
      await tx.query(`INSERT INTO ${pg.escapeIdentifier(schema)}.schema_migrations (name) VALUES ($1)`, [
        file,
      ]);
      log.info({ migration: file, schema }, "migration applied");
    }
  });
}
