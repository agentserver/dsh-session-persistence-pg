/** PostgreSQL schema and row validation for the Session Persistence backend. */

import type { PoolClient } from 'pg'

/** Current physical schema version. SQLite and PostgreSQL own separate counters. */
export const SCHEMA_VERSION = 1

/** PostgreSQL identifiers accepted from deployment configuration. */
export const IDENTIFIER_PATTERN = /^[a-z_][a-z0-9_]*$/

/** Stored session metadata returned by PostgreSQL. */
export interface SessionRow {
  readonly id: string
  readonly header: unknown
  readonly inheritedEventCount: unknown
  readonly eventCount: unknown
  readonly revision: unknown
}

/** Stored event row returned by PostgreSQL. */
export interface EventRow {
  readonly seq: unknown
  readonly event: unknown
}

/** Quote a validated PostgreSQL identifier. */
export function quoteIdentifier(value: string): string {
  if (!IDENTIFIER_PATTERN.test(value)) {
    throw new Error(`PostgreSQL identifier must match ${IDENTIFIER_PATTERN}, got ${JSON.stringify(value)}`)
  }
  return `"${value}"`
}

/** Install the schema and refuse a database owned by another physical version. */
export async function ensureSchema(client: PoolClient, schema: string): Promise<void> {
  const quoted = quoteIdentifier(schema)
  await client.query(`CREATE SCHEMA IF NOT EXISTS ${quoted}`)
  await client.query(`
    CREATE TABLE IF NOT EXISTS ${quoted}.persistence_state (
      singleton smallint PRIMARY KEY CHECK (singleton = 1),
      schema_version integer NOT NULL
    )
  `)
  const state = await client.query<{ schema_version: number }>(
    `SELECT schema_version FROM ${quoted}.persistence_state WHERE singleton = 1`,
  )
  const version = state.rows[0]?.schema_version
  if (version === undefined) {
    await client.query(
      `INSERT INTO ${quoted}.persistence_state (singleton, schema_version) VALUES (1, $1) ON CONFLICT (singleton) DO NOTHING`,
      [SCHEMA_VERSION],
    )
    const installed = await client.query<{ schema_version: number }>(
      `SELECT schema_version FROM ${quoted}.persistence_state WHERE singleton = 1`,
    )
    if (installed.rows[0]?.schema_version !== SCHEMA_VERSION) {
      throw new Error(
        `PostgreSQL session persistence schema is version ${String(installed.rows[0]?.schema_version)}, expected ${String(SCHEMA_VERSION)}`,
      )
    }
  } else if (version !== SCHEMA_VERSION) {
    throw new Error(
      `PostgreSQL session persistence schema is version ${String(version)}, expected ${String(SCHEMA_VERSION)}`,
    )
  }
  await client.query(`
    CREATE TABLE IF NOT EXISTS ${quoted}.sessions (
      id                    text PRIMARY KEY,
      header                jsonb NOT NULL,
      inherited_event_count bigint NOT NULL CHECK (inherited_event_count >= 0),
      event_count           bigint NOT NULL CHECK (event_count >= 0),
      revision              bigint NOT NULL CHECK (revision >= 0)
    )
  `)
  await client.query(`
    CREATE TABLE IF NOT EXISTS ${quoted}.session_events (
      session_id text NOT NULL REFERENCES ${quoted}.sessions(id) ON DELETE CASCADE,
      seq        bigint NOT NULL CHECK (seq >= 0),
      event      jsonb NOT NULL,
      PRIMARY KEY (session_id, seq)
    )
  `)
}

/** Convert a PostgreSQL bigint value after checking the JavaScript safe range. */
export function safeInteger(value: unknown, field: string): number {
  const numeric = typeof value === 'string' ? Number(value) : value
  if (typeof numeric !== 'number' || !Number.isSafeInteger(numeric) || numeric < 0) {
    throw new Error(`stored PostgreSQL ${field} is not a non-negative safe integer: ${String(value)}`)
  }
  return numeric
}
