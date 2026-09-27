/** PostgreSQL transactions and per-session writer ownership. */

import { randomUUID } from 'node:crypto'
import { isAbsolute } from 'node:path'
import pg, { type PoolClient } from 'pg'
import { Session, SessionId as makeSessionId, SESSION_FORMAT_VERSION, adoptSessionEvent } from '@deepseek-ai/dsh-session'
import type { SessionEvent, SessionHeader, SessionId, SessionLogOffset } from '@deepseek-ai/dsh-session'
import {
  assertContiguous,
  assertStoredId,
  materializeCreateHeader,
  SessionAlreadyExistsError,
  SessionPersistenceCorruptionError,
  SessionPersistenceNotFoundError,
  SessionPersistenceRevision,
  SessionFormatUnsupportedError,
  validateStoredEvents,
  type SessionPersistenceRevision as PersistenceRevision,
} from '@deepseek-ai/dsh-session-persistence'
import { ensureSchema, quoteIdentifier, safeInteger, type EventRow, type SessionRow } from './schema.ts'

/** PostgreSQL store configuration resolved by the provider. */
export interface PgStoreOptions {
  readonly connectionString: string
  readonly maxConnections: number
  readonly statementTimeoutMs: number
  readonly schema: string
}

/** Immutable validated data loaded from PostgreSQL. */
export interface StoredLog {
  readonly header: SessionHeader
  readonly inheritedEventCount: SessionLogOffset
  readonly events: readonly SessionEvent[]
  readonly revision: PersistenceRevision
}

/** Dedicated PostgreSQL connection held by one write handle's advisory lock. */
export interface PgWriteLease {
  readonly client: PoolClient
  readonly release: () => Promise<void>
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function optionalString(record: Record<string, unknown>, key: string): string | undefined {
  const value = record[key]
  if (value === undefined) return undefined
  if (typeof value !== 'string') throw new Error(`stored session header field "${key}" must be a string`)
  return value
}

function parseHeader(value: unknown, expectedId: SessionId): SessionHeader {
  if (!isRecord(value)) throw new Error('stored session header is not a JSON object')
  const allowed = new Set(['version', 'id', 'createdAt', 'cwd', 'parentSession', 'isSeeded', 'origin', 'delegationDepth', 'agentPreset'])
  for (const key of Object.keys(value)) {
    if (!allowed.has(key)) throw new Error(`stored session header has unknown field "${key}"`)
  }
  if (value.version !== SESSION_FORMAT_VERSION) {
    throw new SessionFormatUnsupportedError(
      `session "${expectedId}" uses log format v${String(value.version)}, but this backend reads only v${String(SESSION_FORMAT_VERSION)}`,
    )
  }
  if (typeof value.createdAt !== 'number' || !Number.isSafeInteger(value.createdAt) || value.createdAt < 0) {
    throw new Error(`stored session "${expectedId}" has an invalid createdAt`)
  }
  if (value.id !== String(expectedId)) {
    throw new Error(`stored session identity mismatch: requested "${expectedId}", header contains "${String(value.id)}"`)
  }
  if (typeof value.isSeeded !== 'boolean') throw new Error(`stored session "${expectedId}" has an invalid isSeeded flag`)
  const cwd = optionalString(value, 'cwd')
  if (cwd !== undefined && !isAbsolute(cwd)) throw new Error(`stored session "${expectedId}" has a non-absolute cwd`)
  const parentSession = optionalString(value, 'parentSession')
  const origin = value.origin
  if (origin !== undefined && origin !== 'subagent') throw new Error(`stored session "${expectedId}" has an invalid origin`)
  const delegationDepth = value.delegationDepth
  if (delegationDepth !== undefined
    && (typeof delegationDepth !== 'number' || !Number.isSafeInteger(delegationDepth) || delegationDepth < 0)) {
    throw new Error(`stored session "${expectedId}" has an invalid delegationDepth`)
  }
  const agentPreset = optionalString(value, 'agentPreset')
  return {
    version: SESSION_FORMAT_VERSION,
    id: expectedId,
    createdAt: value.createdAt,
    isSeeded: value.isSeeded,
    ...(cwd === undefined ? {} : { cwd }),
    ...(parentSession === undefined ? {} : { parentSession: makeSessionId(parentSession) }),
    ...(origin === undefined ? {} : { origin }),
    ...(delegationDepth === undefined ? {} : { delegationDepth }),
    ...(agentPreset === undefined ? {} : { agentPreset }),
  }
}

function parseEvent(value: unknown): SessionEvent {
  if (!isRecord(value) || typeof value.type !== 'string'
    || typeof value.seq !== 'number' || !Number.isSafeInteger(value.seq) || value.seq < 0
    || typeof value.time !== 'number' || !Number.isSafeInteger(value.time) || !Object.hasOwn(value, 'data')) {
    throw new Error('stored session event has an invalid envelope')
  }
  return adoptSessionEvent(value as SessionEvent)
}

function revisionOf(schema: string, id: SessionId, revision: number): PersistenceRevision {
  return SessionPersistenceRevision(`pg:${schema}:${id}:${String(revision)}`)
}

function isUniqueViolation(error: unknown): boolean {
  return isRecord(error) && error.code === '23505'
}

/**
 * PostgreSQL jsonb rejects the JSON escape for U+0000 (\\u0000), even though
 * JSON.stringify is allowed to emit it. Terminal events can contain NUL bytes,
 * so remove them from persisted string values before sending the JSON to PG.
 *
 * This is intentionally limited to the persistence boundary: the in-memory
 * dsh event remains unchanged, while the durable copy loses only U+0000.
 */
function stringifyEventForPostgres(event: SessionEvent): string {
  return JSON.stringify(event, (_key, value: unknown) => (
    typeof value === 'string' ? value.replace(/\u0000/g, '') : value
  ))
}

/** PostgreSQL operations owned by one provider instance. */
export class PgStore {
  private pool: pg.Pool | undefined
  private opening: Promise<void> | undefined
  private closed = false
  private readonly storeIdentity = randomUUID()

  constructor(private readonly options: PgStoreOptions) {}

  /** Open the pool and validate the configured schema once. */
  async open(): Promise<void> {
    if (this.closed) throw new Error('PostgreSQL session persistence is closed')
    this.opening ??= this.openOnce()
    return this.opening
  }

  private async openOnce(): Promise<void> {
    const pool = new pg.Pool({
      connectionString: this.options.connectionString,
      max: this.options.maxConnections,
      options: `-c synchronous_commit=on -c statement_timeout=${String(this.options.statementTimeoutMs)} -c search_path=${this.options.schema}`,
    })
    this.pool = pool
    try {
      await this.withClient(async (client) => { await ensureSchema(client, this.options.schema) })
    } catch (error) {
      await pool.end()
      this.pool = undefined
      throw error
    }
  }

  /** Close the pool after all checked-out connections return. */
  async close(): Promise<void> {
    if (this.closed) return
    this.closed = true
    await this.opening?.catch(() => {})
    await this.pool?.end()
  }

  /** Acquire a process-independent writer lock for one Session id. */
  async acquireLease(id: SessionId): Promise<PgWriteLease> {
    await this.open()
    const client = await this.requirePool().connect()
    try {
      const result = await client.query<{ acquired: boolean }>(
        'SELECT pg_try_advisory_lock(hashtextextended($1, 0)) AS acquired', [String(id)],
      )
      if (result.rows[0]?.acquired !== true) {
        throw new Error('already-owned')
      }
      let released = false
      return {
        client,
        release: async () => {
          if (released) return
          released = true
          try {
            await client.query('SELECT pg_advisory_unlock(hashtextextended($1, 0))', [String(id)])
          } finally {
            client.release()
          }
        },
      }
    } catch (error) {
      client.release()
      throw error
    }
  }

  /** Check whether a durable row exists. */
  async exists(id: SessionId): Promise<boolean> {
    await this.open()
    const result = await this.requirePool().query(
      `SELECT 1 FROM ${quoteIdentifier(this.options.schema)}.sessions WHERE id = $1`, [String(id)],
    )
    return result.rowCount !== 0
  }

  /** Materialize an empty Session header. */
  async persistHeader(header: SessionHeader, inheritedEventCount: SessionLogOffset): Promise<void> {
    await this.open()
    try {
      await this.transaction(async (client) => {
        await client.query(
          `INSERT INTO ${quoteIdentifier(this.options.schema)}.sessions
            (id, header, inherited_event_count, event_count, revision)
           VALUES ($1, $2::jsonb, $3, 0, 0)`,
          [String(header.id), JSON.stringify(materializeCreateHeader(header)), Number(inheritedEventCount)],
        )
      })
    } catch (error) {
      if (isUniqueViolation(error)) throw new SessionAlreadyExistsError(header.id)
      throw error
    }
  }

  /** Append a contiguous batch and update the per-session revision atomically. */
  async persistBatch(
    header: SessionHeader,
    events: readonly SessionEvent[],
    materialized: boolean,
    inheritedEventCount: SessionLogOffset,
  ): Promise<void> {
    if (events.length === 0) return
    await this.open()
    try {
      await this.transaction(async (client) => {
        const schema = quoteIdentifier(this.options.schema)
        if (!materialized) {
          const inserted = await client.query(
            `INSERT INTO ${schema}.sessions
              (id, header, inherited_event_count, event_count, revision)
             VALUES ($1, $2::jsonb, $3, 0, 0)
             ON CONFLICT (id) DO NOTHING
             RETURNING id`,
            [String(header.id), JSON.stringify(materializeCreateHeader(header)), Number(inheritedEventCount)],
          )
          if (inserted.rowCount === 0) throw new SessionAlreadyExistsError(header.id)
        }
        const locked = await client.query<{ event_count: unknown }>(
          `SELECT event_count FROM ${schema}.sessions WHERE id = $1 FOR UPDATE`, [String(header.id)],
        )
        const row = locked.rows[0]
        if (row === undefined) throw new SessionPersistenceNotFoundError(header.id)
        const next = safeInteger(row.event_count, 'event_count')
        assertContiguous(header.id, events, next)
        for (const event of events) {
          await client.query(
            `INSERT INTO ${schema}.session_events (session_id, seq, event)
             VALUES ($1, $2, $3::jsonb)`,
            [String(header.id), Number(event.seq), stringifyEventForPostgres(event)],
          )
        }
        await client.query(
          `UPDATE ${schema}.sessions
           SET event_count = event_count + $2, revision = revision + 1
           WHERE id = $1`,
          [String(header.id), events.length],
        )
      })
    } catch (error) {
      if (isUniqueViolation(error)) {
        throw new Error(`session "${header.id}" append collided with an existing sequence`)
      }
      throw error
    }
  }

  /** Read and validate one complete logical log. */
  async read(id: SessionId): Promise<StoredLog> {
    await this.open()
    try {
      return await this.transaction(async (client) => {
        const schema = quoteIdentifier(this.options.schema)
        const session = await client.query<SessionRow>(
          `SELECT id, header, inherited_event_count AS "inheritedEventCount", event_count AS "eventCount", revision
           FROM ${schema}.sessions WHERE id = $1`, [String(id)],
        )
        const row = session.rows[0]
        if (row === undefined) throw new SessionPersistenceNotFoundError(id)
        const header = parseHeader(row.header, id)
        assertStoredId(id, header)
        const raw = await client.query<EventRow>(
          `SELECT seq, event FROM ${schema}.session_events WHERE session_id = $1 ORDER BY seq`, [String(id)],
        )
        const events = raw.rows.map(row => parseEvent(row.event))
        assertContiguous(id, events, 0)
        const inheritedEventCount = safeInteger(row.inheritedEventCount, 'inherited_event_count') as SessionLogOffset
        const expected = safeInteger(row.eventCount, 'event_count')
        if (events.length !== expected) {
          throw new Error(`session "${id}" event count is ${String(events.length)}, metadata says ${String(expected)}`)
        }
        validateStoredEvents(header, events)
        Session.fromRestore(id, events, header, inheritedEventCount, 'shared-frozen')
        return {
          header,
          inheritedEventCount,
          events: Object.freeze(events),
          revision: revisionOf(this.storeIdentity, id, safeInteger(row.revision, 'revision')),
        }
      })
    } catch (error) {
      if (error instanceof SessionPersistenceNotFoundError || error instanceof SessionFormatUnsupportedError) throw error
      throw new SessionPersistenceCorruptionError(`stored session "${id}" failed validation: ${String(error)}`, { cause: error })
    }
  }

  /** Read one lightweight metadata snapshot. */
  async stat(id: SessionId): Promise<{
    readonly header: SessionHeader
    readonly revision: PersistenceRevision
    readonly eventCount: number
  } | undefined> {
    await this.open()
    const result = await this.requirePool().query<SessionRow>(
      `SELECT id, header, inherited_event_count AS "inheritedEventCount", event_count AS "eventCount", revision
       FROM ${quoteIdentifier(this.options.schema)}.sessions WHERE id = $1`, [String(id)],
    )
    const row = result.rows[0]
    if (row === undefined) return undefined
    const header = parseHeader(row.header, id)
    return {
      header,
      revision: revisionOf(this.storeIdentity, id, safeInteger(row.revision, 'revision')),
      eventCount: safeInteger(row.eventCount, 'event_count'),
    }
  }

  /** List lightweight metadata snapshots. */
  async list(): Promise<readonly {
    readonly header: SessionHeader
    readonly revision: PersistenceRevision
    readonly eventCount: number
  }[]> {
    await this.open()
    const result = await this.requirePool().query<SessionRow>(
      `SELECT id, header, inherited_event_count AS "inheritedEventCount", event_count AS "eventCount", revision
       FROM ${quoteIdentifier(this.options.schema)}.sessions ORDER BY id`,
    )
    return result.rows.map((row) => {
      const id = makeSessionId(row.id)
      return {
        header: parseHeader(row.header, id),
        revision: revisionOf(this.storeIdentity, id, safeInteger(row.revision, 'revision')),
        eventCount: safeInteger(row.eventCount, 'event_count'),
      }
    })
  }

  private requirePool(): pg.Pool {
    if (this.pool === undefined) throw new Error('PostgreSQL session persistence pool is not open')
    return this.pool
  }

  private async withClient<T>(operation: (client: PoolClient) => Promise<T>): Promise<T> {
    const client = await this.requirePool().connect()
    try {
      return await operation(client)
    } finally {
      client.release()
    }
  }

  private async transaction<T>(operation: (client: PoolClient) => Promise<T>): Promise<T> {
    return this.withClient(async (client) => {
      await client.query('BEGIN')
      try {
        const result = await operation(client)
        await client.query('COMMIT')
        return result
      } catch (error) {
        await client.query('ROLLBACK').catch(() => {})
        throw error
      }
    })
  }
}
