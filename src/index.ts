/** PostgreSQL implementation of the handle-based SessionPersistence seam. */

import { Context } from '@deepseek-ai/cordis'
import z from '@deepseek-ai/schemastery'
import type { SessionHeader, SessionId, SessionLogOffset } from '@deepseek-ai/dsh-session'
import {
  SessionAlreadyExistsError,
  SessionAlreadyOwnedError,
  materializeCreateHeader,
  SessionPersistence,
  SessionPersistenceRevision,
  type SessionAccess,
  type SessionHandle,
  type SessionPersistenceCreateOptions,
  type SessionPersistenceListOptions,
  type SessionPersistenceOpenOptions,
  type SessionPersistenceSnapshot,
  type SessionPersistenceStatOptions,
} from '@deepseek-ai/dsh-session-persistence'
import { PgBackendTracker, PgSessionHandle, mapLeaseError, type PgHandleStorage } from './handle.ts'
import { PgStore, type PgStoreOptions } from './store.ts'

/** PostgreSQL provider configuration. */
export interface Config {
  /** PostgreSQL connection string. Required; credentials belong to the deployment. */
  readonly connectionString: string
  /** Maximum number of pooled connections. @default 10 */
  readonly maxConnections?: number
  /** PostgreSQL statement timeout in milliseconds. @default 30000 */
  readonly statementTimeoutMs?: number
  /** Schema containing the provider tables. @default dsh */
  readonly schema?: string
}

/** Configuration schema. */
export const Config: z<Config> = z.object({
  connectionString: z.string().role('secret').required(),
  maxConnections: z.natural().min(1).default(10),
  statementTimeoutMs: z.natural().min(1).default(30_000),
  schema: z.string().default('dsh'),
})

/** PostgreSQL SessionPersistence service. */
export class PgSessionPersistence extends SessionPersistence {
  static Config = Config
  static inject = ['sessions']

  override readonly name = 'session-persistence-pg'
  private readonly store: PgStore
  private readonly tracker: PgBackendTracker
  private readonly storage: PgHandleStorage

  constructor(ctx: Context, config: Config) {
    super(ctx)
    const resolved = Config(config) as Required<Config>
    const options: PgStoreOptions = {
      connectionString: resolved.connectionString,
      maxConnections: resolved.maxConnections,
      statementTimeoutMs: resolved.statementTimeoutMs,
      schema: resolved.schema,
    }
    this.store = new PgStore(options)
    this.storage = {
      persistBatch: (header, events, materialized, inheritedEventCount) =>
        this.persistBatch(header, events, materialized, inheritedEventCount),
      persistHeader: (header, inheritedEventCount) => this.store.persistHeader(header, inheritedEventCount),
      read: id => this.store.read(id),
      acquireLease: id => this.store.acquireLease(id),
      releaseHandle: (handle, materialized) => { this.tracker.release(handle, materialized) },
      materializeSession: (id) => { this.tracker.materializeSession(id) },
      hasPendingSession: id => this.tracker.hasPending(id),
    }
    this.tracker = new PgBackendTracker(this.name)
    this.tracker.install(ctx)
    ctx.effect(() => async () => {
      await this.tracker.closeAll()
      await this.store.close()
    }, `${this.name} pool and handles`)
  }

  /** Provider-owned append primitive exposed for the shared live-write test seam. */
  persistBatch(
    header: SessionHeader,
    events: readonly import('@deepseek-ai/dsh-session').SessionEvent[],
    materialized: boolean,
    inheritedEventCount: SessionLogOffset,
  ): Promise<void> {
    return this.store.persistBatch(header, events, materialized, inheritedEventCount)
  }

  /** Create a pending write handle; the first append or flush materializes it. */
  async create(header: SessionHeader, options?: SessionPersistenceCreateOptions): Promise<SessionHandle> {
    options?.signal?.throwIfAborted()
    const snapshot = materializeCreateHeader(header)
    if (snapshot.isSeeded && options?.inheritedEventCount === undefined) {
      throw new Error('seeded session header requires an inherited event count')
    }
    if (!snapshot.isSeeded && options?.inheritedEventCount !== undefined && options.inheritedEventCount !== 0) {
      throw new Error('unseeded session header inherited event count must be 0')
    }
    if (await this.store.exists(snapshot.id)) throw new SessionAlreadyExistsError(snapshot.id)
    const inheritedEventCount = (options?.inheritedEventCount ?? 0) as SessionLogOffset
    this.tracker.registerCreated(snapshot, inheritedEventCount)
    const handle = new PgSessionHandle(this.storage, snapshot.id, snapshot, 'write', {
      cursor: 0,
      materialized: false,
      inheritedEventCount,
      events: [],
    })
    return this.tracker.adopt(handle)
  }

  /** Open an existing Session for read or single-writer mutation. */
  async open(id: SessionId, access: SessionAccess, options?: SessionPersistenceOpenOptions): Promise<SessionHandle> {
    options?.signal?.throwIfAborted()
    const pending = this.tracker.pendingOf(id)
    if (pending !== undefined) {
      if (access === 'write') throw new SessionAlreadyOwnedError(id)
      return this.tracker.adopt(new PgSessionHandle(this.storage, id, pending.header, 'read', {
        cursor: 0,
        materialized: false,
        inheritedEventCount: pending.inheritedEventCount,
        events: [],
      }))
    }
    const stored = await this.store.read(id)
    options?.signal?.throwIfAborted()
    if (access === 'read') {
      return this.tracker.adopt(new PgSessionHandle(this.storage, id, stored.header, 'read', {
        cursor: stored.events.length,
        materialized: true,
        inheritedEventCount: stored.inheritedEventCount,
        events: [...stored.events],
      }))
    }
    this.tracker.claimWrite(id)
    let lease
    try {
      lease = await this.store.acquireLease(id)
      return this.tracker.adopt(new PgSessionHandle(this.storage, id, stored.header, 'write', {
        cursor: stored.events.length,
        materialized: true,
        inheritedEventCount: stored.inheritedEventCount,
        events: [...stored.events],
      }, lease))
    } catch (error) {
      this.tracker.releaseClaim(id)
      throw mapLeaseError(id, error)
    }
  }

  /** Flush every active write handle. */
  flush(): Promise<void> {
    return this.tracker.flushAll()
  }

  /** Lightweight metadata observation. */
  async stat(id: SessionId, options?: SessionPersistenceStatOptions): Promise<SessionPersistenceSnapshot | undefined> {
    options?.signal?.throwIfAborted()
    const pending = this.tracker.pendingOf(id)
    if (pending !== undefined) {
      return {
        header: pending.header,
        revision: SessionPersistenceRevision(`pg:pending:${String(id)}`),
      }
    }
    const snapshot = await this.store.stat(id)
    options?.signal?.throwIfAborted()
    return snapshot === undefined ? undefined : snapshot
  }

  /** List every Session metadata row. */
  async list(options?: SessionPersistenceListOptions): Promise<readonly SessionPersistenceSnapshot[]> {
    options?.signal?.throwIfAborted()
    const listed: SessionPersistenceSnapshot[] = [...await this.store.list()]
    const ids = new Set(listed.map(item => item.header.id))
    for (const [id, pending] of this.tracker.pendingEntries()) {
      if (!ids.has(id)) {
        listed.push({
          header: pending.header,
          revision: SessionPersistenceRevision(`pg:pending:${String(id)}`),
        })
      }
    }
    options?.signal?.throwIfAborted()
    return listed
  }

}

export { PgStore } from './store.ts'
export { SCHEMA_VERSION } from './schema.ts'

export default PgSessionPersistence
