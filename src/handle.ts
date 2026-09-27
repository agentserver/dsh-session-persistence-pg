/** PostgreSQL SessionHandle lifecycle, live routing, and teardown. */

import type { Context } from '@deepseek-ai/cordis'
import { errorChain } from '@deepseek-ai/dsh-llm'
import type { Session, SessionEvent, SessionHeader, SessionId, SessionLogOffset } from '@deepseek-ai/dsh-session'
import {
  assertContiguous,
  materializeAppendBatch,
  SessionAlreadyExistsError,
  SessionAlreadyOwnedError,
  SessionHandleClosedError,
  SessionReadOnlyError,
} from '@deepseek-ai/dsh-session-persistence'
import type {
  SessionAccess,
  SessionHandle,
  SessionHandleAppendOptions,
  SessionHandleFlushOptions,
  SessionHandleReadOptions,
  SessionHandleReadResult,
} from '@deepseek-ai/dsh-session-persistence'
import type { PgWriteLease, StoredLog } from './store.ts'

/** Maximum intentional delay before live events are written as one batch. */
export const LIVE_WRITE_BATCH_MAX_DELAY_MS = 200

/** Storage operations used by one handle. */
export interface PgHandleStorage {
  persistBatch(
    header: SessionHeader,
    events: readonly SessionEvent[],
    materialized: boolean,
    inheritedEventCount: SessionLogOffset,
  ): Promise<void>
  persistHeader(header: SessionHeader, inheritedEventCount: SessionLogOffset): Promise<void>
  read(id: SessionId): Promise<StoredLog>
  acquireLease(id: SessionId): Promise<PgWriteLease>
  releaseHandle(handle: PgSessionHandle, materialized: boolean): void
  materializeSession(id: SessionId): void
  hasPendingSession(id: SessionId): boolean
}

/** Mutable per-handle state retained by the provider. */
export interface PgHandleState {
  cursor: number
  materialized: boolean
  inheritedEventCount: SessionLogOffset
  events: SessionEvent[]
}

/** One PostgreSQL-backed SessionHandle. */
export class PgSessionHandle implements SessionHandle {
  private chain: Promise<void> = Promise.resolve()
  private closing: Promise<void> | undefined
  private observedLength = 0
  private buffered: SessionEvent[] = []
  private batchTimer: ReturnType<typeof setTimeout> | undefined
  private draining: Promise<void> | undefined
  private drainPaused = false

  constructor(
    private readonly storage: PgHandleStorage,
    readonly id: SessionId,
    readonly header: SessionHeader,
    readonly access: SessionAccess,
    private readonly state: PgHandleState,
    private lease?: PgWriteLease,
  ) {}

  /** Exact inherited event count carried by the stored header. */
  get inheritedEventCount(): SessionLogOffset {
    return this.state.inheritedEventCount
  }

  /** Read one monotonic logical event slice. */
  async read(
    offset = 0,
    length = Number.MAX_SAFE_INTEGER,
    options?: SessionHandleReadOptions,
  ): Promise<SessionHandleReadResult> {
    this.assertOpen('read')
    if (!Number.isSafeInteger(offset) || offset < 0) {
      throw new TypeError(`read offset must be a non-negative safe integer, got ${String(offset)}`)
    }
    if (!Number.isSafeInteger(length) || length < 0) {
      throw new TypeError(`read length must be a non-negative safe integer, got ${String(length)}`)
    }
    options?.signal?.throwIfAborted()
    if (this.access === 'read' && this.storage.hasPendingSession(this.id)) {
      return { eventState: 'detached', events: [] }
    }
    if (this.access === 'write') {
      this.observedLength = Math.max(this.observedLength, this.state.events.length)
      return { eventState: 'detached', events: structuredClone(this.state.events.slice(offset, offset + length)) }
    }
    const stored = await this.storage.read(this.id)
    options?.signal?.throwIfAborted()
    if (stored.events.length < this.observedLength) {
      throw new Error(
        `session "${this.id}": stored log shrank below a previously observed prefix (${String(stored.events.length)} < ${String(this.observedLength)})`,
      )
    }
    this.observedLength = stored.events.length
    return { eventState: 'detached', events: structuredClone(stored.events.slice(offset, offset + length)) }
  }

  /** Queue one durable append on this handle's mutation chain. */
  async append(events: readonly SessionEvent[], options?: SessionHandleAppendOptions): Promise<void> {
    this.assertOpen('append')
    const batch = materializeAppendBatch(events)
    return this.run('append', async () => {
      options?.signal?.throwIfAborted()
      await this.persistContiguous(batch)
    })
  }

  /** Flush pending live events and materialize an empty created Session. */
  flush(options?: SessionHandleFlushOptions): Promise<void> {
    return this.run('flush', async () => {
      options?.signal?.throwIfAborted()
      if (this.access !== 'write') throw new SessionReadOnlyError(this.id, 'flush')
      if (!this.state.materialized) {
        await this.ensureLease()
        await this.storage.persistHeader(this.header, this.state.inheritedEventCount)
        this.state.materialized = true
        this.storage.materializeSession(this.id)
      }
    })
  }

  /** Drain live writes, release the advisory lease, and forget the handle. */
  close(): Promise<void> {
    return this.closing ??= (async () => {
      const failures: Error[] = []
      try {
        for (;;) {
          await this.drainLive()
          await this.chain
          if (this.buffered.length === 0) break
        }
      } catch (error: unknown) {
        failures.push(error instanceof Error ? error : new Error(errorChain(error)))
        await this.chain.catch(() => {})
      }
      try {
        await this.lease?.release()
      } catch (error: unknown) {
        failures.push(error instanceof Error ? error : new Error(errorChain(error)))
      }
      this.storage.releaseHandle(this, this.state.materialized)
      const failure = failures[0]
      if (failures.length === 1 && failure !== undefined) throw failure
      if (failures.length > 1) throw new AggregateError(failures, `session "${this.id}" close failed`)
    })()
  }

  /** `await using` support. */
  [Symbol.asyncDispose](): Promise<void> {
    return this.close()
  }

  /** Route one committed live event into this write handle. */
  enqueueLive(event: SessionEvent, reportFailure: (error: unknown) => void): void {
    this.buffered.push(structuredClone(event))
    if (this.batchTimer !== undefined || this.drainPaused) return
    this.batchTimer = setTimeout(() => {
      this.batchTimer = undefined
      this.drainLive().catch(reportFailure)
    }, LIVE_WRITE_BATCH_MAX_DELAY_MS)
  }

  /** Drain the routed live buffer through one single-flight operation. */
  drainLive(): Promise<void> {
    return this.draining ??= this.drainBuffered().finally(() => {
      this.draining = undefined
    })
  }

  private async drainBuffered(): Promise<void> {
    if (this.batchTimer !== undefined) {
      clearTimeout(this.batchTimer)
      this.batchTimer = undefined
    }
    this.drainPaused = false
    while (this.buffered.length > 0) {
      await this.enqueueChain(async () => {
        const batch = this.buffered.splice(0)
        try {
          await this.persistContiguous(materializeAppendBatch(batch))
        } catch (error) {
          this.buffered = batch.concat(this.buffered)
          this.drainPaused = true
          throw error
        }
      })
    }
  }

  private async persistContiguous(batch: readonly SessionEvent[]): Promise<void> {
    if (this.access !== 'write') throw new SessionReadOnlyError(this.id, 'append')
    if (batch.length === 0) return
    await this.ensureLease()
    assertContiguous(this.id, batch, this.state.cursor)
    await this.storage.persistBatch(
      this.header,
      batch,
      this.state.materialized,
      this.state.inheritedEventCount,
    )
    this.state.events.push(...batch)
    this.state.cursor += batch.length
    this.state.materialized = true
    this.storage.materializeSession(this.id)
    this.observedLength = this.state.cursor
  }

  private enqueueChain(operation: () => Promise<void>): Promise<void> {
    const next = this.chain.then(operation)
    this.chain = next.catch(() => {})
    return next
  }

  private async run(operation: string, action: () => Promise<void>): Promise<void> {
    this.assertOpen(operation)
    await this.enqueueChain(async () => {
      this.assertOpen(operation)
      await action()
    })
  }

  private assertOpen(operation: string): void {
    if (this.closing !== undefined) throw new SessionHandleClosedError(this.id, operation)
  }

  private async ensureLease(): Promise<void> {
    this.lease ??= await this.storage.acquireLease(this.id)
  }
}

/** Provider-local ownership and live-event tracker. */
export class PgBackendTracker {
  readonly openHandles = new Set<PgSessionHandle>()
  private readonly writers = new Map<SessionId, PgSessionHandle | null>()
  private readonly pending = new Map<SessionId, { readonly header: SessionHeader; readonly inheritedEventCount: SessionLogOffset }>()

  constructor(private readonly label: string) {}

  registerCreated(header: SessionHeader, inheritedEventCount: SessionLogOffset): void {
    if (this.writers.has(header.id)) throw new SessionAlreadyExistsError(header.id)
    this.writers.set(header.id, null)
    this.pending.set(header.id, { header, inheritedEventCount })
  }

  claimWrite(id: SessionId): void {
    if (this.writers.has(id)) throw new SessionAlreadyOwnedError(id)
    this.writers.set(id, null)
  }

  releaseClaim(id: SessionId): void {
    this.writers.delete(id)
  }

  pendingOf(id: SessionId): { readonly header: SessionHeader; readonly inheritedEventCount: SessionLogOffset } | undefined {
    return this.pending.get(id)
  }

  hasPending(id: SessionId): boolean {
    return this.pending.has(id)
  }

  materializeSession(id: SessionId): void {
    this.pending.delete(id)
  }

  pendingEntries(): IterableIterator<[
    SessionId,
    { readonly header: SessionHeader; readonly inheritedEventCount: SessionLogOffset },
  ]> {
    return this.pending.entries()
  }

  adopt(handle: PgSessionHandle): PgSessionHandle {
    this.openHandles.add(handle)
    if (handle.access === 'write') this.writers.set(handle.id, handle)
    return handle
  }

  release(handle: PgSessionHandle, _materialized: boolean): void {
    this.openHandles.delete(handle)
    if (handle.access !== 'write') return
    this.writers.delete(handle.id)
    this.pending.delete(handle.id)
  }

  async flushAll(): Promise<void> {
    const failures: unknown[] = []
    for (const writer of this.writers.values()) {
      if (writer === null) continue
      try {
        await writer.drainLive()
        await writer.flush()
      } catch (error) { failures.push(error) }
    }
    if (failures.length > 0) throw new AggregateError(failures, `${this.label} flush failed`)
  }

  install(ctx: Context): void {
    ctx.on('session/event', (session: Session, event: SessionEvent) => {
      this.writers.get(session.id)?.enqueueLive(event, (error) => {
        ctx.logger.warn(`${this.label}: background write for session "${session.id}" failed: ${String(error)}`)
      })
    })
    ctx.on('session/flush', (session: Session) => {
      const writer = this.writers.get(session.id)
      return writer === undefined || writer === null ? undefined : (async () => {
        await writer.drainLive()
        await writer.flush()
      })()
    })
    ctx.on('session/disposed', (session: Session) => {
      const writer = this.writers.get(session.id)
      writer?.close().catch((error: unknown) => {
        ctx.logger.warn(`${this.label}: final drain for session "${session.id}" failed: ${String(error)}`)
      })
    })
  }

  async closeAll(): Promise<void> {
    const failures: unknown[] = []
    for (const handle of [...this.openHandles]) {
      try { await handle.close() } catch (error) { failures.push(error) }
    }
    if (failures.length > 0) throw new AggregateError(failures, `${this.label} dispose failed`)
  }
}

/** Map an advisory-lock collision to the provider-neutral ownership failure. */
export function mapLeaseError(id: SessionId, error: unknown): Error {
  return error instanceof Error && error.message === 'already-owned'
    ? new SessionAlreadyOwnedError(id)
    : error instanceof Error ? error : new Error(errorChain(error))
}
