import { Context } from '@deepseek-ai/cordis'
import { spawn } from 'node:child_process'
import { once } from 'node:events'
import { fileURLToPath } from 'node:url'
import SessionStore, { SessionId, SessionSeq } from '@deepseek-ai/dsh-session'
import type { SessionEvent } from '@deepseek-ai/dsh-session'
import PgSessionPersistence from '../src/index.ts'
import { describe, expect, it } from 'vitest'
import { runPersistenceContract, meta, oneTurnLog } from './contract.ts'
import { runLiveWritePathContract } from './live-write-contract.ts'

const configuredConnectionString = process.env.DSH_PG_TEST_URL
const connectionString = configuredConnectionString ?? ''

describe.skipIf(configuredConnectionString === undefined)('PostgreSQL SessionPersistence integration', () => {
  let schemaCounter = 0
  const freshSchema = (prefix: string): string => {
    schemaCounter += 1
    return `${prefix}_${process.pid}_${Date.now()}_${schemaCounter}`
  }

  async function mount(schema: string): Promise<{ readonly ctx: Context; readonly fiber: Awaited<ReturnType<Context['plugin']>> }> {
    const ctx = new Context()
    await ctx.plugin(SessionStore)
    const fiber = await ctx.plugin(PgSessionPersistence, { connectionString, schema })
    return { ctx, fiber }
  }

  runPersistenceContract('pg', async () => {
    const schema = freshSchema('contract')
    const { ctx, fiber } = await mount(schema)
    return {
      persistence: ctx.sessionPersistence,
      dispose: async () => { await fiber.dispose() },
      reopen: async () => {
        const next = await mount(schema)
        return { persistence: next.ctx.sessionPersistence, dispose: async () => { await next.fiber.dispose() } }
      },
    }
  })

  runLiveWritePathContract('pg', 200, async () => {
    const schema = freshSchema('live')
    const mounted = await mount(schema)
    return {
      ctx: mounted.ctx,
      remount: async () => (await mount(schema)).ctx,
    }
  })

  it('round-trips a durable append and reopens it through a new provider', async () => {
    const first = new Context()
    await first.plugin(SessionStore)
    const schema = `test_${Date.now()}_${Math.floor(Math.random() * 1_000_000)}`
    const firstFiber = await first.plugin(PgSessionPersistence, { connectionString, schema })
    const header = meta('pg-round-trip', '/work')
    const writer = await first.sessionPersistence.create(header)
    await writer.append([{ type: 'turn/start', seq: SessionSeq(0), time: 1, data: { turn: 1 } }])
    await writer.close()
    await firstFiber.dispose()

    const second = new Context()
    await second.plugin(SessionStore)
    const secondFiber = await second.plugin(PgSessionPersistence, { connectionString, schema })
    try {
      const reader = await second.sessionPersistence.open(header.id, 'read')
      await expect(reader.read()).resolves.toMatchObject({ events: [{ type: 'turn/start', seq: 0 }] })
      await reader.close()
    } finally {
      await secondFiber.dispose()
    }
  })

  it('drops NUL characters before writing events to PostgreSQL jsonb', async () => {
    const first = new Context()
    await first.plugin(SessionStore)
    const schema = `nul_${Date.now()}_${Math.floor(Math.random() * 1_000_000)}`
    const firstFiber = await first.plugin(PgSessionPersistence, { connectionString, schema })
    const header = meta('pg-nul', '/work')
    try {
      const writer = await first.sessionPersistence.create(header)
      const event = {
        type: 'turn/start',
        seq: SessionSeq(0),
        time: 1,
        data: { text: 'before\u0000after' },
      } as unknown as SessionEvent
      await writer.append([event])
      await writer.close()

      const reader = await first.sessionPersistence.open(header.id, 'read')
      await expect(reader.read()).resolves.toMatchObject({
        events: [{ data: { text: 'beforeafter' } }],
      })
      await reader.close()
    } finally {
      await firstFiber.dispose()
    }
  })

  it('allows only one writer across independent provider instances', async () => {
    const schema = freshSchema('concurrent')
    const first = new Context()
    const second = new Context()
    await first.plugin(SessionStore)
    await second.plugin(SessionStore)
    const firstFiber = await first.plugin(PgSessionPersistence, { connectionString, schema })
    const secondFiber = await second.plugin(PgSessionPersistence, { connectionString, schema })
    const header = meta('pg-concurrent', '/work')
    try {
      const seed = await first.sessionPersistence.create(header)
      await seed.append(oneTurnLog())
      await seed.close()
      const [left, right] = await Promise.allSettled([
        first.sessionPersistence.open(header.id, 'write'),
        second.sessionPersistence.open(header.id, 'write'),
      ])
      const opened = [left, right].filter(result => result.status === 'fulfilled')
      const rejected = [left, right].filter(result => result.status === 'rejected')
      expect(opened).toHaveLength(1)
      expect(rejected).toHaveLength(1)
      expect((rejected[0] as PromiseRejectedResult).reason).toMatchObject({ name: 'SessionAlreadyOwnedError' })
      await (opened[0] as PromiseFulfilledResult<Awaited<ReturnType<typeof first.sessionPersistence.open>>>).value.close()
    } finally {
      await Promise.all([firstFiber.dispose(), secondFiber.dispose()])
    }
  })

  it.skipIf(process.platform === 'win32')('releases the writer lock when its owning process is killed', { timeout: 30_000 }, async () => {
    const schema = freshSchema('crash')
    const parent = new Context()
    await parent.plugin(SessionStore)
    const parentFiber = await parent.plugin(PgSessionPersistence, { connectionString, schema })
    const header = meta('pg-crash-lock', '/work')
    let child: ReturnType<typeof spawn> | undefined
    let childExited = false
    try {
      const seed = await parent.sessionPersistence.create(header)
      await seed.append(oneTurnLog())
      await seed.close()
      child = spawn(process.execPath, ['--import', 'tsx/esm', fileURLToPath(new URL('./fixtures/hold-writer.ts', import.meta.url))], {
        env: {
          ...process.env,
          DSH_PG_TEST_URL: connectionString,
          DSH_PG_TEST_SCHEMA: schema,
          DSH_PG_TEST_SESSION: String(header.id),
        },
        stdio: ['ignore', 'pipe', 'pipe'],
      })
      let output = ''
      let errorOutput = ''
      const ready = Promise.withResolvers<undefined>()
      child.stdout?.setEncoding('utf8')
      child.stdout?.on('data', (chunk: string) => { output += chunk })
      child.stdout?.on('data', () => {
        if (output.includes('ready\n')) ready.resolve(undefined)
      })
      child.stderr?.setEncoding('utf8')
      child.stderr?.on('data', (chunk: string) => { errorOutput += chunk })
      child.once('exit', (code, signal) => {
        childExited = true
        if (!output.includes('ready\n')) ready.reject(new Error(`holder exited before ready (code=${String(code)}, signal=${String(signal)}): ${errorOutput}`))
      })
      const timeout = setTimeout(() => {
        ready.reject(new Error(`holder did not become ready: ${errorOutput}`))
      }, 15_000)
      try { await ready.promise } finally { clearTimeout(timeout) }
      await expect(parent.sessionPersistence.open(header.id, 'write')).rejects.toMatchObject({ name: 'SessionAlreadyOwnedError' })
      child.kill('SIGKILL')
      await once(child, 'exit')
      const reopened = await parent.sessionPersistence.open(header.id, 'write')
      await reopened.close()
    } finally {
      if (child !== undefined && !childExited) {
        child.kill('SIGKILL')
        await once(child, 'exit').catch(() => undefined)
      }
      await parentFiber.dispose()
    }
  })
})
