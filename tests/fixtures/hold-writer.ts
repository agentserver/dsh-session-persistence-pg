import { Context } from '@deepseek-ai/cordis'
import SessionStore, { SessionId } from '@deepseek-ai/dsh-session'
import type { SessionAccess } from '@deepseek-ai/dsh-session-persistence'
import PgSessionPersistence from '../../src/index.ts'

const connectionString = process.env.DSH_PG_TEST_URL
const schema = process.env.DSH_PG_TEST_SCHEMA
const sessionId = process.env.DSH_PG_TEST_SESSION
if (connectionString === undefined || schema === undefined || sessionId === undefined) {
  throw new Error('missing PostgreSQL worker configuration')
}

const ctx = new Context()
await ctx.plugin(SessionStore)
const fiber = await ctx.plugin(PgSessionPersistence, { connectionString, schema })
const handle = await ctx.sessionPersistence.open(SessionId(sessionId), 'write' satisfies SessionAccess)
process.stdout.write('ready\n')
await new Promise<void>(() => {})
await handle.close()
await fiber.dispose()
