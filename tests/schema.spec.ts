import { describe, expect, it } from 'vitest'
import { quoteIdentifier, safeInteger, SCHEMA_VERSION } from '../src/schema.ts'

describe('PostgreSQL Session Persistence schema helpers', () => {
  it('quotes deployment identifiers only after validating the identifier grammar', () => {
    expect(quoteIdentifier('dsh_sessions')).toBe('"dsh_sessions"')
    expect(() => quoteIdentifier('dsh-sessions')).toThrow(/must match/)
    expect(() => quoteIdentifier('public.users')).toThrow(/must match/)
  })

  it('admits safe non-negative PostgreSQL bigint values', () => {
    expect(safeInteger('42', 'revision')).toBe(42)
    expect(() => safeInteger('-1', 'revision')).toThrow(/non-negative/)
    expect(() => safeInteger(String(Number.MAX_SAFE_INTEGER + 1), 'revision')).toThrow(/safe integer/)
  })

  it('owns a monotonic physical schema version', () => {
    expect(SCHEMA_VERSION).toBeGreaterThanOrEqual(1)
  })
})
