---
description: "PostgreSQL backend for the handle-based SessionPersistence service."
kind: "package-reference"
---

# @agentserver/dsh-session-persistence-pg

English | [中文](README.zh.md)

## Summary

This package implements the dsh `SessionPersistence` handle contract over PostgreSQL. It stores Session headers and append-only event rows in a configured schema, uses PostgreSQL transactions for durable appends, and uses one advisory writer lock per Session id. The package is opt-in; shipped profiles continue to use JSONL backend.

The backend stores the current Session format only. It validates stored headers and events with the shared persistence helpers and refuses a schema version it does not understand. Tenant identity is deliberately not part of this first backend contract; a later tenant-scoped facade must constrain every operation before it reaches this provider.

## Use this package

Mount the package beside `@deepseek-ai/dsh-session` and configure a PostgreSQL connection:

```yaml
- id: session-persistence-pg
  name: '@agentserver/dsh-session-persistence-pg'
  config:
    connectionString: !!js process.env.DSH_PG_URL
    schema: dsh
```

`connectionString` is required and should be supplied through deployment secret management. `maxConnections` defaults to `10`, `statementTimeoutMs` defaults to `30000`, and `schema` defaults to `dsh`. The provider enables `synchronous_commit=on` for its pool and uses the configured statement timeout.

Each write handle holds a PostgreSQL advisory lock for its Session after the first materializing mutation. A process crash releases that lock with the database connection. Read handles do not acquire writer ownership. `create` is lazy: an empty Session becomes durable when its handle is flushed, while an unmaterialized handle that closes without a write leaves no row.

## Understand the implementation

`src/store.ts` owns schema installation, metadata rows, event rows, transactions, revisions, and advisory locks. `src/handle.ts` owns the provider-local mutation chain, live event batching, monotonic reads, and close drain. `src/index.ts` connects those pieces to `ctx.sessionPersistence` and routes `session/event`, `session/flush`, and `session/disposed`.

The physical tables are `persistence_state`, `sessions`, and `session_events`. The `sessions` row carries the JSON header, inherited-event count, event count, and a monotonic revision. Every append locks the metadata row, verifies the next sequence number, inserts the complete batch, and increments the revision in one transaction. Events remain ordinary JSONB rows in this initial implementation; physical packing and compression are deferred until measurements show a current need.

## Model Experience

None. PostgreSQL rows are decoded back into the same logical `SessionEvent[]` values before Session replay or model request assembly. The database provider changes storage location and concurrency behavior only; it does not add prompt content or model-visible metadata.

**Runtime invariant:** No runtime invariant companion is published; persistence correctness requires backend round-trip, transaction, and writer-lock tests, and this provider exposes no independently maintained in-process relation.

## Known Limitations and Deferred Work

- No tenant authorization or row-level security is included. Deployments must not expose one provider instance to mutually untrusted callers until a tenant-scoped persistence service is mounted above it.
- The provider accepts only the current Session format and does not migrate older generations.
- PostgreSQL is an external service dependency; the package's integration tests must use a real PostgreSQL instance to exercise transactions and advisory locks.
- Attachments, projection cache records, workspace data, and search indexes remain owned by their existing packages.
