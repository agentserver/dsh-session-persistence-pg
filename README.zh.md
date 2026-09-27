---
description: "基于 PostgreSQL 的 handle-based SessionPersistence 后端。"
kind: "package-reference"
---

# @agentserver/dsh-session-persistence-pg

[English](README.md) | 中文

## 摘要

本包在 PostgreSQL 上实现 dsh 的 `SessionPersistence` handle contract。它把 Session header 与仅追加的事件行存入配置的 schema，通过 PostgreSQL 事务完成持久化追加，并为每个 Session id 使用一个 advisory writer lock。本包是可选后端；随产品交付的 profile 仍使用 JSONL。

本后端只接受当前 Session format。它复用共享 persistence 校验函数验证 header 与事件，并拒绝不认识的 schema 版本。第一版刻意不把租户身份放进 backend contract；后续租户 facade 必须在调用进入本 provider 前约束每个操作。

## 使用本包

将本包与 `@deepseek-ai/dsh-session` 一起挂载，并配置 PostgreSQL 连接：

```yaml
- id: session-persistence-pg
  name: '@agentserver/dsh-session-persistence-pg'
  config:
    connectionString: !!js process.env.DSH_PG_URL
    schema: dsh
```

`connectionString` 必填，应通过部署的 secret 管理提供。`maxConnections` 默认 `10`，`statementTimeoutMs` 默认 `30000`，`schema` 默认 `dsh`。连接池启用 `synchronous_commit=on`，并使用配置的 statement timeout。

写 handle 在第一次产生持久化数据的 mutation 后持有该 Session 的 PostgreSQL advisory lock；进程崩溃时数据库连接释放锁。读 handle 不取得写所有权。`create` 采用延迟落盘：空 Session 在 handle flush 时变为持久记录；未写入就关闭的 handle 不留下记录。

## 实现说明

`src/store.ts` 负责 schema 安装、metadata 行、事件行、事务、revision 与 advisory lock。`src/handle.ts` 负责 provider 内 mutation chain、实时事件批处理、单调读取与 close 排空。`src/index.ts` 把这些部分接入 `ctx.sessionPersistence`，并路由 `session/event`、`session/flush` 与 `session/disposed`。

物理表为 `persistence_state`、`sessions` 与 `session_events`。`sessions` 行保存 JSON header、继承事件数、事件数与单调 revision。每次 append 都会锁定 metadata 行、检查下一个 seq、在同一事务中插入完整批次并递增 revision。第一版事件保持普通 JSONB 行；只有测量证明有需要时才考虑物理打包或压缩。

## 模型体验

没有变化。PostgreSQL 行会在 Session replay 或模型请求组装前还原为相同的逻辑 `SessionEvent[]`。数据库后端只改变存储位置与并发行为，不增加 prompt 内容或模型可见元数据。

**运行时不变式：** 不发布 runtime invariant companion；持久化正确性由后端往返、事务与 writer lock 测试负责，本 provider 没有可独立维护且可在进程内比较的关系。

## 已知限制与后续工作

- 本包不提供租户授权或 row-level security。在挂载租户 scoped persistence 之前，不应将同一个 provider 暴露给互不信任的调用方。
- 本包只接受当前 Session format，不迁移旧 generation。
- PostgreSQL 是外部服务依赖；集成测试必须使用真实 PostgreSQL 验证事务与 advisory lock。
- 附件、projection cache、workspace 数据与搜索索引仍由各自包负责。
