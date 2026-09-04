# XGraph 阶段 2 开发完成报告

_PostgreSQL 持久调度与 Scraper Account 池；阶段 2；2026-09-04_

---

## 📋 结论

阶段 2 已完成代码、单元测试和真实 PostgreSQL 17 集成验证。系统现在可以原子创建 Seed Task、持久保存 L0 frontier、多 Worker 并发 claim、在 Worker 崩溃后回收过期租约，并在发出 HTTP 请求前同时保护 Task 总预算和 operation 预算。

Scraper Account 采用账号级状态与 `(account, operation)` 状态分离的模型。Cookie 不写入业务数据库，数据库只保存 `credential_ref`；账号 operation 支持原子 lease、响应头配额回写、cooling/reset、过期租约回收、dead 隔离和显式 standby 启用。

本阶段没有实现 Kafka、Parser、L0-L6 BFS、Timeline 候选流程、API 或前端。

## 🎯 交付范围

### PostgreSQL schema

新增 [schema.sql](../xgraph/storage/schema.sql)，包含：

```text
crawl_tasks
task_operation_budgets
root_trees
account_nodes
account_observations
follow_edges
follow_edge_observations
crawl_frontier
scraper_accounts
account_operation_quota
request_attempts
raw_page_outbox
```

物理账号和边按 Task 去重；Tree 路径与重复发现保存在独立 observation 表，避免去重吞掉跨 Tree 证据。

### Task 与 frontier

[postgres.py](../xgraph/storage/postgres.py)提供：

- 原子创建 Task、Root Tree、L0 node 和 L0 Following frontier
- `FOR UPDATE SKIP LOCKED` 多 Worker claim
- `owner_id + lease_expires_at` Worker 租约
- 过期 `running` frontier 自动重新领取
- 当前 owner 才能 checkpoint/finish 的 fencing
- Task 总请求预算与 operation 请求预算的事务性预留
- 每次预算预留同时创建 `request_attempts` 审计记录
- 节点/边容量原子保护

### Scraper Account Manager

[manager.py](../xgraph/accounts/manager.py)提供：

- 注册 primary / standby 账号及其 operation quota
- `188/15 min` 作为可覆盖的初始 `limit_max/remaining` 基线
- 按剩余配额优先选择账号
- `(account, operation)` 原子 lease
- active 与 standby 分层使用，standby 默认不参与
- 根据响应头回写 `remaining`、`limit_max` 和 `reset_at`
- cooling 到 reset 后自动恢复配额
- 过期 operation lease 自动回收
- 账号失效/认证错误进入 dead
- rate-limit、平台过载和普通失败分别处理

每个 `(account, operation)` 当前只允许一个有效租约，因此过期租约重新领取时 `in_flight` 归一为 1，不累加旧 Worker 遗留值。

## 🔧 上游复用

阶段 2 参考 `twscrape/accounts_pool.py`、`account.py` 和 `queue_client.py` 中已经验证过的原则：

- operation 维度锁定账号
- 时间戳租约代替常驻心跳
- 限流读取 `remaining/reset_at`
- 认证失败与正常 cooling 分开
- 没有可用账号时显式等待/重试，而不是随机超发

XGraph 没有复制上游 SQLite 存储和公开 Pool API，而是将这些原则实现为 PostgreSQL 行锁、共享状态和 XGraph 自有类型。`twscrape/` 保持零修改，`xgraph/` 仍无上游运行时 import。

## 🧪 测试证据

### 单元测试

[test_phase2.py](../tests/xgraph/test_phase2.py)使用 fake async connection 验证：

- 幂等 Task/frontier 插入
- claim SQL 使用 `FOR UPDATE SKIP LOCKED`
- claim 返回强类型 `FrontierItem`
- 无 ready work 返回 `None`
- 丢失 frontier/account lease 时拒绝提交
- cursor checkpoint 需要当前有效 owner
- Seed Task 初始化位于同一事务
- Task/operation 请求预算同时加锁
- request attempt 只能完成一次
- 节点/边容量原子保护
- 账号按剩余配额选择
- 账号注册时初始化三个 operation 和 `188` 默认配额
- rate-limit report 必须提供 reset 时间

### 真实 PostgreSQL 17 集成测试

[test_postgres_integration.py](../tests/xgraph/test_postgres_integration.py)在临时 PostgreSQL 17 实例中验证：

- schema 可完整执行
- 两个 Worker 并发 claim 得到不同 frontier
- frontier lease 过期后由第三个 Worker 接管
- 三个并发请求争抢总额度 2，结果严格为 `1、2、exhausted`
- Task 总计数、Following operation 计数和 attempt 行均为 2
- 节点/边容量不会超过 Task 上限
- 两个 Worker 并发租用不同 Scraper Account
- `remaining=0` 后进入 cooling，reset 到期后恢复
- 过期租约恢复后 `in_flight=1`
- standby 在 primary 可用/占用期间默认不加入，显式开启后才被选择

第一次真实 PG 执行发现 `release()` 的时间参数无法由 asyncpg 自动推断；SQL 已增加显式 `integer/timestamptz/boolean` cast，并在重跑中通过。这类问题无法由 fake connection 发现，已保留真实 PostgreSQL 集成门。

## 📊 指标与门槛

| 指标 | 结果 |
| --- | --- |
| 阶段 2 单元测试 | 16 passed |
| PostgreSQL 集成测试 | 1 passed |
| schema 表数量 | 12 |
| 并发 frontier 重复 claim | 0 |
| 并发 Account operation 重复 lease | 0 |
| 请求预算越界 | 0 |
| 过期 frontier 恢复 | 通过 |
| cooling/reset 恢复 | 通过 |
| standby 默认误用 | 0 |
| Cookie 写入业务数据库 | 0；仅保存 `credential_ref` |

### 阶段 2 退出门槛

```text
至少两个 Scheduler 并行 claim 无重复       PASS
Worker/租约失效后 frontier 可恢复           PASS
账号限流、封禁和认证失效可区分              PASS
Task 与 operation 请求预算零越界            PASS
真实 PostgreSQL schema 和 SQL 可执行         PASS
```

## ⚠️ 未完成项与边界

- 当前没有长期运行的 PostgreSQL 服务配置和迁移版本管理；只交付首版 schema
- `credential_ref` 的 Secret Store 解析尚未接入
- Account Manager 尚未与 ProtocolCollector 串成实际 Scheduler loop
- cursor 跨 Scraper Account 能力尚未 live 验证
- 节点/边容量预留是保守硬保护；阶段 4 Parser 必须按实际新插入数量提交，避免重复数据造成计数浪费
- Task 暂停/继续/终止的用户 API 属于阶段 6
- Kafka Outbox Publisher、Consumer Group 和 DLQ 属于阶段 3

## 🚀 下一阶段

阶段 3 将实现：

```text
raw_page_outbox 可靠发布
x.pages.raw / x.pages.dlq
Outbox Publisher
Parser Consumer Group
processed_events
数据库 consumer offset
assignment_epoch fencing
reparse / archiver / dlq-observer 消费组合同
```

阶段 3 不改变 PostgreSQL 是 Task/frontier/去重事实源的原则；Kafka 只承载已采集的原始响应事件。
