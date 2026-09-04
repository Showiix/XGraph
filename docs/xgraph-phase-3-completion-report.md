# XGraph 阶段 3 开发完成报告

_原始事件管道：transactional outbox、Kafka 流、Parser 消费组；2026-09-04_

---

## 📋 结论

阶段 3 已完成，并在**真实 PostgreSQL 16 与真实 Apache Kafka 3.9** 上验证。系统现在把「不可逆的 X 请求」与「可重复的 Parser 处理」隔离开：页面字节一旦进入 outbox，此前消耗的配额即固化为可反复加工的资产。

本阶段不改变 PostgreSQL 是 Task / frontier / 去重事实源的原则。Kafka 只承载已采集的原始响应事件，不持有任务状态、不做去重、不承载长时分页链。

阶段 4 的 BFS 以 `PageHandler` 接入本阶段的事务边界；阶段 3 已保证该 handler 每个事件恰好被应用一次，失败时与 offset 一同回滚。

## 🎯 交付范围

### 消息层

| 文件 | 交付 |
| --- | --- |
| `xgraph/messaging/topics.py` | `x.pages.raw`、`x.pages.dlq`、消费组命名与 `reparse_group()` |
| `xgraph/messaging/events.py` | `RawPageEvent` 事件契约、`SCHEMA_VERSION`、序列化 |
| `xgraph/messaging/broker.py` | `Producer` / `Consumer` / `Message` / `AssignmentHandler` 契约 |
| `xgraph/messaging/kafka.py` | aiokafka 适配器；数据库 offset 驱动的 rebalance 恢复 |
| `xgraph/messaging/memory.py` | 进程内 Broker，用于测试与无 Kafka 的本地运行 |
| `xgraph/messaging/publisher.py` | `OutboxPublisher`：租约领取、重试退避、终态 dead-letter |
| `xgraph/messaging/consumer.py` | `ParserRuntime`：事务应用、epoch fencing、DLQ |

### 持久化层

| 文件 | 交付 |
| --- | --- |
| `xgraph/storage/events.py` | `PostgresEventStore`：outbox、`processed_events`、offset、DLQ、管道指标 |
| `xgraph/storage/schema.sql` | `processed_events`、`consumer_offsets`、`dlq_events`；outbox 扩展为完整信封；`pages_produced` / `pages_processed` |

## 🔒 一致性设计

### 三处不可原子化的边界，各自的处理

数据库与 Broker 没有共享事务。系统在三个位置正面处理这一点，而不是假装它不存在。

**① 采集 → 持久化。** `record_page()` 在一个事务内写 outbox 行、分页 checkpoint、request attempt 与 `pages_produced`。拆开会让游标越过一个从未存下的页面，而该页面无法免费重取。frontier 租约在同一事务内校验：租约已易主则整体回滚。

**② 持久化 → Broker。** Publisher 必然是 at-least-once：它可能在 `send` 与 `mark_published` 之间崩溃。反过来先标记再发送则会在发送失败时丢页。因此选择重复而非丢失，由消费侧吸收。

**③ Broker → 图。** `apply_event()` 在一个事务内推进 offset、登记 `processed_events`、执行业务 handler、累加 `pages_processed`。offset 不交给 Broker 保存——两个真相源必然分叉。

### Epoch fencing

`consumer_offsets` 行携带 `owner_id` 与 `assignment_epoch`。取得分区时 epoch 自增；旧持有者即使仍在处理批次，其写入也匹配不到任何行，抛出 `FencedConsumerError`。

### 终态事件仍须推进 offset

若只写 DLQ 而不推进 offset，分区会永远卡在一条不可能成功的消息后面。两类终态的处理不同：

| 情形 | 处理 |
| --- | --- |
| 可解析但永久失败（如 `(336)`） | 登记 `processed_events`、推进 offset、计入 `pages_processed`——它必须参与完成判定 |
| 无法解码 | 无 `task_id`，只推进 offset，不计入任务；仅在跨任务视图中可见 |

### 事件不携带解析结果

`RawPageEvent` 只带 payload 与元数据，不带 `users` / `tweets`。保留原始流的意义就是让新版 Parser 重新解释同一批字节；把当下的解析一起发出去等于冻结这个决定。

## 🧪 测试证据

| 文件 | 范围 |
| --- | --- |
| `tests/xgraph/test_phase3.py` | 事件契约、Publisher 重试策略、内存 Broker 语义 |
| `tests/xgraph/test_phase3_integration.py` | 真实 PostgreSQL + 真实 Kafka 的投递保证 |

对照技术设计的阶段 3 测试表：

| 检查 | 测试 |
| --- | --- |
| Producer 双写故障 | `test_a_committed_page_survives_a_publisher_that_never_returns` |
| 事务边界 | `test_recording_a_page_is_one_transaction_with_its_checkpoint`、`test_a_lost_frontier_lease_rolls_back_the_whole_page` |
| 重复发布 | `test_a_republished_page_is_applied_once`、`test_refetching_the_same_page_does_not_enqueue_it_twice` |
| Parser 崩溃 | `test_a_crash_after_the_handler_but_before_the_commit_replays_cleanly` |
| Rebalance fencing | `test_an_evicted_consumer_can_neither_advance_nor_write` |
| DLQ | `test_a_permanently_broken_event_goes_to_the_dlq_and_stops_blocking`、`test_an_undecodable_message_does_not_stall_the_partition` |
| Lag / backlog | `test_pipeline_stats_expose_the_backlog_that_gates_completion` |
| 独立重放 | `test_a_reparse_group_replays_history_without_touching_the_live_parser` |
| Broker 往返 | `test_pages_survive_a_round_trip_through_kafka` |

真实 Broker 抓到一个 fake 测不出的缺陷：aiokafka 要求 rebalance listener 继承 `ConsumerRebalanceListener`，内存 Broker 无从暴露该约束。这是把真实 Kafka 纳入门槛而非仅用 fake 的直接理由。

验证结果：

```text
make check                                   PASS
全量测试                                      263 passed, 28 skipped
PostgreSQL 契约（真实 PG 16）                  15 passed
阶段 3 管道（真实 PG 16 + Kafka 3.9）           13 passed
git diff -- twscrape/ scripts/                为空
```

## 🔭 可观测性

`PostgresEventStore.pipeline_stats()` 给出 `pages_produced`、`pages_processed`、`backlog`、outbox 待发布数与最老未发布时长、dead-letter 数、DLQ 数；`committed_offsets()` 给出各分区已提交位置，用于与 Broker 端 offset 比较得到真实 lag。

`backlog` 是完成判定的第三个条件：frontier 为空且无 in-flight 请求，仍不等于完成——页面可能尚未解析。

## ⚙️ CI 门槛

新增 `pipeline` job（`ci.yml` / `pr.yml`），同时起 PostgreSQL 16 与 Apache Kafka 3.9 service 容器，执行 `make test-pipeline`。

投递保证是 PostgreSQL 与 Broker 之间边界的性质，任一半做成 fake 都证明不了什么，因此两边都用真实服务。跳过即视为未验证。

## ⚠️ 未完成项与边界

- `PageHandler` 目前只有测试实现；写节点、边和下一层 frontier 属阶段 4
- Publisher 与 Parser 尚无常驻进程封装，当前以 `run_once()` 供调度方驱动
- 多 Broker 副本、Leader 切换与 Rebalance 演练属阶段 7；单节点 Kafka 只能作为功能验证
- Kafka 保留期、压缩与磁盘用量的生产配置属阶段 7 部署基线
- 归档消费组（`xgraph-archiver-v1`）已定义命名，冷存储写入未实现

## 🚀 下一阶段

阶段 4：分层 BFS、Task 内去重与 collision、层级 barrier、L6 边界与三条件完成判定。Parser 侧以 `PageHandler` 接入本阶段的事务边界。

## 🔗 相关文档

- [XGraph 技术设计与分阶段实施方案](xgraph-technical-design.md)
- [XGraph 产品需求文档](xgraph-product-prd.md)
- [阶段 2 完成报告](xgraph-phase-2-completion-report.md)
