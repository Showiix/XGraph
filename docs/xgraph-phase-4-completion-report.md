# XGraph 阶段 4 开发完成报告

_L0-L6 关系采集：分层 BFS、层级 barrier、去重与 collision、覆盖率证据；2026-09-04_

---

## 📋 结论

阶段 4 已完成，并在**真实 PostgreSQL 16** 上以合成平台跑通完整 L0-L6 遍历。阶段 2 的 frontier 与阶段 3 的事件流现在组合成一次真正的采集任务：Scheduler 花配额取页，Parser 把页变成图，两者之间隔着 outbox。

尚未使用真实 Scraper Account 跑一份真实 Seed List，因此本报告不表述为生产可用证明。

## 🎯 交付范围

| 文件 | 交付 |
| --- | --- |
| `xgraph/graph/policy.py` | `TraversalPolicy`：深度边界与展开过滤 |
| `xgraph/graph/writer.py` | `GraphPageHandler`：一页原始响应 → 节点 / profile / 边 / 观察 / 下一层 |
| `xgraph/storage/graph.py` | 集合式图写入，全部在事件事务内 |
| `xgraph/storage/layers.py` | 层级 barrier、完成判定、每层指标、覆盖率 |
| `xgraph/scheduler/expansion.py` | `ExpansionScheduler`：claim → 租约 → 请求 → 落库，含终止原因 |
| `xgraph/storage/schema.sql` | `account_profiles`；节点增加覆盖率与展开状态；frontier 增加 `tree_id` |

## 🧭 设计要点

### 层级 barrier 是四个条件，不是一个

`claim_frontier` 只领取 `depth = current_depth` 的行，因此下一层不可能在当前层关闭前开始。而"关闭"必须同时满足：

```text
该层 frontier 无 pending / running / retryable
该层无 in-flight 请求
该层产生的页面全部已发布
该层产生的页面全部已解析
```

**只看 frontier 是不够的。** 管道中间有缓冲：frontier 为空只说明 Scheduler 没活干，不说明它已取到的页面是否已经变成下一层。`LayerState.blocked_by` 直接给出还差哪一条。

### tree_id 是被记录的事实

`tree_id` 随 frontier 行进入事件、再进入 observation。一个被多棵树发现的账号只有一行节点、一行 frontier，但有多行 observation——"从哪棵树、哪个上游、哪一层被发现"因此可查询而非事后推断。

实现过程中这条最初是断的：`FrontierItem` 上没有 `tree_id`，发现路径证据全部丢失。合成图测试直接暴露了它。

### 重复是信号，不是噪声

`xmax = 0` 区分真正的插入与冲突，一次语句同时得到节点的规范深度和是否为 collision。重复被记录为 observation，只结束当前分支——"这个账号被 N 个不同的地方触达"正是产品要找的圈层交叉信号。

指向 Seed 的闭环边（`target_depth = 0`）照常记录。

### 展开过滤不是候选判断

`min_followers_to_expand` 控制遍历成本。被过滤的账号照常写入节点、profile 和边，只是不再消耗一次请求，`expansion_status` 记为 `filtered`、`filter_reason` 可查。

**粉丝数缺失不当作零**：平台未暴露 profile 是证据缺口，不是"这个账号很小"，静默丢弃会让图偏向平台恰好愿意暴露的那部分。

### 覆盖率是存下来的，不是推出来的

`declared_following` / `collected_following` / `termination_reason` 逐账号存储。800 条边在账号声明 800 时与声明 8000 时含义不同，在链路因 cursor 停滞而结束时又不同。`coverage()` 给出截断账号数与平均覆盖率。

### 图写入按集合执行

一页 Following 约 60 个用户，而队列增长比消费快两个数量级。`unnest` 批量写入不是可以之后再优化的细节。

## 🧪 测试证据

`tests/xgraph/test_phase4_integration.py` 以合成平台（固定邻接表 + 分页）驱动完整管道，13 条契约对应技术设计的阶段 4 测试表：

| 检查 | 测试 |
| --- | --- |
| 合成图深度 / L6 不展开 | `test_a_chain_reaches_every_layer_and_stops_at_the_boundary` |
| 最短深度 | `test_a_node_reachable_by_two_paths_is_stored_once_at_its_shortest_depth` |
| 多 Seed | `test_two_seeds_share_nodes_but_keep_separate_trees` |
| collision | `test_a_repeat_discovery_is_recorded_as_a_collision_and_stops_only_its_branch` |
| 闭环边 | `test_an_edge_back_to_a_seed_is_recorded` |
| 层级 barrier | `test_the_next_layer_stays_shut_until_the_current_one_is_parsed`、`test_a_deeper_row_cannot_be_claimed_before_its_layer_opens` |
| 完成判定 | `test_completion_requires_the_parser_to_have_caught_up` |
| 分页恢复 | `test_an_interrupted_chain_resumes_from_its_checkpoint` |
| 过滤审计 | `test_small_accounts_are_recorded_but_never_expanded` |
| 观测覆盖率 | `test_coverage_and_termination_are_stored_rather_than_inferred` |
| 规模指标 | `test_layer_metrics_expose_growth_and_overlap` |
| 边去重 | `test_replaying_the_stream_does_not_duplicate_the_graph` |

```text
make check                                    PASS
全量测试                                       263 passed
真实基础设施门（PG 16 + Kafka 3.9）             41 passed
git diff -- twscrape/ scripts/                 为空
```

## ⚙️ CI 门槛

`make test-graph` 加入 `postgres` job。遍历的正确性由约束和集合式语句保证，fake 复现不了。

## ⚠️ 未完成项与边界

- 尚未使用真实 Scraper Account 跑真实 Seed List
- Scheduler 与 Parser 仍以 `run_once()` 供调用方驱动，没有常驻进程封装
- L1 检查点（PRD 要求 L1 完成后暂停并展示早期信号）属产品层，未实现
- Timeline enrichment、候选准入规则属阶段 5
- 逐 Seed 串行执行未实现；当前是按层封边界，改动需要额外的跨 Tree 邻接复用

## 🚀 下一阶段

阶段 5：候选准入、`TIMELINE` 独立队列与配额桶、帖子过滤与互动聚合、可独立暂停。

## 🔗 相关文档

- [XGraph 技术设计与分阶段实施方案](xgraph-technical-design.md)
- [XGraph 产品需求文档](xgraph-product-prd.md)
- [阶段 3 完成报告](xgraph-phase-3-completion-report.md)
