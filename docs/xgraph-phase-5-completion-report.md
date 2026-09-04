# XGraph 阶段 5 开发完成报告

_Timeline enrichment：候选准入、独立配额桶、样本口径与聚合指标；2026-09-04_

---

## 📋 结论

阶段 5 已完成，在**真实 PostgreSQL 16** 上验证。候选账号的最近合格帖与互动指标现在可以采集，而且整条链路与关系图遍历完全隔离：独立队列、独立配额桶、可单独关停，任何时候都不会让图多等一秒。

尚未使用真实账号采过 Timeline 样本。

## 🎯 交付范围

| 文件 | 交付 |
| --- | --- |
| `xgraph/domain/policies.py` | `CandidatePolicy` / `TimelinePolicy` |
| `xgraph/storage/timeline.py` | 候选准入、帖子入库、指标重算、状态与开关 |
| `xgraph/timeline/writer.py` | `TimelinePageHandler` |
| `xgraph/messaging/routing.py` | 按 operation 分发到对应 handler |
| `xgraph/scheduler/enrichment.py` | `EnrichmentScheduler`：独立循环与终止原因 |
| `xgraph/storage/schema.sql` | `account_posts`、`account_metrics`；节点增加 enrichment 状态与命中条件；任务增加 `timeline_enabled` |

## 🔒 三处隔离

### 层级 barrier 只统计 Following

`claim_frontier` 与层级状态原本对所有 frontier 行一视同仁。若不改，一个在途的 Timeline 请求会把某一层撑开——**等于让图去等它并不需要的数据**。

现在 barrier 只看 `Following`；Timeline 行不受层级门约束，任何时候可领取。这是本阶段唯一一处需要回头改前面阶段的地方。

### 停止判断不读 Parser 的产出

最初的实现用「库里已存多少合格帖」决定要不要翻下一页。**这违反了阶段 3 的解耦前提**：调度器与 Parser 按设计异步，Parser 可能落后数分钟，于是每次都读到 0，链路会一直翻到自然结束。

现在调度器只数自己刚取回的页，库只在链路开始时读一次——用于接续被中断的链路。

集成测试直接暴露了这一点:预期 `scan_limit`，实际拿到 `natural_end`。

### 配额桶分开

Timeline 走 `(账号 × UserTweets)` 桶，与 `Following` 互不影响；Timeline 失败不消耗遍历的重试预算。

## 📐 样本口径

**纯 repost 与 reply 一并入库,标记为不合格。** 丢掉它们会让「扫了多少条」无法核对——4 篇合格出自 20 篇扫描，和 4 篇出自 4 篇，是完全不同的两个账号。

**缺失的浏览量不是零浏览量。** 平台在较老的帖子上不给 views。按零处理会把每个均值都往下拽。因此 `avg_view` 与 `view_sample_count` 成对存储：只有前者的话，两篇帖子的均值看起来和三十篇的一样。

**指标由样本重算，不累加。** 数字始终可追溯到背后的帖子，重复投递也不会让它膨胀。样本按发布时间取最新 N 篇，超采不会导致上报超过承诺的篇数。

**`max_scanned` 小于 `target_posts` 是合法配置。** 合格率约五分之一，一个几乎只转发的账号否则会被翻到远超其价值的深度。我最初把它写成了校验错误——那个校验本身是错的。

## 🎯 准入记录条件，不打分

PRD 明确禁止把启发式包装成「值得投放」的结论。因此准入结果存的是**命中了哪些条件**（`can_dm`、`follower_range`、`network_indegree`、`discovery_paths`、`bio_keyword`、`depth`），审阅者可以看见依据并提出异议。

准入与入队在同一条语句内完成，不存在「被标记为候选但没有工作项」的中间态。

## 🧪 测试证据

`tests/xgraph/test_phase5_integration.py`，13 条契约：

| 检查 | 测试 |
| --- | --- |
| 候选准入 | `test_only_admitted_accounts_get_a_timeline_request` |
| 准入证据 | `test_admission_records_conditions_not_a_score` |
| 独立开关 | `test_disabled_enrichment_admits_nobody` |
| 帖子过滤 | `test_reposts_and_replies_are_stored_but_never_counted` |
| 指标口径 | `test_a_missing_view_count_is_not_zero_views` |
| 样本上限 | `test_the_sample_stops_at_the_target_and_records_its_span` |
| 样本不足 | `test_a_short_sample_records_what_it_actually_found` |
| 扫描预算 | `test_scanning_stops_at_the_budget_when_qualifying_posts_are_rare` |
| 配额隔离 | `test_timeline_work_uses_its_own_rate_limit_bucket` |
| barrier 隔离 | `test_outstanding_timeline_work_does_not_hold_a_layer_open` |
| 不受层级门约束 | `test_timeline_rows_are_not_gated_by_the_layer_barrier` |
| 路由 | `test_a_following_page_is_never_handed_to_the_timeline_handler` |
| 重放幂等 | `test_replaying_a_timeline_page_does_not_change_the_metrics` |

```text
make check                                     PASS
全量测试                                        263 passed
真实基础设施门（PG 16 + Kafka 3.9）              54 passed
git diff -- twscrape/ scripts/                  为空
```

`make test-enrichment` 已接入 CI 的 `postgres` job。

## ⚠️ 未完成项与边界

- 尚未用真实账号采过 Timeline
- 已在途的 Timeline 请求在关闭开关后的收敛策略尚未实现（当前开关只阻止新的准入）
- `max_post_age_days` 已在策略中定义，尚未接入查询
- 候选准入需要人工触发；何时自动触发（例如 L1 检查点之后）属产品层
- API、查询、导出与图谱属阶段 6

## 🚀 下一阶段

阶段 6：任务控制、账号与关系查询、受限子图图谱、CSV / JSON 导出。

## 🔗 相关文档

- [XGraph 技术设计与分阶段实施方案](xgraph-technical-design.md)
- [XGraph 产品需求文档](xgraph-product-prd.md)
- [阶段 4 完成报告](xgraph-phase-4-completion-report.md)
