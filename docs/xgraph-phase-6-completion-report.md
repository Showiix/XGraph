# XGraph 阶段 6 后端完成报告

_产品接口：任务控制、查询、受限子图与带溯源的导出；2026-09-04_

---

## 📋 结论

阶段 6 的**后端已完成**，在真实 PostgreSQL 16 上验证。此前所有数据都在库里但没有出口，取结果只能手写 SQL；现在有了完整的 HTTP 接口。

浏览器前端按约定本轮不做。端到端真实小任务尚未跑过。

## 🎯 交付范围

| 文件 | 交付 |
| --- | --- |
| `xgraph/service/queries.py` | 账号列表、账号详情、关系列表、受限子图 |
| `xgraph/service/tasks.py` | 种子解析、状态机、进度与阻塞原因 |
| `xgraph/service/export.py` | CSV / JSON 导出与 manifest |
| `xgraph/api/app.py` | FastAPI 应用 |
| `xgraph/api/__main__.py` | `python -m xgraph.api` |

## 🔒 三条贯穿全部接口的规则

### 采集机制不属于产品接口

Scraper 别名、`credential_ref`、代理、配额状态与租约持有者**没有任何路由**。产品接口泄露它们，等于把一个运维细节变成对外可见的东西。

测试往库里放了带真实形状的假凭证（`secret://vault/token-abc`、`proxy://user:pw@host`），然后对七个接口的响应正文逐个断言这些字符串不出现。

### 每一行都带着判断它所需的证据

一个不带覆盖率和终止原因的账号行，无论平台返回了全部关系还是十分之一，看起来都一样可信。因此每行携带 `coverage_ratio` 与 `warnings`：

```text
following_truncated        声明 900，实收 800
boundary_not_expanded      L6 边界节点
expansion_failed           展开失败
expansion_filtered         未达展开阈值
multiple_discovery_paths   被多条路径发现
no_view_counts_in_sample   样本中无浏览量
```

### 受限的答案不能看起来像全图

子图响应同时返回 `matched_nodes` 与 `bounded`，并在截断时给出 `warnings`。返回的边两端都在返回的节点集合内——否则前端会画出指向不存在节点的悬空边。

## 📤 导出携带自己的出处

一份只有数据的 CSV，一周后没人记得它出自哪次任务、哪些筛选条件、当时爬完没有。因此每次导出附带 manifest：

```text
task_id · dataset · 实际生效的筛选条件 · 生成时间 · 行数
task_status · expansion_complete · parser_backlog · coverage
```

CSV 正文没有位置放这些，manifest 走 `x-xgraph-manifest` 响应头。

导出按页遍历查询层——内存占用与页大小成正比，而不是与任务规模成正比。测试插入 120 个额外账号，确认导出没有停在第一页。

**ID 全程是字符串。** 电子表格把 snowflake id 当数字读会静默抹掉末几位。

## 🎛️ 控制语义

状态机显式声明合法转移；未定义的转移返回 409 而非静默忽略——**卡住的任务表现为一次被拒绝的操作，而不是一个没反应的按钮**。

暂停时释放无人认领的 `running` frontier 行，恢复时不必等租约过期。**在途请求不动**：那一页已经付过配额，会照常落库。

进度接口返回层级状态与 `blocked_by`、frontier 分布、错误分类、限流等待、`parser_backlog`、每层指标与覆盖率摘要。

## 🧪 测试证据

`tests/xgraph/test_phase6_integration.py`，22 条契约，对照技术设计的阶段 6 测试表：

| 检查 | 测试 |
| --- | --- |
| 控制语义 | `test_pause_releases_leases_and_refuses_undefined_transitions` |
| 查询正确性 | `test_accounts_rank_by_in_network_endorsement`、`test_relationships_expose_tree_depth_and_collision` |
| 数据质量标注 | `test_every_account_carries_its_data_quality_warnings` |
| 导出完整性 | `test_an_export_states_the_task_filters_and_quality_it_came_from`、`test_export_pages_past_the_query_page_size` |
| 图谱边界 | `test_the_subgraph_is_bounded_and_says_so` |
| 分页而非超时 | `test_an_oversized_page_is_refused_rather_than_served` |
| 安全 | `test_no_route_exposes_the_collection_machinery` |
| ID 类型 | `test_ids_stay_strings_all_the_way_out` |
| 排序注入 | `test_an_unsupported_ordering_is_rejected` |

```text
make check                                     PASS
全量测试                                        263 passed
真实基础设施门（PG 16 + Kafka 3.9）              76 passed
git diff -- twscrape/ scripts/                  为空
```

`make test-api` 已接入 CI 的 `postgres` job；`make serve` 启动接口。

## ⚠️ 未完成项与边界

- **浏览器前端未做**，本轮按约定跳过
- 端到端真实小任务（导入 → L6 → Timeline → 查询 → 导出）尚未跑过
- 接口无认证与授权：目前假定本机或受信网络
- P95 < 500 ms 的性能目标未在有规模的数据上测过
- 共同关注查询（PRD 提到的多账号交集）尚未实现
- 触达结果标记（PRD 未要求，但会让工具从一次性名单变成可积累的系统）未实现

## 🚀 下一步

两条路各有理由：

**浏览器前端** —— 补完阶段 6，让非工程角色也能用。
**真实 live smoke** —— 三个未知（关注列表召回率、cursor 能否跨账号、`188/15min` 对应端点）仍未验证，它们决定已完成工作的**意义**而非正确性。

## 🔗 相关文档

- [XGraph 技术设计与分阶段实施方案](xgraph-technical-design.md)
- [XGraph 产品需求文档](xgraph-product-prd.md)
- [阶段 5 完成报告](xgraph-phase-5-completion-report.md)
