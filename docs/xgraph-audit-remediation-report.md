# XGraph 只读审计与修复报告

_对阶段 1、阶段 2 交付物的独立审计及后续修复；2026-09-04_

---

## 📋 结论

审计覆盖 `xgraph/` 全部实现、`xgraph/storage/schema.sql`、CI 配置和三份交付文档。共确认 **5 个已复现缺陷**和 **3 项合同缺口**，全部已修复并附回归测试。

审计不推翻既有架构。技术设计中"XGraph 运行时不 import `twscrape`、自行维护协议兼容性"是显式记录的决定，本次修复在该决定内工作，只补上它缺少的维护机制。

所有结论均在真实 PostgreSQL 16 与真实响应样本上取得，未使用推断。

## 🔍 审计方法

| 手段 | 用途 |
| --- | --- |
| 一次性 PostgreSQL 16 容器 | 执行 `schema.sql`，验证约束与租约状态机 |
| TLS 指纹回显服务 | 对比两个 HTTP 后端的 JA3/JA4 与 ALPN 结果 |
| `tests/mocked-data/raw_*.json` | 在真实响应上验证解析口径 |
| 逐条回退验证 | 每个修复单独回退，确认对应测试变红 |

发现的根因不是单个缺陷，而是**验证缺口**：PostgreSQL 集成测试挂在 `XGRAPH_TEST_DATABASE_URL` 上，未配置即 skip，CI 从未配置。整个持久化层因此从未被执行过。下述缺陷 1 与 2 都属于"一跑就会暴露"的类型。

## 🐛 已确认并修复的缺陷

### 1. 账号池会静默耗尽（严重）

`release()` 的 `state` 分支在 `success=False` 时落到 `ELSE state`，把 `state` 留在 `'leased'`，同时把 `lease_expires_at` 清为 NULL。而租约查询回收过期租约的分支要求 `lease_expires_at IS NOT NULL`，三个 OR 分支一个都不匹配。

实测复现（修复前）：

```text
release(success=False)                  -> state=leased,  lease_expires_at=NULL  -> 永久不可租用
release(success=True, remaining=0)      -> state=ready,   remaining=0            -> 永久不可租用
```

后果不是报错，而是账号池随运行时间缓慢缩小。表现为"越来越慢"，会被误判为账号数量不足。

**修复**：把可用性判据重写为两个互相独立的全集谓词（"未被持有"与"当前有配额"），使每个状态组合都明确落在一侧；`release()` 的 `state` 无条件解析，不再保留 `ELSE state`；配额耗尽而响应未带 reset 头时按 X 的 15 分钟窗口冷却。数据库层追加三条 CHECK，把三种不可租用状态变为不可表示，使同类缺陷无法再静默复发。

### 2. 关系表拒绝指向 Seed 的边（严重）

`follow_edges.target_depth CHECK (BETWEEN 1 AND 6)` 排除了深度 0。L1 及以下账号回头关注 Seed 时，边的目标节点深度正是 0：

```text
INSERT INTO follow_edges(...,'l1-user','seed-a',1,0)
ERROR: violates check constraint "follow_edges_target_depth_check"
```

被拒绝的恰好是闭环边——PRD 中"被多个 Seed 或多条路径发现的次数"和"网络内入度"这两个候选筛选字段的核心构成。

**修复**：`follow_edges` 与 `follow_edge_observations` 的 `target_depth` 改为 `BETWEEN 0 AND 6`。

### 3. 纯转发进入帖子统计（严重）

`parse_tweets` 遍历整个响应体收集 `__typename == "Tweet"` 的对象，而非限定顶层 timeline entry；且转发判定所依赖的 `_tweet_references` 无条件对 `*_status_result` 多解一层 `tweet` 字段，对普通 `Tweet` 恒得空对象，转发因此完全逃过识别。

在真实样本上（修复前）：

```text
顶层 timeline entry     : 21
parse_tweets 返回        : 34        <- 多出 13 条属于其他账号
纯转发包装对象被保留     : 16/16     <- 每条互动计数均为 0
平均点赞：全部记录 350   vs  合格帖 1825      相差 5.2 倍
```

`TweetRecord` 当时没有任何字段能区分转发，下游无法补救。

**修复**：`parse_tweets` 只遍历 `TimelineAddEntries` / `TimelinePinEntry` 下的顶层 entry，过滤推广位与非帖子模块；新增 `TweetKind`（`RETWEET > REPLY > QUOTE > ORIGINAL` 固定优先级）与 `retweeted_tweet_id` / `quoted_tweet_id` / `in_reply_to_user_id`；`_unwrap_tweet` 只在 `TweetWithVisibilityResults` 时多解一层。浏览量补 `ext_views.count` 回退，缺失仍保留为空。

修复后同一样本：21 条顶层记录，16 转发 / 4 原创 / 1 reply，合格样本 4 条，全部为非零互动。

### 4. `event_id` 依赖响应内容（严重）

`event_id` 由 `sha256(operation, account, cursor, 完整 payload)` 计算。同一页重取时互动计数已变化，会得到不同的 `event_id`。技术设计要求"`x.pages.raw` 使用稳定 `event_id`"，且阶段 2 列出 `UNIQUE(task_id, account_id, operation, cursor_in)`；两者都被违反。

后果在阶段 3 才会显现且难以定位：崩溃重试后同一页被入库两次、被 Parser 计数两次，`produced_count` 与 `processed_count` 无法配平，三条件完成判定永远不成立。

**修复**：`page_event_id(operation, account_id, cursor_in)` 只由工作身份派生；`raw_page_outbox` 主键改为 `(task_id, event_id)`，即技术设计的第四条唯一约束。副作用是每页移除了一次 140 KB 的 JSON 序列化。

## 🔧 已修复的合同缺口

### 5. 协议常量副本没有维护机制

`scripts/update-gql-ops.py` 硬编码 `API_FILE = "twscrape/api.py"`，`make update` 不会更新 `xgraph/collector/operations.py` 中的 operation id 与 39 个 feature flag 副本。两份当前完全一致，但没有任何机制维持一致。X 每数周轮换一次 operation id，届时副本静默过期，直到真实采集全线失败才暴露。

这是"不 import 上游"这一决定的必然代价，技术设计承认了后果但没有给出机制。

**修复**：新增 `tests/xgraph/test_protocol_drift.py`，比对两份副本并在不一致时指出具体差异。模拟一次轮换验证：

```text
AssertionError: operation ids drifted from the upstream snapshot:
  {'Following': ('ROTATEDxxxxxxxxxxxxxxx/Following', 'qGZZDF3mp91q7X22s3HxpA/Following')}
```

`docs/upstream/BASELINE.md` 记录了 `make update` 之后的同步步骤，并要求任何新增的协议常量副本同时纳入该守卫。

### 6. 边缘拦截被降级为格式错误

Cloudflare 或边缘返回 HTML 拦截页时，`response.json()` 抛出解码错误，被归类为 `InvalidResponseError`（"响应格式错误"）。两者含义相反：前者说明请求根本没到达 API、账号与出口正在被质询，后者说明 payload 有问题。

**修复**：新增 `BlockedError`，在解析响应体之前按"错误状态码 + `text/html`"识别，并用 `cf-ray` 头区分 Cloudflare 与其他边缘。判据取自上游 `queue_client.py`。

### 7. frontier 没有重试上限

`claim_frontier` 每次领取都递增 `attempt`，包括租约过期后的重新领取，而没有任何上限，也没有把行推到 `failed` 的路径。一个反复失败的节点会永远被重试；阶段 4 的层级 barrier 与完成判定因此永远无法闭合。

**修复**：`crawl_frontier` 新增 `max_attempts`（默认 5）；claim 不再提供超预算的行；`finish_frontier` 在预算用尽时把 `retryable` 提升为 `failed`；新增 `fail_exhausted_frontier()` 清扫因持有者崩溃而卡住的行，使其状态诚实可见。

## 🔐 传输指纹（缺陷 8）

`xgraph/collector/http.py` 原为 48 行裸 `httpx`，而上游对应文件 292 行，提供 `curl-cffi` 浏览器 TLS 伪装。同时 User-Agent 被硬编码为 `Mozilla/5.0 (X11; Linux x86_64) ... Chrome/131.0`。

实测两个后端对同一指纹回显服务的差异：

| | `httpx` | `curl-cffi` |
| --- | --- | --- |
| JA3 | `37f7d09ced1a845dc48872abc1a29d7b` | `46814d365bbcda471e18a8038cc9bd1d` |
| JA4 | `t13d1712h1_...` | `t13d1516h2_...` |
| 协商到的协议 | **HTTP/1.1** | **HTTP/2** |

差异不止密码套件：**httpx 协商到 HTTP/1.1，而 x.com 的 Web 客户端使用 HTTP/2**。用 HTTP/1.1 请求并声称自己是 Chrome，比密码套件顺序不同更直白。同一实测还显示 httpx 后端取到的是一个 Android Nexus 5 的 UA，而握手来自桌面 Linux 的 OpenSSL——第二处自相矛盾。

**修复**：按上游 `twscrape/http.py` 近乎逐行移植双后端传输层，包含 `Response` 统一封装、`HttpError` / `NetworkError` / `ConnectError` / `HttpStatusError` taxonomy、浏览器族解析与 curl 的有限重试。三处适配：

| 适配 | 理由 |
| --- | --- |
| 环境变量改为 `XGRAPH_HTTP_BACKEND` | `TWS_*` 属于上游命名空间 |
| 日志走 loguru | XGraph 没有自己的 logger 模块 |
| **默认后端改为 curl-cffi**（上游默认 httpx） | 上游是通用库，curl 是可选依赖；XGraph 是长期对 X 采集的应用，静默使用可区分的传输会架空它服务的账号池。未安装时回退到 httpx 并发出告警，显式的 `XGRAPH_HTTP_BACKEND` 始终优先 |

配套改动：

- 字面 User-Agent 换成浏览器族提示（`@chrome` / `@safari` / `@firefox` / `@edge`），由传输层解析为真实 UA，并在 curl 后端上对应到匹配的 TLS 指纹。`ScraperCredential.user_agent` 默认即为 `@chrome`
- UA 的 seed 由 Scraper Account alias 派生（对应上游从 username 派生），**同一账号跨重启呈现同一浏览器**；UA 每次重启都变化本身即是信号
- `client.py` 不再自建 `httpx.AsyncClient`，改走同一传输边界；`xclid.py` 的签名会话使用同一 seed
- 显式设置 30s 超时：两个后端默认值不同（httpx 5s、curl 30s），5s 上限在慢出口上会产生虚假失败
- 测试注入的客户端改为实现 `HttpClient` 的适配器，使 mock 路径与生产路径一致

## 🧪 验证缺口的修复

上述缺陷 1、2、7 都属于"接一次真实数据库就会暴露"的类型。根因是 PostgreSQL 契约从未被执行。

**修复**：

- `Makefile` 新增 `test-pg`
- `ci.yml` 与 `pr.yml` 新增独立 `postgres` job，使用 PostgreSQL 16 service 容器（`UNIQUE NULLS NOT DISTINCT` 需要 15+）
- `tests/xgraph/test_postgres_integration.py` 新增 14 项契约测试

每项修复都做过回退验证——移除修复后对应测试变红，恢复后变绿。

| 契约 | 测试 |
| --- | --- |
| 任何 release 路径后账号可恢复 | `test_released_lease_is_always_recoverable`（4 组参数） |
| 崩溃遗留的租约回到池中 | `test_abandoned_lease_returns_to_the_pool` |
| 连续失败进入显式终态 | `test_repeated_failures_reach_an_explicit_terminal_state` |
| 不可租用状态在库层被拒绝 | `test_schema_rejects_unleasable_quota_rows`（3 组参数） |
| 指向 Seed 的闭环边可存储 | `test_edge_pointing_back_at_a_seed_is_storable` |
| L0 观察记录允许空 parent | `test_seed_observation_without_a_parent_is_storable` |
| 重试预算达到终态 | `test_frontier_attempt_budget_reaches_a_terminal_state` |
| 卡住的行可被清扫 | `test_exhausted_frontier_rows_are_swept_to_a_terminal_state` |
| 重取同一页不重复入库 | `test_outbox_is_keyed_by_stable_page_identity` |

## 📊 修复后状态

```text
make check                                  PASS
全量测试                                     PASS
PostgreSQL 契约测试（真实 PG 16）             PASS
默认传输后端                                 curl-cffi（h2 + 浏览器 TLS 指纹）
git diff -- twscrape/ scripts/               为空
xgraph/ 运行时 import twscrape                0
```

## 📝 文档变更

- `xgraph-technical-design.md` 新增「📐 测量基线与证据」一节，登记分页条数、请求账本、容量模型、队列增长率、并发形状、字段路径、解析陷阱、指标口径和分布观测，每条标注**实测 / 单一来源 / 推导**三级来源，并列出三项待验证项
- 同文档更新：协议漂移的维护代价与守卫要求、`event_id` 语义、阶段 1/2 的实现范围与测试门槛、账号可用性不变量、`target_depth` 允许 0、禁止事项
- 同文档新增「传输指纹」小节，登记两个后端的 JA3/JA4 与 ALPN 实测差异
- `upstream/BASELINE.md` 更新 `http.py` 与 `account.py` 的复用条目，并新增 `make update` 之后的协议同步步骤

## ⚠️ 未处理项与建议

以下问题已确认存在，但修复超出本次范围，需要单独决策：

| 项 | 说明 |
| --- | --- |
| **frontier 索引** | `crawl_frontier_ready_idx` 不含 `task_id`，且 claim 的 status 条件为 OR，将走 BitmapOr 后额外排序。千万行时显性化 |
| **数据库迁移** | `schema.sql` 全部使用 `CREATE TABLE IF NOT EXISTS`，已存在的库不会获得新增约束与列。首次生产部署前需要迁移版本管理 |
| **空壳模块** | `messaging/`、`scheduler/`、`service/` 仅有 docstring；`follow_edges`、`follow_edge_observations`、`account_observations`、`raw_page_outbox`、`request_attempts` 尚无写入方。属阶段 3/4 范围，非缺陷 |

## 🔗 相关文档

- [XGraph 技术设计与分阶段实施方案](xgraph-technical-design.md)
- [XGraph 产品需求文档](xgraph-product-prd.md)
- [上游基线](upstream/BASELINE.md)
