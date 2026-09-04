# XGraph 阶段 1 开发完成报告

_独立 X Web GraphQL 协议采集内核；阶段 1；2026-09-04_

---

## 📋 结论

阶段 1 已完成本地、fixture 和 Mock HTTP 验证，交付了 XGraph 自己的协议采集边界。XGraph 现在可以直接构造 X Web GraphQL 请求，处理 `UserByScreenName`、`Following` 和 `UserTweets`，并将响应转换为自有 `PageEnvelope`、`UserProfile` 和 `TweetRecord`。

本阶段没有接入 PostgreSQL、Kafka、L0-L6 BFS、Timeline 候选调度或浏览器。真实 Scraper Account 的 live smoke 需要在安全配置凭证后单独执行，因此不把本报告表述为生产可用证明。

## 🎯 交付范围

### XGraph 自有数据合同

| 文件 | 交付 |
| --- | --- |
| `xgraph/domain/models.py` | `UserProfile`、`TweetRecord` |
| `xgraph/domain/events.py` | `Operation`、`FrontierStatus`、`RateLimitSnapshot`、`PageEnvelope` |
| `xgraph/accounts/models.py` | `ScraperCredential`、账号/operation 状态和租约类型 |

所有 X user ID、post ID 都按字符串保存。`PageEnvelope` 记录 operation、账号、cursor、用户/帖子、响应状态、请求时间、原始 payload 和 rate-limit 快照，但不包含 Cookie 凭证字段。

### 独立协议采集内核

| 文件 | 交付 |
| --- | --- |
| `xgraph/collector/client.py` | X Web GraphQL 请求、认证头、CSRF、transaction-id、页面信封 |
| `xgraph/collector/operations.py` | `UserByScreenName`、`Following`、`UserTweets` operation 和 features |
| `xgraph/collector/parser.py` | User/Tweet 新旧结构归一化、`can_dm`、cursor、顶层 Tweet 解析 |
| `xgraph/collector/errors.py` | rate-limit、账号失效、认证、LoadShed、feature 过期和 HTTP 错误分类 |
| `xgraph/collector/xclid.py` | 复制并适配的 `x-client-transaction-id` 算法 |
| `xgraph/collector/http.py` | 独立 `httpx` transport 边界 |

XGraph 运行时不 import `twscrape`。`twscrape/` 保持上游快照原样，只作为协议和解析实现参考。

## 🔧 复用与隔离

本阶段参考上游基线 `55ac729f39fbbe46e316746a55627a9ed920112c`，复用范围和改造方式记录在[上游复用台账](upstream/BASELINE.md)。核心原则是：

```text
复用成熟算法和协议事实
复制到 xgraph/
收敛为 XGraph 自有合同
不依赖 twscrape 运行时对象、SQLite、CLI 或 telemetry
```

具体复用包括：

- `account.py` 的 `auth_token`/`ct0` 会话要求和请求头约定
- `api.py` 的 GraphQL operation、variables 和 features
- `queue_client.py` 的 rate-limit 与错误码判定顺序
- `utils.py`/`models.py` 的 User/Tweet 嵌套结构归一化思路
- `xclid.py` 的动态 transaction-id 算法
- 上游 `raw_following.json`、`raw_user_by_login.json`、`raw_user_tweets.json` fixture

## 🧪 测试证据

新增测试文件：[tests/xgraph/test_collector.py](../tests/xgraph/test_collector.py)

覆盖范围：

- Following fixture 解析为 60 个 User
- UserByScreenName fixture 解析
- UserTweets fixture 解析和指标字段
- XGraph 解析结果与上游核心 User 字段差分
- GraphQL URL、variables、features 和 operation path
- Bearer、Cookie、`ct0`、`x-csrf-token` 和 transaction-id
- 404 后刷新 transaction-id
- `188` rate-limit headers 进入 PageEnvelope
- `(88)`、`(326)`、`(32)`、`(336)`、`LoadShed` 和 HTTP 错误分类
- 凭证缺失校验
- Cookie secret 不出现在 PageEnvelope repr

验证命令和结果：

```text
make check       PASS
make test        PASS
214 tests passed
git diff --check PASS
上游目录 diff   为空
xgraph import twscrape 检查通过
```

## 📊 指标与门槛

| 指标 | 结果 |
| --- | --- |
| 全量测试 | 214 passed |
| XGraph collector 定向测试 | 16 passed |
| Fixture Following 页用户数 | 60 |
| `can_dm` 字段 | 已解析并测试 |
| cursor | 已解析并测试 |
| rate-limit headers | 已解析并测试 |
| 上游运行时 import | 0 |
| `twscrape/` 本阶段修改 | 0 |
| 真实 live smoke | 待安全配置凭证 |

## ⚠️ 未完成项与边界

- 尚未使用真实 Cookie 向 X Web 发起 live smoke
- 尚未验证 cursor 是否可以跨 Scraper Account 复用
- 尚未实现 Account Manager 的持久账号池
- 尚未实现 PostgreSQL frontier、任务预算和 Worker lease
- 尚未实现 Kafka Outbox、Parser Consumer Group 和 Rebalance fencing
- 尚未实现 L0-L6 BFS、Timeline、API 和 Web
- 非官方 X Web 接口的可观测覆盖率仍需在后续真实任务中记录

因此本阶段的结论是：

> XGraph 的独立协议采集内核在本地和固定响应层面成立，已具备进入阶段 2 的代码基础；它尚未证明真实账号环境下的长期可用性。

## 🚀 下一阶段

阶段 2 的范围是：

```text
XGraph 自有 Scraper Account pool
account × operation 配额状态
188/15 min 默认容量基线
PostgreSQL schema
frontier 原子 claim
Worker lease 和恢复
Task / operation / 节点 / 边预算
```

阶段 2 仍然不接 Kafka；先让 PostgreSQL 驱动的持久任务和账号池独立可测，阶段 3 再加入原始响应 Outbox 与 Kafka 事件流。
