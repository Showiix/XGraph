# XGraph

_基于 twscrape 上游源码参考实现的独立 X 账号关系图谱采集与分析系统_

---

## 📋 当前阶段

项目已完成独立协议级采集内核的本地/fixture 验证，以及 PostgreSQL frontier、请求预算和 Account Manager 的真实 PostgreSQL 集成验证。XGraph 使用自有 Scraper Account 号池和 Cookie 会话直接调用 X Web GraphQL；`twscrape/` 只作为协议、签名、分页、限流与解析逻辑的参考快照，不参与 XGraph 运行时。下一步实现 Kafka 原始响应事件流和 Parser Consumer Group。

产品合同见 [XGraph 产品需求文档](docs/xgraph-product-prd.md)，技术实施见 [XGraph 技术设计与分阶段实施方案](docs/xgraph-technical-design.md)。

## 🏗️ 仓库结构

| 路径 | 职责 |
| --- | --- |
| `twscrape/` | 未修改的上游参考快照，不参与 XGraph 运行时 |
| `xgraph/` | XGraph 独立的采集、任务、存储、BFS、画像、评分、消息和 API |
| `web/` | React 与 Cytoscape.js 浏览器应用 |
| `tests/` | 上游兼容测试和 XGraph 产品测试 |
| `docs/` | 产品 PRD、技术设计和上游基线记录 |
| `scripts/` | GraphQL operation 与固定响应维护脚本 |

上游快照与 XGraph 运行时代码共存但隔离。需要复用上游逻辑时，将经过审查的代码复制/改造到 `xgraph/`，并在 `docs/upstream/BASELINE.md` 记录来源 commit、许可证和改造范围。

## 🔧 本地开发

需要 Python 3.10+ 和 [uv](https://docs.astral.sh/uv/)。

```bash
uv sync
make check
make test
```

当前仍保留上游 `twscrape` CLI：

```bash
uv run twscrape --help
```

## 🔄 上游同步

Git remotes 的约定：

```text
origin    git@github.com:Showiix/XGraph.git
upstream  git@github.com:vladkens/twscrape.git
```

同步上游后，必须先运行采集内核固定响应测试，再运行 XGraph 业务测试。基线详情见 [上游基线](docs/upstream/BASELINE.md)。

## ⚠️ 使用边界

XGraph 只面向公开可见数据。非官方 X Web 接口可能返回不完整关系，不能把“分页自然结束”表述为获得了平台上的全部 following。不得将 Cookie、CSRF token、代理凭据或采集账号信息写入日志、导出文件或 Git。
