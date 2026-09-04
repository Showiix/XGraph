# XGraph changelog

_记录 XGraph 在 twscrape 上的产品改造，不复制上游发布历史_

---

## Unreleased

- 建立独立的 `Showiix/XGraph` 仓库，并保留 `twscrape/` 上游源码快照作为参考
- 保留 `twscrape/` 作为直接二次开发的采集内核
- 新增 `xgraph/`、`web/` 和 `docs/` 产品边界
- `Following` / `UserTweets` 分页链改为在连续 3 个空页后结束。X 几乎从不用空 cursor
  表示列表结束（实测 2,599/2,600 页都带 cursor），原先等待空 cursor 使每条链都跑满页数
  上限——实测 129/130 个账号花满 20 次请求，包括一个关注 0 人的账号
- `declared_following` 回退到已存的 profile。X 的 Following 页不描述被展开的账号本身，
  原实现因此从未写入该字段，覆盖率无法计算
- 覆盖率按终止方分开统计：`truncated`（平台扣留）与 `scan_capped`（我方截断），
  `mean_coverage_ratio` 只对平台结束的链计算。合并统计会把"我们没继续付费"报成
  "平台扣留了 22% 的关系"
- `TerminationReason` 移入 `xgraph.domain`（storage 与 service 都需判定它）
