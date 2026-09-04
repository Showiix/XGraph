"""Read side of the product surface.

Every result carries the evidence needed to judge it. An account row without its
coverage ratio and termination reason looks equally trustworthy whether the
platform returned all of its relationships or a tenth of them, and the PRD is
explicit that the product may not present the second as if it were the first.

Nothing here reads `scraper_accounts` or any credential-bearing column. The
collection machinery is not part of the product surface.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol

from xgraph.domain import SELF_TERMINATIONS

#: Result-set ceiling. Requests above it are answered with a page and a total,
#: never with a truncated body that looks complete.
MAX_PAGE_SIZE = 500

#: A bounded subgraph is a query result, not a dump. Above this the caller is
#: told how many nodes matched and asked to narrow, rather than handed a hairball.
MAX_SUBGRAPH_NODES = 1_000
MAX_SUBGRAPH_EDGES = 5_000


class AsyncPool(Protocol):
    def acquire(self) -> Any: ...


@dataclass(frozen=True, slots=True)
class Page:
    """One page of results, always with the full total beside it."""

    items: list[dict[str, Any]]
    total: int
    limit: int
    offset: int

    @property
    def truncated(self) -> bool:
        return self.offset + len(self.items) < self.total


@dataclass(frozen=True, slots=True)
class AccountFilter:
    search: str | None = None
    depths: tuple[int, ...] = ()
    tree_ids: tuple[str, ...] = ()
    seeds_only: bool = False
    boundary_only: bool = False
    collisions_only: bool = False
    can_dm: bool | None = None
    verified: bool | None = None
    protected: bool | None = None
    min_followers: int | None = None
    max_followers: int | None = None
    min_network_indegree: int | None = None
    has_timeline: bool | None = None
    incomplete_only: bool = False
    order_by: str = "network_indegree"
    descending: bool = True
    limit: int = 100
    offset: int = 0

    ORDERABLE = {
        "network_indegree": "n.network_indegree",
        "followers": "p.followers_count",
        "depth": "n.first_depth",
        "avg_like": "m.avg_like",
        "median_engagement": "m.median_engagement",
        "engagement_rate": "m.engagement_rate",
        "reach_ratio": "m.reach_ratio",
        "latest_post_at": "m.latest_post_at",
        "account_id": "n.account_id",
    }

    def __post_init__(self) -> None:
        if self.order_by not in self.ORDERABLE:
            raise ValueError(f"cannot order by {self.order_by!r}")
        if not 1 <= self.limit <= MAX_PAGE_SIZE:
            raise ValueError(f"limit must be between 1 and {MAX_PAGE_SIZE}")
        if self.offset < 0:
            raise ValueError("offset cannot be negative")

    @property
    def as_dict(self) -> dict[str, Any]:
        """The filter as applied, so an export can reproduce itself."""

        return {
            name: value
            for name, value in (
                ("search", self.search),
                ("depths", list(self.depths) or None),
                ("tree_ids", list(self.tree_ids) or None),
                ("seeds_only", self.seeds_only or None),
                ("boundary_only", self.boundary_only or None),
                ("collisions_only", self.collisions_only or None),
                ("can_dm", self.can_dm),
                ("verified", self.verified),
                ("protected", self.protected),
                ("min_followers", self.min_followers),
                ("max_followers", self.max_followers),
                ("min_network_indegree", self.min_network_indegree),
                ("has_timeline", self.has_timeline),
                ("incomplete_only", self.incomplete_only or None),
                ("order_by", self.order_by),
                ("descending", self.descending),
            )
            if value is not None
        }


@dataclass(frozen=True, slots=True)
class EdgeFilter:
    source_id: str | None = None
    target_id: str | None = None
    tree_ids: tuple[str, ...] = ()
    depths: tuple[int, ...] = ()
    boundary_only: bool = False
    collisions_only: bool = False
    limit: int = 100
    offset: int = 0

    def __post_init__(self) -> None:
        if not 1 <= self.limit <= MAX_PAGE_SIZE:
            raise ValueError(f"limit must be between 1 and {MAX_PAGE_SIZE}")
        if self.offset < 0:
            raise ValueError("offset cannot be negative")

    @property
    def as_dict(self) -> dict[str, Any]:
        return {
            name: value
            for name, value in (
                ("source_id", self.source_id),
                ("target_id", self.target_id),
                ("tree_ids", list(self.tree_ids) or None),
                ("depths", list(self.depths) or None),
                ("boundary_only", self.boundary_only or None),
                ("collisions_only", self.collisions_only or None),
            )
            if value is not None
        }


@dataclass(frozen=True, slots=True)
class Subgraph:
    nodes: list[dict[str, Any]]
    edges: list[dict[str, Any]]
    matched_nodes: int
    node_limit: int
    edge_limit: int
    warnings: list[str] = field(default_factory=list)

    @property
    def bounded(self) -> bool:
        """Whether the caller is seeing less than what matched."""

        return self.matched_nodes > len(self.nodes)


# `network_indegree` is how many accounts inside this task follow the node. It
# is the product's core ranking signal, and unlike the platform follower count
# it cannot be bought.
_ACCOUNT_BASE = """
FROM account_nodes AS n
LEFT JOIN account_profiles AS p ON p.task_id = n.task_id AND p.account_id = n.account_id
LEFT JOIN account_metrics AS m ON m.task_id = n.task_id AND m.account_id = n.account_id
LEFT JOIN LATERAL (
    SELECT count(DISTINCT o.tree_id) AS tree_count,
           count(*) AS observation_count,
           count(*) FILTER (WHERE o.is_collision) AS collision_count,
           array_agg(DISTINCT o.tree_id) AS trees
    FROM account_observations o
    WHERE o.task_id = n.task_id AND o.account_id = n.account_id
) AS obs ON true
LEFT JOIN root_trees AS seed ON seed.task_id = n.task_id AND seed.seed_account_id = n.account_id
WHERE n.task_id = $1
  AND ($2::text IS NULL OR p.username ILIKE '%' || $2 || '%'
       OR p.display_name ILIKE '%' || $2 || '%' OR p.description ILIKE '%' || $2 || '%'
       OR n.account_id = $2)
  AND ($3::int[] IS NULL OR n.first_depth = ANY($3))
  AND ($4::text[] IS NULL OR obs.trees && $4)
  AND (NOT $5::bool OR seed.tree_id IS NOT NULL)
  AND (NOT $6::bool OR n.is_l6_boundary)
  AND (NOT $7::bool OR COALESCE(obs.collision_count, 0) > 0)
  AND ($8::bool IS NULL OR p.can_dm = $8)
  AND ($9::bool IS NULL OR COALESCE(p.verified, false) = $9)
  AND ($10::bool IS NULL OR COALESCE(p.protected, false) = $10)
  AND ($11::int IS NULL OR p.followers_count >= $11)
  AND ($12::int IS NULL OR p.followers_count <= $12)
  AND ($13::int IS NULL OR n.network_indegree >= $13)
  AND ($14::bool IS NULL OR (m.account_id IS NOT NULL) = $14)
  AND (NOT $15::bool OR n.expansion_status IN ('failed', 'filtered')
       OR (n.declared_following > 0 AND n.collected_following < n.declared_following))
"""


class ProductQueries:
    def __init__(self, pool: AsyncPool) -> None:
        self._pool = pool

    def _account_args(self, task_id: str, f: AccountFilter) -> list[Any]:
        return [
            task_id,
            f.search,
            list(f.depths) or None,
            list(f.tree_ids) or None,
            f.seeds_only,
            f.boundary_only,
            f.collisions_only,
            f.can_dm,
            f.verified,
            f.protected,
            f.min_followers,
            f.max_followers,
            f.min_network_indegree,
            f.has_timeline,
            f.incomplete_only,
        ]

    async def accounts(self, task_id: str, f: AccountFilter) -> Page:
        order = f.ORDERABLE[f.order_by]
        direction = "DESC" if f.descending else "ASC"
        args = self._account_args(task_id, f)
        async with self._pool.acquire() as connection:
            total = await connection.fetchval(f"SELECT count(*) {_ACCOUNT_BASE}", *args)
            rows = await connection.fetch(
                f"""
                SELECT n.account_id, n.first_depth, n.is_l6_boundary, n.expansion_status,
                       n.declared_following, n.collected_following, n.termination_reason,
                       n.timeline_reason,
                       n.filter_reason, n.timeline_status, n.candidate_reasons,
                       n.first_seen_at, n.updated_at,
                       p.username, p.display_name, p.description, p.followers_count,
                       p.following_count, p.protected, p.verified, p.blue_verified,
                       p.can_dm, p.location,
                       n.network_indegree,
                       COALESCE(obs.tree_count, 0) AS tree_count,
                       COALESCE(obs.observation_count, 0) AS observation_count,
                       COALESCE(obs.collision_count, 0) AS collision_count,
                       obs.trees,
                       seed.tree_id AS seed_of_tree,
                       m.sample_count, m.scanned_count, m.sample_span_days, m.latest_post_at,
                       m.avg_reply, m.avg_retweet, m.avg_like, m.avg_view,
                       m.view_sample_count, m.median_engagement, m.engagement_rate,
                       m.reach_ratio, m.bookmark_rate
                {_ACCOUNT_BASE}
                ORDER BY {order} {direction} NULLS LAST, n.account_id
                LIMIT ${len(args) + 1} OFFSET ${len(args) + 2};
                """,
                *args,
                f.limit,
                f.offset,
            )
        return Page(
            items=[_account_row(row) for row in rows],
            total=int(total),
            limit=f.limit,
            offset=f.offset,
        )

    async def account_detail(self, task_id: str, account_id: str) -> dict[str, Any] | None:
        page = await self.accounts(
            task_id, AccountFilter(search=account_id, limit=1, order_by="account_id")
        )
        account = next((item for item in page.items if item["account_id"] == account_id), None)
        if account is None:
            return None
        async with self._pool.acquire() as connection:
            paths = await connection.fetch(
                """
                SELECT tree_id, parent_account_id, depth, is_collision, observed_at
                FROM account_observations
                WHERE task_id = $1 AND account_id = $2
                ORDER BY depth, observed_at LIMIT 50;
                """,
                task_id,
                account_id,
            )
            following = await connection.fetch(
                """
                SELECT e.target_account_id, e.target_depth, e.is_l6_boundary,
                       p.username, p.followers_count, p.can_dm
                FROM follow_edges e
                LEFT JOIN account_profiles p
                  ON p.task_id = e.task_id AND p.account_id = e.target_account_id
                WHERE e.task_id = $1 AND e.source_account_id = $2
                ORDER BY p.followers_count DESC NULLS LAST LIMIT 100;
                """,
                task_id,
                account_id,
            )
            followed_by = await connection.fetch(
                """
                SELECT e.source_account_id, e.source_depth, p.username, p.followers_count
                FROM follow_edges e
                LEFT JOIN account_profiles p
                  ON p.task_id = e.task_id AND p.account_id = e.source_account_id
                WHERE e.task_id = $1 AND e.target_account_id = $2
                ORDER BY p.followers_count DESC NULLS LAST LIMIT 100;
                """,
                task_id,
                account_id,
            )
            posts = await connection.fetch(
                """
                SELECT post_id, kind, is_qualifying, text, posted_at, reply_count,
                       retweet_count, like_count, quote_count, bookmark_count, view_count
                FROM account_posts
                WHERE task_id = $1 AND account_id = $2 AND is_qualifying
                ORDER BY posted_at DESC NULLS LAST LIMIT 30;
                """,
                task_id,
                account_id,
            )
        account["discovery_paths"] = [dict(r) for r in paths]
        account["following"] = [dict(r) for r in following]
        account["followed_by_in_network"] = [dict(r) for r in followed_by]
        account["posts"] = [dict(r) for r in posts]
        return account

    async def relationships(self, task_id: str, f: EdgeFilter) -> Page:
        args: list[Any] = [
            task_id,
            f.source_id,
            f.target_id,
            list(f.tree_ids) or None,
            list(f.depths) or None,
            f.boundary_only,
            f.collisions_only,
        ]
        base = """
        FROM follow_edges e
        LEFT JOIN LATERAL (
            SELECT array_agg(DISTINCT o.tree_id) AS trees,
                   bool_or(o.is_collision) AS is_collision
            FROM follow_edge_observations o
            WHERE o.task_id = e.task_id AND o.source_account_id = e.source_account_id
              AND o.target_account_id = e.target_account_id
        ) AS obs ON true
        WHERE e.task_id = $1
          AND ($2::text IS NULL OR e.source_account_id = $2)
          AND ($3::text IS NULL OR e.target_account_id = $3)
          AND ($4::text[] IS NULL OR obs.trees && $4)
          AND ($5::int[] IS NULL OR e.target_depth = ANY($5))
          AND (NOT $6::bool OR e.is_l6_boundary)
          AND (NOT $7::bool OR COALESCE(obs.is_collision, false))
        """
        async with self._pool.acquire() as connection:
            total = await connection.fetchval(f"SELECT count(*) {base}", *args)
            rows = await connection.fetch(
                f"""
                SELECT e.source_account_id, e.target_account_id, e.source_depth,
                       e.target_depth, e.is_l6_boundary, e.first_seen_at,
                       obs.trees, COALESCE(obs.is_collision, false) AS is_collision
                {base}
                ORDER BY e.source_account_id, e.target_account_id
                LIMIT $8 OFFSET $9;
                """,
                *args,
                f.limit,
                f.offset,
            )
        return Page(
            items=[_edge_row(row) for row in rows],
            total=int(total),
            limit=f.limit,
            offset=f.offset,
        )

    async def subgraph(
        self,
        task_id: str,
        *,
        seeds: tuple[str, ...] = (),
        hops: int = 1,
        node_limit: int = 200,
        min_network_indegree: int | None = None,
    ) -> Subgraph:
        """A neighbourhood, capped and honest about the cap.

        Callers get the count of what matched alongside what fits, so a bounded
        answer is visibly bounded rather than looking like the whole graph.
        """

        if hops < 1 or hops > 3:
            raise ValueError("hops must be between 1 and 3")
        node_limit = max(1, min(node_limit, MAX_SUBGRAPH_NODES))
        warnings: list[str] = []
        async with self._pool.acquire() as connection:
            if seeds:
                matched = await connection.fetch(
                    """
                    WITH RECURSIVE reach(account_id, hop) AS (
                        SELECT unnest($2::text[]), 0
                        UNION
                        SELECT e.target_account_id, r.hop + 1
                        FROM reach r
                        JOIN follow_edges e
                          ON e.task_id = $1 AND e.source_account_id = r.account_id
                        WHERE r.hop < $3
                    )
                    SELECT DISTINCT account_id, min(hop) AS hop FROM reach GROUP BY account_id;
                    """,
                    task_id,
                    list(seeds),
                    hops,
                )
            else:
                matched = await connection.fetch(
                    """
                    SELECT n.account_id, n.first_depth AS hop
                    FROM account_nodes n
                    WHERE n.task_id = $1
                      AND ($2::int IS NULL OR n.network_indegree >= $2);
                    """,
                    task_id,
                    min_network_indegree,
                )
            matched_ids = [str(r["account_id"]) for r in matched]
            if len(matched_ids) > node_limit:
                warnings.append(
                    f"{len(matched_ids)} accounts matched; showing the {node_limit} "
                    "most followed inside the network"
                )
            nodes = await connection.fetch(
                """
                SELECT n.account_id, n.first_depth, n.is_l6_boundary, n.expansion_status,
                       n.timeline_status, p.username, p.followers_count, p.can_dm,
                       COALESCE(d.indegree, 0) AS network_indegree,
                       (seed.tree_id IS NOT NULL) AS is_seed
                FROM account_nodes n
                LEFT JOIN account_profiles p
                  ON p.task_id = n.task_id AND p.account_id = n.account_id
                LEFT JOIN root_trees seed
                  ON seed.task_id = n.task_id AND seed.seed_account_id = n.account_id
                LEFT JOIN LATERAL (
                    SELECT count(*) AS indegree FROM follow_edges e
                    WHERE e.task_id = n.task_id AND e.target_account_id = n.account_id
                ) d ON true
                WHERE n.task_id = $1 AND n.account_id = ANY($2::text[])
                ORDER BY COALESCE(d.indegree, 0) DESC, n.account_id
                LIMIT $3;
                """,
                task_id,
                matched_ids,
                node_limit,
            )
            kept = [str(r["account_id"]) for r in nodes]
            edges = await connection.fetch(
                """
                SELECT source_account_id, target_account_id, source_depth, target_depth,
                       is_l6_boundary
                FROM follow_edges
                WHERE task_id = $1
                  AND source_account_id = ANY($2::text[])
                  AND target_account_id = ANY($2::text[])
                LIMIT $3;
                """,
                task_id,
                kept,
                MAX_SUBGRAPH_EDGES,
            )
        if len(edges) >= MAX_SUBGRAPH_EDGES:
            warnings.append(f"edge list capped at {MAX_SUBGRAPH_EDGES}")
        return Subgraph(
            nodes=[dict(r) for r in nodes],
            edges=[dict(r) for r in edges],
            matched_nodes=len(matched_ids),
            node_limit=node_limit,
            edge_limit=MAX_SUBGRAPH_EDGES,
            warnings=warnings,
        )


def _account_row(row: Any) -> dict[str, Any]:
    data = dict(row)
    declared = data.get("declared_following")
    collected = data.get("collected_following") or 0
    data["coverage_ratio"] = round(collected / declared, 4) if declared else None
    # Data-quality flags, surfaced rather than left to be inferred from nulls.
    warnings: list[str] = []
    if data.get("is_l6_boundary"):
        warnings.append("boundary_not_expanded")
    if data.get("expansion_status") == "failed":
        warnings.append("expansion_failed")
    if data.get("expansion_status") == "filtered":
        warnings.append("expansion_filtered")
    if declared and collected < declared:
        # Same arithmetic, two different facts: one says the platform stopped
        # handing over relations, the other says we stopped paying for them.
        # Collapsing them would let a lowered scan cap read as platform truncation.
        reason = data.get("termination_reason")
        if reason in SELF_TERMINATIONS:
            warnings.append("scan_capped")
        else:
            warnings.append("following_truncated")
    if data.get("collision_count"):
        warnings.append("multiple_discovery_paths")
    if data.get("sample_count") is not None and data.get("view_sample_count") == 0:
        warnings.append("no_view_counts_in_sample")
    data["warnings"] = warnings
    data["trees"] = list(data.get("trees") or [])
    data["candidate_reasons"] = list(data.get("candidate_reasons") or [])
    return data


def _edge_row(row: Any) -> dict[str, Any]:
    data = dict(row)
    data["trees"] = list(data.get("trees") or [])
    return data


def utcnow() -> datetime:
    from datetime import timezone

    return datetime.now(timezone.utc)
