"""Layer barrier and completion for the L0-L6 traversal.

A layer is closed only when nothing about it can still change. That is four
conditions, not one, because the pipeline has a buffer in the middle: the
frontier being empty says the scheduler has nothing to do, and says nothing
about whether the pages it already fetched have been turned into the next layer.

Everything here counts `Following` work only. Timeline enrichment shares the
frontier table but is not part of the traversal: letting an outstanding timeline
request hold a layer open would make the graph wait on data it does not need.
"""

from dataclasses import dataclass
from typing import Any, Protocol

from xgraph.domain import PLATFORM_TERMINATIONS, SELF_TERMINATIONS, Operation
from xgraph.messaging.topics import PARSER_GROUP


class AsyncPool(Protocol):
    def acquire(self) -> Any: ...


@dataclass(frozen=True, slots=True)
class LayerState:
    """Why a layer is or is not finished, in the terms an operator can act on."""

    task_id: str
    depth: int
    pending: int
    running: int
    retryable: int
    in_flight_requests: int
    unpublished_pages: int
    unparsed_pages: int

    @property
    def closed(self) -> bool:
        return (
            self.pending == 0
            and self.running == 0
            and self.retryable == 0
            and self.in_flight_requests == 0
            and self.unpublished_pages == 0
            and self.unparsed_pages == 0
        )

    @property
    def blocked_by(self) -> tuple[str, ...]:
        reasons: dict[str, int] = {
            "frontier_pending": self.pending,
            "frontier_running": self.running,
            "frontier_retryable": self.retryable,
            "requests_in_flight": self.in_flight_requests,
            "pages_unpublished": self.unpublished_pages,
            "pages_unparsed": self.unparsed_pages,
        }
        return tuple(name for name, count in reasons.items() if count)


@dataclass(frozen=True, slots=True)
class LayerMetrics:
    depth: int
    nodes: int
    boundary_nodes: int
    edges: int
    observations: int
    collisions: int
    filtered: int
    expansions_complete: int
    requests: int


class PostgresLayerStore:
    def __init__(self, pool: AsyncPool, *, parser_group: str = PARSER_GROUP) -> None:
        self._pool = pool
        self._parser_group = parser_group

    async def layer_state(self, task_id: str, depth: int | None = None) -> LayerState:
        """Inspect one layer, defaulting to the task's currently open one."""

        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                WITH t AS (
                    SELECT task_id, COALESCE($2::int, current_depth) AS depth
                    FROM crawl_tasks WHERE task_id = $1
                )
                SELECT t.depth,
                    (SELECT count(*) FROM crawl_frontier f
                      WHERE f.task_id = t.task_id AND f.depth = t.depth
                        AND f.operation = $4 AND f.status = 'pending') AS pending,
                    (SELECT count(*) FROM crawl_frontier f
                      WHERE f.task_id = t.task_id AND f.depth = t.depth
                        AND f.operation = $4 AND f.status = 'running') AS running,
                    (SELECT count(*) FROM crawl_frontier f
                      WHERE f.task_id = t.task_id AND f.depth = t.depth
                        AND f.operation = $4 AND f.status = 'retryable') AS retryable,
                    (SELECT count(*) FROM request_attempts a
                      JOIN crawl_frontier f ON f.frontier_id = a.frontier_id
                      WHERE a.task_id = t.task_id AND f.depth = t.depth
                        AND f.operation = $4 AND a.outcome = 'started') AS in_flight,
                    (SELECT count(*) FROM raw_page_outbox o
                      WHERE o.task_id = t.task_id AND o.depth = t.depth
                        AND o.operation = $4
                        AND o.published_at IS NULL AND o.dead_lettered_at IS NULL)
                      AS unpublished,
                    (SELECT count(*) FROM raw_page_outbox o
                      LEFT JOIN processed_events p
                        ON p.task_id = o.task_id AND p.event_id = o.event_id
                       AND p.group_id = $3
                      WHERE o.task_id = t.task_id AND o.depth = t.depth
                        AND o.operation = $4
                        AND o.dead_lettered_at IS NULL AND p.event_id IS NULL)
                      AS unparsed
                FROM t;
                """,
                task_id,
                depth,
                self._parser_group,
                Operation.FOLLOWING.value,
            )
        if row is None:
            raise LookupError(f"unknown task {task_id}")
        return LayerState(
            task_id=task_id,
            depth=int(row["depth"]),
            pending=int(row["pending"]),
            running=int(row["running"]),
            retryable=int(row["retryable"]),
            in_flight_requests=int(row["in_flight"]),
            unpublished_pages=int(row["unpublished"]),
            unparsed_pages=int(row["unparsed"]),
        )

    async def advance_layer(self, task_id: str) -> LayerState:
        """Open the next layer, but only once the current one cannot change.

        Returns the state that was evaluated, so a caller that did not advance
        can say which of the four conditions is still open.
        """

        state = await self.layer_state(task_id)
        if not state.closed:
            return state
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                UPDATE crawl_tasks
                SET current_depth = LEAST(current_depth + 1, max_depth + 1),
                    updated_at = now()
                WHERE task_id = $1 AND current_depth = $2;
                """,
                task_id,
                state.depth,
            )
        return state

    async def expansion_complete(self, task_id: str) -> bool:
        """Whether every layer up to the boundary has closed.

        The traversal is finished when the last expandable layer is closed and
        the task has moved past it. L6 accounts exist by then, recorded through
        the L5 pages, and never get a frontier row of their own.
        """

        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                "SELECT current_depth, max_depth FROM crawl_tasks WHERE task_id = $1",
                task_id,
            )
        if row is None:
            raise LookupError(f"unknown task {task_id}")
        if int(row["current_depth"]) <= int(row["max_depth"]):
            return False
        state = await self.layer_state(task_id, int(row["max_depth"]))
        return state.closed

    async def layer_metrics(self, task_id: str) -> list[LayerMetrics]:
        """Per-layer scale, so growth and overlap are visible while it runs."""

        async with self._pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT n.first_depth AS depth,
                       count(*) AS nodes,
                       count(*) FILTER (WHERE n.is_l6_boundary) AS boundary_nodes,
                       count(*) FILTER (WHERE n.expansion_status = 'filtered') AS filtered,
                       count(*) FILTER (WHERE n.expansion_status = 'complete') AS complete,
                       (SELECT count(*) FROM follow_edges e
                         WHERE e.task_id = n.task_id AND e.target_depth = n.first_depth) AS edges,
                       (SELECT count(*) FROM account_observations o
                         WHERE o.task_id = n.task_id AND o.depth = n.first_depth) AS observations,
                       (SELECT count(*) FROM account_observations o
                         WHERE o.task_id = n.task_id AND o.depth = n.first_depth
                           AND o.is_collision) AS collisions,
                       (SELECT count(*) FROM request_attempts a
                         JOIN crawl_frontier f ON f.frontier_id = a.frontier_id
                        WHERE a.task_id = n.task_id AND f.depth = n.first_depth
                          AND a.operation = 'Following') AS requests
                FROM account_nodes n
                WHERE n.task_id = $1
                GROUP BY n.task_id, n.first_depth
                ORDER BY n.first_depth;
                """,
                task_id,
            )
        return [
            LayerMetrics(
                depth=int(r["depth"]),
                nodes=int(r["nodes"]),
                boundary_nodes=int(r["boundary_nodes"]),
                edges=int(r["edges"]),
                observations=int(r["observations"]),
                collisions=int(r["collisions"]),
                filtered=int(r["filtered"]),
                expansions_complete=int(r["complete"]),
                requests=int(r["requests"]),
            )
            for r in rows
        ]

    async def coverage(self, task_id: str) -> dict[str, Any]:
        """Declared vs collected, and how the chains ended.

        Without this the graph looks complete whatever the platform withheld.

        Chains we stopped ourselves are counted apart from chains the platform
        ended. The ratio looks identical either way, but one says "X withheld
        this" and the other says "we chose not to pay for it"; averaged together
        neither can be read, and the first silently becomes the second whenever a
        scan cap is lowered.
        """

        async with self._pool.acquire() as connection:
            summary = await connection.fetchrow(
                """
                SELECT count(*) FILTER (WHERE expansion_status = 'complete') AS expanded,
                       count(*) FILTER (WHERE expansion_status = 'complete'
                                        AND declared_following IS NOT NULL) AS comparable,
                       count(*) FILTER (WHERE expansion_status = 'complete'
                                        AND termination_reason = ANY($2)
                                        AND declared_following > 0
                                        AND collected_following < declared_following)
                         AS truncated,
                       count(*) FILTER (WHERE expansion_status = 'complete'
                                        AND termination_reason = ANY($3)
                                        AND declared_following > 0
                                        AND collected_following < declared_following)
                         AS scan_capped,
                       avg(collected_following::numeric
                           / NULLIF(declared_following, 0))
                         FILTER (WHERE expansion_status = 'complete'
                                 AND termination_reason = ANY($2)
                                 AND declared_following > 0) AS mean_ratio
                FROM account_nodes WHERE task_id = $1;
                """,
                task_id,
                list(PLATFORM_TERMINATIONS),
                list(SELF_TERMINATIONS),
            )
            reasons = await connection.fetch(
                """
                SELECT COALESCE(termination_reason, 'unknown') AS reason, count(*) AS n
                FROM account_nodes
                WHERE task_id = $1 AND expansion_status IN ('complete', 'failed')
                GROUP BY 1 ORDER BY 2 DESC;
                """,
                task_id,
            )
        mean = summary["mean_ratio"]
        return {
            "expanded": int(summary["expanded"]),
            "comparable": int(summary["comparable"]),
            "truncated": int(summary["truncated"]),
            "scan_capped": int(summary["scan_capped"]),
            "mean_coverage_ratio": float(mean) if mean is not None else None,
            "termination_reasons": {str(r["reason"]): int(r["n"]) for r in reasons},
        }
