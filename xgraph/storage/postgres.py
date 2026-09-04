"""Durable Task/Frontier operations for PostgreSQL."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from xgraph.domain import FrontierStatus, Operation


class AsyncConnection(Protocol):
    def transaction(self) -> Any: ...

    async def fetchrow(self, query: str, *args: Any) -> Any: ...

    async def execute(self, query: str, *args: Any) -> str: ...


class AsyncPool(Protocol):
    def acquire(self) -> Any: ...


@dataclass(frozen=True, slots=True)
class FrontierItem:
    frontier_id: int
    task_id: str
    account_id: str
    tree_id: str | None
    operation: Operation
    depth: int
    cursor_in: str | None
    attempt: int
    owner_id: str
    lease_expires_at: datetime


@dataclass(frozen=True, slots=True)
class RequestAttempt:
    attempt_id: int
    task_id: str
    frontier_id: int | None
    scraper_alias: str | None
    operation: Operation
    sequence: int


class BudgetExhaustedError(RuntimeError):
    pass


class PostgresFrontierStore:
    def __init__(self, pool: AsyncPool) -> None:
        self._pool = pool

    async def create_task(
        self,
        task_id: str,
        *,
        max_requests: int = 100_000,
        max_nodes: int = 1_000_000,
        max_edges: int = 5_000_000,
    ) -> None:
        if min(max_requests, max_nodes, max_edges) <= 0:
            raise ValueError("task budgets must be positive")
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.execute(
                """
                INSERT INTO crawl_tasks(task_id, max_requests, max_nodes, max_edges, status)
                VALUES ($1, $2, $3, $4, 'created')
                ON CONFLICT (task_id) DO NOTHING
                """,
                task_id,
                max_requests,
                max_nodes,
                max_edges,
            )
            for operation in Operation:
                await connection.execute(
                    """
                    INSERT INTO task_operation_budgets(task_id, operation, max_requests)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (task_id, operation) DO NOTHING;
                    """,
                    task_id,
                    operation.value,
                    max_requests,
                )

    async def create_seed_task(
        self,
        task_id: str,
        seeds: dict[str, str],
        *,
        max_requests: int = 100_000,
        max_nodes: int = 1_000_000,
        max_edges: int = 5_000_000,
        operation_budgets: dict[Operation, int] | None = None,
        max_depth: int = 5,
        min_followers_to_expand: int = 0,
    ) -> None:
        """Atomically create one Task, its Root Trees, L0 nodes and frontier."""

        if not seeds:
            raise ValueError("at least one seed is required")
        if min(max_requests, max_nodes, max_edges) <= 0:
            raise ValueError("task budgets must be positive")
        if len(set(seeds.values())) != len(seeds):
            raise ValueError("seed account ids must be unique within a task")
        if not 0 <= max_depth <= 5:
            raise ValueError("max_depth must be between 0 and 5")
        if min_followers_to_expand < 0:
            raise ValueError("min_followers_to_expand cannot be negative")
        if len(seeds) > max_nodes:
            raise ValueError("seed count exceeds max_nodes")
        budgets = operation_budgets or {
            Operation.FOLLOWING: max_requests,
            Operation.USER_TWEETS: max_requests,
            Operation.USER_BY_SCREEN_NAME: max_requests,
        }
        if not budgets or any(value <= 0 for value in budgets.values()):
            raise ValueError("operation budgets must be positive")
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.execute(
                """
                INSERT INTO crawl_tasks(
                    task_id, max_requests, max_nodes, max_edges, nodes_created, status,
                    max_depth, min_followers_to_expand
                )
                VALUES ($1, $2, $3, $4, $5, 'running', $6, $7);
                """,
                task_id,
                max_requests,
                max_nodes,
                max_edges,
                len(seeds),
                max_depth,
                min_followers_to_expand,
            )
            for operation, maximum in budgets.items():
                await connection.execute(
                    """
                    INSERT INTO task_operation_budgets(task_id, operation, max_requests)
                    VALUES ($1, $2, $3);
                    """,
                    task_id,
                    operation.value,
                    maximum,
                )
            for tree_id, account_id in seeds.items():
                await connection.execute(
                    """
                    INSERT INTO root_trees(task_id, tree_id, seed_account_id)
                    VALUES ($1, $2, $3);
                    """,
                    task_id,
                    tree_id,
                    account_id,
                )
                await connection.execute(
                    """
                    INSERT INTO account_nodes(task_id, account_id, first_depth, is_l6_boundary)
                    VALUES ($1, $2, 0, false)
                    ON CONFLICT (task_id, account_id) DO NOTHING;
                    """,
                    task_id,
                    account_id,
                )
                await connection.execute(
                    """
                    INSERT INTO crawl_frontier(task_id, account_id, tree_id, operation, depth)
                    VALUES ($1, $2, $3, 'Following', 0)
                    ON CONFLICT (task_id, account_id, operation) DO NOTHING;
                    """,
                    task_id,
                    account_id,
                    tree_id,
                )

    async def enqueue_frontier(
        self,
        task_id: str,
        account_id: str,
        operation: Operation,
        depth: int,
        *,
        cursor_in: str | None = None,
        priority: int = 0,
        tree_id: str | None = None,
    ) -> bool:
        if depth < 0 or depth > 5:
            raise ValueError("frontier depth must be between 0 and 5")
        async with self._pool.acquire() as connection:
            result = await connection.execute(
                """
                INSERT INTO crawl_frontier(task_id, account_id, tree_id, operation, depth,
                                           cursor_in, priority)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (task_id, account_id, operation) DO NOTHING
                """,
                task_id,
                account_id,
                tree_id,
                operation.value,
                depth,
                cursor_in,
                priority,
            )
        return str(result) == "INSERT 0 1"

    async def claim_frontier(
        self,
        *,
        worker_id: str,
        lease_seconds: int = 120,
        operation: Operation | None = None,
    ) -> FrontierItem | None:
        """Take one unit of work, optionally restricted to a single operation.

        Traversal and enrichment run on separate rate-limit buckets, so they are
        normally driven by separate workers claiming their own operation.
        """

        if not worker_id:
            raise ValueError("worker_id is required")
        async with self._pool.acquire() as connection, connection.transaction():
            row = await connection.fetchrow(
                """
                    WITH candidate AS (
                        SELECT f.frontier_id
                        FROM crawl_frontier AS f
                        JOIN crawl_tasks AS t ON t.task_id = f.task_id
                        WHERE t.status = 'running'
                          AND ($3::text IS NULL OR f.operation = $3)
                          -- The layer barrier, and only for the traversal.
                          -- Claiming a deeper row would start a layer before
                          -- the current one closed. Enrichment is not part of
                          -- the traversal and is deliberately not gated: it
                          -- must never hold the graph back.
                          AND (f.operation <> 'Following' OR f.depth = t.current_depth)
                          AND (
                              f.status IN ('pending', 'retryable')
                              OR (f.status = 'running' AND f.lease_expires_at <= now())
                          )
                          AND f.not_before <= now()
                          AND f.attempt < f.max_attempts
                        ORDER BY f.priority DESC, f.depth ASC, f.frontier_id
                        FOR UPDATE OF f SKIP LOCKED
                        LIMIT 1
                    )
                    UPDATE crawl_frontier AS f
                    SET status = 'running',
                        owner_id = $1,
                        lease_expires_at = now() + ($2 * interval '1 second'),
                        attempt = f.attempt + 1,
                        updated_at = now()
                    FROM candidate
                    WHERE f.frontier_id = candidate.frontier_id
                    RETURNING f.frontier_id, f.task_id, f.account_id, f.tree_id, f.operation,
                              f.depth, f.cursor_in, f.attempt, f.owner_id, f.lease_expires_at;
                    """,
                worker_id,
                lease_seconds,
                operation.value if operation is not None else None,
            )
        if row is None:
            return None
        return FrontierItem(
            frontier_id=int(row["frontier_id"]),
            task_id=str(row["task_id"]),
            account_id=str(row["account_id"]),
            tree_id=row["tree_id"],
            operation=Operation(str(row["operation"])),
            depth=int(row["depth"]),
            cursor_in=row["cursor_in"],
            attempt=int(row["attempt"]),
            owner_id=str(row["owner_id"]),
            lease_expires_at=row["lease_expires_at"],
        )

    async def finish_frontier(
        self,
        item: FrontierItem,
        status: FrontierStatus,
        *,
        next_cursor: str | None = None,
        error_class: str | None = None,
        not_before: datetime | None = None,
        refund_attempt: bool = False,
    ) -> FrontierStatus:
        """Close out a claimed work item, and report what it actually became.

        A `retryable` turn that used up the last attempt becomes `failed` here,
        in the same statement. The caller has to be told: it believes the row
        will come round again, so it leaves the account mid-flight — and nothing
        ever comes back to finish it.

        `refund_attempt` gives back the attempt that `claim_frontier` charged.
        The retry budget exists to stop a poisoned row from being retried
        forever; a row that never got as far as sending a request was not
        poisoned, it was unlucky. Charging it means that with more workers than
        scraper accounts — the normal arrangement, since accounts are the scarce
        resource — perfectly good work items are killed off for losing a race.
        """

        if status not in {
            FrontierStatus.COMPLETED,
            FrontierStatus.RETRYABLE,
            FrontierStatus.FAILED,
            FrontierStatus.SKIPPED,
        }:
            raise ValueError("finish_frontier requires a terminal or retryable status")
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                UPDATE crawl_frontier
                SET attempt = CASE WHEN $7 THEN GREATEST(attempt - 1, 0) ELSE attempt END,
                    status = CASE
                        WHEN $2 = 'retryable'
                             AND (CASE WHEN $7 THEN GREATEST(attempt - 1, 0) ELSE attempt END)
                                 >= max_attempts
                        THEN 'failed'
                        ELSE $2
                    END,
                    cursor_in = COALESCE($3, cursor_in),
                    last_error_class = $4,
                    not_before = COALESCE($5, not_before),
                    owner_id = NULL,
                    lease_expires_at = NULL,
                    updated_at = now()
                WHERE frontier_id = $1 AND owner_id = $6
                RETURNING status;
                """,
                item.frontier_id,
                status.value,
                next_cursor,
                error_class,
                not_before,
                item.owner_id,
                refund_attempt,
            )
        if row is None:
            raise RuntimeError("frontier lease was lost before finish")
        return FrontierStatus(row["status"])

    async def fail_exhausted_frontier(self, task_id: str) -> int:
        """Move rows that used up their attempt budget to a terminal state.

        `claim_frontier` stops offering a row once `attempt` reaches
        `max_attempts`, which keeps a poisoned row from burning quota but also
        makes it invisible. Completion is defined over frontier status, so the
        row has to be told it is finished; otherwise the task stays `running`
        forever with nothing left to do.
        """

        async with self._pool.acquire() as connection:
            result = await connection.execute(
                """
                UPDATE crawl_frontier
                SET status = 'failed',
                    owner_id = NULL,
                    lease_expires_at = NULL,
                    last_error_class = COALESCE(last_error_class, 'attempt_budget_exhausted'),
                    updated_at = now()
                WHERE task_id = $1
                  AND attempt >= max_attempts
                  AND status IN ('pending', 'retryable', 'running')
                  AND (status <> 'running' OR lease_expires_at IS NULL OR lease_expires_at <= now());
                """,
                task_id,
            )
        return int(str(result).rsplit(" ", 1)[-1])

    async def checkpoint_frontier(self, item: FrontierItem, next_cursor: str) -> None:
        """Persist the next cursor while retaining the current Worker lease."""

        if not next_cursor:
            raise ValueError("next_cursor is required")
        async with self._pool.acquire() as connection:
            result = await connection.execute(
                """
                UPDATE crawl_frontier
                SET cursor_in = $2, updated_at = now()
                WHERE frontier_id = $1
                  AND status = 'running'
                  AND owner_id = $3
                  AND lease_expires_at > now();
                """,
                item.frontier_id,
                next_cursor,
                item.owner_id,
            )
        if result == "UPDATE 0":
            raise RuntimeError("frontier lease was lost before checkpoint")

    async def reserve_request(
        self,
        task_id: str,
        operation: Operation,
        *,
        frontier_id: int | None = None,
        scraper_alias: str | None = None,
    ) -> RequestAttempt:
        """Atomically consume Task and operation budgets before HTTP."""

        async with self._pool.acquire() as connection, connection.transaction():
            row = await connection.fetchrow(
                """
                SELECT t.requests_attempted, t.max_requests,
                       b.requests_attempted AS operation_attempted,
                       b.max_requests AS operation_max
                FROM crawl_tasks AS t
                JOIN task_operation_budgets AS b ON b.task_id = t.task_id
                WHERE t.task_id = $1 AND t.status = 'running' AND b.operation = $2
                FOR UPDATE OF t, b;
                """,
                task_id,
                operation.value,
            )
            if (
                row is None
                or int(row["requests_attempted"]) >= int(row["max_requests"])
                or int(row["operation_attempted"]) >= int(row["operation_max"])
            ):
                raise BudgetExhaustedError(
                    f"request budget exhausted for task {task_id} operation {operation.value}"
                )
            sequence = int(row["requests_attempted"]) + 1
            await connection.execute(
                """
                UPDATE crawl_tasks
                SET requests_attempted = requests_attempted + 1, updated_at = now()
                WHERE task_id = $1;
                """,
                task_id,
            )
            await connection.execute(
                """
                UPDATE task_operation_budgets
                SET requests_attempted = requests_attempted + 1
                WHERE task_id = $1 AND operation = $2;
                """,
                task_id,
                operation.value,
            )
            attempt = await connection.fetchrow(
                """
                INSERT INTO request_attempts(task_id, frontier_id, scraper_alias, operation)
                VALUES ($1, $2, $3, $4)
                RETURNING attempt_id;
                """,
                task_id,
                frontier_id,
                scraper_alias,
                operation.value,
            )
        if attempt is None:
            raise RuntimeError("request attempt was not created")
        return RequestAttempt(
            attempt_id=int(attempt["attempt_id"]),
            task_id=task_id,
            frontier_id=frontier_id,
            scraper_alias=scraper_alias,
            operation=operation,
            sequence=sequence,
        )

    async def finish_request(
        self,
        attempt: RequestAttempt,
        *,
        outcome: str,
        status_code: int | None = None,
        error_class: str | None = None,
    ) -> None:
        if outcome not in {"succeeded", "rate_limited", "failed"}:
            raise ValueError("invalid request outcome")
        async with self._pool.acquire() as connection:
            result = await connection.execute(
                """
                UPDATE request_attempts
                SET outcome = $2, status_code = $3, error_class = $4, finished_at = now()
                WHERE attempt_id = $1 AND outcome = 'started';
                """,
                attempt.attempt_id,
                outcome,
                status_code,
                error_class,
            )
        if result == "UPDATE 0":
            raise RuntimeError("request attempt is already finished or missing")

    async def record_expansion_outcome(
        self,
        task_id: str,
        account_id: str,
        *,
        status: str,
        termination_reason: str | None = None,
    ) -> None:
        """Store how an account's pagination chain ended.

        Coverage cannot be judged from the edge count alone: 800 collected edges
        mean something different when the account declares 800 than when it
        declares 8000, and different again when the chain stopped because the
        cursor stalled. The reason has to be stored when it is known.
        """

        if status not in {"complete", "failed", "boundary", "filtered"}:
            raise ValueError(f"invalid expansion status {status!r}")
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                UPDATE account_nodes
                SET expansion_status = $3, termination_reason = $4, updated_at = now()
                WHERE task_id = $1 AND account_id = $2;
                """,
                task_id,
                account_id,
                status,
                termination_reason,
            )

    async def reserve_graph_capacity(
        self, task_id: str, *, nodes: int = 0, edges: int = 0
    ) -> tuple[int, int]:
        """Atomically reserve node and edge capacity before Parser writes."""

        if nodes < 0 or edges < 0 or nodes + edges == 0:
            raise ValueError("positive node or edge capacity is required")
        async with self._pool.acquire() as connection, connection.transaction():
            row = await connection.fetchrow(
                """
                UPDATE crawl_tasks
                SET nodes_created = nodes_created + $2,
                    edges_created = edges_created + $3,
                    updated_at = now()
                WHERE task_id = $1
                  AND nodes_created + $2 <= max_nodes
                  AND edges_created + $3 <= max_edges
                RETURNING nodes_created, edges_created;
                """,
                task_id,
                nodes,
                edges,
            )
        if row is None:
            raise BudgetExhaustedError(f"graph capacity exhausted for task {task_id}")
        return int(row["nodes_created"]), int(row["edges_created"])
