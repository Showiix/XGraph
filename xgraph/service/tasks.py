"""Task lifecycle: seed import, control and progress.

Control is expressed as task state, not as signals to workers. A paused task is
one whose frontier no worker will claim from; there is nothing to deliver and
nothing to miss if a worker is restarting at that moment.
"""

import re
from dataclasses import dataclass
from typing import Any, Protocol

from xgraph.storage.layers import PostgresLayerStore
from xgraph.storage.postgres import PostgresFrontierStore

#: Handles are 1-15 characters of ASCII word characters. Anything else is a
#: paste artefact rather than an account.
_HANDLE = re.compile(r"^[A-Za-z0-9_]{1,15}$")

#: Transitions the product exposes. Anything not listed is rejected rather than
#: silently ignored, so a stuck task is visible as a refused transition.
TRANSITIONS: dict[str, set[str]] = {
    "created": {"running", "terminated"},
    "validating": {"running", "failed", "terminated"},
    "running": {"paused", "completed", "failed", "terminated"},
    "paused": {"running", "terminated"},
    "completed": set(),
    "failed": {"running", "terminated"},
    "terminated": set(),
}


class AsyncPool(Protocol):
    def acquire(self) -> Any: ...


class TransitionError(RuntimeError):
    """The requested control action is not available from the current state."""


@dataclass(frozen=True, slots=True)
class SeedImport:
    """The result of normalising a pasted or uploaded seed list."""

    handles: tuple[str, ...]
    duplicates: tuple[str, ...]
    invalid: tuple[str, ...]
    total_lines: int

    @property
    def as_dict(self) -> dict[str, Any]:
        return {
            "handles": list(self.handles),
            "duplicates": list(self.duplicates),
            "invalid": list(self.invalid),
            "total_lines": self.total_lines,
            "valid_count": len(self.handles),
        }


def parse_seed_list(raw: str) -> SeedImport:
    """Normalise handles, URLs and stray whitespace into a confirmed list.

    Duplicates and unparseable lines are reported rather than dropped: the
    import screen has to show what was ignored, or a typo silently becomes a
    missing tree.
    """

    handles: list[str] = []
    seen: set[str] = set()
    duplicates: list[str] = []
    invalid: list[str] = []
    lines = [line.strip() for line in raw.replace(",", "\n").splitlines()]
    present = [line for line in lines if line]
    for line in present:
        handle = line.rsplit("/", 1)[-1].split("?", 1)[0].removeprefix("@").strip()
        if not _HANDLE.match(handle):
            invalid.append(line)
            continue
        key = handle.lower()
        if key in seen:
            duplicates.append(handle)
            continue
        seen.add(key)
        handles.append(handle)
    return SeedImport(
        handles=tuple(handles),
        duplicates=tuple(duplicates),
        invalid=tuple(invalid),
        total_lines=len(present),
    )


class TaskService:
    def __init__(self, pool: AsyncPool) -> None:
        self._pool = pool
        self._frontier = PostgresFrontierStore(pool)
        self._layers = PostgresLayerStore(pool)

    async def list_tasks(self, limit: int = 50) -> list[dict[str, Any]]:
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT t.task_id, t.status, t.current_depth, t.max_depth, t.timeline_enabled,
                       t.requests_attempted, t.max_requests, t.nodes_created, t.edges_created,
                       t.pages_produced, t.pages_processed, t.created_at, t.updated_at,
                       (SELECT count(*) FROM root_trees r WHERE r.task_id = t.task_id) AS seeds
                FROM crawl_tasks t ORDER BY t.created_at DESC LIMIT $1;
                """,
                limit,
            )
        return [dict(r) for r in rows]

    async def transition(self, task_id: str, target: str) -> dict[str, Any]:
        """Move a task to a new state, refusing transitions that are not defined."""

        async with self._pool.acquire() as connection, connection.transaction():
            current = await connection.fetchval(
                "SELECT status FROM crawl_tasks WHERE task_id = $1 FOR UPDATE", task_id
            )
            if current is None:
                raise LookupError(f"unknown task {task_id}")
            if target == current:
                return await self.progress(task_id)
            if target not in TRANSITIONS.get(str(current), set()):
                raise TransitionError(f"cannot move task from {current} to {target}")
            await connection.execute(
                "UPDATE crawl_tasks SET status = $2, updated_at = now() WHERE task_id = $1",
                task_id,
                target,
            )
            if target in {"paused", "terminated"}:
                # Release rows nobody is going to finish, so a resumed task does
                # not have to wait out their leases. In-flight requests are left
                # alone: the page is already paid for and will be recorded.
                await connection.execute(
                    """
                    UPDATE crawl_frontier
                    SET status = 'pending', owner_id = NULL, lease_expires_at = NULL,
                        updated_at = now()
                    WHERE task_id = $1 AND status = 'running' AND owner_id IS NOT NULL;
                    """,
                    task_id,
                )
        return await self.progress(task_id)

    async def progress(self, task_id: str) -> dict[str, Any]:
        """Everything the task page needs, including why it is not moving."""

        async with self._pool.acquire() as connection:
            task = await connection.fetchrow(
                """
                SELECT task_id, status, current_depth, max_depth, timeline_enabled,
                       requests_attempted, max_requests, nodes_created, max_nodes,
                       edges_created, max_edges, pages_produced, pages_processed,
                       min_followers_to_expand, created_at, updated_at
                FROM crawl_tasks WHERE task_id = $1;
                """,
                task_id,
            )
            if task is None:
                raise LookupError(f"unknown task {task_id}")
            frontier = await connection.fetch(
                """
                SELECT operation, status, count(*) AS n
                FROM crawl_frontier WHERE task_id = $1 GROUP BY operation, status;
                """,
                task_id,
            )
            errors = await connection.fetch(
                """
                SELECT COALESCE(last_error_class, 'none') AS error_class, count(*) AS n
                FROM crawl_frontier
                WHERE task_id = $1 AND last_error_class IS NOT NULL
                GROUP BY 1 ORDER BY 2 DESC LIMIT 10;
                """,
                task_id,
            )
            waiting = await connection.fetchrow(
                """
                SELECT count(*) AS n, min(not_before) AS earliest
                FROM crawl_frontier
                WHERE task_id = $1 AND status = 'retryable' AND not_before > now();
                """,
                task_id,
            )
        layer = await self._layers.layer_state(task_id)
        data = dict(task)
        data["layer"] = {
            "depth": layer.depth,
            "closed": layer.closed,
            "blocked_by": list(layer.blocked_by),
            "pending": layer.pending,
            "running": layer.running,
            "retryable": layer.retryable,
            "in_flight_requests": layer.in_flight_requests,
            "unpublished_pages": layer.unpublished_pages,
            "unparsed_pages": layer.unparsed_pages,
        }
        data["frontier"] = [dict(r) for r in frontier]
        data["errors"] = [dict(r) for r in errors]
        data["rate_limit_wait"] = dict(waiting) if waiting else {"n": 0, "earliest": None}
        # The backlog is the third completion condition; showing only the
        # frontier would let a task look finished while pages are unparsed.
        data["parser_backlog"] = int(task["pages_produced"]) - int(task["pages_processed"])
        data["expansion_complete"] = await self._layers.expansion_complete(task_id)
        data["layer_metrics"] = [
            {
                "depth": m.depth,
                "nodes": m.nodes,
                "boundary_nodes": m.boundary_nodes,
                "edges": m.edges,
                "observations": m.observations,
                "collisions": m.collisions,
                "filtered": m.filtered,
                "expansions_complete": m.expansions_complete,
                "requests": m.requests,
            }
            for m in await self._layers.layer_metrics(task_id)
        ]
        data["coverage"] = await self._layers.coverage(task_id)
        return data
