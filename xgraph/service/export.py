"""Exports that carry their own provenance.

A CSV that says only what matched is unusable a week later: nobody remembers
which task it came from, which filters produced it, or whether the crawl had
finished. Every export therefore states the task, the filter as applied, the
generation time and the data-quality summary, and the row count it claims is
the row count it contains.
"""

import csv
import io
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

from .queries import AccountFilter, EdgeFilter, ProductQueries

#: Exports page through the query layer instead of loading everything at once,
#: so a large result costs memory proportional to a page, not to the task.
_BATCH = 500

ACCOUNT_COLUMNS = (
    "account_id",
    "username",
    "display_name",
    "first_depth",
    "seed_of_tree",
    "trees",
    "network_indegree",
    "observation_count",
    "collision_count",
    "followers_count",
    "following_count",
    "can_dm",
    "verified",
    "blue_verified",
    "protected",
    "location",
    "description",
    "expansion_status",
    "declared_following",
    "collected_following",
    "coverage_ratio",
    "termination_reason",
    "is_l6_boundary",
    "timeline_status",
    "candidate_reasons",
    "sample_count",
    "scanned_count",
    "sample_span_days",
    "latest_post_at",
    "avg_reply",
    "avg_retweet",
    "avg_like",
    "avg_view",
    "view_sample_count",
    "median_engagement",
    "engagement_rate",
    "reach_ratio",
    "bookmark_rate",
    "warnings",
)

EDGE_COLUMNS = (
    "source_account_id",
    "target_account_id",
    "source_depth",
    "target_depth",
    "trees",
    "is_collision",
    "is_l6_boundary",
    "first_seen_at",
)

POST_COLUMNS = (
    "account_id",
    "post_id",
    "kind",
    "is_qualifying",
    "posted_at",
    "reply_count",
    "retweet_count",
    "like_count",
    "quote_count",
    "bookmark_count",
    "view_count",
    "text",
)


@dataclass(frozen=True, slots=True)
class ExportManifest:
    """What this file is, so it can be read without asking anyone."""

    task_id: str
    dataset: str
    filters: dict[str, Any]
    generated_at: datetime
    row_count: int
    task_status: str
    expansion_complete: bool
    parser_backlog: int
    coverage: dict[str, Any]

    @property
    def as_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "dataset": self.dataset,
            "filters": self.filters,
            "generated_at": self.generated_at.isoformat(),
            "row_count": self.row_count,
            "task_status": self.task_status,
            "expansion_complete": self.expansion_complete,
            "parser_backlog": self.parser_backlog,
            "coverage": self.coverage,
        }


class ExportService:
    def __init__(self, queries: ProductQueries, pool: Any) -> None:
        self._queries = queries
        self._pool = pool

    async def _context(self, task_id: str) -> dict[str, Any]:
        from xgraph.storage.layers import PostgresLayerStore

        layers = PostgresLayerStore(self._pool)
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                "SELECT status, pages_produced, pages_processed FROM crawl_tasks "
                "WHERE task_id = $1",
                task_id,
            )
        if row is None:
            raise LookupError(f"unknown task {task_id}")
        return {
            "task_status": str(row["status"]),
            "parser_backlog": int(row["pages_produced"]) - int(row["pages_processed"]),
            "expansion_complete": await layers.expansion_complete(task_id),
            "coverage": await layers.coverage(task_id),
        }

    async def accounts(self, task_id: str, f: AccountFilter) -> tuple[list[dict], ExportManifest]:
        rows: list[dict[str, Any]] = []
        offset = f.offset
        while True:
            page = await self._queries.accounts(
                task_id, _with_paging(f, limit=_BATCH, offset=offset)
            )
            rows.extend(page.items)
            offset += len(page.items)
            if not page.items or offset >= page.total:
                break
        return rows, ExportManifest(
            task_id=task_id,
            dataset="accounts",
            filters=f.as_dict,
            generated_at=datetime.now(timezone.utc),
            row_count=len(rows),
            **await self._context(task_id),
        )

    async def relationships(self, task_id: str, f: EdgeFilter) -> tuple[list[dict], ExportManifest]:
        rows: list[dict[str, Any]] = []
        offset = f.offset
        while True:
            page = await self._queries.relationships(
                task_id, _with_paging(f, limit=_BATCH, offset=offset)
            )
            rows.extend(page.items)
            offset += len(page.items)
            if not page.items or offset >= page.total:
                break
        return rows, ExportManifest(
            task_id=task_id,
            dataset="relationships",
            filters=f.as_dict,
            generated_at=datetime.now(timezone.utc),
            row_count=len(rows),
            **await self._context(task_id),
        )

    async def posts(self, task_id: str) -> tuple[list[dict], ExportManifest]:
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT account_id, post_id, kind, is_qualifying, posted_at, reply_count,
                       retweet_count, like_count, quote_count, bookmark_count, view_count,
                       text
                FROM account_posts WHERE task_id = $1
                ORDER BY account_id, posted_at DESC NULLS LAST;
                """,
                task_id,
            )
        items = [dict(r) for r in rows]
        return items, ExportManifest(
            task_id=task_id,
            dataset="posts",
            filters={},
            generated_at=datetime.now(timezone.utc),
            row_count=len(items),
            **await self._context(task_id),
        )


def to_csv(rows: Iterable[dict[str, Any]], columns: tuple[str, ...]) -> str:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(columns), extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow({key: _cell(row.get(key)) for key in columns})
    return buffer.getvalue()


def to_json(rows: list[dict[str, Any]], manifest: ExportManifest) -> str:
    return json.dumps(
        {"manifest": manifest.as_dict, "rows": rows}, default=_cell, ensure_ascii=False
    )


def _cell(value: Any) -> Any:
    """Render a value for export.

    Account and post ids stay strings throughout: a spreadsheet that reads them
    as numbers silently rounds away the last digits of a snowflake id.
    """

    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (list, tuple)):
        return "|".join(str(item) for item in value)
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _with_paging(f: Any, *, limit: int, offset: int) -> Any:
    from dataclasses import replace

    return replace(f, limit=limit, offset=offset)
