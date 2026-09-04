"""PostgreSQL side of the raw page pipeline.

Three responsibilities live here, and all three exist because the broker and the
database cannot share a transaction:

* the **outbox**, so a page that was paid for with X quota is durable before
  anyone tries to publish it;
* **processed_events**, so a redelivered page is applied once;
* **consumer_offsets**, so advancing the stream and writing the graph commit
  together, fenced by an assignment epoch.
"""

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from xgraph.domain import Operation


class AsyncPool(Protocol):
    def acquire(self) -> Any: ...


@dataclass(frozen=True, slots=True)
class OutboxRow:
    task_id: str
    event_id: str
    account_id: str
    operation: Operation
    payload: dict[str, Any]
    schema_version: int
    tree_id: str | None
    depth: int | None
    cursor_in: str | None
    cursor_out: str | None
    status_code: int | None
    requested_at: datetime | None
    received_at: datetime | None
    rate_limit: dict[str, Any] | None
    publish_attempt: int


@dataclass(frozen=True, slots=True)
class PipelineStats:
    """The numbers that say whether the pipeline is healthy or merely quiet.

    `backlog` is the third completion condition: frontier empty and no request
    in flight still does not mean finished while pages sit unparsed.
    """

    pages_produced: int
    pages_processed: int
    outbox_pending: int
    outbox_dead_lettered: int
    oldest_unpublished_seconds: float | None
    dlq_events: int

    @property
    def backlog(self) -> int:
        return self.pages_produced - self.pages_processed

    @property
    def drained(self) -> bool:
        return self.backlog == 0 and self.outbox_pending == 0


class FencedConsumerError(RuntimeError):
    """Raised when a consumer writes for a partition it no longer owns."""


class UnknownTaskError(LookupError):
    """The event belongs to a task that no longer exists.

    The raw stream outlives the tasks that produced it: retention is measured in
    weeks, a task can be deleted at any time, and a replay group reads whatever
    is still retained. An event for a deleted task is therefore normal, and
    terminal — there is nothing to write it against, and no retry will help.
    """


def _json(value: Any) -> str | None:
    return json.dumps(value, separators=(",", ":")) if value is not None else None


class PostgresEventStore:
    def __init__(self, pool: AsyncPool) -> None:
        self._pool = pool
        # Epoch of the assignment this process currently holds, per partition.
        # Repopulated on every assignment, so a restart cannot reuse a stale one.
        self._epochs: dict[tuple[str, str, int], int] = {}

    # --- producer side -------------------------------------------------

    async def record_page(
        self,
        event: Any,
        *,
        frontier_id: int | None = None,
        next_cursor: str | None = None,
        attempt_id: int | None = None,
        checkpoint_owner: str | None = None,
    ) -> bool:
        """Persist one observed page and everything that depends on it.

        The outbox row, the pagination checkpoint, the request outcome and the
        produced counter are written in a single transaction. Splitting them
        would let the crawl advance its cursor past a page that was never
        stored, and the page cannot be re-fetched for free.

        Returns False when the page was already recorded, which is what a retry
        of the same `(account, cursor)` looks like.
        """

        async with self._pool.acquire() as connection, connection.transaction():
            inserted = await connection.execute(
                """
                INSERT INTO raw_page_outbox(
                    event_id, task_id, frontier_id, schema_version, tree_id, account_id,
                    operation, depth, cursor_in, cursor_out, status_code,
                    requested_at, received_at, rate_limit, payload)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14::jsonb,
                        $15::jsonb)
                ON CONFLICT (task_id, event_id) DO NOTHING;
                """,
                event.event_id,
                event.task_id,
                frontier_id,
                event.schema_version,
                event.tree_id,
                event.account_id,
                event.operation.value,
                event.depth,
                event.cursor_in,
                event.cursor_out,
                event.status_code,
                event.requested_at,
                event.received_at,
                _json(event.rate_limit),
                _json(event.payload),
            )
            if str(inserted) != "INSERT 0 1":
                return False

            await connection.execute(
                """
                UPDATE crawl_tasks
                SET pages_produced = pages_produced + 1, updated_at = now()
                WHERE task_id = $1;
                """,
                event.task_id,
            )
            if frontier_id is not None and next_cursor is not None:
                checkpoint = await connection.execute(
                    """
                    UPDATE crawl_frontier
                    SET cursor_in = $2, updated_at = now()
                    WHERE frontier_id = $1
                      AND ($3::text IS NULL OR owner_id = $3);
                    """,
                    frontier_id,
                    next_cursor,
                    checkpoint_owner,
                )
                if str(checkpoint) == "UPDATE 0":
                    raise RuntimeError("frontier lease was lost before the page was recorded")
            if attempt_id is not None:
                await connection.execute(
                    """
                    UPDATE request_attempts
                    SET outcome = 'succeeded', status_code = $2, finished_at = now()
                    WHERE attempt_id = $1 AND outcome = 'started';
                    """,
                    attempt_id,
                    event.status_code,
                )
        return True

    async def claim_unpublished(
        self, *, owner_id: str, limit: int = 20, lease_seconds: int = 60
    ) -> list[OutboxRow]:
        """Take a batch of unpublished pages, leased so publishers do not overlap."""

        if limit < 1:
            raise ValueError("limit must be positive")
        async with self._pool.acquire() as connection, connection.transaction():
            rows = await connection.fetch(
                """
                WITH candidate AS (
                    SELECT task_id, event_id
                    FROM raw_page_outbox
                    WHERE published_at IS NULL
                      AND dead_lettered_at IS NULL
                      AND not_before <= now()
                      AND (publish_lease_expires_at IS NULL OR publish_lease_expires_at <= now())
                    ORDER BY created_at, task_id, event_id
                    FOR UPDATE SKIP LOCKED
                    LIMIT $2
                )
                UPDATE raw_page_outbox AS o
                SET publish_owner = $1,
                    publish_lease_expires_at = now() + ($3 * interval '1 second'),
                    publish_attempt = o.publish_attempt + 1
                FROM candidate AS c
                WHERE o.task_id = c.task_id AND o.event_id = c.event_id
                RETURNING o.task_id, o.event_id, o.account_id, o.operation, o.payload,
                          o.schema_version, o.tree_id, o.depth, o.cursor_in, o.cursor_out,
                          o.status_code, o.requested_at, o.received_at, o.rate_limit,
                          o.publish_attempt;
                """,
                owner_id,
                limit,
                lease_seconds,
            )
        return [
            OutboxRow(
                task_id=str(row["task_id"]),
                event_id=str(row["event_id"]),
                account_id=str(row["account_id"]),
                operation=Operation(str(row["operation"])),
                payload=_loads(row["payload"]),
                schema_version=int(row["schema_version"]),
                tree_id=row["tree_id"],
                depth=row["depth"],
                cursor_in=row["cursor_in"],
                cursor_out=row["cursor_out"],
                status_code=row["status_code"],
                requested_at=row["requested_at"],
                received_at=row["received_at"],
                rate_limit=_loads(row["rate_limit"]) if row["rate_limit"] is not None else None,
                publish_attempt=int(row["publish_attempt"]),
            )
            for row in rows
        ]

    async def mark_published(self, task_id: str, event_id: str) -> None:
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                UPDATE raw_page_outbox
                SET published_at = now(), publish_owner = NULL, publish_lease_expires_at = NULL
                WHERE task_id = $1 AND event_id = $2 AND published_at IS NULL;
                """,
                task_id,
                event_id,
            )

    async def defer_publish(
        self, task_id: str, event_id: str, *, backoff_seconds: int, error: str
    ) -> None:
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                UPDATE raw_page_outbox
                SET not_before = now() + ($3 * interval '1 second'),
                    publish_owner = NULL,
                    publish_lease_expires_at = NULL,
                    last_error = $4
                WHERE task_id = $1 AND event_id = $2;
                """,
                task_id,
                event_id,
                backoff_seconds,
                error[:500],
            )

    async def dead_letter_outbox(
        self, task_id: str, event_id: str, *, error_class: str, detail: str
    ) -> None:
        """Stop retrying a page that the broker will never accept."""

        async with self._pool.acquire() as connection, connection.transaction():
            await connection.execute(
                """
                UPDATE raw_page_outbox
                SET dead_lettered_at = now(),
                    publish_owner = NULL,
                    publish_lease_expires_at = NULL,
                    last_error = $3
                WHERE task_id = $1 AND event_id = $2;
                """,
                task_id,
                event_id,
                detail[:500],
            )
            await connection.execute(
                """
                INSERT INTO dlq_events(task_id, event_id, topic, error_class, error_detail)
                VALUES ($1, $2, 'outbox', $3, $4);
                """,
                task_id,
                event_id,
                error_class,
                detail[:2000],
            )

    # --- consumer side -------------------------------------------------

    async def acquire_partitions(
        self, *, group_id: str, owner_id: str, partitions: list[tuple[str, int]]
    ) -> dict[tuple[str, int], int]:
        """Take ownership of partitions and return the offsets to resume from.

        Bumping `assignment_epoch` here is what fences the previous owner: it
        may still be mid-batch, but every write it attempts carries the old
        epoch and will match zero rows.
        """

        resumed: dict[tuple[str, int], int] = {}
        async with self._pool.acquire() as connection, connection.transaction():
            for topic, partition in partitions:
                row = await connection.fetchrow(
                    """
                    INSERT INTO consumer_offsets(group_id, topic, partition, owner_id,
                                                 assignment_epoch)
                    VALUES ($1, $2, $3, $4, 1)
                    ON CONFLICT (group_id, topic, partition) DO UPDATE SET
                        owner_id = excluded.owner_id,
                        assignment_epoch = consumer_offsets.assignment_epoch + 1,
                        updated_at = now()
                    RETURNING committed_offset, assignment_epoch;
                    """,
                    group_id,
                    topic,
                    partition,
                    owner_id,
                )
                resumed[(topic, partition)] = int(row["committed_offset"])
                self._epochs[(group_id, topic, partition)] = int(row["assignment_epoch"])
        return resumed

    def epoch(self, group_id: str, topic: str, partition: int) -> int:
        return self._epochs.get((group_id, topic, partition), -1)

    async def apply_event(
        self,
        *,
        group_id: str,
        owner_id: str,
        topic: str,
        partition: int,
        offset: int,
        task_id: str,
        event_id: str,
        schema_version: int,
        handler: Any = None,
    ) -> bool:
        """Register, apply and acknowledge one event in a single transaction.

        Returns False when the event was already processed by this group.

        The offset moves inside the same transaction as the business writes, so
        there is no window where the stream has advanced past work that was
        never done, and none where finished work is replayed as new.
        """

        async with self._pool.acquire() as connection, connection.transaction():
            if not await self._advance_offset(
                connection,
                group_id=group_id,
                owner_id=owner_id,
                topic=topic,
                partition=partition,
                offset=offset,
            ):
                return False

            # Checked explicitly rather than left to the foreign key: a replayed
            # event for a deleted task is an expected condition, and a constraint
            # violation would surface it as a crash mid-batch.
            if not await connection.fetchval(
                "SELECT true FROM crawl_tasks WHERE task_id = $1", task_id
            ):
                raise UnknownTaskError(task_id)

            registered = await connection.execute(
                """
                INSERT INTO processed_events(task_id, group_id, event_id, schema_version)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (group_id, task_id, event_id) DO NOTHING;
                """,
                task_id,
                group_id,
                event_id,
                schema_version,
            )
            if str(registered) != "INSERT 0 1":
                return False

            if handler is not None:
                await handler(connection)

            await connection.execute(
                """
                UPDATE crawl_tasks
                SET pages_processed = pages_processed + 1, updated_at = now()
                WHERE task_id = $1;
                """,
                task_id,
            )
        return True

    async def _advance_offset(
        self,
        connection: Any,
        *,
        group_id: str,
        owner_id: str,
        topic: str,
        partition: int,
        offset: int,
    ) -> bool:
        """Move the committed offset forward, or explain why it did not.

        Returns False for a redelivery of an offset already behind us. Raises
        when the partition has been reassigned, because in that case the caller
        must not write anything at all.
        """

        epoch = self.epoch(group_id, topic, partition)
        moved = await connection.execute(
            """
            UPDATE consumer_offsets
            SET committed_offset = $5, updated_at = now()
            WHERE group_id = $1 AND topic = $2 AND partition = $3
              AND owner_id = $4 AND assignment_epoch = $6
              AND committed_offset < $5;
            """,
            group_id,
            topic,
            partition,
            owner_id,
            offset,
            epoch,
        )
        if str(moved) != "UPDATE 0":
            return True
        if await self._still_owns(connection, group_id, topic, partition, owner_id, epoch):
            # Still ours; the offset just did not move backwards. That is a
            # redelivery, not a fencing failure.
            return False
        raise FencedConsumerError(
            f"{owner_id} no longer owns {topic}/{partition} for group {group_id}"
        )

    async def skip_offset(
        self, *, group_id: str, owner_id: str, topic: str, partition: int, offset: int
    ) -> bool:
        """Step past a message that carries no usable task context.

        An undecodable message has no `task_id`, so it cannot be booked against
        a task. The offset still has to move, or the partition stalls behind a
        message that can never succeed.
        """

        async with self._pool.acquire() as connection, connection.transaction():
            return await self._advance_offset(
                connection,
                group_id=group_id,
                owner_id=owner_id,
                topic=topic,
                partition=partition,
                offset=offset,
            )

    @staticmethod
    async def _still_owns(
        connection: Any, group_id: str, topic: str, partition: int, owner_id: str, epoch: int
    ) -> bool:
        row = await connection.fetchrow(
            """
            SELECT 1 FROM consumer_offsets
            WHERE group_id = $1 AND topic = $2 AND partition = $3
              AND owner_id = $4 AND assignment_epoch = $5;
            """,
            group_id,
            topic,
            partition,
            owner_id,
            epoch,
        )
        return row is not None

    async def record_dlq(
        self,
        *,
        topic: str,
        error_class: str,
        detail: str,
        task_id: str | None = None,
        group_id: str | None = None,
        event_id: str | None = None,
        partition: int | None = None,
        offset: int | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO dlq_events(task_id, group_id, event_id, topic, partition,
                                       "offset", error_class, error_detail, payload)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb);
                """,
                task_id,
                group_id,
                event_id,
                topic,
                partition,
                offset,
                error_class,
                detail[:2000],
                _json(payload),
            )

    # --- observability ---------------------------------------------------

    async def pipeline_stats(self, task_id: str | None = None) -> PipelineStats:
        """Snapshot of the raw page pipeline, per task or across all of them."""

        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT
                    COALESCE((SELECT sum(pages_produced) FROM crawl_tasks
                              WHERE $1::text IS NULL OR task_id = $1), 0) AS produced,
                    COALESCE((SELECT sum(pages_processed) FROM crawl_tasks
                              WHERE $1::text IS NULL OR task_id = $1), 0) AS processed,
                    (SELECT count(*) FROM raw_page_outbox
                     WHERE published_at IS NULL AND dead_lettered_at IS NULL
                       AND ($1::text IS NULL OR task_id = $1)) AS pending,
                    (SELECT count(*) FROM raw_page_outbox
                     WHERE dead_lettered_at IS NOT NULL
                       AND ($1::text IS NULL OR task_id = $1)) AS dead_lettered,
                    (SELECT extract(epoch FROM now() - min(created_at)) FROM raw_page_outbox
                     WHERE published_at IS NULL AND dead_lettered_at IS NULL
                       AND ($1::text IS NULL OR task_id = $1)) AS oldest_seconds,
                    (SELECT count(*) FROM dlq_events
                     WHERE $1::text IS NULL OR task_id = $1) AS dlq;
                """,
                task_id,
            )
        oldest = row["oldest_seconds"]
        return PipelineStats(
            pages_produced=int(row["produced"]),
            pages_processed=int(row["processed"]),
            outbox_pending=int(row["pending"]),
            outbox_dead_lettered=int(row["dead_lettered"]),
            oldest_unpublished_seconds=float(oldest) if oldest is not None else None,
            dlq_events=int(row["dlq"]),
        )

    async def committed_offsets(self, group_id: str) -> dict[tuple[str, int], int]:
        """Where each partition of a group stands, for comparison against the broker."""

        async with self._pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT topic, partition, committed_offset
                FROM consumer_offsets
                WHERE group_id = $1
                ORDER BY topic, partition;
                """,
                group_id,
            )
        return {(str(r["topic"]), int(r["partition"])): int(r["committed_offset"]) for r in rows}


def _loads(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value
