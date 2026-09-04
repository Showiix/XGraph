"""Moves durable outbox rows onto the raw page stream."""

from dataclasses import dataclass
from uuid import uuid4

from loguru import logger

from xgraph.storage.events import OutboxRow, PostgresEventStore

from .broker import Producer
from .events import RawPageEvent
from .topics import RAW_TOPIC

#: Publishing is retried this many times before the row is dead-lettered. A page
#: the broker keeps refusing must stop consuming publisher capacity, but it must
#: also stay visible: it was paid for with X quota and cannot be re-fetched free.
DEFAULT_MAX_PUBLISH_ATTEMPTS = 5


@dataclass(frozen=True, slots=True)
class PublishResult:
    claimed: int
    published: int
    deferred: int
    dead_lettered: int


class OutboxPublisher:
    """Publishes committed pages, at least once.

    The publisher can crash between `send` and `mark_published`, which republishes
    the page on the next pass. That is deliberate: the alternative — marking the
    row published first — loses the page if the send then fails, and the database
    and the broker have no shared transaction to make the pair atomic. Consumers
    absorb the duplicate through `processed_events`.
    """

    def __init__(
        self,
        store: PostgresEventStore,
        producer: Producer,
        *,
        topic: str = RAW_TOPIC,
        owner_id: str | None = None,
        batch_size: int = 20,
        max_attempts: int = DEFAULT_MAX_PUBLISH_ATTEMPTS,
        backoff_seconds: int = 5,
    ) -> None:
        self._store = store
        self._producer = producer
        self._topic = topic
        self._owner_id = owner_id or f"publisher-{uuid4()}"
        self._batch_size = batch_size
        self._max_attempts = max_attempts
        self._backoff_seconds = backoff_seconds

    @property
    def owner_id(self) -> str:
        return self._owner_id

    async def run_once(self) -> PublishResult:
        rows = await self._store.claim_unpublished(owner_id=self._owner_id, limit=self._batch_size)
        published = deferred = dead_lettered = 0
        for row in rows:
            event = _to_event(row)
            try:
                await self._producer.send(self._topic, key=event.key, value=event.serialize())
            except Exception as error:  # noqa: BLE001 - the broker's failures are not ours to type
                detail = f"{type(error).__name__}: {error}"
                if row.publish_attempt >= self._max_attempts:
                    await self._store.dead_letter_outbox(
                        row.task_id, row.event_id, error_class="publish_failed", detail=detail
                    )
                    dead_lettered += 1
                    logger.error(
                        f"outbox row dead-lettered after {row.publish_attempt} attempts: {detail}"
                    )
                else:
                    await self._store.defer_publish(
                        row.task_id,
                        row.event_id,
                        backoff_seconds=self._backoff_seconds,
                        error=detail,
                    )
                    deferred += 1
                continue
            await self._store.mark_published(row.task_id, row.event_id)
            published += 1
        return PublishResult(
            claimed=len(rows),
            published=published,
            deferred=deferred,
            dead_lettered=dead_lettered,
        )


def _to_event(row: OutboxRow) -> RawPageEvent:
    return RawPageEvent(
        event_id=row.event_id,
        task_id=row.task_id,
        account_id=row.account_id,
        operation=row.operation,
        payload=row.payload,
        schema_version=row.schema_version,
        tree_id=row.tree_id,
        depth=row.depth,
        cursor_in=row.cursor_in,
        cursor_out=row.cursor_out,
        status_code=row.status_code,
        requested_at=row.requested_at,
        received_at=row.received_at,
        rate_limit=row.rate_limit,
    )
