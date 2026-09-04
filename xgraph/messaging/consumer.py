"""Applies raw page events to PostgreSQL, once each, fenced by assignment epoch."""

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Protocol
from uuid import uuid4

from loguru import logger

from xgraph.storage.events import (
    FencedConsumerError,
    PostgresEventStore,
    UnknownTaskError,
)

from .broker import Consumer, LogBounds, Message, Producer, TopicPartition
from .events import RawPageEvent
from .topics import DLQ_TOPIC, PARSER_GROUP


class PermanentEventError(Exception):
    """An event that will fail identically no matter how many times it is retried.

    Structural drift and undecodable payloads belong here. Retrying them buys
    nothing and, worse, stalls the partition behind them.
    """


class PageHandler(Protocol):
    # Positional-only: the runtime always calls the handler by position, and
    # binding the parameter names here would force every implementation to
    # copy them.
    async def __call__(self, connection: Any, event: RawPageEvent, /) -> None:
        """Write the graph rows this page implies, inside the caller's transaction.

        The connection is already inside the transaction that registers the
        event and moves the offset, so raising here rolls all three back.
        """
        ...


@dataclass(frozen=True, slots=True)
class ConsumeResult:
    polled: int
    applied: int
    duplicates: int
    dead_lettered: int


class ParserRuntime:
    """Drives one consumer: poll, apply transactionally, dead-letter what cannot land.

    Ownership is re-taken on every assignment, and every write carries the epoch
    from that assignment. A consumer that lost its partitions but is still
    mid-batch therefore cannot advance the offset or write the graph.
    """

    def __init__(
        self,
        store: PostgresEventStore,
        handler: PageHandler,
        *,
        group_id: str = PARSER_GROUP,
        owner_id: str | None = None,
        dlq_producer: Producer | None = None,
        dlq_topic: str = DLQ_TOPIC,
    ) -> None:
        self._store = store
        self._consumer: Consumer | None = None
        self._handler = handler
        self._group_id = group_id
        self._owner_id = owner_id or f"parser-{uuid4()}"
        self._dlq_producer = dlq_producer
        self._dlq_topic = dlq_topic

    @property
    def owner_id(self) -> str:
        return self._owner_id

    @property
    def group_id(self) -> str:
        return self._group_id

    def attach(self, consumer: Consumer) -> None:
        """Bind the consumer to poll from.

        The consumer needs `on_assign` at construction and the runtime needs the
        consumer to poll, so one of the two has to be wired afterwards. Doing it
        here keeps the assignment callback owned by the runtime that fences on it.
        """

        self._consumer = consumer

    async def on_assign(
        self, partitions: Sequence[TopicPartition], bounds: LogBounds | None = None
    ) -> dict[TopicPartition, int]:
        """Assignment callback: claim the partitions and report where to resume."""

        committed = await self._store.acquire_partitions(
            group_id=self._group_id,
            owner_id=self._owner_id,
            partitions=list(partitions),
            bounds=bounds or {},
        )
        logger.debug(f"{self._owner_id} took {len(committed)} partition(s) for {self._group_id}")
        return committed

    async def run_once(self, *, timeout_ms: int = 1000, max_records: int = 100) -> ConsumeResult:
        if self._consumer is None:
            raise RuntimeError("attach a consumer before running the parser")
        messages = await self._consumer.poll(timeout_ms=timeout_ms, max_records=max_records)
        applied = duplicates = dead_lettered = 0
        for message in messages:
            outcome = await self.handle(message)
            if outcome == "applied":
                applied += 1
            elif outcome == "duplicate":
                duplicates += 1
            else:
                dead_lettered += 1
        return ConsumeResult(
            polled=len(messages),
            applied=applied,
            duplicates=duplicates,
            dead_lettered=dead_lettered,
        )

    async def handle(self, message: Message) -> str:
        try:
            event = RawPageEvent.deserialize(message.value)
        except Exception as error:  # noqa: BLE001 - anything undecodable is terminal
            await self._dead_letter(
                message, "undecodable_event", f"{type(error).__name__}: {error}"
            )
            # No task context, so the message cannot be booked against a task;
            # the offset still has to move or the partition stalls here forever.
            await self._store.skip_offset(
                group_id=self._group_id,
                owner_id=self._owner_id,
                topic=message.topic,
                partition=message.partition,
                offset=message.offset,
            )
            return "dead_lettered"

        async def apply(connection: Any) -> None:
            await self._handler(connection, event)

        try:
            written = await self._store.apply_event(
                group_id=self._group_id,
                owner_id=self._owner_id,
                topic=message.topic,
                partition=message.partition,
                offset=message.offset,
                task_id=event.task_id,
                event_id=event.event_id,
                schema_version=event.schema_version,
                handler=apply,
            )
        except FencedConsumerError:
            # Losing the partition is not this event's fault; the new owner will
            # deliver it again. Re-raise so the caller stops the batch.
            raise
        except UnknownTaskError:
            # Nothing to write it against. Move past it so a replay of retained
            # history is not stopped by a task somebody deleted.
            await self._dead_letter(
                message,
                "unknown_task",
                f"task {event.task_id} is gone",
                event=event,
                book=False,
            )
            await self._store.skip_offset(
                group_id=self._group_id,
                owner_id=self._owner_id,
                topic=message.topic,
                partition=message.partition,
                offset=message.offset,
            )
            return "dead_lettered"
        except PermanentEventError as error:
            # The failing transaction rolled back, so nothing was recorded. Book
            # the page as terminally handled: it must count towards completion
            # and must not be redelivered forever.
            await self._dead_letter(message, "permanent_parse_error", str(error), event=event)
            return "dead_lettered"
        return "applied" if written else "duplicate"

    async def _dead_letter(
        self,
        message: Message,
        error_class: str,
        detail: str,
        *,
        event: RawPageEvent | None = None,
        book: bool = True,
    ) -> None:
        """Record a terminal failure, and normally book it against its task.

        `book` separates the two halves. Recording the failure always works;
        booking it needs a task to book against, which is precisely what the
        `unknown_task` case does not have. That caller moves the offset itself.
        """

        await self._store.record_dlq(
            topic=message.topic,
            partition=message.partition,
            offset=message.offset,
            error_class=error_class,
            detail=detail,
            task_id=event.task_id if event else None,
            group_id=self._group_id,
            event_id=event.event_id if event else None,
        )
        if self._dlq_producer is not None:
            await self._dlq_producer.send(
                self._dlq_topic, key=event.key if event else None, value=message.value
            )
        logger.error(f"event dead-lettered ({error_class}): {detail}")
        if event is not None and book:
            # Terminal, but still accounted for: register it with no handler so
            # the offset moves and the page counts towards completion.
            await self._store.apply_event(
                group_id=self._group_id,
                owner_id=self._owner_id,
                topic=message.topic,
                partition=message.partition,
                offset=message.offset,
                task_id=event.task_id,
                event_id=event.event_id,
                schema_version=event.schema_version,
                handler=None,
            )
