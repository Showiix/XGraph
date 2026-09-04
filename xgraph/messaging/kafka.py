"""Kafka adapters built on aiokafka.

The consumer deliberately does not use Kafka's own offset storage. Offsets are
committed to PostgreSQL in the same transaction as the graph writes, because
the two systems have no shared transaction: committing the offset first loses a
page on a crash, and committing it last replays one. Only the database can make
"advance the offset" and "write the result" succeed or fail together.
"""

from collections.abc import Sequence
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRebalanceListener
from loguru import logger

from .broker import AssignmentHandler, LogBounds, Message, TopicPartition


class KafkaProducer:
    """Durable-ack producer for the raw page stream."""

    def __init__(
        self,
        bootstrap_servers: str,
        *,
        compression_type: str | None = "gzip",
        client_id: str = "xgraph-outbox-publisher",
        **kwargs: Any,
    ) -> None:
        # acks=all plus idempotence is the stage 7 deployment baseline: a page
        # that the outbox has marked published must survive a broker failover.
        # zstd compresses these payloads better but needs an extra codec, so the
        # default stays on a codec every broker already has.
        self._producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            acks="all",
            enable_idempotence=True,
            compression_type=compression_type,
            client_id=client_id,
            **kwargs,
        )

    async def start(self) -> None:
        await self._producer.start()

    async def stop(self) -> None:
        await self._producer.stop()

    async def send(self, topic: str, *, key: str | None, value: bytes) -> None:
        await self._producer.send_and_wait(
            topic, value=value, key=key.encode() if key is not None else None
        )


class KafkaConsumer:
    """Consumer that resumes from database-held offsets on every assignment."""

    def __init__(
        self,
        bootstrap_servers: str,
        *,
        group_id: str,
        topics: Sequence[str],
        on_assign: AssignmentHandler | None = None,
        client_id: str = "xgraph-parser",
        max_poll_interval_ms: int = 300_000,
        **kwargs: Any,
    ) -> None:
        self._topics = list(topics)
        self._on_assign = on_assign
        self._consumer = AIOKafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            client_id=client_id,
            # Offsets live in PostgreSQL; letting the broker also track them
            # would create a second, conflicting source of truth.
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            max_poll_interval_ms=max_poll_interval_ms,
            **kwargs,
        )

    async def start(self) -> None:
        await self._consumer.start()
        self._consumer.subscribe(self._topics, listener=_AssignmentListener(self))

    async def stop(self) -> None:
        await self._consumer.stop()

    async def poll(self, *, timeout_ms: int = 1000, max_records: int = 100) -> list[Message]:
        batches = await self._consumer.getmany(timeout_ms=timeout_ms, max_records=max_records)
        messages: list[Message] = []
        for tp, records in batches.items():
            for record in records:
                if record.value is None:
                    # A null value is a tombstone, not a page. Nothing writes
                    # them to this topic, but skipping keeps a stray one from
                    # failing the whole batch.
                    continue
                messages.append(
                    Message(
                        topic=tp.topic,
                        partition=tp.partition,
                        offset=record.offset,
                        key=record.key.decode() if record.key is not None else None,
                        value=record.value,
                    )
                )
        return messages

    async def _resume(self, assigned: Sequence[Any]) -> None:
        if self._on_assign is None:
            return
        keys: list[TopicPartition] = [(tp.topic, tp.partition) for tp in assigned]
        # The committed offsets live in PostgreSQL, so they outlive the log they
        # point into: retention deletes the segment under one, and a rebuilt topic
        # starts over at zero while the stored offset stays high. Seeking outside
        # the log raises nothing — the consumer simply waits for records that never
        # arrive — so the handler is given the real range to reconcile against.
        first = await self._consumer.beginning_offsets(list(assigned))
        last = await self._consumer.end_offsets(list(assigned))
        bounds: LogBounds = {
            (tp.topic, tp.partition): (int(first.get(tp, 0)), int(last.get(tp, 0)))
            for tp in assigned
        }
        committed = await self._on_assign(keys, bounds)
        for tp in assigned:
            offset = committed.get((tp.topic, tp.partition), -1)
            if offset < 0:
                # Nothing committed yet: start at the oldest retained page rather
                # than at the head, or the backlog produced before this consumer
                # existed would never be parsed.
                await self._consumer.seek_to_beginning(tp)
                continue
            low = bounds[(tp.topic, tp.partition)][0]
            self._consumer.seek(tp, max(offset + 1, low))
        logger.debug(f"resumed {len(keys)} partition(s) from database offsets")


class _AssignmentListener(ConsumerRebalanceListener):
    """Bridges aiokafka's rebalance callbacks to the database-held offsets."""

    def __init__(self, owner: KafkaConsumer) -> None:
        self._owner = owner

    async def on_partitions_revoked(self, revoked: Sequence[Any]) -> None:
        # Nothing to flush: every offset was already committed inside the same
        # transaction as its business writes.
        return None

    async def on_partitions_assigned(self, assigned: Sequence[Any]) -> None:
        await self._owner._resume(assigned)
