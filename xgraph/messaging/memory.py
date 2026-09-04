"""An in-process broker used by tests and by local runs without Kafka.

It models the parts that change XGraph's behaviour — partitions, monotonic
offsets, group assignment and redelivery of uncommitted messages — and nothing
else. It is not a Kafka emulator and must never back a real crawl.
"""

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass, field

from .broker import AssignmentHandler, Message, TopicPartition


@dataclass
class _Partition:
    messages: list[Message] = field(default_factory=list)


class InMemoryBroker:
    """A broker whose whole state is a dict of partitions."""

    def __init__(self, *, partitions: int = 2) -> None:
        if partitions < 1:
            raise ValueError("a topic needs at least one partition")
        self.partitions = partitions
        self._topics: dict[str, dict[int, _Partition]] = {}
        self._consumers: list[InMemoryConsumer] = []

    def _partition_for(self, topic: str, key: str | None) -> _Partition:
        index = 0 if key is None else hash(key) % self.partitions
        return self._topics.setdefault(topic, {}).setdefault(index, _Partition())

    def partition_index(self, key: str | None) -> int:
        return 0 if key is None else hash(key) % self.partitions

    def messages(self, topic: str) -> list[Message]:
        parts = self._topics.get(topic, {})
        return [m for index in sorted(parts) for m in parts[index].messages]

    async def append(self, topic: str, key: str | None, value: bytes) -> Message:
        partition = self._partition_for(topic, key)
        index = self.partition_index(key)
        message = Message(
            topic=topic, partition=index, offset=len(partition.messages), key=key, value=value
        )
        partition.messages.append(message)
        return message

    def producer(self) -> "InMemoryProducer":
        return InMemoryProducer(self)

    def consumer(
        self, *, topics: Sequence[str], on_assign: AssignmentHandler | None = None
    ) -> "InMemoryConsumer":
        consumer = InMemoryConsumer(self, topics=topics, on_assign=on_assign)
        self._consumers.append(consumer)
        return consumer


class InMemoryProducer:
    def __init__(self, broker: InMemoryBroker) -> None:
        self._broker = broker
        self.started = False
        #: Set to raise on the next send, to exercise publisher crash paths.
        self.fail_next: Exception | None = None

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.started = False

    async def send(self, topic: str, *, key: str | None, value: bytes) -> None:
        if self.fail_next is not None:
            error, self.fail_next = self.fail_next, None
            raise error
        await self._broker.append(topic, key, value)


class InMemoryConsumer:
    def __init__(
        self,
        broker: InMemoryBroker,
        *,
        topics: Sequence[str],
        on_assign: AssignmentHandler | None = None,
    ) -> None:
        self._broker = broker
        self._topics = list(topics)
        self._on_assign = on_assign
        self._positions: dict[TopicPartition, int] = {}
        self.assigned: list[TopicPartition] = []

    async def start(self) -> None:
        await self.assign_all()

    async def stop(self) -> None:
        self.assigned = []

    async def assign_all(self) -> None:
        await self.assign(
            [(topic, index) for topic in self._topics for index in range(self._broker.partitions)]
        )

    async def assign(self, partitions: Sequence[TopicPartition]) -> None:
        """Take ownership of partitions, mirroring a rebalance."""

        self.assigned = list(partitions)
        committed: dict[TopicPartition, int] = {}
        if self._on_assign is not None:
            committed = await self._on_assign(self.assigned)
        for tp in self.assigned:
            self._positions[tp] = committed.get(tp, -1) + 1

    async def poll(self, *, timeout_ms: int = 1000, max_records: int = 100) -> list[Message]:
        await asyncio.sleep(0)
        out: list[Message] = []
        for topic, index in self.assigned:
            partition = self._broker._topics.get(topic, {}).get(index)
            if partition is None:
                continue
            position = self._positions.get((topic, index), 0)
            for message in partition.messages[position:]:
                out.append(message)
                if len(out) >= max_records:
                    return out
        return out

    def advance(self, message: Message) -> None:
        """Move past a message the runtime finished with."""

        tp = (message.topic, message.partition)
        self._positions[tp] = max(self._positions.get(tp, 0), message.offset + 1)
