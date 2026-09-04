"""Broker-neutral contracts for the raw page stream.

XGraph talks to the broker through these three types only. Everything that
decides correctness — offsets, idempotency, task state — lives in PostgreSQL,
so swapping the implementation cannot change what the system guarantees.
"""

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class Message:
    topic: str
    partition: int
    offset: int
    key: str | None
    value: bytes


#: (topic, partition)
TopicPartition = tuple[str, int]

#: The first and last offset the broker still holds for a partition. The
#: committed offsets live in PostgreSQL and therefore outlive the log they point
#: into, so the handler is given the log's real range to reconcile against.
LogBounds = dict[TopicPartition, tuple[int, int]]

#: Called when partitions are assigned. Returns the offset already committed for
#: each partition; the consumer resumes from the next one. This is where the
#: runtime takes ownership of a partition and invalidates the previous owner.
AssignmentHandler = Callable[
    [Sequence[TopicPartition], LogBounds], Awaitable[dict[TopicPartition, int]]
]


class Producer(Protocol):
    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def send(self, topic: str, *, key: str | None, value: bytes) -> None:
        """Publish one message and wait for the broker to acknowledge it.

        Must not return before the write is durable: the outbox row is marked
        published straight afterwards, and an unacknowledged send would drop a
        page that the database already believes has left the building.
        """
        ...


class Consumer(Protocol):
    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def poll(self, *, timeout_ms: int = 1000, max_records: int = 100) -> list[Message]: ...
