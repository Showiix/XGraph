"""Stage 3 pipeline semantics against an in-process broker.

These cover the parts that do not need a real broker: event shape, publisher
retry policy and the runtime's dead-letter behaviour. The delivery guarantees
themselves are asserted against real PostgreSQL and real Kafka in
`test_phase3_integration.py`, because they are properties of the transaction
boundary, not of this code.
"""

from dataclasses import replace
from datetime import datetime, timezone

import pytest

from xgraph.domain import Operation, PageEnvelope, RateLimitSnapshot
from xgraph.messaging import (
    DLQ_TOPIC,
    RAW_TOPIC,
    InMemoryBroker,
    Message,
    RawPageEvent,
    reparse_group,
)
from xgraph.messaging.publisher import OutboxPublisher
from xgraph.storage.events import OutboxRow


def envelope(**overrides: object) -> PageEnvelope:
    base = PageEnvelope(
        event_id="event-1",
        schema_version=1,
        operation=Operation.FOLLOWING,
        source_account_id="123",
        cursor_in=None,
        cursor_out="cursor-2",
        users=(),
        rate_limit=RateLimitSnapshot(
            limit=188, remaining=187, reset_at=datetime(2030, 1, 1, tzinfo=timezone.utc)
        ),
        status_code=200,
        requested_at=datetime(2026, 9, 4, tzinfo=timezone.utc),
        received_at=datetime(2026, 9, 4, tzinfo=timezone.utc),
        raw_payload={"data": {"user": {}}},
    )
    return replace(base, **overrides) if overrides else base


def outbox_row(**overrides: object) -> OutboxRow:
    base = OutboxRow(
        task_id="task-1",
        event_id="event-1",
        account_id="123",
        operation=Operation.FOLLOWING,
        payload={"data": {}},
        schema_version=1,
        tree_id="tree-a",
        depth=0,
        cursor_in=None,
        cursor_out="cursor-2",
        status_code=200,
        requested_at=None,
        received_at=None,
        rate_limit=None,
        publish_attempt=1,
    )
    return replace(base, **overrides) if overrides else base


class FakeStore:
    def __init__(self, rows: list[OutboxRow]) -> None:
        self._rows = rows
        self.published: list[str] = []
        self.deferred: list[tuple[str, str]] = []
        self.dead_lettered: list[tuple[str, str]] = []

    async def claim_unpublished(self, *, owner_id: str, limit: int = 20, lease_seconds: int = 60):
        rows, self._rows = self._rows[:limit], self._rows[limit:]
        return rows

    async def mark_published(self, task_id: str, event_id: str) -> None:
        self.published.append(event_id)

    async def defer_publish(self, task_id, event_id, *, backoff_seconds, error) -> None:
        self.deferred.append((event_id, error))

    async def dead_letter_outbox(self, task_id, event_id, *, error_class, detail) -> None:
        self.dead_lettered.append((event_id, error_class))


def test_event_round_trips_without_losing_the_payload() -> None:
    event = RawPageEvent.from_envelope(envelope(), task_id="task-1", tree_id="tree-a", depth=2)
    restored = RawPageEvent.deserialize(event.serialize())

    assert restored == event
    assert restored.payload == {"data": {"user": {}}}
    assert restored.operation is Operation.FOLLOWING
    assert restored.rate_limit == {
        "limit": 188,
        "remaining": 187,
        "reset_at": "2030-01-01T00:00:00+00:00",
    }


def test_event_carries_no_parsed_interpretation() -> None:
    """Shipping today's parse alongside the bytes would freeze the parser version."""

    body = RawPageEvent.from_envelope(envelope(), task_id="task-1").to_dict()

    assert "users" not in body
    assert "tweets" not in body
    assert body["payload"] == {"data": {"user": {}}}


def test_event_is_keyed_by_account_so_one_chain_stays_ordered() -> None:
    event = RawPageEvent.from_envelope(envelope(), task_id="task-1")
    assert event.key == "123"

    broker = InMemoryBroker(partitions=4)
    keys = {broker.partition_index(f"account-{i}") for i in range(50)}
    assert broker.partition_index("123") == broker.partition_index("123")
    assert len(keys) > 1, "all accounts landing on one partition removes the parallelism"


def test_reparse_group_is_distinct_from_the_live_parser() -> None:
    from xgraph.messaging import PARSER_GROUP

    group = reparse_group(2, "2026-09-04")
    assert group != PARSER_GROUP
    assert "reparse" in group


@pytest.mark.asyncio
async def test_publisher_marks_rows_published_only_after_the_broker_acknowledges() -> None:
    broker = InMemoryBroker()
    producer = broker.producer()
    store = FakeStore([outbox_row()])
    publisher = OutboxPublisher(store, producer)  # ty: ignore[invalid-argument-type]

    result = await publisher.run_once()

    assert result.published == 1
    assert store.published == ["event-1"]
    assert len(broker.messages(RAW_TOPIC)) == 1


@pytest.mark.asyncio
async def test_publisher_defers_instead_of_dropping_when_the_broker_refuses() -> None:
    """The page was paid for with X quota; a failed send must not lose it."""

    broker = InMemoryBroker()
    producer = broker.producer()
    producer.fail_next = ConnectionError("broker unavailable")
    store = FakeStore([outbox_row(publish_attempt=1)])
    publisher = OutboxPublisher(store, producer, max_attempts=5)  # ty: ignore[invalid-argument-type]

    result = await publisher.run_once()

    assert (result.published, result.deferred, result.dead_lettered) == (0, 1, 0)
    assert store.published == []
    assert store.deferred[0][0] == "event-1"
    assert broker.messages(RAW_TOPIC) == []


@pytest.mark.asyncio
async def test_publisher_dead_letters_a_row_the_broker_keeps_refusing() -> None:
    """Retrying forever would consume publisher capacity without ever draining."""

    broker = InMemoryBroker()
    producer = broker.producer()
    producer.fail_next = ConnectionError("broker unavailable")
    store = FakeStore([outbox_row(publish_attempt=5)])
    publisher = OutboxPublisher(store, producer, max_attempts=5)  # ty: ignore[invalid-argument-type]

    result = await publisher.run_once()

    assert result.dead_lettered == 1
    assert store.dead_lettered == [("event-1", "publish_failed")]


def test_dlq_topic_is_separate_from_the_raw_stream() -> None:
    assert DLQ_TOPIC != RAW_TOPIC


@pytest.mark.asyncio
async def test_in_memory_consumer_resumes_from_the_offsets_it_is_handed() -> None:
    broker = InMemoryBroker(partitions=1)
    producer = broker.producer()
    for i in range(3):
        await producer.send(RAW_TOPIC, key=None, value=f"m{i}".encode())

    async def on_assign(partitions, bounds):
        assert bounds == {(RAW_TOPIC, 0): (0, 3)}, "the handler is told the log's real range"
        return dict.fromkeys(partitions, 0)  # offset 0 already handled

    consumer = broker.consumer(topics=[RAW_TOPIC], on_assign=on_assign)
    await consumer.start()
    messages = await consumer.poll()

    assert [m.offset for m in messages] == [1, 2]
    assert isinstance(messages[0], Message)
