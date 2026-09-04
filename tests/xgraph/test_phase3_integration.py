"""Stage 3 delivery guarantees against real PostgreSQL and real Kafka.

Every property here is a property of the transaction boundary between the two
systems, so none of it can be established with fakes. A skipped run is not a
passing run: it is the same gap that let the account pool ship unleasable.
"""

import os
from typing import Any

import pytest
import pytest_asyncio

from xgraph.domain import Operation
from xgraph.messaging import (
    DLQ_TOPIC,
    PARSER_GROUP,
    RAW_TOPIC,
    InMemoryBroker,
    Message,
    OutboxPublisher,
    ParserRuntime,
    PermanentEventError,
    RawPageEvent,
    reparse_group,
)
from xgraph.storage import (
    FencedConsumerError,
    PostgresEventStore,
    PostgresFrontierStore,
    apply_schema,
    create_pool,
)

pytestmark = pytest.mark.skipif(
    not os.getenv("XGRAPH_TEST_DATABASE_URL"),
    reason="XGRAPH_TEST_DATABASE_URL is not configured",
)

KAFKA = os.getenv("XGRAPH_TEST_KAFKA_BOOTSTRAP")
requires_kafka = pytest.mark.skipif(not KAFKA, reason="XGRAPH_TEST_KAFKA_BOOTSTRAP is not set")

TABLES = (
    "dlq_events, consumer_offsets, processed_events, raw_page_outbox, request_attempts, "
    "account_operation_quota, scraper_accounts, follow_edge_observations, follow_edges, "
    "account_observations, crawl_frontier, task_operation_budgets, account_nodes, "
    "root_trees, crawl_tasks"
)


@pytest_asyncio.fixture(loop_scope="function")
async def pool():
    pool = await create_pool(os.environ["XGRAPH_TEST_DATABASE_URL"], min_size=1, max_size=8)
    try:
        await apply_schema(pool)
        async with pool.acquire() as connection:
            await connection.execute(f"TRUNCATE {TABLES} RESTART IDENTITY CASCADE")
        yield pool
    finally:
        await pool.close()


@pytest_asyncio.fixture(loop_scope="function")
async def task(pool):
    frontier = PostgresFrontierStore(pool)
    await frontier.create_seed_task("task-1", {"tree-a": "seed-a"})
    return "task-1"


def event(event_id: str = "event-1", *, task_id: str = "task-1", account: str = "seed-a"):
    return RawPageEvent(
        event_id=event_id,
        task_id=task_id,
        account_id=account,
        operation=Operation.FOLLOWING,
        payload={"data": {"user": {"result": {"rest_id": account}}}},
        tree_id="tree-a",
        depth=0,
        cursor_out="cursor-2",
        status_code=200,
    )


class RecordingHandler:
    """Stands in for the stage 4 parser; writes one row per applied event."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.fail_with: Exception | None = None

    async def __call__(self, connection: Any, page: RawPageEvent) -> None:
        if self.fail_with is not None:
            raise self.fail_with
        self.calls.append(page.event_id)
        await connection.execute(
            "INSERT INTO account_nodes(task_id, account_id, first_depth) "
            "VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
            page.task_id,
            f"observed-{page.event_id}",
            1,
        )


async def _observed(pool) -> int:
    async with pool.acquire() as connection:
        return int(
            await connection.fetchval(
                "SELECT count(*) FROM account_nodes WHERE account_id LIKE 'observed-%'"
            )
        )


async def _run(store, broker, handler, *, group_id=PARSER_GROUP, owner_id="parser-a"):
    runtime = ParserRuntime(store, handler, group_id=group_id, owner_id=owner_id)
    consumer = broker.consumer(topics=[RAW_TOPIC], on_assign=runtime.on_assign)
    runtime.attach(consumer)
    await consumer.start()
    return runtime, consumer


# --- producer side ---------------------------------------------------------


@pytest.mark.asyncio
async def test_recording_a_page_is_one_transaction_with_its_checkpoint(pool, task):
    """The cursor must not advance past a page that was never stored."""

    store = PostgresEventStore(pool)
    frontier = PostgresFrontierStore(pool)
    item = await frontier.claim_frontier(worker_id="worker-a")
    assert item is not None

    recorded = await store.record_page(
        event(), frontier_id=item.frontier_id, next_cursor="cursor-2", checkpoint_owner="worker-a"
    )

    assert recorded is True
    async with pool.acquire() as connection:
        row = await connection.fetchrow(
            "SELECT c.cursor_in, t.pages_produced, "
            "(SELECT count(*) FROM raw_page_outbox) AS outbox "
            "FROM crawl_frontier c, crawl_tasks t WHERE t.task_id = 'task-1'"
        )
    assert (row["cursor_in"], row["pages_produced"], row["outbox"]) == ("cursor-2", 1, 1)


@pytest.mark.asyncio
async def test_a_lost_frontier_lease_rolls_back_the_whole_page(pool, task):
    """Half a write is worse than none: the page would be stored under a stale cursor."""

    store = PostgresEventStore(pool)
    frontier = PostgresFrontierStore(pool)
    item = await frontier.claim_frontier(worker_id="worker-a")
    assert item is not None
    async with pool.acquire() as connection:
        await connection.execute("UPDATE crawl_frontier SET owner_id = 'worker-b'")

    with pytest.raises(RuntimeError, match="lease was lost"):
        await store.record_page(
            event(),
            frontier_id=item.frontier_id,
            next_cursor="cursor-2",
            checkpoint_owner="worker-a",
        )

    stats = await store.pipeline_stats("task-1")
    assert (stats.pages_produced, stats.outbox_pending) == (0, 0)


@pytest.mark.asyncio
async def test_refetching_the_same_page_does_not_enqueue_it_twice(pool, task):
    store = PostgresEventStore(pool)

    assert await store.record_page(event()) is True
    assert await store.record_page(event()) is False

    stats = await store.pipeline_stats("task-1")
    assert (stats.pages_produced, stats.outbox_pending) == (1, 1)


@pytest.mark.asyncio
async def test_a_committed_page_survives_a_publisher_that_never_returns(pool, task):
    """Exit gate: a page in the outbox is eventually publishable after a crash."""

    store = PostgresEventStore(pool)
    broker = InMemoryBroker()
    await store.record_page(event())

    producer = broker.producer()
    producer.fail_next = ConnectionError("publisher died mid-send")
    crashed = OutboxPublisher(store, producer, backoff_seconds=0)
    assert (await crashed.run_once()).deferred == 1
    assert broker.messages(RAW_TOPIC) == []

    recovered = OutboxPublisher(store, broker.producer())
    assert (await recovered.run_once()).published == 1
    assert len(broker.messages(RAW_TOPIC)) == 1
    assert (await store.pipeline_stats("task-1")).outbox_pending == 0


@pytest.mark.asyncio
async def test_a_leased_row_is_not_claimed_twice_concurrently(pool, task):
    store = PostgresEventStore(pool)
    await store.record_page(event())

    first = await store.claim_unpublished(owner_id="publisher-a", lease_seconds=60)
    second = await store.claim_unpublished(owner_id="publisher-b", lease_seconds=60)

    assert len(first) == 1
    assert second == []


# --- consumer side ---------------------------------------------------------


@pytest.mark.asyncio
async def test_a_republished_page_is_applied_once(pool, task):
    """Exit gate: duplicate events must not double-count or double-write."""

    store = PostgresEventStore(pool)
    broker = InMemoryBroker(partitions=1)
    producer = broker.producer()
    body = event().serialize()
    await producer.send(RAW_TOPIC, key="seed-a", value=body)
    await producer.send(RAW_TOPIC, key="seed-a", value=body)

    handler = RecordingHandler()
    runtime, consumer = await _run(store, broker, handler)
    for message in await consumer.poll():
        await runtime.handle(message)
        consumer.advance(message)

    stats = await store.pipeline_stats("task-1")
    assert handler.calls == ["event-1"]
    assert stats.pages_processed == 1
    assert await _observed(pool) == 1


@pytest.mark.asyncio
async def test_a_crash_after_the_handler_but_before_the_commit_replays_cleanly(pool, task):
    """Exit gate: killing the parser mid-event must not silently drop the page."""

    store = PostgresEventStore(pool)
    broker = InMemoryBroker(partitions=1)
    await broker.producer().send(RAW_TOPIC, key="seed-a", value=event().serialize())

    handler = RecordingHandler()
    handler.fail_with = RuntimeError("parser killed")
    runtime, consumer = await _run(store, broker, handler)
    message = (await consumer.poll())[0]
    with pytest.raises(RuntimeError, match="parser killed"):
        await runtime.handle(message)

    stats = await store.pipeline_stats("task-1")
    assert stats.pages_processed == 0, "a rolled-back event must not count as processed"
    assert (await store.committed_offsets(PARSER_GROUP)) == {(RAW_TOPIC, 0): -1}
    assert await _observed(pool) == 0

    handler.fail_with = None
    assert await runtime.handle(message) == "applied"
    assert (await store.pipeline_stats("task-1")).pages_processed == 1


@pytest.mark.asyncio
async def test_an_evicted_consumer_can_neither_advance_nor_write(pool, task):
    """Exit gate: a rebalanced-out worker must not write behind the new owner's back."""

    store_old = PostgresEventStore(pool)
    store_new = PostgresEventStore(pool)
    broker = InMemoryBroker(partitions=1)
    await broker.producer().send(RAW_TOPIC, key="seed-a", value=event().serialize())

    handler = RecordingHandler()
    old, consumer = await _run(store_old, broker, handler, owner_id="parser-old")
    message = (await consumer.poll())[0]

    # A rebalance hands the partition to a second consumer.
    new = ParserRuntime(store_new, handler, owner_id="parser-new")
    await new.on_assign([(RAW_TOPIC, 0)])

    with pytest.raises(FencedConsumerError):
        await old.handle(message)
    assert await _observed(pool) == 0
    assert (await store_old.committed_offsets(PARSER_GROUP)) == {(RAW_TOPIC, 0): -1}

    assert await new.handle(message) == "applied"
    assert await _observed(pool) == 1


@pytest.mark.asyncio
async def test_a_permanently_broken_event_goes_to_the_dlq_and_stops_blocking(pool, task):
    """A page that can never parse must not stall the partition behind it."""

    store = PostgresEventStore(pool)
    broker = InMemoryBroker(partitions=1)
    producer = broker.producer()
    await producer.send(RAW_TOPIC, key="seed-a", value=event("bad").serialize())
    await producer.send(RAW_TOPIC, key="seed-a", value=event("good").serialize())

    handler = RecordingHandler()
    dlq = broker.producer()
    runtime = ParserRuntime(store, handler, dlq_producer=dlq)
    consumer = broker.consumer(topics=[RAW_TOPIC], on_assign=runtime.on_assign)
    runtime.attach(consumer)
    await consumer.start()

    messages = await consumer.poll()
    handler.fail_with = PermanentEventError("(336) features cannot be null")
    assert await runtime.handle(messages[0]) == "dead_lettered"
    handler.fail_with = None
    assert await runtime.handle(messages[1]) == "applied"

    stats = await store.pipeline_stats("task-1")
    assert stats.dlq_events == 1
    assert stats.pages_processed == 2, "the terminal page must still be accounted for"
    assert handler.calls == ["good"]
    assert len(broker.messages(DLQ_TOPIC)) == 1
    assert (await store.committed_offsets(PARSER_GROUP)) == {(RAW_TOPIC, 0): 1}


@pytest.mark.asyncio
async def test_an_undecodable_message_does_not_stall_the_partition(pool, task):
    store = PostgresEventStore(pool)
    broker = InMemoryBroker(partitions=1)
    producer = broker.producer()
    await producer.send(RAW_TOPIC, key="seed-a", value=b"{not json")
    await producer.send(RAW_TOPIC, key="seed-a", value=event("good").serialize())

    handler = RecordingHandler()
    runtime, consumer = await _run(store, broker, handler)
    for message in await consumer.poll():
        await runtime.handle(message)

    assert handler.calls == ["good"]
    assert (await store.committed_offsets(PARSER_GROUP)) == {(RAW_TOPIC, 0): 1}
    # An undecodable message carries no task, so it can only appear in the
    # cross-task view. Per-task stats would otherwise claim a clean pipeline.
    assert (await store.pipeline_stats()).dlq_events == 1
    assert (await store.pipeline_stats("task-1")).dlq_events == 0


@pytest.mark.asyncio
async def test_a_reparse_group_replays_history_without_touching_the_live_parser(pool, task):
    """Exit gate: a parser fix costs a replay, not a re-crawl."""

    store = PostgresEventStore(pool)
    broker = InMemoryBroker(partitions=1)
    await broker.producer().send(RAW_TOPIC, key="seed-a", value=event().serialize())

    live = RecordingHandler()
    runtime, consumer = await _run(store, broker, live)
    for message in await consumer.poll():
        await runtime.handle(message)
        consumer.advance(message)
    assert (await consumer.poll()) == []

    replay_handler = RecordingHandler()
    group = reparse_group(2, "test")
    replay = ParserRuntime(PostgresEventStore(pool), replay_handler, group_id=group)
    replay_consumer = broker.consumer(topics=[RAW_TOPIC], on_assign=replay.on_assign)
    await replay_consumer.start()
    for message in await replay_consumer.poll():
        await replay.handle(message)

    assert live.calls == ["event-1"]
    assert replay_handler.calls == ["event-1"], "a fresh group starts from the beginning"
    assert (await store.committed_offsets(PARSER_GROUP)) == {(RAW_TOPIC, 0): 0}
    assert (await store.committed_offsets(group)) == {(RAW_TOPIC, 0): 0}


@pytest.mark.asyncio
async def test_pipeline_stats_expose_the_backlog_that_gates_completion(pool, task):
    store = PostgresEventStore(pool)
    broker = InMemoryBroker(partitions=1)
    await store.record_page(event())

    pending = await store.pipeline_stats("task-1")
    assert (pending.backlog, pending.outbox_pending, pending.drained) == (1, 1, False)
    assert pending.oldest_unpublished_seconds is not None

    await OutboxPublisher(store, broker.producer()).run_once()
    runtime, consumer = await _run(store, broker, RecordingHandler())
    for message in await consumer.poll():
        await runtime.handle(message)

    drained = await store.pipeline_stats("task-1")
    assert (drained.backlog, drained.outbox_pending, drained.drained) == (0, 0, True)


# --- real broker -----------------------------------------------------------


@requires_kafka
@pytest.mark.asyncio
async def test_pages_survive_a_round_trip_through_kafka(pool, task):
    """The adapters must carry key, partition and payload without reinterpreting them."""

    from xgraph.messaging.kafka import KafkaConsumer, KafkaProducer

    store = PostgresEventStore(pool)
    await store.record_page(event("kafka-1"))

    producer = KafkaProducer(str(KAFKA))
    await producer.start()
    try:
        published = await OutboxPublisher(store, producer).run_once()
    finally:
        await producer.stop()
    assert published.published == 1

    handler = RecordingHandler()
    group = reparse_group(1, f"it-{os.getpid()}")
    runtime = ParserRuntime(store, handler, group_id=group)
    consumer = KafkaConsumer(
        str(KAFKA), group_id=group, topics=[RAW_TOPIC], on_assign=runtime.on_assign
    )
    runtime.attach(consumer)
    await consumer.start()
    try:
        seen: list[Message] = []
        for _ in range(20):
            seen = [m for m in await consumer.poll(timeout_ms=1000) if b"kafka-1" in m.value]
            if seen:
                break
        assert seen, "the published page never came back from Kafka"
        assert await runtime.handle(seen[0]) == "applied"
    finally:
        await consumer.stop()

    assert handler.calls == ["kafka-1"]
    assert (await store.pipeline_stats("task-1")).pages_processed == 1


@pytest.mark.asyncio
async def test_a_replayed_event_for_a_deleted_task_does_not_stop_the_stream(pool, task):
    """The raw stream outlives the tasks that produced it.

    Retention is measured in weeks, a task can be deleted at any time, and a
    replay group reads whatever is still retained. If an orphaned event stopped
    the consumer, one deleted task would block every replay behind it.
    """

    store = PostgresEventStore(pool)
    broker = InMemoryBroker(partitions=1)
    producer = broker.producer()
    await producer.send(RAW_TOPIC, key="seed-a", value=event("orphan", task_id="gone").serialize())
    await producer.send(RAW_TOPIC, key="seed-a", value=event("kept").serialize())

    handler = RecordingHandler()
    runtime, consumer = await _run(store, broker, handler)
    outcomes = [await runtime.handle(message) for message in await consumer.poll()]

    assert outcomes == ["dead_lettered", "applied"]
    assert handler.calls == ["kept"], "the live task's page still lands"
    assert (await store.committed_offsets(PARSER_GROUP)) == {(RAW_TOPIC, 0): 1}
    async with pool.acquire() as connection:
        row = await connection.fetchrow(
            "SELECT error_class FROM dlq_events WHERE event_id = 'orphan'"
        )
    assert row["error_class"] == "unknown_task"
