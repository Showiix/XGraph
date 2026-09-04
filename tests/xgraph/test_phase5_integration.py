"""Stage 5: timeline enrichment as an independent, pausable side channel.

The properties that matter here are about isolation — enrichment must not hold
the graph back, must not spend the traversal's quota, and must not be able to
run for accounts nobody admitted — plus the sample rules, which decide whether
the resulting numbers mean anything.
"""

import os
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
import pytest_asyncio

from xgraph.collector.parser import parse_tweets
from xgraph.domain import (
    CandidatePolicy,
    Operation,
    PageEnvelope,
    RateLimitSnapshot,
    TimelinePolicy,
)
from xgraph.graph import GraphPageHandler
from xgraph.messaging import (
    RAW_TOPIC,
    InMemoryBroker,
    OutboxPublisher,
    ParserRuntime,
    PermanentEventError,
    RawPageEvent,
    RoutingPageHandler,
)
from xgraph.scheduler import EnrichmentScheduler, SampleOutcome
from xgraph.storage import PostgresEventStore, PostgresFrontierStore, apply_schema, create_pool
from xgraph.storage.layers import PostgresLayerStore
from xgraph.storage.timeline import PostgresTimelineStore
from xgraph.timeline import TimelinePageHandler

pytestmark = pytest.mark.skipif(
    not os.getenv("XGRAPH_TEST_DATABASE_URL"),
    reason="XGRAPH_TEST_DATABASE_URL is not configured",
)

TABLES = (
    "account_metrics, account_posts, dlq_events, consumer_offsets, processed_events, "
    "raw_page_outbox, request_attempts, account_operation_quota, scraper_accounts, "
    "follow_edge_observations, follow_edges, account_observations, account_profiles, "
    "crawl_frontier, task_operation_budgets, account_nodes, root_trees, crawl_tasks"
)

NOW = datetime(2026, 9, 1, tzinfo=timezone.utc)


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


def tweet_entry(
    post_id: str,
    author: str,
    *,
    kind: str = "original",
    likes: int = 100,
    views: int | None = 1000,
    age_days: int = 0,
    bookmarks: int = 5,
) -> dict[str, Any]:
    legacy: dict[str, Any] = {
        "id_str": post_id,
        "user_id_str": author,
        "full_text": f"post {post_id}",
        "created_at": (NOW - timedelta(days=age_days)).strftime("%a %b %d %H:%M:%S +0000 %Y"),
        "reply_count": 3,
        "retweet_count": 7,
        "favorite_count": likes,
        "quote_count": 1,
        "bookmark_count": bookmarks,
        "conversation_id_str": post_id,
    }
    if kind == "retweet":
        # A repost wrapper reports zero engagement of its own; the real counts
        # live on the post it wraps.
        legacy |= {
            "favorite_count": 0,
            "reply_count": 0,
            "retweet_count": 0,
            "retweeted_status_result": {
                "result": {"__typename": "Tweet", "rest_id": f"orig-{post_id}"}
            },
        }
    if kind == "reply":
        legacy |= {"in_reply_to_status_id_str": "someone-else", "in_reply_to_user_id_str": "other"}
    if kind == "quote":
        legacy |= {"is_quote_status": True, "quoted_status_id_str": f"q-{post_id}"}
    result: dict[str, Any] = {
        "__typename": "Tweet",
        "rest_id": post_id,
        "legacy": legacy,
        "core": {"user_results": {"result": {"__typename": "User", "rest_id": author}}},
    }
    if views is not None:
        result["views"] = {"count": str(views)}
    return {
        "entryId": f"tweet-{post_id}",
        "content": {
            "entryType": "TimelineTimelineItem",
            "itemContent": {"itemType": "TimelineTweet", "tweet_results": {"result": result}},
        },
    }


def timeline_payload(entries: list[dict[str, Any]], cursor_out: str | None) -> dict[str, Any]:
    items = list(entries)
    if cursor_out:
        items.append(
            {
                "entryId": "cursor-bottom",
                "content": {"entryType": "TimelineTimelineCursor"},
                "cursorType": "Bottom",
                "value": cursor_out,
            }
        )
    return {
        "data": {
            "user": {
                "result": {
                    "timeline": {
                        "timeline": {
                            "instructions": [{"type": "TimelineAddEntries", "entries": items}]
                        }
                    }
                }
            }
        }
    }


class FakeTimeline:
    """Serves timeline pages from a declared post list."""

    def __init__(self, posts: dict[str, list[dict[str, Any]]], *, page_size: int = 5) -> None:
        self.posts = posts
        self.page_size = page_size
        self.requests: list[tuple[str, str | None]] = []

    async def user_tweets_page(self, account_id: str, cursor: str | None = None) -> PageEnvelope:
        self.requests.append((account_id, cursor))
        entries = self.posts.get(account_id, [])
        start = int(cursor.split("-")[1]) if cursor else 0
        window = entries[start : start + self.page_size]
        nxt = start + self.page_size
        cursor_out = f"cursor-{nxt}" if nxt < len(entries) else None
        payload = timeline_payload(window, cursor_out)
        return PageEnvelope(
            event_id=f"tl:{account_id}:{cursor or 'start'}",
            schema_version=1,
            operation=Operation.USER_TWEETS,
            source_account_id=account_id,
            cursor_in=cursor,
            cursor_out=cursor_out,
            users=(),
            rate_limit=RateLimitSnapshot(limit=188, remaining=90, reset_at=None),
            status_code=200,
            requested_at=NOW,
            received_at=NOW,
            raw_payload=payload,
            # The real collector parses the page it just received; a fake that
            # omitted this would hide the scheduler's dependency on it.
            tweets=parse_tweets(payload),
        )

    async def aclose(self) -> None:
        return None


class Accounts:
    """Records which rate-limit bucket each request was leased from."""

    class _Lease:
        def __init__(self, operation: str) -> None:
            self.alias = "scraper-a"
            self.operation = operation
            self.owner_id = "worker"

    def __init__(self) -> None:
        self.leased: list[str] = []

    async def lease(self, operation: str, *, owner_id: str, **kwargs):
        self.leased.append(operation)
        return self._Lease(operation)

    async def release(self, lease, **kwargs) -> None:
        return None

    async def report(self, lease, error_class: str, **kwargs) -> None:
        return None


class Rig:
    def __init__(
        self, pool, timeline_platform: FakeTimeline, *, policy: TimelinePolicy | None = None
    ):
        self.pool = pool
        self.platform = timeline_platform
        self.frontier = PostgresFrontierStore(pool)
        self.events = PostgresEventStore(pool)
        self.timeline = PostgresTimelineStore(pool)
        self.layers = PostgresLayerStore(pool)
        self.accounts = Accounts()
        self.policy = policy or TimelinePolicy()
        self.broker = InMemoryBroker(partitions=1)
        self.handler = RoutingPageHandler(
            {
                Operation.FOLLOWING: GraphPageHandler(),
                Operation.USER_TWEETS: TimelinePageHandler(self.timeline, self.policy),
            }
        )
        self.enricher = EnrichmentScheduler(
            self.frontier,
            self.accounts,
            self.events,
            self.timeline,
            self._collector,
            policy=self.policy,
            worker_id="enricher-a",
        )
        self.publisher = OutboxPublisher(self.events, self.broker.producer())
        self.parser = ParserRuntime(self.events, self.handler, owner_id="parser-a")
        self.consumer = self.broker.consumer(topics=[RAW_TOPIC], on_assign=self.parser.on_assign)
        self.parser.attach(self.consumer)

    async def _collector(self, alias: str, /):
        return self.platform

    async def start(self) -> None:
        await self.consumer.start()

    async def pump(self) -> None:
        """One enrichment step, then publish and parse whatever it produced."""

        await self.enricher.run_once()
        await self.publisher.run_once()
        for message in await self.consumer.poll():
            await self.parser.handle(message)
            self.consumer.advance(message)

    async def drain(self, rounds: int = 30) -> None:
        for _ in range(rounds):
            before = len(self.platform.requests)
            await self.pump()
            if len(self.platform.requests) == before:
                return


async def seed_graph(pool, accounts: dict[str, dict[str, Any]]) -> None:
    """Put a finished traversal in place so admission has something to read."""

    frontier = PostgresFrontierStore(pool)
    await frontier.create_seed_task("t", {"tree-a": "seed"})
    async with pool.acquire() as connection:
        for account_id, attrs in accounts.items():
            await connection.execute(
                "INSERT INTO account_nodes(task_id, account_id, first_depth) VALUES "
                "('t', $1, $2) ON CONFLICT DO NOTHING",
                account_id,
                attrs.get("depth", 1),
            )
            await connection.execute(
                "INSERT INTO account_profiles(task_id, account_id, username, description, "
                "followers_count, can_dm, protected) VALUES ('t', $1, $2, $3, $4, $5, $6) "
                "ON CONFLICT (task_id, account_id) DO UPDATE SET "
                "followers_count = excluded.followers_count, can_dm = excluded.can_dm",
                account_id,
                f"user{account_id}",
                attrs.get("bio", ""),
                attrs.get("followers", 10_000),
                attrs.get("can_dm", True),
                attrs.get("protected", False),
            )
            for source in attrs.get("followed_by", []):
                await connection.execute(
                    "INSERT INTO follow_edges(task_id, source_account_id, target_account_id, "
                    "source_depth, target_depth) VALUES ('t', $1, $2, 0, 1) "
                    "ON CONFLICT DO NOTHING",
                    source,
                    account_id,
                )


async def _one(pool, sql: str, *args):
    async with pool.acquire() as connection:
        return await connection.fetchrow(sql, *args)


# --- admission -------------------------------------------------------------


@pytest.mark.asyncio
async def test_only_admitted_accounts_get_a_timeline_request(pool):
    """Exit gate: no timeline traffic outside the candidate set."""

    await seed_graph(
        pool,
        {
            "reachable": {"followers": 20_000, "can_dm": True},
            "unreachable": {"followers": 20_000, "can_dm": False},
            "tiny": {"followers": 50, "can_dm": True},
            "locked": {"followers": 20_000, "can_dm": True, "protected": True},
        },
    )
    store = PostgresTimelineStore(pool)
    admitted = await store.select_candidates(
        "t", CandidatePolicy(require_can_dm=True, min_followers=1_000)
    )
    assert admitted == 1

    rig = Rig(pool, FakeTimeline({"reachable": [tweet_entry("p1", "reachable")]}))
    await rig.start()
    await rig.drain()

    assert {account for account, _ in rig.platform.requests} == {"reachable"}
    row = await _one(
        pool, "SELECT candidate_reasons FROM account_nodes WHERE account_id='reachable'"
    )
    assert set(row["candidate_reasons"]) == {"can_dm", "not_protected", "follower_range"}


@pytest.mark.asyncio
async def test_admission_records_conditions_not_a_score(pool):
    """The PRD forbids presenting a heuristic as a verdict."""

    await seed_graph(
        pool,
        {
            "a": {"followers": 5_000, "bio": "building AI tools", "followed_by": ["seed"]},
        },
    )
    store = PostgresTimelineStore(pool)
    await store.select_candidates(
        "t",
        CandidatePolicy(
            min_followers=1_000, bio_keywords=("AI",), min_network_indegree=1, max_depth=3
        ),
    )

    row = await _one(pool, "SELECT candidate_reasons FROM account_nodes WHERE account_id='a'")
    assert set(row["candidate_reasons"]) == {
        "not_protected",
        "follower_range",
        "bio_keyword",
        "network_indegree",
        "depth",
    }


@pytest.mark.asyncio
async def test_disabled_enrichment_admits_nobody(pool):
    await seed_graph(pool, {"a": {}})
    store = PostgresTimelineStore(pool)
    await store.set_timeline_enabled("t", False)

    assert await store.select_candidates("t", CandidatePolicy()) == 0
    row = await _one(pool, "SELECT count(*) AS n FROM crawl_frontier WHERE operation='UserTweets'")
    assert row["n"] == 0


# --- sample rules ----------------------------------------------------------


@pytest.mark.asyncio
async def test_reposts_and_replies_are_stored_but_never_counted(pool):
    """The measured page had 16 reposts in 21 entries; counting them halves every average."""

    entries = (
        [tweet_entry(f"rt{i}", "a", kind="retweet") for i in range(6)]
        + [tweet_entry("re1", "a", kind="reply")]
        + [tweet_entry("o1", "a", likes=1000), tweet_entry("q1", "a", kind="quote", likes=500)]
    )
    await seed_graph(pool, {"a": {}})
    store = PostgresTimelineStore(pool)
    await store.select_candidates("t", CandidatePolicy())

    rig = Rig(pool, FakeTimeline({"a": entries}, page_size=20))
    await rig.start()
    await rig.drain()

    kinds = await _one(
        pool,
        "SELECT count(*) FILTER (WHERE is_qualifying) AS qualifying, count(*) AS scanned, "
        "count(*) FILTER (WHERE kind='retweet') AS reposts, "
        "count(*) FILTER (WHERE kind='reply') AS replies "
        "FROM account_posts WHERE account_id='a'",
    )
    assert (kinds["qualifying"], kinds["scanned"]) == (2, 9)
    assert (kinds["reposts"], kinds["replies"]) == (6, 1), "kept as evidence of what was scanned"

    metrics = await store.metrics("t", "a")
    assert metrics is not None
    assert metrics.sample_count == 2 and metrics.scanned_count == 9
    assert metrics.avg_like == pytest.approx(750.0), "reposts would have dragged this to 167"


@pytest.mark.asyncio
async def test_a_missing_view_count_is_not_zero_views(pool):
    entries = [
        tweet_entry("p1", "a", views=2000),
        tweet_entry("p2", "a", views=None),
        tweet_entry("p3", "a", views=4000),
    ]
    await seed_graph(pool, {"a": {"followers": 1_000}})
    store = PostgresTimelineStore(pool)
    await store.select_candidates("t", CandidatePolicy())

    rig = Rig(pool, FakeTimeline({"a": entries}, page_size=10))
    await rig.start()
    await rig.drain()

    metrics = await store.metrics("t", "a")
    assert metrics is not None
    assert metrics.sample_count == 3
    assert metrics.view_sample_count == 2, "the average is over a smaller population"
    assert metrics.avg_view == pytest.approx(3000.0), "not 2000, which averaging in a zero gives"
    assert metrics.reach_ratio == pytest.approx(3.0)

    stored = await _one(pool, "SELECT view_count FROM account_posts WHERE post_id='p2'")
    assert stored["view_count"] is None


@pytest.mark.asyncio
async def test_the_sample_stops_at_the_target_and_records_its_span(pool):
    entries = [tweet_entry(f"p{i}", "a", age_days=i) for i in range(12)]
    await seed_graph(pool, {"a": {}})
    store = PostgresTimelineStore(pool)
    await store.select_candidates("t", CandidatePolicy())

    rig = Rig(
        pool, FakeTimeline({"a": entries}, page_size=3), policy=TimelinePolicy(target_posts=5)
    )
    await rig.start()
    await rig.drain()

    metrics = await store.metrics("t", "a")
    assert metrics is not None
    assert metrics.sample_count == 5, "capped at the target even though more were fetched"
    assert metrics.sample_span_days == pytest.approx(4.0)
    assert metrics.latest_post_at == NOW

    result_status = await _one(
        pool, "SELECT timeline_status, termination_reason FROM account_nodes WHERE account_id='a'"
    )
    assert result_status["timeline_status"] == "complete"
    assert result_status["termination_reason"] == SampleOutcome.TARGET_REACHED.value


@pytest.mark.asyncio
async def test_a_short_sample_records_what_it_actually_found(pool):
    """Fewer than the target is a real answer, not a failure."""

    await seed_graph(pool, {"a": {}})
    store = PostgresTimelineStore(pool)
    await store.select_candidates("t", CandidatePolicy())

    rig = Rig(pool, FakeTimeline({"a": [tweet_entry("p1", "a")]}, page_size=10))
    await rig.start()
    await rig.drain()

    metrics = await store.metrics("t", "a")
    assert metrics is not None and metrics.sample_count == 1
    row = await _one(pool, "SELECT termination_reason FROM account_nodes WHERE account_id='a'")
    assert row["termination_reason"] == SampleOutcome.NATURAL_END.value


@pytest.mark.asyncio
async def test_scanning_stops_at_the_budget_when_qualifying_posts_are_rare(pool):
    """An account that only reposts must not be paged forever."""

    entries = [tweet_entry(f"rt{i}", "a", kind="retweet") for i in range(60)]
    await seed_graph(pool, {"a": {}})
    store = PostgresTimelineStore(pool)
    await store.select_candidates("t", CandidatePolicy())

    rig = Rig(
        pool,
        FakeTimeline({"a": entries}, page_size=5),
        policy=TimelinePolicy(target_posts=30, max_scanned=20),
    )
    await rig.start()
    await rig.drain()

    row = await _one(pool, "SELECT termination_reason FROM account_nodes WHERE account_id='a'")
    assert row["termination_reason"] == SampleOutcome.SCAN_LIMIT.value
    assert len(rig.platform.requests) <= 5, "the scan budget bounded the requests"


# --- isolation from the traversal ------------------------------------------


@pytest.mark.asyncio
async def test_timeline_work_uses_its_own_rate_limit_bucket(pool):
    await seed_graph(pool, {"a": {}})
    store = PostgresTimelineStore(pool)
    await store.select_candidates("t", CandidatePolicy())

    rig = Rig(pool, FakeTimeline({"a": [tweet_entry("p1", "a")]}))
    await rig.start()
    await rig.drain()

    assert set(rig.accounts.leased) == {Operation.USER_TWEETS.value}
    budgets = await _one(
        pool,
        "SELECT operation, requests_attempted FROM task_operation_budgets "
        "WHERE task_id='t' AND requests_attempted > 0",
    )
    assert budgets["operation"] == Operation.USER_TWEETS.value


@pytest.mark.asyncio
async def test_outstanding_timeline_work_does_not_hold_a_layer_open(pool):
    """Exit gate: enrichment must never block the graph from finishing."""

    await seed_graph(pool, {"a": {}})
    store = PostgresTimelineStore(pool)
    frontier = PostgresFrontierStore(pool)
    layers = PostgresLayerStore(pool)

    # Close the traversal's only layer.
    async with pool.acquire() as connection:
        await connection.execute(
            "UPDATE crawl_frontier SET status='completed' WHERE operation='Following'"
        )
        await connection.execute("UPDATE crawl_tasks SET max_depth = 0")

    await store.select_candidates("t", CandidatePolicy())
    pending = await _one(
        pool,
        "SELECT count(*) AS n FROM crawl_frontier WHERE operation='UserTweets' "
        "AND status='pending'",
    )
    assert pending["n"] == 1, "timeline work is outstanding"

    state = await layers.layer_state("t")
    assert state.closed, "yet the layer is closed"
    await layers.advance_layer("t")
    assert await layers.expansion_complete("t")
    assert await frontier.claim_frontier(worker_id="w", operation=Operation.FOLLOWING) is None


@pytest.mark.asyncio
async def test_timeline_rows_are_not_gated_by_the_layer_barrier(pool):
    """Enrichment of an L1 account must not wait for layer 1 to open."""

    await seed_graph(pool, {"a": {"depth": 1}})
    store = PostgresTimelineStore(pool)
    await store.select_candidates("t", CandidatePolicy())

    async with pool.acquire() as connection:
        depth = await connection.fetchval("SELECT current_depth FROM crawl_tasks WHERE task_id='t'")
    assert depth == 0, "the traversal is still on layer 0"

    frontier = PostgresFrontierStore(pool)
    claimed = await frontier.claim_frontier(worker_id="w", operation=Operation.USER_TWEETS)
    assert claimed is not None and claimed.account_id == "a"


@pytest.mark.asyncio
async def test_a_following_page_is_never_handed_to_the_timeline_handler(pool):
    """A page with no registered handler must fail loudly, not be dropped."""

    router = RoutingPageHandler(
        {Operation.USER_TWEETS: TimelinePageHandler(PostgresTimelineStore(pool))}
    )
    event = RawPageEvent(
        event_id="e",
        task_id="t",
        account_id="a",
        operation=Operation.FOLLOWING,
        payload={},
    )
    with pytest.raises(PermanentEventError, match="no handler registered"):
        await router(None, event)


@pytest.mark.asyncio
async def test_replaying_a_timeline_page_does_not_change_the_metrics(pool):
    """Metrics are recomputed from the sample, so a redelivery is a no-op."""

    await seed_graph(pool, {"a": {}})
    store = PostgresTimelineStore(pool)
    await store.select_candidates("t", CandidatePolicy())

    rig = Rig(
        pool, FakeTimeline({"a": [tweet_entry("p1", "a"), tweet_entry("p2", "a")]}, page_size=10)
    )
    await rig.start()
    await rig.drain()
    before = await store.metrics("t", "a")

    for message in await rig.broker.consumer(topics=[RAW_TOPIC]).poll():
        await rig.parser.handle(message)
    after = await store.metrics("t", "a")

    assert before is not None and after is not None
    assert (before.sample_count, before.avg_like) == (after.sample_count, after.avg_like)
