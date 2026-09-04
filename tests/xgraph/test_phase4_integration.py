"""Stage 4: the L0-L6 traversal, end to end on a synthetic graph.

The traversal is only meaningful against a real database: layer closure, node
and edge uniqueness and depth assignment are all enforced by constraints and
set-based statements that a fake cannot reproduce.

The synthetic platform below stands in for X. It answers Following pages from a
declared adjacency map, so the expected shape of the resulting graph is known
exactly and the assertions are about the traversal, not about parsing.
"""

import os
from datetime import datetime, timezone
from typing import Any

import pytest
import pytest_asyncio

from xgraph.collector.parser import parse_users
from xgraph.domain import Operation, PageEnvelope, RateLimitSnapshot, UserProfile
from xgraph.graph import GraphPageHandler, TraversalPolicy
from xgraph.messaging import RAW_TOPIC, InMemoryBroker, OutboxPublisher, ParserRuntime
from xgraph.scheduler import AccountLeasing, ExpansionScheduler, TerminationReason
from xgraph.scheduler.expansion import EMPTY_PAGE_LIMIT
from xgraph.storage import PostgresEventStore, PostgresFrontierStore, apply_schema, create_pool
from xgraph.storage.layers import PostgresLayerStore

pytestmark = pytest.mark.skipif(
    not os.getenv("XGRAPH_TEST_DATABASE_URL"),
    reason="XGRAPH_TEST_DATABASE_URL is not configured",
)

TABLES = (
    "dlq_events, consumer_offsets, processed_events, raw_page_outbox, request_attempts, "
    "account_operation_quota, scraper_accounts, follow_edge_observations, follow_edges, "
    "account_observations, account_profiles, crawl_frontier, task_operation_budgets, "
    "account_nodes, root_trees, crawl_tasks"
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


class FakePlatform:
    """A synthetic X: a fixed adjacency map served one page at a time.

    Faithful to X in two ways that are easy to get wrong and that hide real
    defects when they are wrong. It does not describe the account being expanded
    on that account's own Following page — X returns a bare
    `{"__typename": "User"}` there — and it does not announce the end of a list
    by withholding the cursor; it keeps handing out cursors and empty pages, as
    2,599 of 2,600 live pages did. Set `null_cursor_at_end` for the endpoints and
    fixtures that do close a chain that way.
    """

    def __init__(
        self,
        following: dict[str, list[str]],
        *,
        page_size: int = 2,
        followers: dict[str, int] | None = None,
        null_cursor_at_end: bool = False,
    ) -> None:
        self.following = following
        self.page_size = page_size
        self.followers = followers or {}
        self.null_cursor_at_end = null_cursor_at_end
        self.requests: list[tuple[str, str | None]] = []

    def profile(self, account_id: str) -> UserProfile:
        return UserProfile(
            id=account_id,
            username=f"user{account_id}",
            display_name=f"User {account_id}",
            description="",
            followers_count=self.followers.get(account_id, 10_000),
            following_count=len(self.following.get(account_id, [])),
            created_at=None,
            protected=False,
            verified=False,
            blue_verified=False,
            can_dm=True,
            location=None,
            avatar_url=None,
            banner_url=None,
        )

    def _entry(self, account_id: str) -> dict[str, Any]:
        profile = self.profile(account_id)
        return {
            "content": {
                "entryType": "TimelineTimelineItem",
                "itemContent": {
                    "itemType": "TimelineUser",
                    "user_results": {
                        "result": {
                            "__typename": "User",
                            "rest_id": account_id,
                            "core": {"screen_name": profile.username, "name": profile.display_name},
                            "relationship_counts": {
                                "followers": profile.followers_count,
                                "following": profile.following_count,
                            },
                            "dm_permissions": {"can_dm": True},
                        }
                    },
                },
            }
        }

    async def following_page(self, account_id: str, cursor: str | None = None) -> PageEnvelope:
        self.requests.append((account_id, cursor))
        targets = self.following.get(account_id, [])
        start = int(cursor.split("-")[1]) if cursor else 0
        window = targets[start : start + self.page_size]
        nxt = start + self.page_size
        entries = [self._entry(t) for t in window]
        exhausted = nxt >= len(targets)
        cursor_out = None if (exhausted and self.null_cursor_at_end) else f"cursor-{nxt}"
        if cursor_out:
            entries.append(
                {
                    "content": {"entryType": "TimelineTimelineCursor"},
                    "cursorType": "Bottom",
                    "value": cursor_out,
                }
            )
        payload = {
            "data": {
                "user": {
                    "result": {
                        "timeline": {
                            "timeline": {
                                "instructions": [{"type": "TimelineAddEntries", "entries": entries}]
                            }
                        }
                    }
                }
            }
        }
        now = datetime.now(timezone.utc)
        return PageEnvelope(
            event_id=f"{account_id}:{cursor or 'start'}",
            schema_version=1,
            operation=Operation.FOLLOWING,
            source_account_id=account_id,
            cursor_in=cursor,
            cursor_out=cursor_out,
            # Parsed by the same function the real collector uses, so the
            # envelope cannot drift from what production sees.
            users=parse_users(payload),
            rate_limit=RateLimitSnapshot(limit=188, remaining=100, reset_at=None),
            status_code=200,
            requested_at=now,
            received_at=now,
            raw_payload=payload,
        )

    async def aclose(self) -> None:
        return None


class Pipeline:
    """Wires the stage 2/3/4 pieces the way a deployment would."""

    def __init__(self, pool, platform: FakePlatform, *, policy: TraversalPolicy | None = None):
        self.pool = pool
        self.platform = platform
        self.frontier = PostgresFrontierStore(pool)
        self.events = PostgresEventStore(pool)
        self.layers = PostgresLayerStore(pool)
        self.broker = InMemoryBroker(partitions=1)
        self.handler = GraphPageHandler(policy)
        # Typed as the contract, not the stub: tests swap in pools that run out.
        self.accounts: AccountLeasing = _AlwaysAvailableAccounts()
        self.scheduler = ExpansionScheduler(
            self.frontier, self.accounts, self.events, self._collector, worker_id="worker-a"
        )
        self.publisher = OutboxPublisher(self.events, self.broker.producer())
        self.parser = ParserRuntime(self.events, self.handler, owner_id="parser-a")
        self.consumer = self.broker.consumer(topics=[RAW_TOPIC], on_assign=self.parser.on_assign)
        self.parser.attach(self.consumer)

    async def _collector(self, alias: str, /):
        return self.platform

    async def start(self) -> None:
        await self.consumer.start()

    async def drain_layer(self) -> None:
        """Fetch, publish and parse until the open layer stops changing."""

        for _ in range(200):
            worked = await self.scheduler.run_once() is not None
            worked |= (await self.publisher.run_once()).published > 0
            for message in await self.consumer.poll():
                await self.parser.handle(message)
                self.consumer.advance(message)
                worked = True
            if not worked:
                return
        raise AssertionError("layer did not settle")

    async def run(self, task_id: str, *, max_layers: int = 8) -> None:
        await self.start()
        for _ in range(max_layers):
            await self.drain_layer()
            before = (await self.layers.layer_state(task_id)).depth
            await self.layers.advance_layer(task_id)
            if (await self.layers.layer_state(task_id)).depth == before:
                return


class _AlwaysAvailableAccounts:
    """Stands in for the account pool; leasing is covered by the stage 2 gate."""

    class _Lease:
        alias = "scraper-a"
        operation = "Following"
        owner_id = "worker-a"

    async def lease(self, operation: str, *, owner_id: str, **kwargs):
        return self._Lease()

    async def release(self, lease, **kwargs) -> None:
        return None

    async def report(self, lease, error_class: str, **kwargs) -> None:
        return None


async def _rows(pool, sql: str, *args) -> list[dict[str, Any]]:
    async with pool.acquire() as connection:
        return [dict(r) for r in await connection.fetch(sql, *args)]


async def _depths(pool) -> dict[str, int]:
    rows = await _rows(pool, "SELECT account_id, first_depth FROM account_nodes")
    return {r["account_id"]: r["first_depth"] for r in rows}


# --- traversal shape -------------------------------------------------------


@pytest.mark.asyncio
async def test_a_chain_reaches_every_layer_and_stops_at_the_boundary(pool):
    """Exit gate: L0-L6 complete, and L6 never gets a request of its own."""

    chain = {f"a{i}": [f"a{i + 1}"] for i in range(8)}
    platform = FakePlatform(chain)
    pipe = Pipeline(pool, platform)
    await pipe.frontier.create_seed_task("t", {"tree-a": "a0"})
    await pipe.run("t")

    depths = await _depths(pool)
    assert depths == {f"a{i}": i for i in range(7)}, "one node per layer, a6 is the boundary"
    assert "a7" not in depths, "the boundary must not be expanded"

    requested = {account for account, _ in platform.requests}
    assert requested == {f"a{i}" for i in range(6)}, "only L0-L5 are requested"

    boundary = await _rows(pool, "SELECT account_id FROM account_nodes WHERE is_l6_boundary")
    assert [r["account_id"] for r in boundary] == ["a6"]
    frontier = await _rows(pool, "SELECT count(*) AS n FROM crawl_frontier WHERE depth = 6")
    assert frontier[0]["n"] == 0


@pytest.mark.asyncio
async def test_a_node_reachable_by_two_paths_is_stored_once_at_its_shortest_depth(pool):
    """Layer order makes the first durable claim the shortest path, by construction."""

    platform = FakePlatform({"s": ["x", "y"], "x": ["z"], "y": ["z"], "z": []}, page_size=5)
    pipe = Pipeline(pool, platform)
    await pipe.frontier.create_seed_task("t", {"tree-a": "s"})
    await pipe.run("t")

    depths = await _depths(pool)
    assert depths == {"s": 0, "x": 1, "y": 1, "z": 2}

    edges = await _rows(pool, "SELECT source_account_id, target_account_id FROM follow_edges")
    assert len(edges) == 4, "both paths into z are recorded as edges"
    # z follows nobody, so its chain is nothing but the empty pages that end it.
    # A second expansion would double this; the collision must not cause one.
    assert sum(1 for account, _ in platform.requests if account == "z") == EMPTY_PAGE_LIMIT


@pytest.mark.asyncio
async def test_two_seeds_share_nodes_but_keep_separate_trees(pool):
    platform = FakePlatform({"s1": ["shared"], "s2": ["shared"], "shared": []}, page_size=5)
    pipe = Pipeline(pool, platform)
    await pipe.frontier.create_seed_task("t", {"tree-a": "s1", "tree-b": "s2"})
    await pipe.run("t")

    nodes = await _rows(pool, "SELECT count(*) AS n FROM account_nodes WHERE account_id='shared'")
    assert nodes[0]["n"] == 1, "one physical node per account"

    trees = await _rows(
        pool,
        "SELECT DISTINCT tree_id FROM account_observations WHERE account_id='shared' ORDER BY 1",
    )
    assert [r["tree_id"] for r in trees] == ["tree-a", "tree-b"], "both discovery paths kept"


@pytest.mark.asyncio
async def test_a_repeat_discovery_is_recorded_as_a_collision_and_stops_only_its_branch(pool):
    platform = FakePlatform({"s": ["x", "y"], "x": ["y"], "y": ["deep"], "deep": []}, page_size=5)
    pipe = Pipeline(pool, platform)
    await pipe.frontier.create_seed_task("t", {"tree-a": "s"})
    await pipe.run("t")

    collisions = await _rows(
        pool, "SELECT account_id, depth FROM account_observations WHERE is_collision"
    )
    assert [(r["account_id"], r["depth"]) for r in collisions] == [("y", 2)]
    assert (await _depths(pool))["y"] == 1, "the collision keeps the shorter depth"
    assert "deep" in await _depths(pool), "other branches keep going"


@pytest.mark.asyncio
async def test_an_edge_back_to_a_seed_is_recorded(pool):
    """Closure edges carry the in-network signal the product ranks on."""

    platform = FakePlatform({"s": ["x"], "x": ["s"]}, page_size=5)
    pipe = Pipeline(pool, platform)
    await pipe.frontier.create_seed_task("t", {"tree-a": "s"})
    await pipe.run("t")

    edges = await _rows(
        pool,
        "SELECT source_account_id, target_account_id, target_depth FROM follow_edges "
        "ORDER BY source_account_id",
    )
    assert [(e["source_account_id"], e["target_account_id"], e["target_depth"]) for e in edges] == [
        ("s", "x", 1),
        ("x", "s", 0),
    ]


# --- layer barrier and completion ------------------------------------------


@pytest.mark.asyncio
async def test_the_next_layer_stays_shut_until_the_current_one_is_parsed(pool):
    """Exit gate: an unparsed page keeps the layer open even with an empty frontier."""

    platform = FakePlatform({"s": ["x"], "x": ["y"], "y": []}, page_size=5)
    pipe = Pipeline(pool, platform)
    await pipe.frontier.create_seed_task("t", {"tree-a": "s"})
    await pipe.start()

    assert await pipe.scheduler.run_once() is not None
    assert await pipe.scheduler.run_once() is None, "L0 has only the seed"

    # The frontier is empty, but the page has not been parsed into L1 yet.
    seed_pages = 1 + EMPTY_PAGE_LIMIT  # one page of users, then the run that ends it
    state = await pipe.layers.layer_state("t")
    assert (state.pending, state.running) == (0, 0)
    assert state.unpublished_pages == seed_pages
    assert not state.closed
    assert "pages_unpublished" in state.blocked_by

    while (await pipe.publisher.run_once()).published:
        pass
    state = await pipe.layers.layer_state("t")
    assert state.unparsed_pages == seed_pages and not state.closed

    for message in await pipe.consumer.poll():
        await pipe.parser.handle(message)
        pipe.consumer.advance(message)

    state = await pipe.layers.advance_layer("t")
    assert state.closed
    assert (await pipe.layers.layer_state("t")).depth == 1


@pytest.mark.asyncio
async def test_a_deeper_row_cannot_be_claimed_before_its_layer_opens(pool):
    platform = FakePlatform({"s": ["x"], "x": []}, page_size=5)
    pipe = Pipeline(pool, platform)
    await pipe.frontier.create_seed_task("t", {"tree-a": "s"})
    await pipe.start()
    await pipe.drain_layer()

    async with pool.acquire() as connection:
        queued = await connection.fetchval(
            "SELECT count(*) FROM crawl_frontier WHERE depth = 1 AND status = 'pending'"
        )
    assert queued == 1, "L1 work exists"
    assert await pipe.scheduler.run_once() is None, "but the layer is still shut"

    await pipe.layers.advance_layer("t")
    assert await pipe.scheduler.run_once() is not None


@pytest.mark.asyncio
async def test_completion_requires_the_parser_to_have_caught_up(pool):
    platform = FakePlatform({"s": []}, page_size=5)
    pipe = Pipeline(pool, platform)
    await pipe.frontier.create_seed_task("t", {"tree-a": "s"}, max_depth=0)
    await pipe.start()

    await pipe.scheduler.run_once()
    assert not await pipe.layers.expansion_complete("t"), "the page is still unparsed"

    await pipe.publisher.run_once()
    for message in await pipe.consumer.poll():
        await pipe.parser.handle(message)
        pipe.consumer.advance(message)
    await pipe.layers.advance_layer("t")

    assert await pipe.layers.expansion_complete("t")


# --- resumption, filtering, evidence ---------------------------------------


@pytest.mark.asyncio
async def test_an_interrupted_chain_resumes_from_its_checkpoint(pool):
    """Exit gate: killing the scheduler mid-chain must not refetch or lose pages."""

    platform = FakePlatform({"s": ["a", "b", "c", "d"], **{k: [] for k in "abcd"}}, page_size=2)
    pipe = Pipeline(pool, platform)
    await pipe.frontier.create_seed_task("t", {"tree-a": "s"})
    await pipe.start()

    # Stop the chain after its first page by capping the run.
    pipe.scheduler._max_pages = 1  # noqa: SLF001
    first = await pipe.scheduler.run_once()
    assert first is not None and first.pages == 1

    async with pool.acquire() as connection:
        checkpoint = await connection.fetchval(
            "SELECT cursor_in FROM crawl_frontier WHERE account_id = 's'"
        )
    assert checkpoint == "cursor-2"

    pipe.scheduler._max_pages = 40  # noqa: SLF001
    async with pool.acquire() as connection:
        await connection.execute("UPDATE crawl_frontier SET status='pending' WHERE account_id='s'")
    resumed = await pipe.scheduler.run_once()
    assert resumed is not None and resumed.reason is TerminationReason.EMPTY_PAGES

    cursors = [cursor for account, cursor in platform.requests if account == "s"]
    assert cursors[:2] == [None, "cursor-2"], "the chain continued instead of restarting"
    assert "cursor-2" not in cursors[2:], "and did not refetch the page it checkpointed"


@pytest.mark.asyncio
async def test_small_accounts_are_recorded_but_never_expanded(pool):
    """The follower filter controls traversal cost; it does not delete evidence."""

    platform = FakePlatform(
        {"s": ["big", "small"], "big": [], "small": ["hidden"]},
        page_size=5,
        followers={"big": 50_000, "small": 10},
    )
    pipe = Pipeline(pool, platform, policy=TraversalPolicy(min_followers_to_expand=1_000))
    await pipe.frontier.create_seed_task("t", {"tree-a": "s"}, min_followers_to_expand=1_000)
    await pipe.run("t")

    depths = await _depths(pool)
    assert "small" in depths, "the filtered account stays in the graph"
    assert "hidden" not in depths, "but nothing behind it is fetched"

    rows = await _rows(
        pool,
        "SELECT account_id, expansion_status, filter_reason FROM account_nodes "
        "WHERE account_id IN ('big','small') ORDER BY account_id",
    )
    assert rows[1]["expansion_status"] == "filtered"
    assert rows[1]["filter_reason"] == "below_follower_threshold"
    profiles = await _rows(pool, "SELECT can_dm FROM account_profiles WHERE account_id='small'")
    assert profiles[0]["can_dm"] is True, "its profile is still queryable"


@pytest.mark.asyncio
async def test_a_chain_ends_on_empty_pages_rather_than_the_page_cap(pool):
    """X never withholds the cursor, so the page cap must not be the stopping rule.

    Waiting for a null cursor costs the cap on every account however few people
    it follows: in the live run 129 of 130 accounts spent all 20 of their pages,
    including one that follows nobody.
    """

    platform = FakePlatform({"s": ["a", "b"], **{k: [] for k in "ab"}}, page_size=1)
    pipe = Pipeline(pool, platform)
    await pipe.frontier.create_seed_task("t", {"tree-a": "s"})
    await pipe.start()

    result = await pipe.scheduler.run_once()
    assert result is not None
    assert result.reason is TerminationReason.EMPTY_PAGES
    assert result.pages == 2 + EMPTY_PAGE_LIMIT, "two pages of users, then the run that ends it"
    assert result.pages < pipe.scheduler._max_pages  # noqa: SLF001


@pytest.mark.asyncio
async def test_coverage_and_termination_are_stored_rather_than_inferred(pool):
    platform = FakePlatform({"s": ["a"], "a": ["p", "q"], "p": [], "q": []}, page_size=2)
    pipe = Pipeline(pool, platform)
    await pipe.frontier.create_seed_task("t", {"tree-a": "s"})
    await pipe.run("t")

    # 'a' never appears on its own Following page. Its declared count comes from
    # the profile stored when 's' discovered it, which is the only place X put it.
    rows = await _rows(
        pool,
        "SELECT declared_following, collected_following, termination_reason, expansion_status "
        "FROM account_nodes WHERE account_id = 'a'",
    )
    assert rows[0]["declared_following"] == 2
    assert rows[0]["collected_following"] == 2
    assert rows[0]["termination_reason"] == "empty_pages"
    assert rows[0]["expansion_status"] == "complete"

    seed = await _rows(pool, "SELECT declared_following FROM account_nodes WHERE account_id = 's'")
    assert seed[0]["declared_following"] is None, (
        "nothing ever stated the seed's following count, and an unknown is left unknown "
        "rather than filled in as complete"
    )

    coverage = await pipe.layers.coverage("t")
    assert coverage["truncated"] == 0
    assert coverage["scan_capped"] == 0
    assert coverage["mean_coverage_ratio"] == pytest.approx(1.0)
    assert coverage["termination_reasons"]["empty_pages"] >= 1


@pytest.mark.asyncio
async def test_a_scan_cap_is_not_counted_as_platform_truncation(pool):
    """Both stops produce the same ratio; only one is a statement about X."""

    platform = FakePlatform({"a": list("bcdefghij"), **{k: [] for k in "bcdefghij"}}, page_size=2)
    platform.following["s"] = ["a"]
    pipe = Pipeline(pool, platform)
    await pipe.frontier.create_seed_task("t", {"tree-a": "s"}, max_depth=1)
    await pipe.start()
    await pipe.drain_layer()
    await pipe.layers.advance_layer("t")
    pipe.scheduler._max_pages = 2  # noqa: SLF001
    await pipe.drain_layer()

    rows = await _rows(
        pool,
        "SELECT declared_following, collected_following, termination_reason "
        "FROM account_nodes WHERE account_id = 'a'",
    )
    assert rows[0]["termination_reason"] == "page_limit"
    assert rows[0]["collected_following"] == 4 and rows[0]["declared_following"] == 9

    coverage = await pipe.layers.coverage("t")
    assert coverage["scan_capped"] == 1, "we stopped this chain"
    assert coverage["truncated"] == 0, "X did not withhold anything"
    assert coverage["mean_coverage_ratio"] is None, (
        "a chain we cut short says nothing about what X was willing to hand over, "
        "so it must not move the platform coverage figure"
    )


@pytest.mark.asyncio
async def test_layer_metrics_expose_growth_and_overlap(pool):
    platform = FakePlatform({"s": ["x", "y"], "x": ["z"], "y": ["z"], "z": []}, page_size=5)
    pipe = Pipeline(pool, platform)
    await pipe.frontier.create_seed_task("t", {"tree-a": "s"})
    await pipe.run("t")

    metrics = {m.depth: m for m in await pipe.layers.layer_metrics("t")}
    assert metrics[1].nodes == 2
    assert metrics[2].nodes == 1
    assert metrics[2].collisions == 1, "z is reached twice"
    assert sum(m.requests for m in metrics.values()) == len(platform.requests)


@pytest.mark.asyncio
async def test_replaying_the_stream_does_not_duplicate_the_graph(pool):
    """Exit gate: repeats must not produce duplicate nodes, edges or counts."""

    platform = FakePlatform({"s": ["x"], "x": []}, page_size=5)
    pipe = Pipeline(pool, platform)
    await pipe.frontier.create_seed_task("t", {"tree-a": "s"})
    await pipe.run("t")

    before = await _rows(
        pool,
        "SELECT (SELECT count(*) FROM account_nodes) AS nodes, "
        "(SELECT count(*) FROM follow_edges) AS edges, "
        "(SELECT nodes_created FROM crawl_tasks WHERE task_id='t') AS charged",
    )

    for message in await pipe.broker.consumer(topics=[RAW_TOPIC]).poll():
        await pipe.parser.handle(message)

    after = await _rows(
        pool,
        "SELECT (SELECT count(*) FROM account_nodes) AS nodes, "
        "(SELECT count(*) FROM follow_edges) AS edges, "
        "(SELECT nodes_created FROM crawl_tasks WHERE task_id='t') AS charged",
    )
    assert before == after


# --- budget exhaustion ------------------------------------------------------


class _CountingAccounts:
    """A pool of exactly one account, so a single leaked lease empties it."""

    class _Lease:
        alias = "scraper-a"
        operation = "Following"
        owner_id = "worker-a"

    def __init__(self) -> None:
        self.held = 0
        self.max_held = 0

    async def lease(self, operation: str, *, owner_id: str, **kwargs):
        if self.held >= 1:
            from xgraph.accounts.manager import NoAvailableAccountError

            raise NoAvailableAccountError("pool empty")
        self.held += 1
        self.max_held = max(self.max_held, self.held)
        return self._Lease()

    async def release(self, lease, **kwargs) -> None:
        self.held -= 1

    async def report(self, lease, error_class: str, **kwargs) -> None:
        self.held -= 1


@pytest.mark.asyncio
async def test_an_exhausted_budget_does_not_strand_the_leased_account(pool):
    """The account is leased before the budget is charged, so the failure path
    has to give it back. A pool of two is emptied by two such escapes, and the
    crawl then reports "no available account" while every account still has
    quota left."""

    platform = FakePlatform({"s": ["a", "b"], "a": [], "b": []}, page_size=5)
    pipe = Pipeline(pool, platform)
    accounts = _CountingAccounts()
    pipe.accounts = accounts
    pipe.scheduler = ExpansionScheduler(
        pipe.frontier, accounts, pipe.events, pipe._collector, worker_id="worker-a"
    )
    # One request of budget: the seed's first page spends it, the next claim
    # leases an account and then finds the budget gone.
    await pipe.frontier.create_seed_task("t", {"tree-a": "s"}, max_requests=1)
    await pipe.start()

    assert await pipe.scheduler.run_once() is not None
    async with pool.acquire() as connection:
        await connection.execute("UPDATE crawl_frontier SET status='pending'")

    result = await pipe.scheduler.run_once()
    assert result is not None
    assert result.reason is TerminationReason.BUDGET_EXHAUSTED
    assert accounts.held == 0, "the lease was not returned"

    # The pool still works afterwards.
    assert await accounts.lease("Following", owner_id="probe") is not None


@pytest.mark.asyncio
async def test_losing_the_race_for_an_account_does_not_spend_the_retry_budget(pool):
    """Accounts are the scarce resource, so workers routinely outnumber them.

    A claim that never reached the network was unlucky, not poisoned. Charging
    it means a perfectly good work item is killed off after a few lost races.
    """

    platform = FakePlatform({"s": ["a"], "a": []}, page_size=5)
    pipe = Pipeline(pool, platform)

    class _EmptyPool:
        async def lease(self, operation: str, *, owner_id: str, **kwargs):
            from xgraph.accounts.manager import NoAvailableAccountError

            raise NoAvailableAccountError("pool empty")

        async def release(self, lease, **kwargs) -> None: ...

        async def report(self, lease, error_class: str, **kwargs) -> None: ...

    pipe.accounts = _EmptyPool()
    pipe.scheduler = ExpansionScheduler(
        pipe.frontier, pipe.accounts, pipe.events, pipe._collector, worker_id="worker-a"
    )
    await pipe.frontier.create_seed_task("t", {"tree-a": "s"})
    async with pool.acquire() as connection:
        await connection.execute("UPDATE crawl_frontier SET max_attempts = 3")

    for _ in range(6):
        async with pool.acquire() as connection:
            await connection.execute(
                "UPDATE crawl_frontier SET not_before = now() WHERE status = 'retryable'"
            )
        result = await pipe.scheduler.run_once()
        assert result is not None and result.error_class == "no_available_account"

    async with pool.acquire() as connection:
        row = await connection.fetchrow("SELECT status, attempt FROM crawl_frontier")
    assert row["status"] == "retryable", "six lost races must not kill the work item"
    assert row["attempt"] == 0


@pytest.mark.asyncio
async def test_an_unclassified_collector_error_does_not_take_down_the_worker(pool):
    """Workers run under `gather`, so one unhandled class stops all of them.

    The failure table cannot be complete: the platform is free to answer in a
    way nobody has seen. An unknown answer has to degrade one chain.
    """

    from xgraph.collector.errors import CollectorError

    class _Surprising(CollectorError):
        pass

    class _BrokenPlatform(FakePlatform):
        async def following_page(self, account_id, cursor=None):
            raise _Surprising("something nobody wrote a branch for")

    pipe = Pipeline(pool, _BrokenPlatform({"s": []}))
    await pipe.frontier.create_seed_task("t", {"tree-a": "s"})

    result = await pipe.scheduler.run_once()

    assert result is not None, "the worker survived"
    assert result.error_class == "_Surprising"
    async with pool.acquire() as connection:
        row = await connection.fetchrow(
            "SELECT status, last_error_class, not_before > now() AS backed_off FROM crawl_frontier"
        )
    assert row["status"] == "retryable"
    assert row["backed_off"] is True, "an unknown failure must not be retried immediately"


@pytest.mark.asyncio
async def test_an_html_interstitial_is_a_block_at_any_status(pool):
    """An edge challenge can arrive with 200; the content type is the signal."""

    from xgraph.collector.errors import BlockedError, blocked_error

    assert isinstance(
        blocked_error(200, {"content-type": "text/html; charset=utf-8", "cf-ray": "abc"}),
        BlockedError,
    )
    assert blocked_error(200, {"content-type": "application/json"}) is None
