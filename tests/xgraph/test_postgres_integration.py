import asyncio
import os
from datetime import datetime, timedelta, timezone

import asyncpg
import pytest
import pytest_asyncio

from xgraph.accounts.manager import NoAvailableAccountError, PostgresAccountManager
from xgraph.collector.client import page_event_id
from xgraph.domain import FrontierStatus, Operation
from xgraph.storage import BudgetExhaustedError, PostgresFrontierStore, apply_schema, create_pool

pytestmark = pytest.mark.skipif(
    not os.getenv("XGRAPH_TEST_DATABASE_URL"),
    reason="XGRAPH_TEST_DATABASE_URL is not configured",
)


@pytest.mark.asyncio
async def test_postgres_claim_quota_and_budget_contracts():
    pool = await create_pool(os.environ["XGRAPH_TEST_DATABASE_URL"], min_size=1, max_size=6)
    try:
        await apply_schema(pool)
        async with pool.acquire() as connection:
            await connection.execute(
                "TRUNCATE raw_page_outbox, request_attempts, account_operation_quota, "
                "scraper_accounts, crawl_frontier, follow_edges, account_nodes, "
                "root_trees, crawl_tasks RESTART IDENTITY CASCADE"
            )

        frontier = PostgresFrontierStore(pool)
        await frontier.create_seed_task(
            "task-integration",
            {"tree-a": "seed-a", "tree-b": "seed-b"},
            max_requests=2,
        )
        first, second = await asyncio.gather(
            frontier.claim_frontier(worker_id="worker-a"),
            frontier.claim_frontier(worker_id="worker-b"),
        )
        assert first is not None and second is not None
        assert first.frontier_id != second.frontier_id
        assert {first.account_id, second.account_id} == {"seed-a", "seed-b"}

        async def reserve():
            try:
                attempt = await frontier.reserve_request("task-integration", Operation.FOLLOWING)
                return attempt.sequence
            except BudgetExhaustedError:
                return "exhausted"

        reservations = await asyncio.gather(reserve(), reserve(), reserve())
        assert sorted(reservations, key=str) == [1, 2, "exhausted"]
        async with pool.acquire() as connection:
            counts = await connection.fetchrow(
                """
                SELECT t.requests_attempted,
                       b.requests_attempted AS operation_attempted,
                       (SELECT count(*) FROM request_attempts WHERE task_id = t.task_id) AS attempts
                FROM crawl_tasks AS t
                JOIN task_operation_budgets AS b ON b.task_id = t.task_id
                WHERE t.task_id = $1 AND b.operation = 'Following'
                """,
                "task-integration",
            )
        assert tuple(counts.values()) == (2, 2, 2)

        assert await frontier.reserve_graph_capacity("task-integration", nodes=2, edges=3) == (4, 3)
        with pytest.raises(BudgetExhaustedError):
            await frontier.reserve_graph_capacity("task-integration", nodes=1_000_000)

        async with pool.acquire() as connection:
            await connection.execute(
                "UPDATE crawl_frontier SET lease_expires_at = now() - interval '1 second' "
                "WHERE frontier_id = $1",
                first.frontier_id,
            )
        reclaimed = await frontier.claim_frontier(worker_id="worker-c")
        assert reclaimed is not None
        assert reclaimed.frontier_id == first.frontier_id
        assert reclaimed.owner_id == "worker-c"

        accounts = PostgresAccountManager(pool)
        await accounts.register_account(
            alias="scraper-a", credential_ref="secret://a", user_agent="ua-a"
        )
        await accounts.register_account(
            alias="scraper-b", credential_ref="secret://b", user_agent="ua-b"
        )
        lease_a, lease_b = await asyncio.gather(
            accounts.lease("Following", owner_id="worker-a"),
            accounts.lease("Following", owner_id="worker-b"),
        )
        assert lease_a.alias != lease_b.alias
        await accounts.release(
            lease_a,
            remaining=0,
            limit_max=188,
            reset_at=datetime.now(timezone.utc) - timedelta(seconds=1),
        )
        recovered = await accounts.lease("Following", owner_id="worker-c")
        assert recovered.alias == lease_a.alias
        async with pool.acquire() as connection:
            in_flight = await connection.fetchval(
                "SELECT in_flight FROM account_operation_quota "
                "WHERE alias = $1 AND operation = 'Following'",
                recovered.alias,
            )
        assert in_flight == 1

        await accounts.register_account(
            alias="scraper-standby",
            credential_ref="secret://standby",
            user_agent="ua-standby",
            standby=True,
        )
        await asyncio.gather(
            accounts.lease("UserTweets", owner_id="worker-a"),
            accounts.lease("UserTweets", owner_id="worker-b"),
        )
        with pytest.raises(NoAvailableAccountError):
            await accounts.lease("UserTweets", owner_id="worker-d")
        standby = await accounts.lease("UserTweets", owner_id="worker-d", allow_standby=True)
        assert standby.alias == "scraper-standby"
    finally:
        await pool.close()


TABLES = (
    "raw_page_outbox, request_attempts, account_operation_quota, scraper_accounts, "
    "follow_edge_observations, follow_edges, account_observations, crawl_frontier, "
    "task_operation_budgets, account_nodes, root_trees, crawl_tasks"
)


@pytest_asyncio.fixture(loop_scope="function")
async def pool():
    pool = await create_pool(os.environ["XGRAPH_TEST_DATABASE_URL"], min_size=1, max_size=6)
    try:
        await apply_schema(pool)
        async with pool.acquire() as connection:
            await connection.execute(f"TRUNCATE {TABLES} RESTART IDENTITY CASCADE")
        yield pool
    finally:
        await pool.close()


async def _one_account(pool, *, alias: str = "scraper-a") -> PostgresAccountManager:
    accounts = PostgresAccountManager(pool)
    await accounts.register_account(
        alias=alias,
        credential_ref=f"secret://{alias}",
        user_agent="ua",
        operations=("Following",),
    )
    return accounts


async def _is_leasable(accounts: PostgresAccountManager) -> bool:
    try:
        lease = await accounts.lease("Following", owner_id="probe")
    except NoAvailableAccountError:
        return False
    await accounts.release(lease, remaining=100)
    return True


@pytest.mark.parametrize(
    ("label", "release_kwargs"),
    [
        # A failed request whose response carried no rate-limit headers at all.
        ("failure without headers", {"success": False}),
        # Quota exhausted but the reset header was missing from the response.
        ("exhausted without reset", {"success": True, "remaining": 0}),
        ("exhausted with reset", {"success": True, "remaining": 0, "reset_at": None}),
        ("normal success", {"success": True, "remaining": 50}),
    ],
)
@pytest.mark.asyncio
async def test_released_lease_is_always_recoverable(pool, label, release_kwargs):
    """No release path may leave an (account, operation) pair permanently unleasable.

    A stranded row shrinks the pool with no error and no metric, so the crawl
    slows to a halt and looks like a shortage of accounts rather than a defect.
    """

    if label == "exhausted with reset":
        release_kwargs["reset_at"] = datetime.now(timezone.utc) + timedelta(minutes=5)
    accounts = await _one_account(pool)
    lease = await accounts.lease("Following", owner_id="worker-a")
    await accounts.release(lease, **release_kwargs)

    if not await _is_leasable(accounts):
        # Cooling is legitimate, but only if the cooldown actually ends.
        async with pool.acquire() as connection:
            state, reset_at = await connection.fetchrow(
                "SELECT state, reset_at FROM account_operation_quota WHERE alias = 'scraper-a'"
            )
            assert state == "cooling", f"{label}: unleasable while state={state}"
            assert reset_at is not None, f"{label}: cooling without a reset time"
            await connection.execute(
                "UPDATE account_operation_quota SET reset_at = now() - interval '1 second'"
            )
        assert await _is_leasable(accounts), f"{label}: still unleasable after the reset elapsed"


@pytest.mark.asyncio
async def test_abandoned_lease_returns_to_the_pool(pool):
    accounts = await _one_account(pool)
    await accounts.lease("Following", owner_id="worker-a", lease_seconds=1)

    assert not await _is_leasable(accounts), "a live lease must stay exclusive"
    async with pool.acquire() as connection:
        await connection.execute(
            "UPDATE account_operation_quota SET lease_expires_at = now() - interval '1 second'"
        )
    assert await _is_leasable(accounts), "an expired lease must return to the pool"


@pytest.mark.asyncio
async def test_repeated_failures_reach_an_explicit_terminal_state(pool):
    """Exhausting the error budget must disable the row, not silently strand it."""

    accounts = PostgresAccountManager(pool, error_budget=3)
    await accounts.register_account(
        alias="scraper-a", credential_ref="secret://a", user_agent="ua", operations=("Following",)
    )
    for _ in range(3):
        lease = await accounts.lease("Following", owner_id="worker-a")
        await accounts.release(lease, success=False)

    async with pool.acquire() as connection:
        state = await connection.fetchval(
            "SELECT state FROM account_operation_quota WHERE alias = 'scraper-a'"
        )
    assert state == "disabled"
    with pytest.raises(NoAvailableAccountError):
        await accounts.lease("Following", owner_id="worker-b")


@pytest.mark.parametrize(
    ("column", "value"),
    [("remaining", 0), ("state", "cooling"), ("state", "leased")],
)
@pytest.mark.asyncio
async def test_schema_rejects_unleasable_quota_rows(pool, column, value):
    """The states the lease query can never select again must be unrepresentable."""

    await _one_account(pool)
    async with pool.acquire() as connection:
        with pytest.raises(asyncpg.CheckViolationError):
            await connection.execute(
                f"UPDATE account_operation_quota SET {column} = $1, reset_at = NULL, "
                "lease_owner = NULL WHERE alias = 'scraper-a'",
                value,
            )


@pytest.mark.asyncio
async def test_edge_pointing_back_at_a_seed_is_storable(pool):
    """Closure edges carry the strongest circle signal and must not be rejected.

    An L1 account that follows one of the Seeds produces an edge whose target
    sits at depth 0. Rejecting it would drop exactly the evidence the product
    uses to rank accounts by in-network endorsement.
    """

    frontier = PostgresFrontierStore(pool)
    await frontier.create_seed_task("task-closure", {"tree-a": "seed-a"})
    async with pool.acquire() as connection:
        await connection.execute(
            "INSERT INTO follow_edges(task_id, source_account_id, target_account_id, "
            "source_depth, target_depth) VALUES ('task-closure', 'l1-user', 'seed-a', 1, 0)"
        )
        depth = await connection.fetchval(
            "SELECT target_depth FROM follow_edges WHERE target_account_id = 'seed-a'"
        )
    assert depth == 0


@pytest.mark.asyncio
async def test_seed_observation_without_a_parent_is_storable(pool):
    """L0 observations have no parent; the natural key must tolerate NULL."""

    frontier = PostgresFrontierStore(pool)
    await frontier.create_seed_task("task-seed", {"tree-a": "seed-a"})
    async with pool.acquire() as connection:
        for _ in range(2):
            await connection.execute(
                "INSERT INTO account_observations(task_id, tree_id, account_id, "
                "parent_account_id, depth) VALUES ('task-seed', 'tree-a', 'seed-a', NULL, 0) "
                "ON CONFLICT DO NOTHING"
            )
        count = await connection.fetchval("SELECT count(*) FROM account_observations")
    assert count == 1


@pytest.mark.asyncio
async def test_frontier_attempt_budget_reaches_a_terminal_state(pool):
    """A poisoned row must stop being offered and must stop being 'in progress'.

    Completion is defined over frontier status, so a row that is invisible to
    claim but still `pending` keeps the task running forever with nothing to do.
    """

    frontier = PostgresFrontierStore(pool)
    await frontier.create_seed_task("task-poison", {"tree-a": "seed-a"})
    async with pool.acquire() as connection:
        await connection.execute("UPDATE crawl_frontier SET max_attempts = 2")

    for _ in range(2):
        item = await frontier.claim_frontier(worker_id="worker-a")
        assert item is not None
        await frontier.finish_frontier(item, FrontierStatus.RETRYABLE, error_class="transport")

    assert await frontier.claim_frontier(worker_id="worker-a") is None
    async with pool.acquire() as connection:
        status = await connection.fetchval("SELECT status FROM crawl_frontier")
    assert status == "failed", "the last retry within budget must become terminal"


@pytest.mark.asyncio
async def test_exhausted_frontier_rows_are_swept_to_a_terminal_state(pool):
    frontier = PostgresFrontierStore(pool)
    await frontier.create_seed_task("task-sweep", {"tree-a": "seed-a"})
    async with pool.acquire() as connection:
        await connection.execute("UPDATE crawl_frontier SET max_attempts = 1")
    item = await frontier.claim_frontier(worker_id="worker-a")
    assert item is not None  # the holder then dies without finishing
    async with pool.acquire() as connection:
        await connection.execute(
            "UPDATE crawl_frontier SET lease_expires_at = now() - interval '1 second'"
        )

    assert await frontier.claim_frontier(worker_id="worker-b") is None
    assert await frontier.fail_exhausted_frontier("task-sweep") == 1
    async with pool.acquire() as connection:
        row = await connection.fetchrow("SELECT status, last_error_class FROM crawl_frontier")
    assert row["status"] == "failed"
    assert row["last_error_class"] == "attempt_budget_exhausted"


@pytest.mark.asyncio
async def test_outbox_is_keyed_by_stable_page_identity(pool):
    """Re-fetching a page must not enqueue it twice.

    `event_id` identifies the work, not the bytes, so a retry after a crash
    collides with the row already stored instead of double-counting the page.
    """

    frontier = PostgresFrontierStore(pool)
    await frontier.create_seed_task("task-outbox", {"tree-a": "seed-a"})
    event_id = page_event_id(Operation.FOLLOWING, "seed-a", None)
    assert event_id == page_event_id(Operation.FOLLOWING, "seed-a", None)
    assert event_id != page_event_id(Operation.FOLLOWING, "seed-a", "cursor-2")

    async with pool.acquire() as connection:
        for _ in range(2):
            await connection.execute(
                "INSERT INTO raw_page_outbox(event_id, task_id, account_id, operation, payload) "
                "VALUES ($1, 'task-outbox', 'seed-a', 'Following', '{}'::jsonb) "
                "ON CONFLICT DO NOTHING",
                event_id,
            )
        count = await connection.fetchval("SELECT count(*) FROM raw_page_outbox")
    assert count == 1
