from datetime import datetime, timezone

import pytest

from xgraph.accounts import AccountLease, OperationState
from xgraph.accounts.manager import NoAvailableAccountError, PostgresAccountManager
from xgraph.domain import FrontierStatus, Operation
from xgraph.storage import (
    BudgetExhaustedError,
    FrontierItem,
    PostgresFrontierStore,
    RequestAttempt,
)


class FakeTransaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None


class FakeConnection:
    def __init__(self, *, rows=None, execute_result="UPDATE 1"):
        self.rows = list(rows or [])
        self.execute_result = execute_result
        self.queries: list[tuple[str, tuple]] = []

    def transaction(self):
        return FakeTransaction()

    async def fetchrow(self, query: str, *args):
        self.queries.append((query, args))
        return self.rows.pop(0) if self.rows else None

    async def execute(self, query: str, *args):
        self.queries.append((query, args))
        return self.execute_result


class FakeAcquire:
    def __init__(self, connection: FakeConnection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, *args):
        return None


class FakePool:
    def __init__(self, connection: FakeConnection):
        self.connection = connection

    def acquire(self):
        return FakeAcquire(self.connection)


@pytest.mark.asyncio
async def test_create_and_enqueue_use_idempotent_postgres_commands():
    connection = FakeConnection(execute_result="INSERT 0 1")
    store = PostgresFrontierStore(FakePool(connection))

    await store.create_task("task-1", max_requests=188, max_nodes=10, max_edges=20)
    inserted = await store.enqueue_frontier(
        "task-1", "account-1", Operation.FOLLOWING, 0, priority=3
    )

    assert inserted is True
    assert any(
        "ON CONFLICT (task_id, account_id, operation) DO NOTHING" in query
        for query, _ in connection.queries
    )
    assert connection.queries[0][1] == ("task-1", 188, 10, 20)


@pytest.mark.asyncio
async def test_claim_frontier_returns_typed_lease_and_uses_skip_locked():
    expires_at = datetime(2026, 9, 4, tzinfo=timezone.utc)
    connection = FakeConnection(
        rows=[
            {
                "frontier_id": 7,
                "task_id": "task-1",
                "account_id": "account-1",
                "tree_id": "tree-a",
                "operation": "Following",
                "depth": 0,
                "cursor_in": None,
                "attempt": 1,
                "owner_id": "worker-1",
                "lease_expires_at": expires_at,
            }
        ]
    )
    item = await PostgresFrontierStore(FakePool(connection)).claim_frontier(worker_id="worker-1")

    assert item is not None
    assert item.operation is Operation.FOLLOWING
    assert item.owner_id == "worker-1"
    assert "FOR UPDATE OF f SKIP LOCKED" in connection.queries[0][0]
    assert "f.lease_expires_at <= now()" in connection.queries[0][0]


@pytest.mark.asyncio
async def test_claim_frontier_returns_none_when_no_ready_work():
    connection = FakeConnection()
    item = await PostgresFrontierStore(FakePool(connection)).claim_frontier(worker_id="worker-1")

    assert item is None


@pytest.mark.asyncio
async def test_finish_frontier_requires_current_owner():
    connection = FakeConnection(execute_result="UPDATE 0")
    store = PostgresFrontierStore(FakePool(connection))
    item = FrontierItem(
        frontier_id=7,
        task_id="task-1",
        account_id="account-1",
        tree_id="tree-a",
        operation=Operation.FOLLOWING,
        depth=0,
        cursor_in=None,
        attempt=1,
        owner_id="worker-1",
        lease_expires_at=datetime.now(timezone.utc),
    )

    with pytest.raises(RuntimeError, match="lease was lost"):
        await store.finish_frontier(item, FrontierStatus.COMPLETED)


@pytest.mark.asyncio
async def test_checkpoint_frontier_requires_live_owner_lease():
    connection = FakeConnection(execute_result="UPDATE 0")
    item = FrontierItem(
        frontier_id=7,
        task_id="task-1",
        account_id="account-1",
        tree_id="tree-a",
        operation=Operation.FOLLOWING,
        depth=0,
        cursor_in=None,
        attempt=1,
        owner_id="worker-1",
        lease_expires_at=datetime.now(timezone.utc),
    )

    with pytest.raises(RuntimeError, match="lease was lost"):
        await PostgresFrontierStore(FakePool(connection)).checkpoint_frontier(item, "next")


@pytest.mark.asyncio
async def test_seed_task_creation_is_one_transaction():
    connection = FakeConnection()
    await PostgresFrontierStore(FakePool(connection)).create_seed_task(
        "task-1", {"tree-a": "seed-a", "tree-b": "seed-b"}, max_requests=188
    )

    assert len(connection.queries) == 10
    assert "INSERT INTO crawl_tasks" in connection.queries[0][0]
    assert (
        sum("INSERT INTO task_operation_budgets" in query for query, _ in connection.queries) == 3
    )
    assert sum("INSERT INTO root_trees" in query for query, _ in connection.queries) == 2
    assert sum("INSERT INTO crawl_frontier" in query for query, _ in connection.queries) == 2


@pytest.mark.asyncio
async def test_request_budget_is_reserved_atomically():
    connection = FakeConnection(
        rows=[
            {
                "requests_attempted": 187,
                "max_requests": 188,
                "operation_attempted": 10,
                "operation_max": 188,
            },
            {"attempt_id": 42},
        ]
    )
    attempt = await PostgresFrontierStore(FakePool(connection)).reserve_request(
        "task-1", Operation.FOLLOWING, frontier_id=7, scraper_alias="scraper-1"
    )

    assert attempt.sequence == 188
    assert attempt.attempt_id == 42
    assert "FOR UPDATE OF t, b" in connection.queries[0][0]


@pytest.mark.asyncio
async def test_request_budget_exhaustion_is_explicit():
    connection = FakeConnection()
    with pytest.raises(BudgetExhaustedError):
        await PostgresFrontierStore(FakePool(connection)).reserve_request(
            "task-1", Operation.FOLLOWING
        )


@pytest.mark.asyncio
async def test_request_attempt_can_only_finish_once():
    connection = FakeConnection(execute_result="UPDATE 0")
    attempt = RequestAttempt(42, "task-1", 7, "scraper-1", Operation.FOLLOWING, 1)

    with pytest.raises(RuntimeError, match="already finished"):
        await PostgresFrontierStore(FakePool(connection)).finish_request(
            attempt, outcome="succeeded", status_code=200
        )


@pytest.mark.asyncio
async def test_graph_capacity_reservation_is_atomic():
    connection = FakeConnection(rows=[{"nodes_created": 10, "edges_created": 20}])
    result = await PostgresFrontierStore(FakePool(connection)).reserve_graph_capacity(
        "task-1", nodes=2, edges=3
    )

    assert result == (10, 20)
    assert "nodes_created + $2 <= max_nodes" in connection.queries[0][0]


@pytest.mark.asyncio
async def test_account_manager_leases_highest_available_quota():
    expires_at = datetime(2026, 9, 4, tzinfo=timezone.utc)
    connection = FakeConnection(
        rows=[
            {
                "alias": "scraper-1",
                "operation": "Following",
                "lease_owner": "worker-1",
                "lease_expires_at": expires_at,
            }
        ]
    )
    lease = await PostgresAccountManager(FakePool(connection)).lease(
        "Following", owner_id="worker-1"
    )

    assert lease == AccountLease("scraper-1", "Following", "worker-1", expires_at)
    assert "THEN q.limit_max ELSE q.remaining END DESC" in connection.queries[0][0]
    assert "FOR UPDATE OF q SKIP LOCKED" in connection.queries[0][0]
    assert "q.lease_expires_at <= now()" in connection.queries[0][0]
    assert connection.queries[0][1] == ("Following", "worker-1", 60, False)


@pytest.mark.asyncio
async def test_account_registration_seeds_each_operation_with_its_own_quota():
    """The endpoints differ by more than threefold; one number would waste most of it."""

    connection = FakeConnection()
    await PostgresAccountManager(FakePool(connection)).register_account(
        alias="scraper-1",
        credential_ref="secret://scraper-1",
        user_agent="ua",
    )

    assert len(connection.queries) == 4
    assert "INSERT INTO scraper_accounts" in connection.queries[0][0]
    seeded = {args[1]: args[-1] for _, args in connection.queries[1:]}
    assert seeded == {"Following": 500, "UserTweets": 150, "UserByScreenName": 150}


@pytest.mark.asyncio
async def test_an_explicit_default_overrides_every_operation():
    connection = FakeConnection()
    await PostgresAccountManager(FakePool(connection)).register_account(
        alias="scraper-1",
        credential_ref="secret://scraper-1",
        user_agent="ua",
        default_limit=42,
    )

    assert all(args[-1] == 42 for _, args in connection.queries[1:])


@pytest.mark.asyncio
async def test_account_manager_no_available_account_is_retryable():
    manager = PostgresAccountManager(FakePool(FakeConnection()))

    with pytest.raises(NoAvailableAccountError):
        await manager.lease("Following", owner_id="worker-1")


@pytest.mark.asyncio
async def test_rate_limit_report_requires_reset_time():
    manager = PostgresAccountManager(FakePool(FakeConnection()))
    lease = AccountLease("scraper-1", "Following", "worker-1", datetime.now(timezone.utc))

    with pytest.raises(ValueError, match="require reset_at"):
        await manager.report(lease, "rate_limited")


@pytest.mark.asyncio
async def test_account_release_rejects_lost_operation_lease():
    connection = FakeConnection(execute_result="UPDATE 0")
    manager = PostgresAccountManager(FakePool(connection))
    lease = AccountLease("scraper-1", "Following", "worker-1", datetime.now(timezone.utc))

    with pytest.raises(RuntimeError, match="lease was lost"):
        await manager.release(lease, remaining=187, limit_max=188)


def test_operation_state_contract_has_expected_values():
    assert OperationState.READY.value == "ready"
    assert OperationState.COOLING.value == "cooling"
