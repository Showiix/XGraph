"""Approach order, circles and introduction paths, against the real fact store.

Every answer here is a ranking, so a mistake does not raise — it produces a
confident order that happens to put the wrong name first. The fixture is built so
that the obvious ranking and the correct one disagree.
"""

import os
from typing import Any

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from xgraph.api import create_app
from xgraph.service import AccountFilter, OutreachQueries, ProductQueries
from xgraph.storage import apply_schema, create_pool

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

#: Sixty accounts whose following lists were collected. Large enough that the
#: ratio ceiling — the voter total over an account's own followers — leaves room
#: for peers to sit below it, which is where the two rankings disagree.
VOTERS = [f"v{i:02d}" for i in range(1, 61)]

#: Who each candidate is followed by, chosen so that the obvious ranking and the
#: correct one disagree, in both the approach order and the circle.
FOLLOWED_BY: dict[str, list[str]] = {
    # The widest audience, and the reference account for affinity.
    "hub": VOTERS[:24],
    # Second by in-degree and worth nothing after `hub`: its audience is a
    # subset, so a degree-ordered plan spends its second slot here for nothing.
    # Being a subset also pins its overlap ratio to the ceiling.
    "twin": VOTERS[:20],
    # Smaller, also entirely inside `hub`, so it reaches the same ceiling ratio
    # on a third of the evidence.
    "tiny": VOTERS[:9],
    # Almost inside `hub` but not quite, so its ratio sits *below* the ceiling
    # while its evidence is far stronger than `tiny`'s. Ranking on the ratio puts
    # it under `tiny`; ranking on how far the overlap sits from chance does not.
    "wide": VOTERS[:22] + VOTERS[49:51],
    # Reaches six people no one else does — the only account worth a second slot.
    "edge": VOTERS[24:30],
    # Unreachable, so it must never appear in a plan filtered to can_dm.
    "silent": VOTERS[:22],
    # Exists so the remaining voters have collected lists of their own. Without
    # it they are not voters at all and every ratio is measured against a circle
    # two thirds smaller than the one that was actually crawled.
    "filler": VOTERS[30:49] + VOTERS[51:],
}

#: A voter is an account whose following list produced at least one edge. The
#: rest exist as accounts and follow nobody, which is the ordinary case for
#: anything below the layer that was expanded.
ACTIVE_VOTERS = sorted({voter for voters in FOLLOWED_BY.values() for voter in voters})

#: `silent` and `filler` cannot be messaged; the rest can.
REACHABLE = {"hub", "twin", "edge", "tiny", "wide"}


@pytest_asyncio.fixture(loop_scope="function")
async def pool():
    pool = await create_pool(os.environ["XGRAPH_TEST_DATABASE_URL"], min_size=1, max_size=8)
    try:
        await apply_schema(pool)
        async with pool.acquire() as connection:
            await connection.execute(f"TRUNCATE {TABLES} RESTART IDENTITY CASCADE")
            await _fixture(connection)
        yield pool
    finally:
        await pool.close()


@pytest_asyncio.fixture(loop_scope="function")
async def client(pool):
    app = create_app(pool=pool)
    async with (
        AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as http,
        app.router.lifespan_context(app),
    ):
        yield http


async def _fixture(c: Any) -> None:
    await c.execute(
        "INSERT INTO crawl_tasks(task_id, status, current_depth, max_depth) "
        "VALUES ('t', 'running', 1, 5)"
    )
    await c.execute(
        "INSERT INTO root_trees(task_id, tree_id, seed_account_id) VALUES ('t','tree-a','v01')"
    )

    accounts = VOTERS + list(FOLLOWED_BY)
    for account in accounts:
        await c.execute(
            "INSERT INTO account_nodes(task_id, account_id, first_depth, expansion_status) "
            "VALUES ('t', $1, 1, 'complete')",
            account,
        )
        await c.execute(
            "INSERT INTO account_profiles(task_id, account_id, username, followers_count, "
            "following_count, can_dm, protected) VALUES ('t', $1, $1, 50000, 500, $2, false)",
            account,
            account in REACHABLE or account.startswith("v"),
        )
    # One voter that cannot be messaged, so an introduction list has something to
    # leave out other than by accident.
    await c.execute(
        "UPDATE account_profiles SET can_dm = false WHERE task_id='t' AND account_id='v01'"
    )
    await c.execute(
        "UPDATE account_profiles SET protected = true WHERE task_id='t' AND account_id='v02'"
    )

    for target, voters in FOLLOWED_BY.items():
        for voter in voters:
            await c.execute(
                "INSERT INTO follow_edges(task_id, source_account_id, target_account_id, "
                "source_depth, target_depth) VALUES ('t', $1, $2, 1, 1)",
                voter,
                target,
            )
    # Mirrors what `insert_edges` maintains alongside the rows; without it the
    # fixture describes a graph production never produces.
    await c.execute(
        """
        UPDATE account_nodes AS n
        SET network_indegree = (SELECT count(*) FROM follow_edges e
                                WHERE e.task_id = n.task_id
                                  AND e.target_account_id = n.account_id)
        WHERE n.task_id = 't';
        """
    )


def _plan(pool, **filters):
    """The reachable set, which is the only one an approach order is about."""

    f = AccountFilter(limit=1, can_dm=True, **filters)
    return f, ProductQueries(pool), OutreachQueries(pool)


# --- approach order ---------------------------------------------------------


@pytest.mark.asyncio
async def test_the_order_prefers_reach_it_does_not_already_have(pool):
    """Exit gate: the second pick is the one that adds people, not the one with
    the second-most followers.

    Ranking by in-degree keeps choosing the same corner of the circle, because
    the runner-up is largely followed by the people who already follow the
    winner. The list grows long before it grows broad.
    """

    f, queries, outreach = _plan(pool)
    ids = await queries.account_ids("t", f, cap=100)
    plan = await outreach.approach_plan("t", ids, picks=5)

    order = [step.username for step in plan.steps]
    assert order[0] == "hub", "the widest audience is still the right first pick"
    assert order[1] == "edge", (
        "`twin` has the second-largest audience and adds nobody; `edge` is smaller and "
        "reaches six people no one else does"
    )
    assert plan.steps[0].new_reach == 24
    assert plan.steps[1].new_reach == 6
    assert plan.steps[1].cumulative_reach == 30


@pytest.mark.asyncio
async def test_the_order_stops_when_nothing_is_left_to_reach(pool):
    """A longer list that adds nobody looks like progress and is not."""

    f, queries, outreach = _plan(pool)
    ids = await queries.account_ids("t", f, cap=100)
    plan = await outreach.approach_plan("t", ids, picks=20)

    assert len(plan.steps) < len(ids), "it did not pad the list out to the request"
    assert all(step.new_reach > 0 for step in plan.steps)
    assert any("already reached" in warning for warning in plan.warnings)


@pytest.mark.asyncio
async def test_the_order_is_computed_over_the_accounts_that_were_asked_for(pool):
    """The best accounts to approach among everyone are not the best among the
    ones that can be approached."""

    queries, outreach = ProductQueries(pool), OutreachQueries(pool)
    everyone = await queries.account_ids("t", AccountFilter(limit=1), cap=100)
    reachable = await queries.account_ids("t", AccountFilter(limit=1, can_dm=True), cap=100)
    assert "silent" in everyone and "silent" not in reachable

    plan = await outreach.approach_plan("t", reachable, picks=5)
    assert all(step.username != "silent" for step in plan.steps)
    assert all(step.can_dm for step in plan.steps)


@pytest.mark.asyncio
async def test_coverage_share_is_measured_against_the_whole_circle(pool):
    """Against the accounts whose lists were collected, not against the selection.

    Measured against itself every plan covers everything it can, and the number
    says nothing about how much of the circle it actually holds.
    """

    outreach = OutreachQueries(pool)
    alone = await outreach.approach_plan("t", ["tiny"], picks=5)
    assert alone.voters_total == len(ACTIVE_VOTERS)
    assert alone.steps[0].cumulative_reach == len(FOLLOWED_BY["tiny"])
    assert alone.steps[0].coverage_share == pytest.approx(
        len(FOLLOWED_BY["tiny"]) / len(ACTIVE_VOTERS), abs=1e-4
    )
    assert alone.steps[0].coverage_share < 1.0, (
        "a plan containing one small account does not cover the circle, however "
        "completely it covers itself"
    )

    everyone = await outreach.approach_plan(
        "t", await ProductQueries(pool).account_ids("t", AccountFilter(limit=1), cap=100), picks=5
    )
    assert everyone.steps[-1].coverage_share == pytest.approx(
        everyone.steps[-1].cumulative_reach / len(ACTIVE_VOTERS), abs=1e-4
    )


# --- circles ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_affinity_is_ranked_by_evidence_not_by_the_ratio(pool):
    """Exit gate: the ratio saturates, so it cannot order peers of different sizes.

    It is bounded by the voter total over this account's own followers, so any
    peer whose audience happens to sit inside this one's reaches that ceiling
    exactly — one with nine voters as surely as one with twenty. `wide` sits just
    below the ceiling on far more evidence, and belongs above `tiny`, which the
    ratio alone will never say.
    """

    peers = {p.username: p for p in await OutreachQueries(pool).affinity("t", "hub", limit=20)}
    tiny, wide = peers["tiny"], peers["wide"]

    ceiling = len(ACTIVE_VOTERS) / len(FOLLOWED_BY["hub"])
    assert tiny.lift == pytest.approx(ceiling, abs=0.01), "pinned to the ceiling"
    assert wide.lift < tiny.lift, "and by the ratio alone `wide` looks like the weaker peer"

    assert wide.excess_sigma > tiny.excess_sigma
    order = [p.username for p in await OutreachQueries(pool).affinity("t", "hub", limit=20)]
    assert order.index("wide") < order.index("tiny"), (
        "evidence decides the order; the ratio is only the readable number"
    )


@pytest.mark.asyncio
async def test_affinity_ignores_peers_with_too_little_in_common(pool):
    """Below the floor the question is not how surprising the overlap is."""

    peers = await OutreachQueries(pool).affinity("t", "edge", limit=10)
    assert all(peer.shared_voters >= 8 for peer in peers)


# --- introductions ----------------------------------------------------------


@pytest.mark.asyncio
async def test_introductions_only_lists_accounts_that_can_be_reached(pool):
    """A follower who cannot be messaged is not a path to anyone."""

    result = await OutreachQueries(pool).introductions("t", "hub", limit=50)
    names = [row["username"] for row in result["introducers"]]

    assert names, "hub is followed by two dozen accounts"
    assert "v01" not in names, "cannot be messaged"
    assert "v02" not in names, "protected"
    assert set(names) <= set(FOLLOWED_BY["hub"]), "and every one of them follows hub"
    assert result["total"] == len(names)


# --- the HTTP surface -------------------------------------------------------


@pytest.mark.asyncio
async def test_the_plan_endpoint_takes_the_same_filters_as_the_listing(client):
    response = await client.get(
        "/api/tasks/t/outreach/plan", params={"picks": 5, "can_dm": True, "min_followers": 1}
    )
    assert response.status_code == 200
    body = response.json()
    assert [step["username"] for step in body["steps"]][:2] == ["hub", "edge"]
    assert body["voters_total"] == len(ACTIVE_VOTERS)
    assert body["filters"]["can_dm"] is True
    assert all(step["can_dm"] for step in body["steps"])


@pytest.mark.asyncio
async def test_the_outreach_endpoints_do_not_leak_collection_machinery(client):
    for path in (
        "/api/tasks/t/outreach/plan",
        "/api/tasks/t/accounts/hub/affinity",
        "/api/tasks/t/accounts/hub/introductions",
    ):
        body = (await client.get(path)).text
        for secret in ("credential_ref", "scraper", "proxy", "auth_token"):
            assert secret not in body, f"{path} exposed {secret}"
