"""Stage 6: the product surface, against the real fact store.

The queries carry the data-quality evidence, so a correctness bug here does not
show up as an error — it shows up as a confident answer that happens to be
wrong. Everything is therefore asserted against a database whose contents the
test set up itself.
"""

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from xgraph.api import create_app
from xgraph.service import AccountFilter, EdgeFilter, ProductQueries, parse_seed_list
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

NOW = datetime(2026, 9, 1, tzinfo=timezone.utc)


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
    """A small finished task with every data-quality state represented."""

    await c.execute(
        "INSERT INTO crawl_tasks(task_id, status, current_depth, max_depth, "
        "pages_produced, pages_processed) VALUES ('t', 'running', 1, 5, 10, 8)"
    )
    await c.execute(
        "INSERT INTO root_trees(task_id, tree_id, seed_account_id) "
        "VALUES ('t','tree-a','seed'), ('t','tree-b','seed2')"
    )

    accounts = [
        # id,      depth, boundary, status,     declared, collected, timeline
        ("seed", 0, False, "complete", 3, 3, "none"),
        ("seed2", 0, False, "complete", 1, 1, "none"),
        ("hub", 1, False, "complete", 900, 800, "complete"),  # truncated
        ("small", 1, False, "filtered", None, 0, "none"),
        ("broken", 1, False, "failed", 50, 12, "none"),
        ("edge6", 6, True, "boundary", None, 0, "none"),
    ]
    for account_id, depth, boundary, status, declared, collected, timeline in accounts:
        await c.execute(
            "INSERT INTO account_nodes(task_id, account_id, first_depth, is_l6_boundary, "
            "expansion_status, declared_following, collected_following, termination_reason, "
            "timeline_status) VALUES ('t',$1,$2,$3,$4,$5,$6,$7,$8)",
            account_id,
            depth,
            boundary,
            status,
            declared,
            collected,
            "natural_end" if status == "complete" else None,
            timeline,
        )
    profiles = [
        ("seed", "seedy", 5_000, True, False, False, "founder"),
        ("seed2", "seedtwo", 4_000, True, False, False, ""),
        ("hub", "hubby", 120_000, True, True, False, "AI researcher"),
        ("small", "smallfry", 40, True, False, False, ""),
        ("broken", "brokenacct", 9_000, False, False, True, ""),
        ("edge6", "faraway", 700, True, False, False, ""),
    ]
    for account_id, username, followers, can_dm, verified, protected, bio in profiles:
        await c.execute(
            "INSERT INTO account_profiles(task_id, account_id, username, display_name, "
            "description, followers_count, following_count, can_dm, verified, protected) "
            "VALUES ('t',$1,$2,$3,$4,$5,100,$6,$7,$8)",
            account_id,
            username,
            username.title(),
            bio,
            followers,
            can_dm,
            verified,
            protected,
        )
    edges = [
        ("seed", "hub", 0, 1),
        ("seed", "small", 0, 1),
        ("seed", "broken", 0, 1),
        ("seed2", "hub", 0, 1),
        ("hub", "seed", 1, 0),
        ("hub", "edge6", 1, 6),
    ]
    for source, target, sd, td in edges:
        await c.execute(
            "INSERT INTO follow_edges(task_id, source_account_id, target_account_id, "
            "source_depth, target_depth, is_l6_boundary) VALUES ('t',$1,$2,$3,$4,$5)",
            source,
            target,
            sd,
            td,
            td == 6,
        )
        await c.execute(
            "INSERT INTO follow_edge_observations(task_id, tree_id, source_account_id, "
            "target_account_id, source_depth, target_depth, is_collision) "
            "VALUES ('t', $1, $2, $3, $4, $5, $6)",
            "tree-b" if source == "seed2" else "tree-a",
            source,
            target,
            sd,
            td,
            source == "seed2",
        )
    # `hub` is reached from both trees: the second is a collision.
    for tree, collision in (("tree-a", False), ("tree-b", True)):
        await c.execute(
            "INSERT INTO account_observations(task_id, tree_id, account_id, "
            "parent_account_id, depth, is_collision) VALUES ('t',$1,'hub',$2,1,$3)",
            tree,
            "seed" if tree == "tree-a" else "seed2",
            collision,
        )
    await c.execute(
        "INSERT INTO account_metrics(task_id, account_id, sample_count, scanned_count, "
        "sample_span_days, latest_post_at, avg_like, avg_view, view_sample_count, "
        "median_engagement, engagement_rate, reach_ratio) "
        "VALUES ('t','hub',12,60,30,$1,850,42000,10,700,0.02,0.35)",
        NOW - timedelta(days=1),
    )
    await c.execute(
        "INSERT INTO account_posts(task_id, account_id, post_id, kind, is_qualifying, "
        "text, posted_at, like_count, view_count) VALUES "
        "('t','hub','p1','original',true,'hello',$1,900,50000), "
        "('t','hub','p2','retweet',false,'rt',$1,0,2000)",
        NOW - timedelta(days=1),
    )
    # Collection machinery that must never surface through the API.
    await c.execute(
        "INSERT INTO scraper_accounts(alias, credential_ref, user_agent, proxy_ref) "
        "VALUES ('scraper-a','secret://vault/token-abc','@chrome','proxy://user:pw@host')"
    )


# --- seed import -----------------------------------------------------------


def test_seed_parsing_reports_what_it_ignored() -> None:
    """A typo that vanishes here becomes a missing tree nobody notices."""

    result = parse_seed_list(
        "openai\n@openai\nhttps://x.com/anthropic?s=20\n  \nnot a handle!\nwaytoolongahandlename"
    )

    assert result.handles == ("openai", "anthropic")
    assert result.duplicates == ("openai",)
    assert result.invalid == ("not a handle!", "waytoolongahandlename")
    assert result.total_lines == 5


@pytest.mark.asyncio
async def test_seed_parse_endpoint_returns_the_breakdown(client) -> None:
    response = await client.post("/api/seeds/parse", json={"text": "@a, b, a"})

    assert response.status_code == 200
    assert response.json() == {
        "handles": ["a", "b"],
        "duplicates": ["a"],
        "invalid": [],
        "total_lines": 3,
        "valid_count": 2,
    }


# --- task control ----------------------------------------------------------


@pytest.mark.asyncio
async def test_pause_releases_leases_and_refuses_undefined_transitions(client, pool) -> None:
    async with pool.acquire() as c:
        await c.execute(
            "INSERT INTO crawl_frontier(task_id, account_id, operation, depth, status, "
            "owner_id, lease_expires_at) VALUES "
            "('t','hub','Following',1,'running','worker-a', now() + interval '5 min')"
        )

    paused = await client.post("/api/tasks/t/control", json={"action": "pause"})
    assert paused.status_code == 200
    assert paused.json()["status"] == "paused"

    async with pool.acquire() as c:
        row = await c.fetchrow("SELECT status, owner_id FROM crawl_frontier")
    assert (row["status"], row["owner_id"]) == ("pending", None), (
        "resumes without waiting on leases"
    )

    # `paused -> completed` is not a transition the product offers.
    async with pool.acquire() as c:
        await c.execute("UPDATE crawl_tasks SET status='completed'")
    refused = await client.post("/api/tasks/t/control", json={"action": "pause"})
    assert refused.status_code == 409


@pytest.mark.asyncio
async def test_progress_says_why_a_layer_is_not_moving(client) -> None:
    body = (await client.get("/api/tasks/t")).json()

    assert body["status"] == "running"
    assert body["parser_backlog"] == 2, "produced 10, processed 8"
    assert body["layer"]["depth"] == 1
    assert "layer_metrics" in body and "coverage" in body
    assert body["coverage"]["truncated"] == 1, "hub collected 800 of 900"


@pytest.mark.asyncio
async def test_unknown_task_is_a_404_not_a_crash(client) -> None:
    assert (await client.get("/api/tasks/nope")).status_code == 404


# --- account queries -------------------------------------------------------


@pytest.mark.asyncio
async def test_accounts_rank_by_in_network_endorsement(pool) -> None:
    """Follower count can be bought; being followed inside the network cannot."""

    page = await ProductQueries(pool).accounts("t", AccountFilter())

    assert page.total == 6
    assert page.items[0]["account_id"] == "hub"
    assert page.items[0]["network_indegree"] == 2
    assert page.items[0]["tree_count"] == 2, "reached from both trees"
    assert page.items[0]["collision_count"] == 1


@pytest.mark.asyncio
async def test_every_account_carries_its_data_quality_warnings(pool) -> None:
    page = await ProductQueries(pool).accounts("t", AccountFilter(limit=50))
    by_id = {item["account_id"]: item for item in page.items}

    assert "following_truncated" in by_id["hub"]["warnings"]
    assert by_id["hub"]["coverage_ratio"] == pytest.approx(0.8889, abs=1e-4)
    assert "multiple_discovery_paths" in by_id["hub"]["warnings"]
    assert "boundary_not_expanded" in by_id["edge6"]["warnings"]
    assert "expansion_failed" in by_id["broken"]["warnings"]
    assert "expansion_filtered" in by_id["small"]["warnings"]
    assert by_id["seed"]["warnings"] == [], "a complete account claims nothing extra"
    assert by_id["small"]["coverage_ratio"] is None, "no declared count, so no ratio"


@pytest.mark.asyncio
async def test_filters_narrow_without_losing_the_total(pool) -> None:
    q = ProductQueries(pool)

    reachable = await q.accounts("t", AccountFilter(can_dm=True, min_followers=1_000))
    assert {item["account_id"] for item in reachable.items} == {"seed", "seed2", "hub"}

    incomplete = await q.accounts("t", AccountFilter(incomplete_only=True))
    assert {item["account_id"] for item in incomplete.items} == {"hub", "small", "broken"}

    by_tree = await q.accounts("t", AccountFilter(tree_ids=("tree-b",)))
    assert {item["account_id"] for item in by_tree.items} == {"hub"}

    enriched = await q.accounts("t", AccountFilter(has_timeline=True))
    assert {item["account_id"] for item in enriched.items} == {"hub"}


@pytest.mark.asyncio
async def test_a_page_states_the_full_total_it_is_a_slice_of(pool) -> None:
    page = await ProductQueries(pool).accounts("t", AccountFilter(limit=2))

    assert len(page.items) == 2 and page.total == 6
    assert page.truncated is True


@pytest.mark.asyncio
async def test_account_detail_carries_paths_edges_and_posts(client) -> None:
    body = (await client.get("/api/tasks/t/accounts/hub")).json()

    assert body["username"] == "hubby"
    assert {p["tree_id"] for p in body["discovery_paths"]} == {"tree-a", "tree-b"}
    assert {e["target_account_id"] for e in body["following"]} == {"seed", "edge6"}
    assert {e["source_account_id"] for e in body["followed_by_in_network"]} == {"seed", "seed2"}
    assert [p["post_id"] for p in body["posts"]] == ["p1"], "only qualifying posts"
    assert body["view_sample_count"] == 10 and body["sample_count"] == 12


@pytest.mark.asyncio
async def test_ids_stay_strings_all_the_way_out(client) -> None:
    """A spreadsheet reading a snowflake id as a number rounds its last digits."""

    body = (await client.get("/api/tasks/t/accounts?limit=50")).json()
    assert all(isinstance(item["account_id"], str) for item in body["items"])

    csv_body = (await client.get("/api/tasks/t/export/posts?fmt=csv")).text
    assert "p1" in csv_body


# --- relationships ---------------------------------------------------------


@pytest.mark.asyncio
async def test_relationships_expose_tree_depth_and_collision(pool) -> None:
    page = await ProductQueries(pool).relationships("t", EdgeFilter(target_id="hub"))

    by_source = {item["source_account_id"]: item for item in page.items}
    assert by_source["seed"]["trees"] == ["tree-a"]
    assert by_source["seed2"]["is_collision"] is True
    assert by_source["seed"]["target_depth"] == 1


@pytest.mark.asyncio
async def test_a_closure_edge_back_to_a_seed_is_queryable(pool) -> None:
    page = await ProductQueries(pool).relationships("t", EdgeFilter(source_id="hub"))
    by_target = {item["target_account_id"]: item for item in page.items}

    assert by_target["seed"]["target_depth"] == 0
    assert by_target["edge6"]["is_l6_boundary"] is True


# --- subgraph --------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_subgraph_is_bounded_and_says_so(client) -> None:
    """A capped answer must never look like the whole graph."""

    body = (await client.get("/api/tasks/t/subgraph?seed=seed&hops=2&node_limit=2")).json()

    assert len(body["nodes"]) == 2
    assert body["matched_nodes"] > 2
    assert body["bounded"] is True
    assert body["warnings"], "the cap is stated, not silent"
    assert all(
        edge["source_account_id"] in {n["account_id"] for n in body["nodes"]}
        for edge in body["edges"]
    ), "no edge points outside the returned nodes"


@pytest.mark.asyncio
async def test_a_one_hop_neighbourhood_stops_at_one_hop(client) -> None:
    body = (await client.get("/api/tasks/t/subgraph?seed=seed&hops=1&node_limit=50")).json()

    assert {n["account_id"] for n in body["nodes"]} == {"seed", "hub", "small", "broken"}
    assert body["bounded"] is False


@pytest.mark.asyncio
async def test_subgraph_nodes_carry_their_status(client) -> None:
    body = (await client.get("/api/tasks/t/subgraph?min_network_indegree=1")).json()
    by_id = {n["account_id"]: n for n in body["nodes"]}

    assert by_id["hub"]["network_indegree"] == 2
    assert by_id["seed"]["is_seed"] is True
    assert "edge6" in by_id and by_id["edge6"]["is_l6_boundary"] is True


# --- exports ---------------------------------------------------------------


@pytest.mark.asyncio
async def test_an_export_states_the_task_filters_and_quality_it_came_from(client) -> None:
    response = await client.get(
        "/api/tasks/t/export/accounts?fmt=json&can_dm=true&min_followers=1000"
    )
    body = response.json()
    manifest = body["manifest"]

    assert manifest["task_id"] == "t"
    assert manifest["dataset"] == "accounts"
    assert manifest["filters"] == {
        "can_dm": True,
        "min_followers": 1000,
        "order_by": "network_indegree",
        "descending": True,
    }
    assert manifest["row_count"] == len(body["rows"]) == 3
    assert manifest["task_status"] == "running"
    assert manifest["expansion_complete"] is False
    assert manifest["parser_backlog"] == 2, "the export says the crawl was still catching up"
    assert manifest["coverage"]["truncated"] == 1


@pytest.mark.asyncio
async def test_a_csv_carries_its_manifest_in_the_headers(client) -> None:
    response = await client.get("/api/tasks/t/export/accounts")

    assert response.headers["content-type"].startswith("text/csv")
    manifest = json.loads(response.headers["x-xgraph-manifest"])
    assert manifest["task_id"] == "t" and manifest["row_count"] == 6
    lines = response.text.strip().splitlines()
    assert len(lines) == 7, "header plus every row the manifest claims"
    assert "coverage_ratio" in lines[0] and "warnings" in lines[0]


@pytest.mark.asyncio
async def test_export_pages_past_the_query_page_size(client, pool) -> None:
    """The export must not silently stop at one page of results."""

    async with pool.acquire() as c:
        for i in range(120):
            await c.execute(
                "INSERT INTO account_nodes(task_id, account_id, first_depth) VALUES ('t', $1, 2)",
                f"bulk-{i}",
            )
    response = await client.get("/api/tasks/t/export/accounts?fmt=json")
    body = response.json()

    assert body["manifest"]["row_count"] == 126
    assert len(body["rows"]) == 126


# --- security --------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_route_exposes_the_collection_machinery(client) -> None:
    """Scraper identities are an operational detail, not part of the product."""

    secrets = ("secret://vault/token-abc", "proxy://user:pw@host", "scraper-a", "credential_ref")
    responses = [
        await client.get("/api/tasks/t"),
        await client.get("/api/tasks/t/accounts?limit=50"),
        await client.get("/api/tasks/t/accounts/hub"),
        await client.get("/api/tasks/t/relationships"),
        await client.get("/api/tasks/t/subgraph"),
        await client.get("/api/tasks/t/export/accounts?fmt=json"),
        await client.get("/api/tasks/t/export/relationships?fmt=json"),
    ]
    for response in responses:
        assert response.status_code == 200
        for secret in secrets:
            assert secret not in response.text, f"{secret} leaked through {response.url}"


@pytest.mark.asyncio
async def test_an_oversized_page_is_refused_rather_than_served(client) -> None:
    """Above the ceiling the caller is told to page, not handed a slow query."""

    assert (await client.get("/api/tasks/t/accounts?limit=100000")).status_code == 422
    assert (await client.get("/api/tasks/t/subgraph?hops=9")).status_code == 422


@pytest.mark.asyncio
async def test_an_unsupported_ordering_is_rejected() -> None:
    with pytest.raises(ValueError, match="cannot order by"):
        AccountFilter(order_by="; DROP TABLE account_nodes")
