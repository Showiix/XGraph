"""Graph writes for the L0-L6 traversal.

Every statement here runs on a connection supplied by the caller, because all
of it belongs inside the transaction that registers the event and moves the
consumer offset. A page that is half-applied is worse than one not applied at
all: the offset would have moved past work that was never done.

The writes are set-based rather than per-account. A Following page carries about
sixty users, and the queue grows two orders of magnitude faster than it drains,
so a round trip per user is not an optimisation detail.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from xgraph.domain import Operation, UserProfile


@dataclass(frozen=True, slots=True)
class NodeUpsert:
    account_id: str
    depth: int
    inserted: bool


@dataclass(frozen=True, slots=True)
class PageWriteResult:
    """What one page actually added, as opposed to what it referenced."""

    observed: int
    new_nodes: int
    new_edges: int
    collisions: int
    boundary_nodes: int
    queued: int
    filtered: int


class GraphCapacityError(RuntimeError):
    """The task's node or edge budget would be exceeded by this page."""


async def upsert_nodes(
    connection: Any, *, task_id: str, account_ids: list[str], depth: int, boundary: bool
) -> dict[str, NodeUpsert]:
    """Insert nodes at `depth`, returning each one's canonical depth.

    `xmax = 0` distinguishes a real insert from a conflict, which is how a
    collision is detected without a second query. Nodes that already exist keep
    their original depth: layers are processed in order, so the depth already
    recorded is the shortest one.
    """

    if not account_ids:
        return {}
    rows = await connection.fetch(
        """
        INSERT INTO account_nodes(task_id, account_id, first_depth, is_l6_boundary,
                                  expansion_status)
        SELECT $1, candidate, $2, $3, CASE WHEN $3 THEN 'boundary' ELSE 'pending' END
        FROM unnest($4::text[]) AS candidate
        ON CONFLICT (task_id, account_id) DO UPDATE SET updated_at = now()
        RETURNING account_id, first_depth, (xmax = 0) AS inserted;
        """,
        task_id,
        depth,
        boundary,
        account_ids,
    )
    return {
        str(row["account_id"]): NodeUpsert(
            account_id=str(row["account_id"]),
            depth=int(row["first_depth"]),
            inserted=bool(row["inserted"]),
        )
        for row in rows
    }


async def upsert_profiles(
    connection: Any, *, task_id: str, profiles: Sequence[UserProfile]
) -> None:
    """Record the profile fields the Following response handed us for free."""

    if not profiles:
        return
    await connection.execute(
        """
        INSERT INTO account_profiles(task_id, account_id, username, display_name,
            description, followers_count, following_count, created_at, protected,
            verified, blue_verified, can_dm, location)
        SELECT $1, p.account_id, p.username, p.display_name, p.description,
               p.followers_count, p.following_count, p.created_at, p.protected,
               p.verified, p.blue_verified, p.can_dm, p.location
        FROM unnest($2::text[], $3::text[], $4::text[], $5::text[], $6::int[], $7::int[],
                    $8::timestamptz[], $9::bool[], $10::bool[], $11::bool[], $12::bool[],
                    $13::text[])
             AS p(account_id, username, display_name, description, followers_count,
                  following_count, created_at, protected, verified, blue_verified,
                  can_dm, location)
        ON CONFLICT (task_id, account_id) DO UPDATE SET
            username = excluded.username,
            display_name = excluded.display_name,
            description = excluded.description,
            followers_count = excluded.followers_count,
            following_count = excluded.following_count,
            created_at = excluded.created_at,
            protected = excluded.protected,
            verified = excluded.verified,
            blue_verified = excluded.blue_verified,
            can_dm = excluded.can_dm,
            location = excluded.location,
            observed_at = now();
        """,
        task_id,
        [p.id for p in profiles],
        [p.username for p in profiles],
        [p.display_name for p in profiles],
        [p.description for p in profiles],
        [p.followers_count for p in profiles],
        [p.following_count for p in profiles],
        [p.created_at for p in profiles],
        [p.protected for p in profiles],
        [p.verified for p in profiles],
        [p.blue_verified for p in profiles],
        [p.can_dm for p in profiles],
        [p.location for p in profiles],
    )


async def insert_edges(
    connection: Any,
    *,
    task_id: str,
    tree_id: str | None,
    source_id: str,
    source_depth: int,
    targets: list[NodeUpsert],
    boundary_depth: int,
) -> int:
    """Write the observed relationships, returning how many were new.

    An edge is recorded whatever its target's depth is, including an edge that
    points back at a Seed. Those closure edges carry the strongest in-network
    signal the product has, so filtering them out here would remove exactly the
    evidence the ranking depends on.
    """

    if not targets:
        return 0
    ids = [t.account_id for t in targets]
    depths = [t.depth for t in targets]
    rows = await connection.fetch(
        """
        INSERT INTO follow_edges(task_id, source_account_id, target_account_id,
                                 source_depth, target_depth, is_l6_boundary)
        SELECT $1, $2, e.target_id, $3, e.target_depth, e.target_depth >= $6
        FROM unnest($4::text[], $5::int[]) AS e(target_id, target_depth)
        ON CONFLICT (task_id, source_account_id, target_account_id) DO NOTHING
        RETURNING target_account_id;
        """,
        task_id,
        source_id,
        source_depth,
        ids,
        depths,
        boundary_depth,
    )
    if tree_id is not None:
        await connection.execute(
            """
            INSERT INTO follow_edge_observations(task_id, tree_id, source_account_id,
                target_account_id, source_depth, target_depth, is_collision)
            SELECT $1, $2, $3, e.target_id, $4, e.target_depth, e.is_collision
            FROM unnest($5::text[], $6::int[], $7::bool[])
                 AS e(target_id, target_depth, is_collision)
            ON CONFLICT (task_id, tree_id, source_account_id, target_account_id)
            DO NOTHING;
            """,
            task_id,
            tree_id,
            source_id,
            source_depth,
            ids,
            depths,
            [not t.inserted for t in targets],
        )
    return len(rows)


async def record_observations(
    connection: Any,
    *,
    task_id: str,
    tree_id: str | None,
    parent_account_id: str | None,
    depth: int,
    targets: list[NodeUpsert],
) -> None:
    """Keep the discovery path, including the repeats.

    A repeat is not noise to be suppressed: "this account was reached from N
    different places" is the circle-overlap signal the product exists to find.
    """

    if tree_id is None or not targets:
        return
    await connection.execute(
        """
        INSERT INTO account_observations(task_id, tree_id, account_id, parent_account_id,
                                         depth, is_collision)
        SELECT $1, $2, o.account_id, $3, $4, o.is_collision
        FROM unnest($5::text[], $6::bool[]) AS o(account_id, is_collision)
        ON CONFLICT DO NOTHING;
        """,
        task_id,
        tree_id,
        parent_account_id,
        depth,
        [t.account_id for t in targets],
        [not t.inserted for t in targets],
    )


async def queue_expansions(
    connection: Any, *, task_id: str, tree_id: str | None, candidates: list[NodeUpsert], depth: int
) -> int:
    """Open the next layer for nodes that are allowed to expand."""

    if not candidates:
        return 0
    ids = [c.account_id for c in candidates]
    rows = await connection.fetch(
        """
        WITH queued AS (
            INSERT INTO crawl_frontier(task_id, account_id, tree_id, operation, depth)
            SELECT $1, candidate, $5, $2, $3 FROM unnest($4::text[]) AS candidate
            ON CONFLICT (task_id, account_id, operation) DO NOTHING
            RETURNING account_id
        )
        UPDATE account_nodes
        SET expansion_status = 'queued', updated_at = now()
        WHERE task_id = $1 AND account_id IN (SELECT account_id FROM queued)
        RETURNING account_id;
        """,
        task_id,
        Operation.FOLLOWING.value,
        depth,
        ids,
        tree_id,
    )
    return len(rows)


async def mark_filtered(
    connection: Any, *, task_id: str, account_ids: list[str], reason: str
) -> None:
    if not account_ids:
        return
    await connection.execute(
        """
        UPDATE account_nodes
        SET expansion_status = 'filtered', filter_reason = $3, updated_at = now()
        WHERE task_id = $1 AND account_id = ANY($2::text[])
          AND expansion_status = 'pending';
        """,
        task_id,
        account_ids,
        reason,
    )


async def add_collected(connection: Any, *, task_id: str, account_id: str, collected: int) -> None:
    await connection.execute(
        """
        UPDATE account_nodes
        SET collected_following = collected_following + $3, updated_at = now()
        WHERE task_id = $1 AND account_id = $2;
        """,
        task_id,
        account_id,
        collected,
    )


async def set_declared_following(
    connection: Any, *, task_id: str, account_id: str, declared: int | None
) -> None:
    """Record how many accounts this one says it follows.

    A Following page describes the accounts being followed, not the follower, so
    the count is almost never in the page that triggers this write — X returns a
    bare `{"__typename": "User"}` for the source. It is in the profile row
    stored when this account was itself discovered, so fall back to that.
    Without the fallback the field is null for every expanded account and the
    coverage ratio, which exists to show where the graph is incomplete, can
    never be computed at all.

    Never regresses to null: an unknown count leaves whatever is already there.
    """

    await connection.execute(
        """
        UPDATE account_nodes
        SET declared_following = COALESCE(
                $3::int,
                (SELECT following_count FROM account_profiles
                 WHERE task_id = $1 AND account_id = $2),
                declared_following
            ),
            updated_at = now()
        WHERE task_id = $1 AND account_id = $2;
        """,
        task_id,
        account_id,
        declared,
    )


async def consume_capacity(connection: Any, *, task_id: str, nodes: int, edges: int) -> None:
    """Charge the task budget for rows this page actually added.

    Charging for referenced rows instead of new ones would let a heavily
    overlapping crawl exhaust its budget on data it already had.
    """

    if nodes == 0 and edges == 0:
        return
    row = await connection.fetchrow(
        """
        UPDATE crawl_tasks
        SET nodes_created = nodes_created + $2,
            edges_created = edges_created + $3,
            updated_at = now()
        WHERE task_id = $1
          AND nodes_created + $2 <= max_nodes
          AND edges_created + $3 <= max_edges
        RETURNING nodes_created;
        """,
        task_id,
        nodes,
        edges,
    )
    if row is None:
        raise GraphCapacityError(f"graph capacity exhausted for task {task_id}")
