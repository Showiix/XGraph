"""Turns one raw page into graph rows, inside the event's transaction."""

from dataclasses import dataclass
from typing import Any

from loguru import logger

from xgraph.collector.parser import parse_users
from xgraph.domain import Operation, UserProfile
from xgraph.messaging import PermanentEventError, RawPageEvent
from xgraph.storage import graph as writes
from xgraph.storage.graph import NodeUpsert, PageWriteResult

from .policy import TraversalPolicy


@dataclass(frozen=True, slots=True)
class _Split:
    """Discovered accounts, grouped by what happens to them next."""

    expandable: list[NodeUpsert]
    filtered: list[str]
    boundary: list[NodeUpsert]


class GraphPageHandler:
    """Applies a Following page to the graph.

    Everything written here lands in the transaction that registers the event
    and advances the consumer offset, so a page is either fully reflected in the
    graph or not reflected at all.
    """

    def __init__(self, policy: TraversalPolicy | None = None) -> None:
        self._policy = policy or TraversalPolicy()
        self.last_result: PageWriteResult | None = None

    async def __call__(self, connection: Any, event: RawPageEvent, /) -> None:
        if event.operation is not Operation.FOLLOWING:
            # Timeline pages belong to stage 5 and have their own handler; a
            # page this handler cannot interpret must not be silently dropped.
            raise PermanentEventError(f"graph handler cannot apply {event.operation.value}")
        if event.depth is None:
            raise PermanentEventError("a Following page without a depth cannot be placed")

        try:
            profiles = parse_users(event.payload)
        except Exception as error:  # noqa: BLE001 - a payload we cannot read is terminal
            raise PermanentEventError(f"unreadable Following payload: {error}") from error

        result = await self._apply(connection, event, profiles)
        self.last_result = result
        logger.debug(
            f"page {event.event_id}: +{result.new_nodes} nodes, +{result.new_edges} edges, "
            f"{result.collisions} collisions, {result.queued} queued"
        )

    async def _apply(
        self, connection: Any, event: RawPageEvent, profiles: tuple[UserProfile, ...]
    ) -> PageWriteResult:
        task_id, source_id = event.task_id, event.account_id
        source_depth = int(event.depth or 0)
        target_depth = source_depth + 1
        boundary = self._policy.is_boundary(target_depth)

        # X does not describe the account being expanded on its own Following
        # page, so this is normally absent; the declared count then comes from
        # the profile stored when this account was discovered.
        by_id = {p.id: p for p in profiles}
        source_profile = by_id.pop(source_id, None)
        discovered = list(by_id.values())

        nodes = await writes.upsert_nodes(
            connection,
            task_id=task_id,
            account_ids=[p.id for p in discovered],
            depth=target_depth,
            boundary=boundary,
        )
        await writes.upsert_profiles(connection, task_id=task_id, profiles=profiles)
        await writes.set_declared_following(
            connection,
            task_id=task_id,
            account_id=source_id,
            declared=source_profile.following_count if source_profile else None,
        )

        targets = [nodes[p.id] for p in discovered if p.id in nodes]
        new_edges = await writes.insert_edges(
            connection,
            task_id=task_id,
            tree_id=event.tree_id,
            source_id=source_id,
            source_depth=source_depth,
            targets=targets,
            boundary_depth=self._policy.boundary_depth,
        )
        await writes.record_observations(
            connection,
            task_id=task_id,
            tree_id=event.tree_id,
            parent_account_id=source_id,
            depth=target_depth,
            targets=targets,
        )

        split = self._split(targets, by_id, target_depth)
        queued = await writes.queue_expansions(
            connection,
            task_id=task_id,
            tree_id=event.tree_id,
            candidates=split.expandable,
            depth=target_depth,
        )
        await writes.mark_filtered(
            connection,
            task_id=task_id,
            account_ids=split.filtered,
            reason="below_follower_threshold",
        )
        await writes.add_collected(
            connection, task_id=task_id, account_id=source_id, collected=len(discovered)
        )

        new_nodes = sum(1 for t in targets if t.inserted)
        await writes.consume_capacity(connection, task_id=task_id, nodes=new_nodes, edges=new_edges)
        return PageWriteResult(
            observed=len(discovered),
            new_nodes=new_nodes,
            new_edges=new_edges,
            collisions=len(targets) - new_nodes,
            boundary_nodes=len(split.boundary),
            queued=queued,
            filtered=len(split.filtered),
        )

    def _split(
        self, targets: list[NodeUpsert], profiles: dict[str, UserProfile], target_depth: int
    ) -> _Split:
        expandable: list[NodeUpsert] = []
        filtered: list[str] = []
        boundary: list[NodeUpsert] = []
        for target in targets:
            if not self._policy.may_expand(target.depth):
                boundary.append(target)
                continue
            if not target.inserted:
                # Already known, so it already has an expansion of its own or a
                # recorded reason not to. Queueing it again would spend a second
                # request on the same account.
                continue
            if self._policy.filter_reason(profiles.get(target.account_id)) is not None:
                filtered.append(target.account_id)
                continue
            expandable.append(target)
        return _Split(expandable=expandable, filtered=filtered, boundary=boundary)
