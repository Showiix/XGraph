"""Turns one timeline page into stored posts and derived metrics."""

from typing import Any

from loguru import logger

from xgraph.collector.parser import parse_tweets
from xgraph.domain import Operation, TimelinePolicy
from xgraph.messaging import PermanentEventError, RawPageEvent
from xgraph.storage.timeline import PostgresTimelineStore


class TimelinePageHandler:
    """Applies a UserTweets page inside the event's transaction.

    Reposts and replies are stored alongside qualifying posts rather than
    discarded. They are what makes the scan count verifiable: an account with
    four qualifying posts out of twenty scanned is a different account from one
    with four out of four, and only the stored non-qualifying rows can tell them
    apart.
    """

    def __init__(self, store: PostgresTimelineStore, policy: TimelinePolicy | None = None) -> None:
        self._store = store
        self._policy = policy or TimelinePolicy()

    async def __call__(self, connection: Any, event: RawPageEvent, /) -> None:
        if event.operation is not Operation.USER_TWEETS:
            raise PermanentEventError(f"timeline handler cannot apply {event.operation.value}")
        try:
            posts = parse_tweets(event.payload)
        except Exception as error:  # noqa: BLE001 - a payload we cannot read is terminal
            raise PermanentEventError(f"unreadable timeline payload: {error}") from error

        # A timeline page also carries the originals behind every repost, which
        # belong to other accounts; `parse_tweets` already limits itself to the
        # page's own top-level entries.
        own = [post for post in posts if post.author_id in (None, event.account_id)]
        stored = await self._store.store_posts(
            connection, task_id=event.task_id, account_id=event.account_id, posts=own
        )
        await self._store.recompute_metrics(
            connection,
            task_id=event.task_id,
            account_id=event.account_id,
            target_posts=self._policy.target_posts,
        )
        qualifying = sum(1 for post in own if post.is_qualifying)
        logger.debug(
            f"timeline {event.account_id}: +{stored} posts ({qualifying} qualifying) "
            f"from {len(own)} scanned"
        )
