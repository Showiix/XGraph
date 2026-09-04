"""Drives timeline collection for admitted candidates.

Deliberately a separate loop from the traversal. They spend different rate-limit
buckets, they fail for different reasons, and the graph must be able to finish
while enrichment is paused, behind, or switched off entirely.
"""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Protocol
from uuid import uuid4

from loguru import logger

from xgraph.accounts.manager import NoAvailableAccountError
from xgraph.collector.errors import (
    CollectorError,
    InvalidResponseError,
    PlatformOverloadedError,
    RateLimitedError,
    TransportError,
)
from xgraph.domain import FrontierStatus, Operation, PageEnvelope, TimelinePolicy
from xgraph.messaging.events import RawPageEvent
from xgraph.storage.events import PostgresEventStore
from xgraph.storage.postgres import BudgetExhaustedError, FrontierItem, PostgresFrontierStore
from xgraph.storage.timeline import PostgresTimelineStore

from .expansion import (
    ACCOUNT_ERRORS,
    EMPTY_PAGE_LIMIT,
    STARVED_BACKOFF_SECONDS,
    TERMINAL_PAGE_ERRORS,
    AccountLeasing,
    _error_class,
    _in,
)


class SampleOutcome(str, Enum):
    """Why a timeline chain stopped, recorded per account."""

    TARGET_REACHED = "target_reached"
    SCAN_LIMIT = "scan_limit"
    NATURAL_END = "natural_end"
    EMPTY_PAGES = "empty_pages"
    PAGE_LIMIT = "page_limit"
    CURSOR_STALLED = "cursor_stalled"
    DISABLED = "timeline_disabled"
    FAILED = "failed"


class TimelineCollector(Protocol):
    async def user_tweets_page(
        self, account_id: str, cursor: str | None = None
    ) -> PageEnvelope: ...

    async def aclose(self) -> None: ...


class TimelineCollectorFactory(Protocol):
    async def __call__(self, alias: str, /) -> TimelineCollector: ...


@dataclass(frozen=True, slots=True)
class EnrichmentResult:
    account_id: str
    pages: int
    outcome: SampleOutcome | None
    error_class: str | None = None


class EnrichmentScheduler:
    def __init__(
        self,
        frontier: PostgresFrontierStore,
        accounts: AccountLeasing,
        events: PostgresEventStore,
        timeline: PostgresTimelineStore,
        collector_factory: TimelineCollectorFactory,
        *,
        policy: TimelinePolicy | None = None,
        worker_id: str | None = None,
        lease_seconds: int = 300,
    ) -> None:
        self._frontier = frontier
        self._accounts = accounts
        self._events = events
        self._timeline = timeline
        self._collector_factory = collector_factory
        self._policy = policy or TimelinePolicy()
        self._worker_id = worker_id or f"enricher-{uuid4()}"
        self._lease_seconds = lease_seconds

    @property
    def worker_id(self) -> str:
        return self._worker_id

    async def run_once(self) -> EnrichmentResult | None:
        item = await self._frontier.claim_frontier(
            worker_id=self._worker_id,
            lease_seconds=self._lease_seconds,
            operation=Operation.USER_TWEETS,
        )
        if item is None:
            return None
        try:
            return await self._collect(item)
        except BudgetExhaustedError as error:
            # Enrichment shares the task budget but must not consume the
            # traversal's retry allowance; the chain simply waits.
            await self._halt(item, "budget_exhausted", refund_attempt=True)
            logger.info(f"timeline halted for {item.account_id}: {error}")
            return EnrichmentResult(item.account_id, 0, None, "budget_exhausted")

    async def _collect(self, item: FrontierItem) -> EnrichmentResult:
        cursor = item.cursor_in
        seen: set[str] = set()
        pages = 0
        empty_pages = 0
        await self._timeline.set_timeline_status(item.task_id, item.account_id, "collecting")

        # The stopping rule counts what this loop fetched, not what the parser
        # has stored. The two run independently by design — the parser may be
        # minutes behind — so reading its output here would make the scheduler
        # page on forever whenever enrichment falls behind. The database is
        # consulted once, to pick up where an interrupted chain left off.
        qualifying, scanned = await self._timeline.qualifying_progress(
            item.task_id, item.account_id
        )

        while True:
            if self._policy.enough(qualifying, scanned):
                outcome = (
                    SampleOutcome.TARGET_REACHED
                    if qualifying >= self._policy.target_posts
                    else SampleOutcome.SCAN_LIMIT
                )
                return await self._finish(item, outcome, pages)
            if pages >= self._policy.max_pages:
                return await self._finish(item, SampleOutcome.PAGE_LIMIT, pages)

            try:
                lease = await self._accounts.lease(
                    Operation.USER_TWEETS.value, owner_id=self._worker_id
                )
            except NoAvailableAccountError:
                # Without a delay the row is immediately claimable again and
                # every idle worker spins on it, burning CPU while the pool is
                # the thing that is actually short.
                await self._halt(
                    item,
                    "no_available_account",
                    refund_attempt=True,
                    not_before=_in(STARVED_BACKOFF_SECONDS),
                )
                return EnrichmentResult(item.account_id, pages, None, "no_available_account")

            try:
                attempt = await self._frontier.reserve_request(
                    item.task_id,
                    Operation.USER_TWEETS,
                    frontier_id=item.frontier_id,
                    scraper_alias=getattr(lease, "alias", None),
                )
            except BaseException:
                # The account was leased before the budget was charged. Letting
                # this propagate would strand the lease until it expires, and a
                # pool of two accounts is emptied by two such escapes.
                await self._accounts.release(lease, success=True)
                raise
            collector = await self._collector_factory(getattr(lease, "alias", ""))
            try:
                envelope = await collector.user_tweets_page(item.account_id, cursor)
            except RateLimitedError as error:
                await self._accounts.report(lease, "rate_limited", reset_at=error.reset_at)
                await self._frontier.finish_request(attempt, outcome="rate_limited")
                # Hold the row until the window the platform named. Retrying
                # before then cannot succeed and only spends worker turns.
                await self._halt(
                    item, "rate_limited", refund_attempt=True, not_before=error.reset_at
                )
                return EnrichmentResult(item.account_id, pages, None, "rate_limited")
            except ACCOUNT_ERRORS as error:
                cls = _error_class(error)
                await self._accounts.report(lease, cls)
                await self._frontier.finish_request(attempt, outcome="failed", error_class=cls)
                await self._halt(item, cls)
                return EnrichmentResult(item.account_id, pages, None, cls)
            except TERMINAL_PAGE_ERRORS as error:
                cls = _error_class(error)
                await self._accounts.release(lease, success=False)
                await self._frontier.finish_request(attempt, outcome="failed", error_class=cls)
                return await self._finish(item, SampleOutcome.FAILED, pages, error_class=cls)
            except (PlatformOverloadedError, TransportError, InvalidResponseError) as error:
                cls = _error_class(error)
                await self._accounts.release(lease, success=False)
                await self._frontier.finish_request(attempt, outcome="failed", error_class=cls)
                await self._halt(item, cls)
                return EnrichmentResult(item.account_id, pages, None, cls)
            except CollectorError as error:
                # Catch-all by design. An unclassified error must degrade this
                # one chain, never the worker: `gather` propagates, so a single
                # unhandled class would stop every worker in the process.
                cls = _error_class(error)
                logger.warning(f"unclassified collector error ({cls}): {error}")
                await self._accounts.release(lease, success=False)
                await self._frontier.finish_request(attempt, outcome="failed", error_class=cls)
                await self._halt(item, cls, not_before=_in(30))
                return EnrichmentResult(item.account_id, pages, None, cls)
            finally:
                await collector.aclose()

            await self._accounts.release(
                lease,
                remaining=envelope.rate_limit.remaining,
                limit_max=envelope.rate_limit.limit,
                reset_at=envelope.rate_limit.reset_at,
            )
            await self._events.record_page(
                RawPageEvent.from_envelope(
                    envelope, task_id=item.task_id, tree_id=item.tree_id, depth=item.depth
                ),
                frontier_id=item.frontier_id,
                next_cursor=envelope.cursor_out,
                attempt_id=attempt.attempt_id,
                checkpoint_owner=item.owner_id,
            )
            pages += 1
            own = [post for post in envelope.tweets if post.author_id in (None, item.account_id)]
            scanned += len(own)
            qualifying += sum(1 for post in own if post.is_qualifying)

            if envelope.tweets:
                empty_pages = 0
            else:
                # Same end-of-list problem as the Following chain, on the tighter
                # bucket: UserTweets allows 50 requests per window against
                # Following's 500, so a chain that pages to its cap on an account
                # with nothing to show is proportionally more expensive.
                empty_pages += 1
                if empty_pages >= EMPTY_PAGE_LIMIT:
                    return await self._finish(item, SampleOutcome.EMPTY_PAGES, pages)

            if envelope.cursor_out is None:
                return await self._finish(item, SampleOutcome.NATURAL_END, pages)
            if envelope.cursor_out in seen:
                return await self._finish(item, SampleOutcome.CURSOR_STALLED, pages)
            seen.add(envelope.cursor_out)
            cursor = envelope.cursor_out

    async def _halt(
        self,
        item: FrontierItem,
        error_class: str,
        *,
        refund_attempt: bool = False,
        not_before: datetime | None = None,
    ) -> None:
        """End this turn expecting another, and record it if there is not one.

        The frontier turns a retryable finish into a terminal one when the
        attempt budget runs out. Without reading that back the account keeps a
        `collecting` status no worker is backing: the row is finished, nothing
        will claim it again, and the sample is neither complete nor retried.
        """

        outcome = await self._frontier.finish_frontier(
            item,
            FrontierStatus.RETRYABLE,
            error_class=error_class,
            refund_attempt=refund_attempt,
            not_before=not_before,
        )
        if outcome is FrontierStatus.FAILED:
            logger.warning(
                f"timeline for {item.account_id} exhausted its attempts on {error_class}"
            )
            await self._timeline.set_timeline_status(
                item.task_id, item.account_id, "failed", reason=SampleOutcome.FAILED.value
            )

    async def _finish(
        self,
        item: FrontierItem,
        outcome: SampleOutcome,
        pages: int,
        *,
        error_class: str | None = None,
    ) -> EnrichmentResult:
        failed = outcome is SampleOutcome.FAILED
        await self._frontier.finish_frontier(
            item,
            FrontierStatus.FAILED if failed else FrontierStatus.COMPLETED,
            error_class=error_class,
        )
        await self._timeline.set_timeline_status(
            item.task_id,
            item.account_id,
            "failed" if failed else "complete",
            reason=outcome.value,
        )
        return EnrichmentResult(item.account_id, pages, outcome, error_class)
