"""Drives one Following expansion: claim, lease, request, persist.

This is the irreversible half of the system. It decides which request to spend
quota on, and hands the resulting bytes to PostgreSQL before anything tries to
interpret them. It never parses a page into graph rows; that happens on the
replayable side, behind the outbox.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol
from uuid import uuid4

from loguru import logger

from xgraph.accounts.manager import NoAvailableAccountError
from xgraph.collector.errors import (
    AccountUnavailableError,
    AuthenticationError,
    BlockedError,
    CollectorError,
    FeaturesOutdatedError,
    InvalidResponseError,
    PlatformOverloadedError,
    RateLimitedError,
    TransportError,
)
from xgraph.domain import FrontierStatus, Operation, PageEnvelope, TerminationReason
from xgraph.messaging.events import RawPageEvent
from xgraph.storage.events import PostgresEventStore
from xgraph.storage.postgres import BudgetExhaustedError, FrontierItem, PostgresFrontierStore

#: Errors that end the account's chain rather than the worker's turn. The
#: account is not at fault and the page is not retryable, so the chain stops
#: with a recorded reason instead of consuming the retry budget.
TERMINAL_PAGE_ERRORS = (FeaturesOutdatedError,)

#: Consecutive pages with no users before the chain is treated as finished.
#: X does not signal the end of a Following list by withholding the cursor — in a
#: 2,600-page live run exactly one page came back without one — so a chain that
#: waits for a null cursor runs to its page cap on every account, however few
#: people it follows. X does interleave empty pages mid-list, so a single one is
#: not the end; three in a row is upstream twscrape's threshold and the live run
#: showed tightening it would cost coverage.
EMPTY_PAGE_LIMIT = 3

#: How long a work item waits after finding the account pool empty. Long enough
#: that idle workers stop contending for it, short enough that a freed account is
#: picked up promptly.
STARVED_BACKOFF_SECONDS = 15


def _in(seconds: int) -> datetime:
    return datetime.now(timezone.utc) + timedelta(seconds=seconds)


#: Errors that mean the scraper account, not the request, is the problem.
ACCOUNT_ERRORS = (AccountUnavailableError, AuthenticationError, BlockedError)


class AccountLeasing(Protocol):
    """What the scheduler needs from the account pool, and nothing more.

    Typed as a contract rather than the concrete pool so that the quota source
    can change — a different backing store, a stub in a traversal test — without
    the scheduler knowing.
    """

    async def lease(self, operation: str, *, owner_id: str, **kwargs: Any) -> Any: ...

    async def release(self, lease: Any, **kwargs: Any) -> None: ...

    async def report(self, lease: Any, error_class: str, **kwargs: Any) -> None: ...


class Collector(Protocol):
    async def following_page(self, account_id: str, cursor: str | None = None) -> PageEnvelope: ...

    async def aclose(self) -> None: ...


class CollectorFactory(Protocol):
    async def __call__(self, alias: str, /) -> Collector:
        """Resolve a leased account alias into a ready collector.

        The alias is resolved through the caller's secret store; credentials
        never travel through PostgreSQL.
        """
        ...


@dataclass(frozen=True, slots=True)
class ExpansionResult:
    account_id: str
    pages: int
    reason: TerminationReason | None
    error_class: str | None = None


class ExpansionScheduler:
    def __init__(
        self,
        frontier: PostgresFrontierStore,
        accounts: AccountLeasing,
        events: PostgresEventStore,
        collector_factory: CollectorFactory,
        *,
        worker_id: str | None = None,
        max_pages_per_chain: int = 40,
        lease_seconds: int = 300,
    ) -> None:
        self._frontier = frontier
        self._accounts = accounts
        self._events = events
        self._collector_factory = collector_factory
        self._worker_id = worker_id or f"scheduler-{uuid4()}"
        self._max_pages = max_pages_per_chain
        self._lease_seconds = lease_seconds

    @property
    def worker_id(self) -> str:
        return self._worker_id

    async def run_once(self) -> ExpansionResult | None:
        """Take one account off the frontier and page through its following list."""

        item = await self._frontier.claim_frontier(
            worker_id=self._worker_id, lease_seconds=self._lease_seconds
        )
        if item is None:
            return None
        try:
            return await self._expand(item)
        except BudgetExhaustedError as error:
            # Not this account's fault; leave it claimable for the next window.
            await self._halt(item, "budget_exhausted", refund_attempt=True)
            logger.info(f"expansion halted for {item.account_id}: {error}")
            return ExpansionResult(item.account_id, 0, TerminationReason.BUDGET_EXHAUSTED)

    async def _expand(self, item: FrontierItem) -> ExpansionResult:
        cursor = item.cursor_in
        seen_cursors: set[str] = set()
        pages = 0
        empty_pages = 0

        while True:
            if pages >= self._max_pages:
                return await self._finish(item, TerminationReason.PAGE_LIMIT, pages)

            try:
                lease = await self._accounts.lease(
                    Operation.FOLLOWING.value, owner_id=self._worker_id
                )
            except NoAvailableAccountError:
                # The chain is unfinished but nothing is wrong with it; keep the
                # cursor and let a later pass continue from the checkpoint.
                # Without a delay the row is immediately claimable again and
                # every idle worker spins on it, burning CPU while the pool is
                # the thing that is actually short.
                await self._halt(
                    item,
                    "no_available_account",
                    refund_attempt=True,
                    not_before=_in(STARVED_BACKOFF_SECONDS),
                )
                return ExpansionResult(item.account_id, pages, None, "no_available_account")

            try:
                attempt = await self._frontier.reserve_request(
                    item.task_id,
                    Operation.FOLLOWING,
                    frontier_id=item.frontier_id,
                    scraper_alias=lease.alias,
                )
            except BaseException:
                # The account was leased before the budget was charged. Letting
                # this propagate would strand the lease until it expires, and a
                # pool of two accounts is emptied by two such escapes.
                await self._accounts.release(lease, success=True)
                raise
            collector = await self._collector_factory(lease.alias)
            try:
                envelope = await collector.following_page(item.account_id, cursor)
            except RateLimitedError as error:
                await self._accounts.report(lease, "rate_limited", reset_at=error.reset_at)
                await self._frontier.finish_request(attempt, outcome="rate_limited")
                # Hold the row until the window the platform named. Retrying
                # before then cannot succeed and only spends worker turns.
                await self._halt(
                    item, "rate_limited", refund_attempt=True, not_before=error.reset_at
                )
                return ExpansionResult(item.account_id, pages, None, "rate_limited")
            except ACCOUNT_ERRORS as error:
                await self._accounts.report(lease, _error_class(error))
                await self._frontier.finish_request(
                    attempt, outcome="failed", error_class=_error_class(error)
                )
                await self._halt(item, _error_class(error))
                return ExpansionResult(item.account_id, pages, None, _error_class(error))
            except TERMINAL_PAGE_ERRORS as error:
                await self._accounts.release(lease, success=False)
                await self._frontier.finish_request(
                    attempt, outcome="failed", error_class=_error_class(error)
                )
                return await self._finish(
                    item, TerminationReason.FAILED, pages, error_class=_error_class(error)
                )
            except (PlatformOverloadedError, TransportError, InvalidResponseError) as error:
                await self._accounts.release(lease, success=False)
                await self._frontier.finish_request(
                    attempt, outcome="failed", error_class=_error_class(error)
                )
                await self._halt(item, _error_class(error))
                return ExpansionResult(item.account_id, pages, None, _error_class(error))
            except CollectorError as error:
                # Catch-all by design. An unclassified error must degrade this
                # one chain, never the worker: `gather` propagates, so a single
                # unhandled class would stop every worker in the process.
                cls = _error_class(error)
                logger.warning(f"unclassified collector error ({cls}): {error}")
                await self._accounts.release(lease, success=False)
                await self._frontier.finish_request(attempt, outcome="failed", error_class=cls)
                await self._halt(item, cls, not_before=_in(30))
                return ExpansionResult(item.account_id, pages, None, cls)
            finally:
                await collector.aclose()

            await self._accounts.release(
                lease,
                remaining=envelope.rate_limit.remaining,
                limit_max=envelope.rate_limit.limit,
                reset_at=envelope.rate_limit.reset_at,
            )

            # The page is durable before anything interprets it: the outbox row,
            # the cursor checkpoint and the request outcome commit together.
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

            if envelope.users:
                empty_pages = 0
            else:
                # An empty page is not by itself the end: X interleaves them
                # mid-list. A run of them is the only end-of-list signal it gives.
                empty_pages += 1
                if empty_pages >= EMPTY_PAGE_LIMIT:
                    return await self._finish(item, TerminationReason.EMPTY_PAGES, pages)

            if envelope.cursor_out is None:
                return await self._finish(item, TerminationReason.NATURAL_END, pages)
            if envelope.cursor_out in seen_cursors:
                # The platform is handing back a cursor we already followed;
                # continuing would loop forever on the same page.
                return await self._finish(item, TerminationReason.CURSOR_STALLED, pages)
            seen_cursors.add(envelope.cursor_out)
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
        attempt budget runs out. Without reading that back the account is left
        mid-expansion for good: the row is finished, nothing will claim it again,
        and its status still says a worker is on it.
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
                f"expansion for {item.account_id} exhausted its attempts on {error_class}"
            )
            await self._record_termination(item, TerminationReason.FAILED, failed=True)

    async def _finish(
        self,
        item: FrontierItem,
        reason: TerminationReason,
        pages: int,
        *,
        error_class: str | None = None,
    ) -> ExpansionResult:
        status = (
            FrontierStatus.FAILED
            if reason is TerminationReason.FAILED
            else FrontierStatus.COMPLETED
        )
        await self._frontier.finish_frontier(item, status, error_class=error_class)
        await self._record_termination(item, reason, failed=status is FrontierStatus.FAILED)
        return ExpansionResult(item.account_id, pages, reason, error_class)

    async def _record_termination(
        self, item: FrontierItem, reason: TerminationReason, *, failed: bool
    ) -> None:
        await self._frontier.record_expansion_outcome(
            item.task_id,
            item.account_id,
            status="failed" if failed else "complete",
            termination_reason=reason.value,
        )


def _error_class(error: Exception) -> str:
    mapping: dict[type[Exception], str] = {
        AccountUnavailableError: "account_unavailable",
        AuthenticationError: "authentication",
        BlockedError: "blocked",
        FeaturesOutdatedError: "features_outdated",
        PlatformOverloadedError: "platform_overloaded",
        RateLimitedError: "rate_limited",
        TransportError: "transport",
    }
    for cls, name in mapping.items():
        if isinstance(error, cls):
            return name
    return type(error).__name__
