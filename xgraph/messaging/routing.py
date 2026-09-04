"""Dispatches a raw page to the handler that understands its operation.

One consumer group reads the whole stream, so a single handler would have to
know about every kind of page. Routing keeps the traversal and the enrichment
writers independent, and — because an unroutable page raises rather than being
skipped — makes a page nobody handles visible instead of silently lost.
"""

from typing import Any

from xgraph.domain import Operation

from .consumer import PageHandler, PermanentEventError
from .events import RawPageEvent


class RoutingPageHandler:
    def __init__(self, handlers: dict[Operation, PageHandler]) -> None:
        if not handlers:
            raise ValueError("at least one handler is required")
        self._handlers = dict(handlers)

    async def __call__(self, connection: Any, event: RawPageEvent, /) -> None:
        handler = self._handlers.get(event.operation)
        if handler is None:
            raise PermanentEventError(f"no handler registered for {event.operation.value}")
        await handler(connection, event)
