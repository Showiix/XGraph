"""Error classes and response classification for X Web GraphQL."""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping


class CollectorError(Exception):
    """Base class for protocol collector failures."""


class TransportError(CollectorError):
    pass


class InvalidResponseError(CollectorError):
    pass


class AuthenticationError(CollectorError):
    pass


class AccountUnavailableError(CollectorError):
    pass


class FeaturesOutdatedError(CollectorError):
    pass


class PlatformOverloadedError(CollectorError):
    pass


class BlockedError(CollectorError):
    """The edge returned an interstitial instead of the GraphQL response.

    This is distinct from a malformed response: the request never reached the
    API, so the account and its egress are suspect while the payload is not.
    """

    def __init__(self, source: str, status_code: int) -> None:
        super().__init__(f"blocked by {source} with HTTP {status_code}")
        self.source = source
        self.status_code = status_code


@dataclass(frozen=True, slots=True)
class RateLimitedError(CollectorError):
    reset_at: datetime

    def __str__(self) -> str:
        return f"rate limited until {self.reset_at.isoformat()}"


def blocked_error(status_code: int, headers: Mapping[str, str]) -> BlockedError | None:
    """Detect an edge block before attempting to parse the body.

    An HTML body on an error status means the response came from the edge, not
    from the GraphQL API. Parsing it first would surface a generic decode error
    and hide the fact that the account/egress pair is being challenged.
    Mirrors twscrape/queue_client.py at upstream commit 55ac729.
    """

    content_type = headers.get("content-type", "")
    # Not gated on the status code: an interstitial can arrive with 200, and the
    # content type is the honest signal that this did not come from the API.
    if "text/html" in content_type:
        return BlockedError("Cloudflare" if "cf-ray" in headers else "HTML", status_code)
    return None


def _header_int(headers: Mapping[str, str], name: str) -> int | None:
    try:
        value = headers.get(name)
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def response_error(
    status_code: int, headers: Mapping[str, str], payload: dict[str, Any]
) -> CollectorError | None:
    """Return the terminal/retry signal encoded by an X Web response.

    The ordering follows twscrape/queue_client.py at upstream commit 55ac729.
    """

    remaining = _header_int(headers, "x-rate-limit-remaining")
    reset = _header_int(headers, "x-rate-limit-reset")
    raw_errors = payload.get("errors")
    errors = raw_errors if isinstance(raw_errors, list) else []
    codes = {item.get("code") for item in errors if isinstance(item, dict)}
    messages = [str(item.get("message", "")) for item in errors if isinstance(item, dict)]

    if 336 in codes or any("features cannot be null" in message.lower() for message in messages):
        return FeaturesOutdatedError("X GraphQL feature flags are outdated")
    if remaining == 0 and reset is not None and reset > 0:
        return RateLimitedError(datetime.fromtimestamp(reset, timezone.utc))
    if 88 in codes and remaining is not None and remaining > 0:
        return AccountUnavailableError("rate-limit error with remaining quota")
    if 326 in codes:
        return AccountUnavailableError("access denied for scraper account")
    if 32 in codes:
        return AuthenticationError("scraper account session is invalid")
    if -1 in codes and any("loadshed" in message.lower() for message in messages):
        return PlatformOverloadedError("X platform load shed")
    if status_code == 403 and not errors:
        return AuthenticationError("scraper account session is invalid")
    if status_code >= 400:
        return InvalidResponseError(f"X Web returned HTTP {status_code}")
    return None
