"""Request and page contracts owned by XGraph."""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any

from .models import TweetRecord, UserProfile


class Operation(str, Enum):
    USER_BY_SCREEN_NAME = "UserByScreenName"
    FOLLOWING = "Following"
    USER_TWEETS = "UserTweets"


class FrontierStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    RETRYABLE = "retryable"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass(frozen=True, slots=True)
class RateLimitSnapshot:
    limit: int | None
    remaining: int | None
    reset_at: datetime | None


@dataclass(frozen=True, slots=True)
class PageEnvelope:
    """One observed X Web page, without credentials or business traversal state."""

    event_id: str
    schema_version: int
    operation: Operation
    source_account_id: str
    cursor_in: str | None
    cursor_out: str | None
    users: tuple[UserProfile, ...]
    rate_limit: RateLimitSnapshot
    status_code: int
    requested_at: datetime
    received_at: datetime
    raw_payload: dict[str, Any]
    tweets: tuple[TweetRecord, ...] = ()
