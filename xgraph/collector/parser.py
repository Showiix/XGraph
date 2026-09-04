"""Minimal User and cursor parsing for XGraph.

The nested/legacy normalization follows twscrape/utils.py and models.py at
upstream commit 55ac729f39fbbe46e316746a55627a9ed920112c (MIT).
"""

import email.utils
from collections.abc import Iterator
from datetime import datetime
from typing import Any

from xgraph.domain import TweetKind, TweetRecord, UserProfile

from .errors import InvalidResponseError


def _walk(value: Any) -> Iterator[dict[str, Any]]:
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk(child)


def _mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _optional_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _optional_bool(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None


def _created_at(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None


def parse_user_object(obj: dict[str, Any]) -> UserProfile | None:
    legacy = _mapping(obj.get("legacy"))
    core = _mapping(obj.get("core"))
    relationship_counts = _mapping(obj.get("relationship_counts"))
    profile_bio = _mapping(obj.get("profile_bio"))
    privacy = _mapping(obj.get("privacy"))
    verification = _mapping(obj.get("verification"))
    dm_permissions = _mapping(obj.get("dm_permissions"))
    avatar = _mapping(obj.get("avatar"))
    banner = _mapping(obj.get("banner"))
    location_obj = obj.get("location")

    account_id = obj.get("rest_id") or legacy.get("id_str")
    username = core.get("screen_name") or legacy.get("screen_name")
    if account_id is None or not username:
        return None

    location = (
        location_obj.get("location")
        if isinstance(location_obj, dict)
        else location_obj or legacy.get("location")
    )
    return UserProfile(
        id=str(account_id),
        username=str(username),
        display_name=str(core.get("name") or legacy.get("name") or ""),
        description=str(profile_bio.get("description") or legacy.get("description") or ""),
        followers_count=_optional_int(
            relationship_counts.get("followers", legacy.get("followers_count"))
        ),
        following_count=_optional_int(
            relationship_counts.get("following", legacy.get("friends_count"))
        ),
        created_at=_created_at(core.get("created_at") or legacy.get("created_at")),
        protected=_optional_bool(privacy.get("protected", legacy.get("protected"))),
        verified=_optional_bool(verification.get("verified", legacy.get("verified"))),
        blue_verified=_optional_bool(obj.get("is_blue_verified")),
        can_dm=_optional_bool(dm_permissions.get("can_dm")),
        location=str(location) if location is not None else None,
        avatar_url=avatar.get("image_url") or legacy.get("profile_image_url_https"),
        banner_url=banner.get("image_url") or legacy.get("profile_banner_url"),
    )


def parse_users(payload: dict[str, Any]) -> tuple[UserProfile, ...]:
    users: dict[str, UserProfile] = {}
    for obj in _walk(payload):
        if obj.get("__typename") != "User":
            continue
        user = parse_user_object(obj)
        if user is not None:
            users.setdefault(user.id, user)
    return tuple(users.values())


def parse_single_user(payload: dict[str, Any]) -> UserProfile:
    users = parse_users(payload)
    if len(users) != 1:
        raise InvalidResponseError(f"expected one user, found {len(users)}")
    return users[0]


def parse_bottom_cursor(payload: dict[str, Any]) -> str | None:
    for obj in _walk(payload):
        value = obj.get("value")
        if obj.get("cursorType") == "Bottom" and isinstance(value, str):
            return value
    return None


#: Instruction blocks that carry top-level timeline entries. Everything else in
#: a timeline response (nested quote/retweet originals, conversation previews,
#: user recommendation modules) belongs to other accounts or is not a post, and
#: must not enter this account's sample.
_ENTRY_INSTRUCTIONS = ("TimelineAddEntries", "TimelinePinEntry")


def _timeline_entries(payload: dict[str, Any]) -> Iterator[dict[str, Any]]:
    """Yield the top-level timeline entries of a timeline response."""

    for obj in _walk(payload):
        instruction = obj.get("type")
        if instruction not in _ENTRY_INSTRUCTIONS:
            continue
        if instruction == "TimelineAddEntries":
            entries = obj.get("entries")
            if isinstance(entries, list):
                yield from (entry for entry in entries if isinstance(entry, dict))
        elif instruction == "TimelinePinEntry":
            entry = obj.get("entry")
            if isinstance(entry, dict):
                yield entry


def _tweet_result(item_content: Any) -> dict[str, Any] | None:
    """Unwrap one `TimelineTweet` item into its tweet object."""

    content = _mapping(item_content)
    if content.get("itemType") != "TimelineTweet":
        return None
    if content.get("promotedMetadata"):
        # Promoted posts are ads served into the timeline, not the account's own
        # content, and must not contribute to its engagement profile.
        return None
    return _unwrap_tweet(content.get("tweet_results")) or None


def _entry_tweets(entry: dict[str, Any]) -> Iterator[dict[str, Any]]:
    """Yield the tweet objects a single top-level entry contributes."""

    content = _mapping(entry.get("content"))
    entry_type = content.get("entryType") or content.get("__typename")
    if entry_type == "TimelineTimelineItem":
        result = _tweet_result(content.get("itemContent"))
        if result is not None:
            yield result
    elif entry_type == "TimelineTimelineModule":
        # Modules hold either self-thread continuations or non-tweet blocks such
        # as who-to-follow; `_tweet_result` filters the latter out.
        for item in content.get("items") or []:
            result = _tweet_result(_mapping(_mapping(item).get("item")).get("itemContent"))
            if result is not None:
                yield result


def _tweet_id(obj: dict[str, Any]) -> str | None:
    legacy = _mapping(obj.get("legacy"))
    value = obj.get("rest_id") or legacy.get("id_str")
    return str(value) if value is not None else None


def _unwrap_tweet(results: Any) -> dict[str, Any]:
    """Resolve a `*_results` block to the tweet object it wraps.

    Only `TweetWithVisibilityResults` nests the post one level deeper. Applying
    that unwrap unconditionally silently yields an empty object for every plain
    `Tweet`, which is how retweet wrappers escape classification.
    """

    result = _mapping(_mapping(results).get("result"))
    if result.get("__typename") == "TweetWithVisibilityResults":
        return _mapping(result.get("tweet"))
    return result


def _nested_tweet_id(legacy: dict[str, Any], id_key: str, result_key: str) -> str | None:
    """Resolve a referenced tweet id from either the flat or the nested form.

    Current responses only carry the nested `*_status_result` form; the flat
    `*_status_id_str` field is kept for older payloads and fixtures.
    """

    value = legacy.get(id_key)
    if value is not None:
        return str(value)
    result = _unwrap_tweet(legacy.get(result_key))
    value = result.get("rest_id") or _mapping(result.get("legacy")).get("id_str")
    return str(value) if value is not None else None


def _views(obj: dict[str, Any]) -> int | None:
    """Read the view count, keeping a missing value distinct from zero."""

    for key in ("views", "ext_views"):
        count = _optional_int(_mapping(obj.get(key)).get("count"))
        if count is not None:
            return count
    return None


def _classify(legacy: dict[str, Any], retweeted_id: str | None, quoted_id: str | None) -> TweetKind:
    """Resolve exactly one post kind; see `TweetKind` for the precedence."""

    if retweeted_id is not None:
        return TweetKind.RETWEET
    if legacy.get("in_reply_to_status_id_str") is not None:
        return TweetKind.REPLY
    if quoted_id is not None or legacy.get("is_quote_status"):
        return TweetKind.QUOTE
    return TweetKind.ORIGINAL


def parse_tweets(payload: dict[str, Any]) -> tuple[TweetRecord, ...]:
    """Normalize the top-level posts of a timeline response.

    Only objects reachable through a top-level timeline entry are returned. A
    response also embeds the originals behind every retweet and quote, which
    belong to other accounts; counting those would pollute the profile of the
    account being sampled.
    """

    tweets: dict[str, TweetRecord] = {}
    for entry in _timeline_entries(payload):
        for obj in _entry_tweets(entry):
            tweet_id = _tweet_id(obj)
            if tweet_id is None or tweet_id in tweets:
                continue
            legacy = _mapping(obj.get("legacy"))
            core = _mapping(obj.get("core"))
            author_result = _mapping(_mapping(core.get("user_results")).get("result"))
            author_id = author_result.get("rest_id") or legacy.get("user_id_str")
            retweeted_id = _nested_tweet_id(
                legacy, "retweeted_status_id_str", "retweeted_status_result"
            )
            quoted_id = _nested_tweet_id(legacy, "quoted_status_id_str", "quoted_status_result")
            reply_id = legacy.get("in_reply_to_status_id_str")
            reply_user_id = legacy.get("in_reply_to_user_id_str")
            conversation_id = legacy.get("conversation_id_str")
            tweets[tweet_id] = TweetRecord(
                id=tweet_id,
                author_id=str(author_id) if author_id is not None else None,
                text=str(legacy.get("full_text") or ""),
                created_at=_created_at(legacy.get("created_at")),
                kind=_classify(legacy, retweeted_id, quoted_id),
                reply_count=_optional_int(legacy.get("reply_count")) or 0,
                retweet_count=_optional_int(legacy.get("retweet_count")) or 0,
                like_count=_optional_int(legacy.get("favorite_count")) or 0,
                quote_count=_optional_int(legacy.get("quote_count")) or 0,
                bookmark_count=_optional_int(legacy.get("bookmark_count")) or 0,
                view_count=_views(obj),
                conversation_id=str(conversation_id) if conversation_id is not None else None,
                in_reply_to_tweet_id=str(reply_id) if reply_id is not None else None,
                in_reply_to_user_id=str(reply_user_id) if reply_user_id is not None else None,
                retweeted_tweet_id=retweeted_id,
                quoted_tweet_id=quoted_id,
            )
    return tuple(tweets.values())
