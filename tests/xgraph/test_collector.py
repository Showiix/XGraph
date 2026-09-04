import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any, cast

import httpx
import pytest

from xgraph.accounts import ScraperCredential
from xgraph.collector import ProtocolCollector
from xgraph.collector.errors import (
    AccountUnavailableError,
    AuthenticationError,
    FeaturesOutdatedError,
    InvalidResponseError,
    PlatformOverloadedError,
    RateLimitedError,
    response_error,
)
from xgraph.collector.http import HttpClient, HttpMethod, Response
from xgraph.collector.parser import parse_bottom_cursor, parse_tweets, parse_users
from xgraph.domain import Operation, TweetKind

FIXTURES = Path(__file__).parents[1] / "mocked-data"


class MockHttpClient(HttpClient):
    """Drive a mock transport through the real collector HTTP boundary.

    Injecting an `httpx.AsyncClient` directly would bypass `Response`, the error
    taxonomy and the header handling that both backends share, so the tests
    would stop covering the code path production uses.
    """

    backend = "httpx-mock"

    def __init__(self, handler: Any) -> None:
        self._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    @property
    def cookies(self) -> Any:
        return self._client.cookies

    @property
    def headers(self) -> Any:
        return self._client.headers

    async def request(self, method: HttpMethod, url: str, **kwargs: Any) -> Response:
        # `_RawResponse` declares plain attributes while httpx exposes read-only
        # properties; the production wrapper receives an untyped awaitable, so the
        # cast keeps this adapter equivalent without editing the ported module.
        return Response(cast(Any, await self._client.request(method, url, **kwargs)))

    async def aclose(self) -> None:
        await self._client.aclose()


def fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


def credential() -> ScraperCredential:
    return ScraperCredential(
        alias="test-scraper",
        cookies={"auth_token": "auth-secret", "ct0": "csrf-secret"},
        user_agent="XGraph Test Browser/1.0",
    )


class FixedSigner:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, bool]] = []

    async def generate(self, method: str, path: str, *, refresh: bool = False) -> str:
        self.calls.append((method, path, refresh))
        return f"transaction-{len(self.calls)}"


def test_following_fixture_is_normalized_without_upstream_runtime() -> None:
    payload = fixture("raw_following.json")
    users = parse_users(payload)

    assert len(users) == 60
    assert users[0].id == "236209129"
    assert users[0].username == "leslieberland"
    assert users[0].followers_count == 127312
    assert users[0].following_count == 3814
    assert users[0].description == "CMO Verizon"
    assert users[0].can_dm is False
    assert parse_bottom_cursor(payload)


def test_user_lookup_fixture_has_one_normalized_user() -> None:
    users = parse_users(fixture("raw_user_by_login.json"))

    assert len(users) == 1
    assert users[0].id == "2244994945"
    assert users[0].username == "XDevelopers"


def test_user_tweets_fixture_is_normalized() -> None:
    tweets = parse_tweets(fixture("raw_user_tweets.json"))

    # The fixture page holds 21 top-level tweet entries: 1 pinned, 18 timeline
    # items and 2 self-thread items. The response also embeds the original
    # behind every retweet; those belong to other accounts and must not appear.
    assert len(tweets) == 21
    assert tweets[0].id == "2019881223666233717"
    assert tweets[0].author_id == "2244994945"
    assert tweets[0].kind is TweetKind.ORIGINAL
    assert tweets[0].reply_count == 783
    assert tweets[0].retweet_count == 832
    assert tweets[0].like_count == 5569
    assert tweets[0].view_count == 2525462


def test_user_tweets_classifies_every_top_level_post() -> None:
    tweets = parse_tweets(fixture("raw_user_tweets.json"))
    by_kind = Counter(tweet.kind for tweet in tweets)

    assert by_kind == {TweetKind.RETWEET: 16, TweetKind.ORIGINAL: 4, TweetKind.REPLY: 1}
    assert all(tweet.retweeted_tweet_id for tweet in tweets if tweet.kind is TweetKind.RETWEET)


def test_pure_reposts_never_enter_the_qualifying_sample() -> None:
    """Repost wrappers report zero engagement and would halve every average."""

    tweets = parse_tweets(fixture("raw_user_tweets.json"))
    qualifying = [tweet for tweet in tweets if tweet.is_qualifying]

    assert len(qualifying) == 4
    assert all(tweet.kind is not TweetKind.RETWEET for tweet in qualifying)
    assert all(tweet.kind is not TweetKind.REPLY for tweet in qualifying)
    assert not [
        tweet
        for tweet in qualifying
        if tweet.reply_count == 0 and tweet.retweet_count == 0 and tweet.like_count == 0
    ]

    polluted = mean(tweet.like_count for tweet in tweets)
    clean = mean(tweet.like_count for tweet in qualifying)
    assert clean > polluted * 4


def test_who_to_follow_module_contributes_no_posts() -> None:
    """Timeline modules also carry user recommendations, which are not posts."""

    tweets = parse_tweets(fixture("raw_user_tweets.json"))
    users = {user.id for user in parse_users(fixture("raw_user_tweets.json"))}

    assert users, "the fixture does contain recommended users"
    assert not users & {tweet.id for tweet in tweets}


def test_core_fields_match_upstream_models_on_following_fixture() -> None:
    from twscrape.models import parse_users as upstream_parse_users

    payload = fixture("raw_following.json")
    ours = {user.id: user for user in parse_users(payload)}
    upstream = {str(user.id): user for user in upstream_parse_users(cast(Any, payload))}

    assert ours.keys() == upstream.keys()
    for account_id, user in ours.items():
        expected = upstream[account_id]
        assert user.username == expected.username
        assert user.display_name == expected.displayname
        assert user.description == expected.rawDescription
        assert user.followers_count == expected.followersCount
        assert user.following_count == expected.friendsCount
        assert user.protected == expected.protected
        assert user.verified == expected.verified


@pytest.mark.asyncio
async def test_following_page_builds_x_web_request_and_envelope() -> None:
    payload = fixture("raw_following.json")
    signer = FixedSigner()
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            headers={
                "content-type": "application/json",
                "x-rate-limit-limit": "188",
                "x-rate-limit-remaining": "187",
                "x-rate-limit-reset": "4102444800",
            },
            json=payload,
        )

    client = MockHttpClient(handler)
    collector = ProtocolCollector(credential(), client=client, signer=signer)
    envelope = await collector.following_page("123", cursor="cursor-in")
    await client.aclose()

    assert len(requests) == 1
    request = requests[0]
    assert request.url.path == "/i/api/graphql/qGZZDF3mp91q7X22s3HxpA/Following"
    variables = json.loads(request.url.params["variables"])
    assert variables == {
        "userId": "123",
        "count": 20,
        "includePromotedContent": False,
        "cursor": "cursor-in",
    }
    features = json.loads(request.url.params["features"])
    assert features["responsive_web_graphql_timeline_navigation_enabled"] is True
    assert request.headers["authorization"].startswith("Bearer ")
    assert request.headers["x-csrf-token"] == "csrf-secret"
    assert request.headers["x-client-transaction-id"] == "transaction-1"
    assert "auth-secret" in request.headers["cookie"]
    assert envelope.operation is Operation.FOLLOWING
    assert envelope.source_account_id == "123"
    assert envelope.cursor_in == "cursor-in"
    assert envelope.cursor_out is not None
    assert len(envelope.users) == 60
    assert envelope.rate_limit.limit == 188
    assert envelope.rate_limit.remaining == 187
    assert envelope.rate_limit.reset_at == datetime.fromtimestamp(4102444800, timezone.utc)
    assert "auth-secret" not in repr(envelope)
    assert "csrf-secret" not in repr(envelope)


@pytest.mark.asyncio
async def test_user_lookup_strips_at_and_refreshes_transaction_id_after_404() -> None:
    signer = FixedSigner()
    responses = [
        httpx.Response(404, json={}),
        httpx.Response(200, json=fixture("raw_user_by_login.json")),
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        return responses.pop(0)

    client = MockHttpClient(handler)
    collector = ProtocolCollector(credential(), client=client, signer=signer)
    envelope = await collector.user_by_screen_name("@TwitterDev")
    await client.aclose()

    assert envelope.operation is Operation.USER_BY_SCREEN_NAME
    assert envelope.source_account_id == "2244994945"
    assert [call[2] for call in signer.calls] == [False, True]


@pytest.mark.asyncio
async def test_user_tweets_page_uses_timeline_operation() -> None:
    signer = FixedSigner()
    payload = fixture("raw_user_tweets.json")

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/i/api/graphql/SXVCYB8XHSS25nzIljNtZA/UserTweets"
        variables = json.loads(request.url.params["variables"])
        assert variables["userId"] == "2244994945"
        assert variables["count"] == 40
        return httpx.Response(
            200,
            headers={"x-rate-limit-limit": "188", "x-rate-limit-remaining": "187"},
            json=payload,
        )

    client = MockHttpClient(handler)
    collector = ProtocolCollector(credential(), client=client, signer=signer)
    envelope = await collector.user_tweets_page("2244994945")
    await client.aclose()

    assert envelope.operation is Operation.USER_TWEETS
    assert envelope.tweets


@pytest.mark.parametrize(
    ("status_code", "headers", "payload", "expected"),
    [
        (
            200,
            {"x-rate-limit-remaining": "0", "x-rate-limit-reset": "4102444800"},
            {},
            RateLimitedError,
        ),
        (
            200,
            {"x-rate-limit-remaining": "10"},
            {"errors": [{"code": 88, "message": "Rate limit exceeded"}]},
            AccountUnavailableError,
        ),
        (
            200,
            {},
            {"errors": [{"code": 326, "message": "Authorization: Denied by access control"}]},
            AccountUnavailableError,
        ),
        (
            200,
            {},
            {"errors": [{"code": 32, "message": "Could not authenticate you"}]},
            AuthenticationError,
        ),
        (
            200,
            {},
            {"errors": [{"code": 336, "message": "The following features cannot be null"}]},
            FeaturesOutdatedError,
        ),
        (
            200,
            {},
            {"errors": [{"code": -1, "message": "LoadShed"}]},
            PlatformOverloadedError,
        ),
        (403, {}, {}, AuthenticationError),
        (500, {}, {}, InvalidResponseError),
    ],
)
def test_response_error_classification(
    status_code: int,
    headers: dict[str, str],
    payload: dict,
    expected: type[Exception],
) -> None:
    error = response_error(status_code, headers, payload)
    assert isinstance(error, expected)


def test_scraper_credential_requires_web_session_cookies() -> None:
    with pytest.raises(ValueError, match="auth_token"):
        ScraperCredential(alias="missing", cookies={"ct0": "csrf"}, user_agent="ua")


@pytest.mark.asyncio
async def test_a_throttled_request_is_a_rate_limit_not_a_broken_response() -> None:
    """X answers a throttled request with plain text, not the JSON error envelope.

    Classifying it from the payload never happens: the body fails to parse first,
    so a normal, temporary condition is reported as a malformed response and the
    account takes the blame. Five of those in a row disable an account that was
    working the whole time.
    """

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            429,
            headers={
                "content-type": "text/plain;charset=utf-8",
                "x-rate-limit-remaining": "0",
                "x-rate-limit-reset": "4102444800",
            },
            content=b"Rate limit exceeded",
        )

    client = MockHttpClient(handler)
    collector = ProtocolCollector(credential(), client=client, signer=FixedSigner())
    with pytest.raises(RateLimitedError) as caught:
        await collector.following_page("123")
    await client.aclose()
    assert caught.value.reset_at == datetime.fromtimestamp(4102444800, timezone.utc)


@pytest.mark.asyncio
async def test_a_throttled_request_without_a_reset_header_still_waits() -> None:
    """A rate limit that does not say when it lifts is still a rate limit."""

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            429, headers={"content-type": "text/plain"}, content=b"Rate limit exceeded"
        )

    client = MockHttpClient(handler)
    collector = ProtocolCollector(credential(), client=client, signer=FixedSigner())
    with pytest.raises(RateLimitedError) as caught:
        await collector.following_page("123")
    await client.aclose()
    assert caught.value.reset_at > datetime.now(timezone.utc)
