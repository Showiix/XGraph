"""Authenticated X Web GraphQL client owned by XGraph.

Request construction and 404 transaction-id refresh behavior are adapted from
twscrape/account.py and queue_client.py at upstream commit 55ac729 (MIT).
"""

import hashlib
import json
from datetime import datetime, timezone
from types import TracebackType
from typing import Any, Mapping, Protocol
from urllib.parse import urlparse

from xgraph.accounts import ScraperCredential
from xgraph.domain import Operation, PageEnvelope, RateLimitSnapshot, TweetRecord, UserProfile

from .errors import InvalidResponseError, TransportError, blocked_error, response_error
from .http import HttpClient, HttpError, Response, credential_seed, format_error, make_client
from .operations import USER_LOOKUP_FEATURES, encode_params, operation_url
from .parser import parse_bottom_cursor, parse_single_user, parse_tweets, parse_users
from .xclid import XClIdGen

#: Per-request timeout. The two backends disagree on their defaults (httpx 5s,
#: curl-cffi 30s), and a 5s ceiling produces spurious failures on slow egress,
#: so the value is set explicitly rather than inherited.
REQUEST_TIMEOUT = 30

BEARER_TOKEN = (
    "Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D"
    "1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"
)


class TransactionIdSigner(Protocol):
    async def generate(self, method: str, path: str, *, refresh: bool = False) -> str: ...


class XWebTransactionIdSigner:
    def __init__(self, credential: ScraperCredential) -> None:
        self._credential = credential
        self._generator: XClIdGen | None = None

    async def generate(self, method: str, path: str, *, refresh: bool = False) -> str:
        if self._generator is None or refresh:
            self._generator = await XClIdGen.create(
                proxy=self._credential.proxy,
                cookies=dict(self._credential.cookies),
                user_agent=self._credential.user_agent,
                seed=credential_seed(self._credential.alias),
            )
        return self._generator.calc(method, path)


class ProtocolCollector:
    """Fetch one account lookup or Following page from X Web GraphQL."""

    def __init__(
        self,
        credential: ScraperCredential,
        *,
        client: HttpClient | None = None,
        signer: TransactionIdSigner | None = None,
    ) -> None:
        self.credential = credential
        self._owns_client = client is None
        # The User-Agent is a hint, not a literal: the transport resolves it to a
        # real UA string and, on the curl backend, to a matching TLS fingerprint.
        # Setting it here as a plain header would claim one browser while the
        # handshake shows another. The seed keeps the choice stable per account.
        self._client = client or make_client(
            proxy=credential.proxy,
            headers={"user-agent": credential.user_agent, **self._headers()},
            seed=credential_seed(credential.alias),
        )
        self._client.headers.update(self._headers())
        for name, value in credential.cookies.items():
            self._client.cookies.set(name, value, domain=".x.com")
        self._signer = signer or XWebTransactionIdSigner(credential)

    def _headers(self) -> dict[str, str]:
        """Static request headers. The User-Agent is deliberately not here."""

        if "ct0" not in self.credential.cookies:
            raise ValueError("credential is missing the ct0 cookie required for x-csrf-token")
        return {
            **self.credential.extra_headers,
            "authorization": BEARER_TOKEN,
            "content-type": "application/json",
            "x-csrf-token": self.credential.cookies["ct0"],
            "x-twitter-active-user": "yes",
            "x-twitter-client-language": "en",
        }

    async def __aenter__(self) -> "ProtocolCollector":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def user_by_screen_name(self, username: str) -> PageEnvelope:
        username = username.removeprefix("@").strip()
        if not username:
            raise ValueError("username is required")
        variables = {"screen_name": username, "withSafetyModeUserFields": True}
        response, payload, requested_at, received_at = await self._request(
            Operation.USER_BY_SCREEN_NAME,
            variables,
            USER_LOOKUP_FEATURES,
        )
        user = parse_single_user(payload)
        return self._envelope(
            operation=Operation.USER_BY_SCREEN_NAME,
            source_account_id=user.id,
            cursor_in=None,
            cursor_out=None,
            users=(user,),
            response=response,
            payload=payload,
            requested_at=requested_at,
            received_at=received_at,
        )

    async def following_page(self, account_id: str, cursor: str | None = None) -> PageEnvelope:
        if not account_id:
            raise ValueError("account_id is required")
        variables: dict[str, Any] = {
            "userId": str(account_id),
            "count": 20,
            "includePromotedContent": False,
        }
        if cursor is not None:
            variables["cursor"] = cursor
        response, payload, requested_at, received_at = await self._request(
            Operation.FOLLOWING,
            variables,
            {},
        )
        return self._envelope(
            operation=Operation.FOLLOWING,
            source_account_id=str(account_id),
            cursor_in=cursor,
            cursor_out=parse_bottom_cursor(payload),
            users=parse_users(payload),
            response=response,
            payload=payload,
            requested_at=requested_at,
            received_at=received_at,
        )

    async def user_tweets_page(self, account_id: str, cursor: str | None = None) -> PageEnvelope:
        if not account_id:
            raise ValueError("account_id is required")
        variables: dict[str, Any] = {
            "userId": str(account_id),
            "count": 40,
            "includePromotedContent": True,
            "withQuickPromoteEligibilityTweetFields": True,
            "withVoice": True,
            "withV2Timeline": True,
        }
        if cursor is not None:
            variables["cursor"] = cursor
        response, payload, requested_at, received_at = await self._request(
            Operation.USER_TWEETS,
            variables,
            {},
        )
        return self._envelope(
            operation=Operation.USER_TWEETS,
            source_account_id=str(account_id),
            cursor_in=cursor,
            cursor_out=parse_bottom_cursor(payload),
            users=parse_users(payload),
            tweets=parse_tweets(payload),
            response=response,
            payload=payload,
            requested_at=requested_at,
            received_at=received_at,
        )

    async def _request(
        self,
        operation: Operation,
        variables: dict[str, Any],
        features: dict[str, bool],
    ) -> tuple[Response, dict[str, Any], datetime, datetime]:
        url = operation_url(operation)
        path = urlparse(url).path or "/"
        params = encode_params(variables, features)
        response: Response | None = None
        requested_at = datetime.now(timezone.utc)
        for attempt in range(3):
            try:
                transaction_id = await self._signer.generate("GET", path, refresh=attempt > 0)
                response = await self._client.get(
                    url,
                    params=params,
                    headers={"x-client-transaction-id": transaction_id},
                    timeout=REQUEST_TIMEOUT,
                )
            except HttpError as error:
                # Connect and network failures are retried by the transport and
                # then surfaced; the account manager decides whether the egress
                # or the account is at fault, so keep the distinction readable.
                raise TransportError(format_error(error)) from error
            if response.status_code != 404:
                break
        received_at = datetime.now(timezone.utc)
        if response is None:
            raise TransportError("request did not return a response")
        if blocked := blocked_error(response.status_code, response.headers):
            raise blocked
        try:
            raw_payload = response.json()
        except json.JSONDecodeError as error:
            # Carry a short, sanitised sample. Without it the next occurrence is
            # as opaque as this one, and the difference between an interstitial,
            # an error page and a truncated body is not recoverable after the
            # fact.
            sample = " ".join(response.text[:200].split())
            raise InvalidResponseError(
                f"X Web returned non-JSON content "
                f"(status {response.status_code}, "
                f"type {response.headers.get('content-type', '?')}): {sample}"
            ) from error
        if not isinstance(raw_payload, dict):
            raise InvalidResponseError("X Web returned a non-object JSON payload")
        payload: dict[str, Any] = raw_payload
        if error := response_error(response.status_code, response.headers, payload):
            raise error
        return response, payload, requested_at, received_at

    def _envelope(
        self,
        *,
        operation: Operation,
        source_account_id: str,
        cursor_in: str | None,
        cursor_out: str | None,
        users: tuple[UserProfile, ...],
        tweets: tuple[TweetRecord, ...] = (),
        response: Response,
        payload: dict[str, Any],
        requested_at: datetime,
        received_at: datetime,
    ) -> PageEnvelope:
        return PageEnvelope(
            event_id=page_event_id(operation, source_account_id, cursor_in),
            schema_version=1,
            operation=operation,
            source_account_id=source_account_id,
            cursor_in=cursor_in,
            cursor_out=cursor_out,
            users=users,
            tweets=tweets,
            rate_limit=_rate_limit(response.headers),
            status_code=response.status_code,
            requested_at=requested_at,
            received_at=received_at,
            raw_payload=payload,
        )


def page_event_id(operation: Operation, source_account_id: str, cursor_in: str | None) -> str:
    """Stable identity of one page of one pagination chain.

    The identity is the work being done, not the bytes that came back. Fetching
    the same page twice — a retry after a crash, or a redelivered work item —
    must produce the same `event_id`, otherwise the outbox stores the page
    twice, the Parser counts it twice, and the three-condition completion test
    can never balance. Together with `task_id` this is the design's
    `UNIQUE(task_id, account_id, operation, cursor_in)` page key.
    """

    identity = f"{operation.value}\0{source_account_id}\0{cursor_in or ''}"
    return hashlib.sha256(identity.encode()).hexdigest()


def _rate_limit(headers: Mapping[str, str]) -> RateLimitSnapshot:
    def integer(name: str) -> int | None:
        try:
            value = headers.get(name)
            return int(value) if value is not None else None
        except ValueError:
            return None

    reset = integer("x-rate-limit-reset")
    return RateLimitSnapshot(
        limit=integer("x-rate-limit-limit"),
        remaining=integer("x-rate-limit-remaining"),
        reset_at=datetime.fromtimestamp(reset, timezone.utc) if reset is not None else None,
    )
