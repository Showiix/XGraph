"""HTTP transport boundary for the XGraph collector.

Ported from twscrape/http.py at upstream commit
55ac729f39fbbe46e316746a55627a9ed920112c (MIT), with three adaptations:

* the backend override reads `XGRAPH_HTTP_BACKEND` rather than `TWS_HTTP_BACKEND`
* logging goes to loguru directly, since XGraph has no logger module of its own
* nothing else — the browser families, retry counts and error taxonomy are the
  upstream values, because they encode which transports X currently accepts

Two backends are supported. `curl` impersonates a real browser's TLS and HTTP/2
fingerprint; `httpx` does not, and its handshake is distinguishable from the
browser its User-Agent claims to be. Prefer `curl` for anything that touches X.
"""

import importlib.util
import os
import random
from abc import ABC, abstractmethod
from typing import Any, Literal, Protocol, cast

from fake_useragent import UserAgent
from loguru import logger

HttpMethod = Literal["GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD", "TRACE", "PATCH"]

_UNSET = object()
_CURL_MAX_RETRIES = 3
_LOGGED_BACKENDS: set[str] = set()

# https://curl-impersonate.readthedocs.io/en/latest/fingerprints.html
_BROWSER_FAMILIES = {"chrome", "safari", "firefox", "edge"}

_ua = UserAgent()


def _resolve_browser(hint: str | None, seed: int | None = None) -> tuple[str, str]:
    """Return (ua_string, family) for a @hint or a real UA string."""
    if hint and not hint.startswith("@"):
        return hint, "chrome"
    family = hint[1:].lower() if hint else "chrome"
    if family not in _BROWSER_FAMILIES:
        family = "chrome"
    if seed is not None:
        uas = [x["useragent"] for x in _ua.data_browsers if family in (x["browser"] or "").lower()]
        if uas:
            return random.Random(seed).choice(uas), family
    return cast(str, getattr(_ua, family, _ua.chrome)), family


class _RawResponse(Protocol):
    status_code: int
    text: str
    content: bytes
    headers: Any
    url: Any
    request: Any

    def json(self) -> Any: ...


class Response:
    """Thin wrapper around httpx.Response or curl_cffi.Response."""

    def __init__(self, rep: _RawResponse):
        self._rep = rep
        self._json: Any = _UNSET

    @property
    def status_code(self) -> int:
        return self._rep.status_code

    @property
    def text(self) -> str:
        return self._rep.text

    @property
    def content(self) -> bytes:
        return self._rep.content

    @property
    def headers(self) -> Any:
        return self._rep.headers

    @property
    def url(self) -> Any:
        return self._rep.url

    @property
    def request(self) -> Any:
        return self._rep.request

    def json(self) -> Any:
        if self._json is _UNSET:
            self._json = self._rep.json()
        return self._json

    def raise_for_status(self) -> None:
        if self._rep.status_code >= 400:
            raise HttpStatusError(f"HTTP {self._rep.status_code}", response=self)


class HttpError(Exception): ...


class NetworkError(HttpError): ...


class ConnectError(HttpError): ...


class HttpStatusError(HttpError):
    def __init__(self, message: str, *, response: Response):
        super().__init__(message)
        self.response = response


def format_error(error: Exception) -> str:
    name = type(error).__name__
    if isinstance(error, HttpStatusError):
        return f"{name} {error.response.status_code}"
    if isinstance(error, HttpError):
        return name
    return f"{name}: {error}"


class HttpClient(ABC):
    backend: str

    @abstractmethod
    async def request(self, method: HttpMethod, url: str, **kwargs: Any) -> Response: ...

    async def get(self, url: str, **kwargs: Any) -> Response:
        return await self.request("GET", url, **kwargs)

    async def post(self, url: str, **kwargs: Any) -> Response:
        return await self.request("POST", url, **kwargs)

    @abstractmethod
    async def aclose(self) -> None: ...

    async def __aenter__(self) -> "HttpClient":
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.aclose()

    @property
    @abstractmethod
    def cookies(self) -> Any: ...

    @property
    @abstractmethod
    def headers(self) -> Any: ...


class HttpxClient(HttpClient):
    backend = "httpx"

    def __init__(
        self,
        *,
        proxy: str | None = None,
        headers: dict | None = None,
        cookies: dict | None = None,
        seed: int | None = None,
    ):
        import httpx
        from httpx import AsyncHTTPTransport

        self._httpx = httpx
        transport = AsyncHTTPTransport(retries=3)
        resolved_headers = dict(headers or {})
        ua_string, _ = _resolve_browser(resolved_headers.get("user-agent"), seed=seed)
        resolved_headers["user-agent"] = ua_string
        self._client = httpx.AsyncClient(
            proxy=proxy,
            follow_redirects=True,
            transport=transport,
            headers=resolved_headers,
            cookies=cookies or {},
        )

    @property
    def cookies(self) -> Any:
        return self._client.cookies

    @property
    def headers(self) -> Any:
        return self._client.headers

    async def request(self, method: HttpMethod, url: str, **kwargs: Any) -> Response:
        return await self._wrap(self._client.request(method, url, **kwargs))

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _wrap(self, coro: Any) -> Response:
        hx = self._httpx
        try:
            return Response(await coro)
        except (hx.ConnectError, hx.ConnectTimeout) as e:
            raise ConnectError(str(e)) from e
        except (hx.ReadTimeout, hx.WriteTimeout, hx.PoolTimeout, hx.ProxyError) as e:
            raise NetworkError(str(e)) from e


class CurlClient(HttpClient):
    backend = "curl"

    def __init__(
        self, *, proxy: str | None = None, headers: dict | None = None, cookies: dict | None = None
    ):
        from curl_cffi.requests import AsyncSession, BrowserTypeLiteral

        _, family = _resolve_browser((headers or {}).get("user-agent"))
        # strip user-agent — curl_cffi sets its own UA for the impersonated profile
        safe_headers = {k: v for k, v in (headers or {}).items() if k.lower() != "user-agent"}
        self._session = AsyncSession(
            impersonate=cast(BrowserTypeLiteral, family),
            proxy=proxy,
            allow_redirects=True,
            headers=safe_headers,
        )
        if cookies:
            self._session.cookies.update(cookies)

    @property
    def cookies(self) -> Any:
        return self._session.cookies

    @property
    def headers(self) -> Any:
        return self._session.headers

    async def request(self, method: HttpMethod, url: str, **kwargs: Any) -> Response:
        last_err: Exception | None = None
        for _ in range(_CURL_MAX_RETRIES + 1):
            try:
                return await self._wrap(self._session.request(method, url, **kwargs))
            except NetworkError as e:
                last_err = e
        if last_err is not None:
            raise last_err
        raise RuntimeError("CurlClient request failed without an error")

    async def aclose(self) -> None:
        await self._session.close()

    async def _wrap(self, coro: Any) -> Response:
        try:
            return Response(await coro)
        except Exception as e:
            from curl_cffi.requests import errors as _curl_errors

            if isinstance(e, _curl_errors.RequestsError):
                # libcurl error codes: 5=COULDNT_RESOLVE_PROXY, 6=COULDNT_RESOLVE_HOST, 7=COULDNT_CONNECT
                if getattr(e, "code", -1) in {5, 6, 7}:
                    raise ConnectError(str(e)) from e
                raise NetworkError(str(e)) from e
            raise


def _detect_backend() -> str:
    """Pick a backend, preferring the one X cannot trivially tell from a browser.

    Unlike upstream, which defaults to `httpx`, XGraph defaults to `curl` when
    curl-cffi is importable. Measured against a fingerprint echo, the two
    backends differ in more than TLS ciphers:

        httpx   ja4 t13d1712h1_...   negotiates HTTP/1.1
        curl    ja4 t13d1516h2_...   negotiates HTTP/2

    x.com's web client speaks HTTP/2. Falling back to HTTP/1.1 while presenting
    a Chrome User-Agent is a plainer signal than the cipher list, so a silent
    default onto `httpx` would undermine the account pool it is meant to serve.
    An explicit `XGRAPH_HTTP_BACKEND` always wins.
    """

    forced = os.getenv("XGRAPH_HTTP_BACKEND", "").lower().strip()
    has_curl = importlib.util.find_spec("curl_cffi") is not None

    if forced == "curl":
        if not has_curl:
            raise ImportError(
                "XGRAPH_HTTP_BACKEND=curl but curl-cffi is not installed. "
                "Run: pip install 'xgraph[curl]'"
            )
        return "curl"

    if forced == "httpx":
        if importlib.util.find_spec("httpx") is None:
            raise ImportError(
                "XGRAPH_HTTP_BACKEND=httpx but httpx is not installed. Run: pip install xgraph"
            )
        return "httpx"

    if forced == "":
        if has_curl:
            return "curl"
        logger.warning(
            "curl-cffi is not installed; falling back to the httpx transport, whose TLS "
            "fingerprint and HTTP/1.1 negotiation are distinguishable from a browser. "
            "Install 'xgraph[curl]' before running against X."
        )
        return "httpx"

    raise ValueError(f"Invalid XGRAPH_HTTP_BACKEND={forced!r}. Expected 'curl' or 'httpx'.")


def make_client(
    backend: str | None = None,
    *,
    proxy: str | None = None,
    headers: dict | None = None,
    cookies: dict | None = None,
    seed: int | None = None,
) -> HttpClient:
    if backend is None:
        backend = _detect_backend()

    if backend not in _LOGGED_BACKENDS:
        name = "curl-cffi" if backend == "curl" else backend
        logger.debug(f"Using {name} HTTP client")
        _LOGGED_BACKENDS.add(backend)

    if backend == "curl":
        return CurlClient(proxy=proxy, headers=headers, cookies=cookies)
    if backend == "httpx":
        return HttpxClient(proxy=proxy, headers=headers, cookies=cookies, seed=seed)

    raise ValueError(f"Unknown backend: {backend!r}. Expected 'curl' or 'httpx'.")


def credential_seed(alias: str) -> int:
    """Derive a stable UA seed from a Scraper Account alias.

    An account whose User-Agent changes between restarts is itself a signal, so
    the seed is a pure function of the alias: the same account always presents
    the same browser. Mirrors twscrape/account.py, which seeds from the username.
    """

    import hashlib

    return int(hashlib.sha256(alias.encode()).hexdigest()[:8], 16)
