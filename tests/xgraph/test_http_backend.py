"""Contracts for the collector HTTP boundary.

The transport is the only place where XGraph's requests can be told apart from
a browser's before any credential is even checked, so the fingerprint-related
behaviour is asserted here rather than left to the backend implementations.
"""

import importlib.util

import pytest

from xgraph.accounts import DEFAULT_BROWSER_HINT, ScraperCredential
from xgraph.collector.http import (
    ConnectError,
    CurlClient,
    HttpClient,
    HttpError,
    HttpStatusError,
    HttpxClient,
    NetworkError,
    _detect_backend,
    _resolve_browser,
    credential_seed,
    format_error,
    make_client,
)

CURL_AVAILABLE = importlib.util.find_spec("curl_cffi") is not None
requires_curl = pytest.mark.skipif(not CURL_AVAILABLE, reason="curl-cffi is not installed")


@pytest.mark.parametrize("family", ["chrome", "safari", "firefox", "edge"])
def test_browser_hint_resolves_to_a_real_user_agent(family: str) -> None:
    ua, resolved = _resolve_browser(f"@{family}")

    assert resolved == family
    assert not ua.startswith("@"), "the sentinel must never reach the wire"
    assert "Mozilla/" in ua


def test_unknown_browser_hint_falls_back_to_chrome() -> None:
    _, family = _resolve_browser("@netscape")
    assert family == "chrome"


def test_literal_user_agent_passes_through() -> None:
    literal = "Mozilla/5.0 (Macintosh) AppleWebKit/537.36 Chrome/141.0 Safari/537.36"
    ua, family = _resolve_browser(literal)

    assert ua == literal
    assert family == "chrome"


def test_seeded_user_agent_is_stable_across_processes() -> None:
    """A User-Agent that changes between restarts is itself a signal."""

    seed = credential_seed("scraper-a")
    first, _ = _resolve_browser("@chrome", seed=seed)
    second, _ = _resolve_browser("@chrome", seed=seed)

    assert first == second
    assert credential_seed("scraper-a") == seed
    assert credential_seed("scraper-b") != seed


def test_different_accounts_get_different_user_agents() -> None:
    seeds = {credential_seed(f"scraper-{i}") for i in range(20)}
    agents = {_resolve_browser("@chrome", seed=seed)[0] for seed in seeds}

    assert len(seeds) == 20
    assert len(agents) > 1, "all accounts sharing one User-Agent defeats the pool"


def test_credentials_default_to_a_browser_hint_not_a_frozen_string() -> None:
    credential = ScraperCredential(alias="scraper-a", cookies={"auth_token": "a", "ct0": "b"})
    assert credential.user_agent == DEFAULT_BROWSER_HINT


@pytest.mark.asyncio
async def test_httpx_backend_puts_a_resolved_user_agent_on_the_client() -> None:
    client = HttpxClient(headers={"user-agent": "@firefox"}, seed=credential_seed("scraper-a"))
    try:
        ua = client.headers["user-agent"]
        assert not ua.startswith("@")
        assert "Firefox" in ua
    finally:
        await client.aclose()


@requires_curl
def test_curl_backend_impersonates_the_hinted_family() -> None:
    client = CurlClient(headers={"user-agent": "@safari"})

    # curl-cffi supplies the User-Agent belonging to the impersonated profile;
    # carrying our own would risk contradicting the TLS fingerprint.
    assert "user-agent" not in {key.lower() for key in client.headers}
    assert client._session.impersonate == "safari"  # noqa: SLF001


@requires_curl
@pytest.mark.asyncio
async def test_both_backends_expose_the_same_cookie_api() -> None:
    """`client.py` and `xclid.py` both scope cookies to `.x.com` on either backend."""

    for client in (HttpxClient(), CurlClient()):
        try:
            client.cookies.set("auth_token", "secret", domain=".x.com")
            assert client.cookies.get("auth_token", domain=".x.com") == "secret"
            assert isinstance(client, HttpClient)
            assert client.backend in {"httpx", "curl"}
        finally:
            await client.aclose()


@requires_curl
def test_backend_detection_prefers_curl_when_available(monkeypatch: pytest.MonkeyPatch) -> None:
    """The default must not be the transport X can tell apart from a browser."""

    monkeypatch.delenv("XGRAPH_HTTP_BACKEND", raising=False)
    assert _detect_backend() == "curl"


def test_backend_can_be_forced_to_httpx(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("XGRAPH_HTTP_BACKEND", "httpx")
    assert _detect_backend() == "httpx"


@requires_curl
def test_backend_can_be_forced_to_curl(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("XGRAPH_HTTP_BACKEND", "curl")
    assert _detect_backend() == "curl"
    client = make_client()
    assert client.backend == "curl"


def test_unknown_backend_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("XGRAPH_HTTP_BACKEND", "wget")
    with pytest.raises(ValueError, match="Expected 'curl' or 'httpx'"):
        _detect_backend()


def test_transport_errors_are_named_without_leaking_the_url() -> None:
    assert format_error(ConnectError("proxy://user:pass@host unreachable")) == "ConnectError"
    assert format_error(NetworkError("read timeout")) == "NetworkError"
    assert issubclass(ConnectError, HttpError)
    assert issubclass(NetworkError, HttpError)
    assert issubclass(HttpStatusError, HttpError)
