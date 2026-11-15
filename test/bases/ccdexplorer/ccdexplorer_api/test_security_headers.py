"""The api answers with the headers a browser needs to treat it carefully.

The api origin hosts the account pages (login, reset, plans, keys), so it is
a browser-facing origin as much as the site is, and until now it sent nothing:
no nosniff, no referrer policy, no frame restriction. Reset and verification
tokens travel in url paths here, which is why the referrer policy matters
most -- without one, following any link off a reset page tells that site
the token.
"""

import httpx
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

from ccdexplorer.ccdexplorer_api.app.security_headers import (
    API_SECURITY_HEADERS,
    SecurityHeadersMiddleware,
)


def _client(route_headers: dict | None = None):
    app = FastAPI()

    @app.get("/plain")
    def plain():
        return PlainTextResponse("ok")

    @app.get("/own")
    def own():
        return PlainTextResponse("ok", headers=route_headers or {})

    app.add_middleware(SecurityHeadersMiddleware)
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t")


async def test_every_response_carries_the_headers():
    async with _client() as c:
        r = await c.get("/plain")
    for name, value in API_SECURITY_HEADERS.items():
        assert r.headers[name] == value


async def test_the_referrer_is_never_sent_off_the_page():
    async with _client() as c:
        r = await c.get("/plain")
    assert r.headers["referrer-policy"] == "no-referrer"


async def test_the_api_cannot_be_framed_by_another_origin():
    async with _client() as c:
        r = await c.get("/plain")
    assert "frame-ancestors 'self'" in r.headers["content-security-policy"]


async def test_a_header_the_route_set_itself_is_left_alone():
    async with _client({"Referrer-Policy": "same-origin"}) as c:
        r = await c.get("/own")
    assert r.headers["referrer-policy"] == "same-origin"


async def test_the_real_app_sends_them_even_when_it_refuses(monkeypatch):
    """A 401 is answered by a middleware inside the app, so this is the check
    that the headers are outermost rather than just present on happy paths."""
    from conftest import build_test_app

    app = build_test_app()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as c:
        r = await c.get("/v2/mainnet/block/last_final")  # no key: refused
    assert r.status_code in (401, 403, 429)
    assert r.headers["x-content-type-options"] == "nosniff"
