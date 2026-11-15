"""What the site tells a browser about other origins.

The site registered its CORS policy twice, each with credentials and every
method allowed, for the api origin -- which hosts the weaker account pages. A
flaw there could then act as a signed-in site visitor. The policy is now one
registration, and carries no credentials.
"""

import httpx2 as httpx
from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

from ccdexplorer.ccdexplorer_site.app.factory import (
    CORS_ORIGINS,
    SECURITY_HEADERS,
    add_cors,
)


def _app():
    app = FastAPI()

    @app.get("/x")
    def x():
        return {"ok": True}

    add_cors(app)
    return app


def _client(app):
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t")


def test_the_policy_is_registered_once():
    app = _app()
    assert [m.cls for m in app.user_middleware].count(CORSMiddleware) == 1


async def test_the_api_origin_may_read_but_not_with_the_session():
    async with _client(_app()) as c:
        r = await c.get("/x", headers={"Origin": "https://api.ccdexplorer.io"})
    assert r.headers["access-control-allow-origin"] == "https://api.ccdexplorer.io"
    assert "access-control-allow-credentials" not in r.headers


async def test_a_stranger_origin_gets_nothing():
    async with _client(_app()) as c:
        r = await c.get("/x", headers={"Origin": "https://evil.example"})
    assert "access-control-allow-origin" not in r.headers


async def test_a_cross_origin_write_is_not_permitted():
    async with _client(_app()) as c:
        r = await c.options(
            "/x",
            headers={
                "Origin": "https://api.ccdexplorer.io",
                "Access-Control-Request-Method": "DELETE",
            },
        )
    assert r.status_code == 400  # starlette answers a refused preflight with 400


def test_the_origins_are_the_ones_that_were_there():
    assert "https://api.ccdexplorer.io" in CORS_ORIGINS


def test_transport_security_is_stated():
    assert SECURITY_HEADERS["Strict-Transport-Security"].startswith("max-age=")
    assert "includeSubDomains" not in SECURITY_HEADERS["Strict-Transport-Security"]
