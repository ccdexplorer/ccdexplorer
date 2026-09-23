"""A crawler must be able to HEAD an og:image.

Starlette adds HEAD to any route registered for GET. FastAPI does not, so every
.../image.png on this site answered HTTP 405 with `{"detail":"Method Not
Allowed"}` -- measured against production before this was fixed. Most link
unfurlers HEAD an image to check its type and size before downloading it, and
some take a 405 as the image being unavailable and drop the preview entirely.
"""

from fastapi import APIRouter, FastAPI
from fastapi.routing import APIRoute
from fastapi.responses import Response
from starlette.testclient import TestClient

from ccdexplorer.ccdexplorer_site.app.factory import allow_head_on_images
from ccdexplorer.ccdexplorer_site.app.routers import account, og, statistics
from ccdexplorer.ccdexplorer_site.app.state import get_httpx_client

import httpx2 as httpx


def _app_with_a_couple_of_routes():
    router = APIRouter()

    @router.get("/plots/{net}/some_chart/image.png")
    async def chart(net: str):
        return Response(content=b"\x89PNG\r\n\x1a\n", media_type="image/png")

    @router.get("/{net}/some_page")
    async def page(net: str):
        return Response(content="hello", media_type="text/html")

    app = FastAPI()
    app.include_router(router)
    return app


def test_an_image_route_gains_head():
    app = _app_with_a_couple_of_routes()
    allow_head_on_images(app)

    with TestClient(app) as client:
        response = client.head("/plots/mainnet/some_chart/image.png")
    assert response.status_code == 200
    assert response.headers["content-type"] == "image/png"


def test_an_image_route_is_405_without_it():
    """The bug this exists to prevent coming back."""
    with TestClient(_app_with_a_couple_of_routes()) as client:
        assert client.head("/plots/mainnet/some_chart/image.png").status_code == 405


def test_a_page_route_is_left_alone():
    """Unfurlers GET pages because they need the HTML; it is images they HEAD."""
    app = _app_with_a_couple_of_routes()
    allow_head_on_images(app)

    with TestClient(app) as client:
        assert client.head("/mainnet/some_page").status_code == 405


def test_the_card_route_answers_head():
    """The real route, not a stand-in for it."""
    app = FastAPI()
    app.include_router(og.router)
    app.api_url = "https://api.example.test"
    app.dependency_overrides[get_httpx_client] = lambda: httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(404, json={}))
    )
    allow_head_on_images(app)

    with TestClient(app) as client:
        response = client.head("/og/mainnet/block/52029165/image.png")
    assert response.status_code == 200
    assert response.headers["content-type"] == "image/png"
    assert int(response.headers["content-length"]) > 0


def test_every_real_image_route_is_covered():
    """Against the actual routers, not a stand-in.

    The filter matches on the path ending, so this is the check that the
    site's own routes are spelled the way the filter expects -- if someone
    registers a card at .../card.png it will silently miss.
    """
    app = FastAPI()
    for module in (statistics, account, og):
        app.include_router(module.router)

    images = [r for r in app.routes if isinstance(r, APIRoute) and r.path.endswith("image.png")]
    others = [r for r in app.routes if isinstance(r, APIRoute) and not r.path.endswith("image.png")]
    assert len(images) >= 20, "the plot and card image routes should all be here"

    allow_head_on_images(app)

    assert all("HEAD" in route.methods for route in images)
    assert not any("HEAD" in route.methods for route in others)
