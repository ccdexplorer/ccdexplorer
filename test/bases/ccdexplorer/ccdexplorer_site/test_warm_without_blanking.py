"""The warmer must not blank a chart in order to redraw it.

It evicted each cache entry and then re-requested the path, because asking for
a cached chart does not redraw it. Between those two steps the chart had no
entry at all, so anyone who asked for it right then paid a full kaleido render
-- and the render lock is global and first-come, so they could also wait behind
whichever chart the sweep was drawing.

That was a fair trade at eighteen seconds an hour. It is forty chart routes and
two themes now -- eighty renders a sweep -- so the hole is open far more often
than the comment describing it assumes.

So the refresh renders first and overwrites, and the entry is never absent.
"""

import asyncio

import plotly.graph_objects as go
import pytest
from starlette.requests import Request

from ccdexplorer.ccdexplorer_site.app import utils


def _request(path, query=b"theme=light"):
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": path,
            "raw_path": path.encode(),
            "query_string": query,
            "headers": [],
            "scheme": "https",
            "server": ("ccdexplorer.io", 443),
            "root_path": "",
        }
    )


PATH = "/plots/mainnet/ccd_kraken_1d/image.png"


@pytest.fixture
def renders(monkeypatch):
    """Count renders, and make each one distinguishable."""
    calls = []

    def fake_to_image(fig, **kwargs):
        calls.append(kwargs)
        return b"\x89PNG\r\n\x1a\n" + bytes([len(calls)])

    monkeypatch.setattr(utils.pio, "to_image", fake_to_image)
    utils._PLOT_IMAGES = utils.PngCache()
    return calls


async def test_a_refresh_redraws_even_though_it_is_cached(renders):
    """Without this the warmer would serve the stale image back to itself and
    quietly stop refreshing anything -- worse than the hole it replaces."""
    await utils.return_plot_response(go.Figure(), _request(PATH), "t")
    assert len(renders) == 1

    with utils.forcing_plot_render():
        await utils.return_plot_response(go.Figure(), _request(PATH), "t")

    assert len(renders) == 2, "the refresh served the cached image instead of redrawing"


async def test_an_ordinary_request_still_uses_the_cache(renders):
    await utils.return_plot_response(go.Figure(), _request(PATH), "t")
    await utils.return_plot_response(go.Figure(), _request(PATH), "t")

    assert len(renders) == 1


async def test_the_entry_is_never_absent_while_a_refresh_runs(renders):
    """The whole point: a reader arriving mid-refresh gets the old image at
    once rather than waiting for the new one."""
    await utils.return_plot_response(go.Figure(), _request(PATH), "t")
    key = utils.plot_cache_key(PATH, "light")
    before = utils._PLOT_IMAGES.get(key)[0]

    seen = []
    original = utils.pio.to_image

    def slow_render(fig, **kwargs):
        # Whatever a concurrent reader would find at the worst moment: while
        # the replacement is being drawn.
        seen.append(utils._PLOT_IMAGES.get(key))
        return original(fig, **kwargs)

    utils.pio.to_image = slow_render
    try:
        with utils.forcing_plot_render():
            await utils.return_plot_response(go.Figure(), _request(PATH), "t")
    finally:
        utils.pio.to_image = original

    assert seen and seen[0] is not None, "the chart was blank while being redrawn"
    assert seen[0][0] == before, "the old image was replaced before the new one existed"


async def test_the_refresh_actually_replaces_what_is_served(renders):
    await utils.return_plot_response(go.Figure(), _request(PATH), "t")
    key = utils.plot_cache_key(PATH, "light")
    first = utils._PLOT_IMAGES.get(key)[0]

    with utils.forcing_plot_render():
        await utils.return_plot_response(go.Figure(), _request(PATH), "t")

    assert utils._PLOT_IMAGES.get(key)[0] != first


async def test_the_flag_does_not_leak_to_later_requests(renders):
    with utils.forcing_plot_render():
        await utils.return_plot_response(go.Figure(), _request(PATH), "t")
    assert len(renders) == 1

    await utils.return_plot_response(go.Figure(), _request(PATH), "t")

    assert len(renders) == 1, "every later request is now redrawing"


async def test_the_flag_does_not_leak_across_concurrent_requests(renders):
    """One chart being refreshed must not make every other request redraw."""
    other = "/plots/mainnet/ccd_kraken_1h/image.png"
    await utils.return_plot_response(go.Figure(), _request(other), "t")
    assert len(renders) == 1

    async def refresh():
        with utils.forcing_plot_render():
            await utils.return_plot_response(go.Figure(), _request(PATH), "t")

    async def ordinary():
        await utils.return_plot_response(go.Figure(), _request(other), "t")

    await asyncio.gather(refresh(), ordinary())

    assert len(renders) == 2, "the ordinary request was dragged into a redraw"


async def test_a_refresh_is_not_reachable_from_a_real_request(renders):
    """The signal must not be something a visitor can set, or anyone could make
    the site redraw every chart on demand."""
    await utils.return_plot_response(go.Figure(), _request(PATH), "t")

    for query in (b"theme=light&refresh=1", b"theme=light&force=true"):
        await utils.return_plot_response(go.Figure(), _request(PATH, query), "t")

    assert len(renders) == 1, "a query parameter forced a redraw"


# --- the signal has to survive the trip the warmer actually makes -----------
#
# The warmer reaches the route in-process through httpx's ASGITransport. If the
# ContextVar did not carry across that boundary the refresh would quietly serve
# the cached image back to itself and nothing would ever be redrawn again --
# which is worse than the hole this replaced, and completely silent. So this
# goes through a real ASGI app rather than calling the function directly.


async def test_the_refresh_signal_reaches_the_route_through_asgi(renders):
    import httpx2 as httpx
    from fastapi import FastAPI, Request as FastAPIRequest
    from fastapi.responses import Response

    app = FastAPI()

    @app.get("/plots/{net}/probe_chart/image.png", response_class=Response)
    async def probe(request: FastAPIRequest, net: str):
        return await utils.return_plot_response(go.Figure(), request, "probe")

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://warmer") as client:
        path = "/plots/mainnet/probe_chart/image.png"

        await client.get(path, params={"theme": "light"})
        assert len(renders) == 1

        await client.get(path, params={"theme": "light"})
        assert len(renders) == 1, "a plain request redrew"

        with utils.forcing_plot_render():
            await client.get(path, params={"theme": "light"})

    assert len(renders) == 2, (
        "the refresh signal did not reach the handler through ASGITransport -- "
        "the warmer would serve the cache back to itself and never redraw"
    )
