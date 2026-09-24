"""A chart card must not be re-rendered for every crawler that asks for it.

/plots/<net>/<name>/image.png renders a Plotly figure through kaleido, which
measured at ~0.9s per image against production, every time, with no cache. A
global lock serialises those renders, so a link going around a group turned a
burst of crawlers into a queue: the tenth waited nine seconds, and ten
identical Chromium renders had been paid for.

The image branch is a GET with no body, so get_theme_from_request always
returns "dark" and add_watermark_to_plot always fires -- the path is the whole
of the input, and therefore the whole of the cache key.
"""

import asyncio

import plotly.graph_objects as go
import pytest
from starlette.requests import Request

from ccdexplorer.ccdexplorer_site.app import utils


def _request(path):
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": path,
            "raw_path": path.encode(),
            "query_string": b"",
            "headers": [],
            "scheme": "https",
            "server": ("ccdexplorer.io", 443),
            "root_path": "",
        }
    )


def _figure():
    return go.Figure(data=[go.Scatter(x=[1, 2, 3], y=[1, 4, 9])])


@pytest.fixture
def renders(monkeypatch):
    """Count kaleido renders, and make each one slow enough to overlap."""
    calls = []

    def fake_to_image(fig, **kwargs):
        calls.append(kwargs)
        return b"\x89PNG\r\n\x1a\n" + bytes([len(calls)])

    monkeypatch.setattr(utils.pio, "to_image", fake_to_image)
    utils._PLOT_IMAGES = utils.PngCache()
    return calls


async def test_the_second_request_is_served_without_rendering(renders):
    path = "/plots/mainnet/accounts_per_day/image.png"
    first = await utils.return_plot_response(_figure(), _request(path), "Accounts")
    second = await utils.return_plot_response(_figure(), _request(path), "Accounts")

    assert len(renders) == 1
    assert first.body == second.body


async def test_the_response_tells_the_caller_it_may_be_cached(renders):
    response = await utils.return_plot_response(
        _figure(), _request("/plots/mainnet/accounts_per_day/image.png"), "Accounts"
    )
    assert response.media_type == "image/png"
    assert response.headers["cache-control"] == f"public, max-age={utils.PLOT_IMAGE_TTL}"


async def test_a_burst_of_crawlers_costs_one_render(renders):
    """The case that hurt: ten simultaneous misses for the same link."""
    path = "/plots/mainnet/accounts_per_day/image.png"
    responses = await asyncio.gather(
        *(utils.return_plot_response(_figure(), _request(path), "Accounts") for _ in range(10))
    )

    assert len(renders) == 1
    assert len({bytes(r.body) for r in responses}) == 1


async def test_different_charts_are_cached_separately(renders):
    """The key is the whole path, so per-account charts do not collide."""
    await utils.return_plot_response(
        _figure(), _request("/plots/mainnet/accounts_per_day/image.png"), "A"
    )
    await utils.return_plot_response(
        _figure(), _request("/plots/mainnet/daily_limits/image.png"), "B"
    )
    assert len(renders) == 2


async def test_the_render_still_asks_for_the_size_it_always_did(renders):
    await utils.return_plot_response(
        _figure(), _request("/plots/mainnet/accounts_per_day/image.png"), "Accounts"
    )
    assert renders[0] == {"format": "png", "width": 720}


# --- warming --------------------------------------------------------------
#
# A cached chart costs microseconds and a cold one costs about a second of
# headless Chromium, serialised behind one lock. The chart bot turned that from
# a rare cost into a common one: an inline picker asks Telegram to fetch all
# eighteen thumbnails at once, so one expiry means eighteen simultaneous cold
# renders queued behind each other.


def test_the_warm_interval_is_shorter_than_the_ttl():
    """Otherwise entries expire before anything refreshes them, and the timer
    changes nothing for the person who arrives in the gap."""
    assert utils.PLOT_WARM_INTERVAL_MINUTES * 60 < utils.PLOT_IMAGE_TTL


def test_the_chart_list_comes_from_the_routes_not_a_list():
    """So a chart added to the site is warmed without anyone remembering."""
    from fastapi import FastAPI

    from ccdexplorer.ccdexplorer_site.app.routers import account, statistics

    app = FastAPI()
    for module in (statistics, account):
        app.include_router(module.router)

    paths = utils.plot_image_paths(app)
    assert len(paths) >= 18
    assert all(p.startswith("/plots/mainnet/") and p.endswith("/image.png") for p in paths)


def test_the_per_account_chart_is_not_warmed():
    """plot_info carries ccd_balance_usd_value, which has no /plots route.
    Warming it would 404 once an hour, forever."""
    from fastapi import FastAPI

    from ccdexplorer.ccdexplorer_site.app.routers import account, statistics

    app = FastAPI()
    for module in (statistics, account):
        app.include_router(module.router)

    assert "ccd_balance_usd_value" in utils.plot_info
    assert not any("ccd_balance_usd_value" in p for p in utils.plot_image_paths(app))


def test_evicting_forces_the_next_request_to_redraw(renders):
    """Asking for a cached chart does not redraw it, so the warmer has to drop
    the entry first -- otherwise it would refresh nothing."""
    path = "/plots/mainnet/accounts_per_day/image.png"
    utils._PLOT_IMAGES.put(path, b"stale", 3600)
    assert utils._PLOT_IMAGES.get(path) is not None

    utils.evict_plot_image(path)
    assert utils._PLOT_IMAGES.get(path) is None


def test_evicting_something_absent_is_harmless():
    utils.evict_plot_image("/plots/mainnet/never_rendered/image.png")
