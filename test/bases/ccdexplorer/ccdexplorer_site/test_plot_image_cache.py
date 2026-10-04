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


def _request(path, query=""):
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": path,
            "raw_path": path.encode(),
            "query_string": query.encode(),
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
    assert renders[0] == {"format": "png", "width": 720, "height": None}


async def test_a_cover_is_asked_for_at_thumbnail_size(renders):
    """?cover=1 is how the index covers are generated: the same chart
    through the same route, without the chrome that is unreadable at this
    size. Going through the route rather than the generic builder is what
    lets the Kraken candles and PLT's TVL be covers at all."""
    await utils.return_plot_response(
        _figure(),
        _request("/plots/mainnet/accounts_per_day/image.png", query="cover=1"),
        "Accounts",
    )
    assert renders[0] == {
        "format": "png",
        "width": utils.COVER_WIDTH,
        "height": utils.COVER_HEIGHT,
    }


async def test_a_cover_is_a_separate_cache_entry(renders):
    """Without that it would hand back the full-size chart it had already
    drawn, and the generator would write that as the thumbnail."""
    path = "/plots/mainnet/accounts_per_day/image.png"
    await utils.return_plot_response(_figure(), _request(path), "Accounts")
    await utils.return_plot_response(_figure(), _request(path, query="cover=1"), "Accounts")

    assert len(renders) == 2


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
    from ccdexplorer.ccdexplorer_site.app.routers.charts import generated

    app = FastAPI()
    # generated too: the chart images moved there when they gained a
    # grouping and a range, and that is most of them now.
    for module in (statistics, account, generated):
        app.include_router(module.router)

    paths = utils.plot_image_paths(app)
    assert len(paths) >= 18
    assert all(p.startswith("/plots/mainnet/") and p.endswith("/image.png") for p in paths)


def test_the_per_account_chart_is_not_warmed():
    """plot_info carries ccd_balance_usd_value, which has no /plots route.
    Warming it would 404 once an hour, forever."""
    from fastapi import FastAPI

    from ccdexplorer.ccdexplorer_site.app.routers import account, statistics
    from ccdexplorer.ccdexplorer_site.app.routers.charts import generated

    app = FastAPI()
    # generated too: the chart images moved there when they gained a
    # grouping and a range, and that is most of them now.
    for module in (statistics, account, generated):
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


# --- themes ---------------------------------------------------------------
#
# The image routes are GETs, so get_theme_from_request found no body and every
# chart image rendered dark. Fine on the site, which is dark; wrong in a
# Telegram chat, which mostly is not.


def test_a_theme_query_param_is_honoured():
    from starlette.requests import Request

    async def theme_of(query):
        scope = {
            "type": "http",
            "method": "GET",
            "path": "/plots/mainnet/x/image.png",
            "query_string": query.encode(),
            "headers": [],
            "scheme": "https",
            "server": ("ccdexplorer.io", 443),
            "root_path": "",
        }
        request = Request(scope)
        request._body = b""
        return await utils.get_theme_from_request(request)

    import asyncio

    assert asyncio.run(theme_of("theme=light")) == "light"
    assert asyncio.run(theme_of("theme=dark")) == "dark"
    # Was dark. A GET with no body is an image route, and the only callers
    # that reach it without saying anything are link unfurlers and Telegram,
    # which land in chats that are mostly light. A reader on the dark site
    # still gets dark, from the bsTheme cookie -- which is what the cookie
    # was added for. Eight charts the bot offers were black rectangles
    # because this said dark and theme_from_query said light.
    assert asyncio.run(theme_of("")) == "light", "a request that says nothing gets light"
    # Anything else is refused rather than passed through, because the theme
    # becomes a cache key -- see below. Refused means it falls back like a
    # request that named no theme at all, which is now light.
    assert asyncio.run(theme_of("theme=chartreuse")) == "light"
    assert asyncio.run(theme_of("theme=../../etc")) == "light"


def test_the_two_themes_are_cached_separately():
    """Keyed on the path alone, the first render would win and everyone after
    it would get the wrong colours."""
    path = "/plots/mainnet/accounts_per_day/image.png"
    assert utils.plot_cache_key(path, "dark") != utils.plot_cache_key(path, "light")


def test_only_the_two_real_themes_can_become_cache_keys():
    """An unchecked theme would let anyone mint unlimited entries in an
    in-process store by varying a query string."""
    assert set(utils.PLOT_THEMES) == {"dark", "light"}


# --- what the warmer actually asks for -------------------------------------
#
# The charts gained path-shaped urls, and the bare /plots route answers a
# url still carrying ?grouping=... with a 308 to the path that means the
# same thing. The warmer was still asking in the query form, and its httpx
# client does not follow redirects: every spec-backed chart recorded a 308,
# counted as not warmed, and stayed cold for an hour at a time. Nothing
# failed -- the warmer prints the status and carries on -- so the only
# symptom was a chart that took a second to appear.


def _warmer_app():
    from fastapi import FastAPI

    from ccdexplorer.ccdexplorer_site.app.routers import statistics
    from ccdexplorer.ccdexplorer_site.app.routers.charts import generated

    app = FastAPI()
    for module in (statistics, generated):
        app.include_router(module.router)
    return app


def test_every_warm_target_renders_rather_than_redirecting(renders, monkeypatch):
    """The warmer's client does not follow redirects, so a target that
    answers 308 warms nothing. Asked of the real routes, because the bug
    was in the agreement between the target list and the routing."""
    from starlette.testclient import TestClient

    from ccdexplorer.ccdexplorer_site.app.routers.charts import generated

    async def no_rows(spec, app, state):
        return []

    monkeypatch.setattr(generated, "_fetch", no_rows)

    # The generated router alone: the charts statistics.py still draws fetch
    # from the api, which is not what this is about, and whether statistics
    # shadows a generated route is asked in test_generated_image_routes.py.
    from fastapi import FastAPI

    app = FastAPI()
    app.include_router(generated.router)
    targets = utils.plot_warm_targets(app)
    assert targets, "nothing to warm means the test proves nothing"

    with TestClient(app, follow_redirects=False) as client:
        not_warmed = [
            (path, params, response.status_code)
            for path, params in targets
            for response in [client.get(path, params={**params, "theme": "dark"})]
            if response.status_code != 200
        ]
    assert not_warmed == []


def test_a_configurable_chart_is_warmed_at_the_url_the_bot_asks_for():
    """Its buttons and its og:image both point at the path form, and that is
    a different cache key from the bare url -- warming one leaves the other
    cold."""
    targets = dict(utils.plot_warm_targets(_warmer_app()))
    paths = set(targets)

    assert any(p.startswith("/plots/mainnet/transaction_fees/weekly/") for p in paths), (
        "no canonical path-form target for a spec-backed chart"
    )
    assert "/plots/mainnet/transaction_fees/image.png" in paths, (
        "the stateless image is the og:image of the shareable plot page"
    )


def test_a_warm_target_carries_no_addressing_parameters():
    """A parameter that addresses the chart belongs in the path now. Left in
    the query it is the thing the bare route redirects on."""
    for path, params in utils.plot_warm_targets(_warmer_app()):
        assert not (set(params) & {"grouping", "window", "from", "to"}), path


# --- the response depends on the cookie, so caches have to be told --------
#
# /plots/<name>/image.png with no theme parameter answers light or dark
# depending on bsTheme, under `Cache-Control: public`. Without Vary: Cookie
# any shared cache may hand one reader's theme to the next -- and the
# browser did exactly that to a single reader, serving the dark tiles it had
# cached earlier to a page that had just switched to light.


async def test_a_themed_image_says_it_varies_by_cookie(renders):
    response = await utils.return_plot_response(
        _figure(), _request("/plots/mainnet/accounts_growth/image.png"), "Accounts"
    )
    assert "cookie" in response.headers.get("vary", "").lower()


async def test_it_is_still_publicly_cacheable(renders):
    """Vary is not a reason to stop caching: the images are expensive and
    most requests carry no cookie at all."""
    response = await utils.return_plot_response(
        _figure(), _request("/plots/mainnet/accounts_growth/image.png"), "Accounts"
    )
    assert response.headers["cache-control"].startswith("public")


# --- the shareable plot page and its own figure have to agree -------------
#
# /plots/<net>/<name> answers a small page with the chart drawn into it.
# The page hardcoded data-bs-theme="dark" while the figure followed the
# cookie, which used to be dark too, so they agreed by accident. Once a
# request that says nothing started getting light -- which is what Telegram
# and its in-app browser send -- the caption link opened a dark page with a
# light chart in it.


def _page_request(cookie=None):
    headers = [(b"cookie", cookie.encode())] if cookie else []
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/plots/mainnet/ccd_kraken_4h",
        "raw_path": b"/plots/mainnet/ccd_kraken_4h",
        "query_string": b"",
        "headers": headers,
        "scheme": "https",
        "server": ("ccdexplorer.io", 443),
        "root_path": "",
    }
    request = Request(scope)
    request._body = b""
    return request


def _plot_page(request):
    from pathlib import Path

    from fastapi.templating import Jinja2Templates

    project = Path(__file__).resolve().parents[4] / "projects" / "ccdexplorer_site"
    request.scope["app"] = type(
        "app", (), {"templates": Jinja2Templates(directory=project / "templates"), "env": {}}
    )()
    return request


async def test_the_plot_page_is_drawn_in_the_theme_its_chart_is():
    for cookie, theme in [(None, "light"), ("bsTheme=dark", "dark"), ("bsTheme=light", "light")]:
        request = _plot_page(_page_request(cookie))
        response = await utils.return_plot_response(_figure(), request, "CCD on Kraken")
        body = response.body.decode()
        assert f'data-bs-theme="{theme}"' in body, (cookie, body[:200])


# --- where "View chart on CCDExplorer.io" goes ----------------------------
#
# plot_info carries a page_url per chart, hardcoded to a /statistics tab and
# to mainnet. Those tabs are not where a chart lives any more: the link on
# the Kraken plot page sent the reader to /mainnet/statistics/exchanges
# rather than to anything showing that chart.


async def _page_link(name, net="mainnet"):
    from pathlib import Path

    from fastapi.templating import Jinja2Templates

    path = f"/plots/{net}/{name}"
    scope = {
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
    request = Request(scope)
    request._body = b""
    project = Path(__file__).resolve().parents[4] / "projects" / "ccdexplorer_site"
    request.scope["app"] = type(
        "app", (), {"templates": Jinja2Templates(directory=project / "templates"), "env": {}}
    )()
    response = await utils.return_plot_response(_figure(), request, name)
    import re

    found = re.search(r'href="([^"]*)">View chart on CCDExplorer\.io', response.body.decode())
    return found.group(1) if found else None


async def test_a_chart_with_a_page_links_to_its_page():
    assert await _page_link("transaction_fees") == "/mainnet/charts/transaction-fees"


async def test_a_chart_with_no_page_links_to_its_category():
    """The Kraken candles have no configurable page, so the nearest thing
    that shows them is the category they sit in."""
    assert await _page_link("ccd_kraken_4h") == "/mainnet/charts/category/exchanges"


async def test_no_plot_page_still_points_at_statistics():
    for name in ("transaction_fees", "ccd_kraken_4h"):
        assert "/statistics" not in (await _page_link(name) or "")


async def test_the_link_follows_the_net_it_was_asked_on():
    """plot_info hardcoded mainnet into every one of them."""
    assert (await _page_link("transaction_fees", "testnet")).startswith("/testnet/")


async def test_a_chart_with_no_spec_keeps_whatever_it_had():
    """Per-account charts are not in the registry, and their link is not
    this change's business."""
    link = await _page_link("ccd_balance_usd_value")
    assert link is None or "/charts/" not in link


async def test_not_one_plot_page_links_to_statistics():
    """Asked of every chart rather than a sample.

    The previous version of this derived the link from the registry and
    fell back to plot_info for anything with no spec -- and
    staking_validator_staked_amounts has none, so it kept the
    /mainnet/statistics/validators link it always had. Spot-checking two
    charts missed it.
    """
    offenders = []
    for name in utils.plot_info:
        if name == "ccd_balance_usd_value":
            # Per-account, and its link is to the account, not to a chart.
            continue
        link = await _page_link(name)
        if link is None or "/statistics" in link or "/charts/" not in link:
            offenders.append((name, link))
    assert offenders == [], offenders


async def test_a_chart_with_no_spec_still_lands_somewhere_showing_it():
    assert await _page_link("staking_validator_staked_amounts") == (
        "/mainnet/charts/category/staking"
    )
