"""An intraday candle chart cannot be cached for an hour.

/plots/<net>/<name>/image.png caches its render for PLOT_IMAGE_TTL, which is an
hour. That is right for a chart of the last year and wrong for a chart of the
last two hours: ccd_kraken_1m draws 120 one-minute candles, so an hour-old image
has missed half of what it claims to show.

It only became visible once the chart bot started busting Telegram's cache: up to
then the image never reached anyone twice anyway. The two caches have to agree,
or the bot refetches an image the site has not redrawn.
"""

import plotly.graph_objects as go
import pytest
from starlette.requests import Request

from ccdexplorer.ccdexplorer_site.app import og_cards, utils


def _request(path, query=b""):
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


@pytest.fixture
def renders(monkeypatch):
    calls = []

    def fake_to_image(fig, **kwargs):
        calls.append(kwargs)
        return b"\x89PNG\r\n\x1a\n" + bytes([len(calls)])

    monkeypatch.setattr(utils.pio, "to_image", fake_to_image)
    utils._PLOT_IMAGES = utils.PngCache()
    return calls


@pytest.mark.parametrize(
    ("name", "ttl"),
    [
        ("ccd_kraken_1m", 60),
        ("ccd_kraken_5m", 300),
        ("ccd_kraken_15m", 900),
        ("ccd_kraken_30m", 1800),
    ],
)
def test_an_intraday_candle_chart_is_cached_for_its_own_interval(name, ttl):
    assert utils.plot_image_ttl(f"/plots/mainnet/{name}/image.png") == ttl


@pytest.mark.parametrize(
    "name", ["ccd_kraken_1h", "ccd_kraken_4h", "ccd_kraken_1d", "staking_validator_count"]
)
def test_everything_else_keeps_the_default_hour(name):
    assert utils.plot_image_ttl(f"/plots/mainnet/{name}/image.png") == utils.PLOT_IMAGE_TTL


def test_an_unknown_path_gets_the_default():
    assert utils.plot_image_ttl("/plots/mainnet/whatever_else/image.png") == utils.PLOT_IMAGE_TTL
    assert utils.plot_image_ttl("/not/a/plot/path") == utils.PLOT_IMAGE_TTL


async def test_the_response_advertises_the_chart_s_own_max_age(renders):
    """So Telegram and any proxy are told the truth about how long it is good."""
    response = await utils.return_plot_response(
        go.Figure(), _request("/plots/mainnet/ccd_kraken_1m/image.png"), "t"
    )

    assert response.headers["cache-control"] == "public, max-age=60"


async def test_a_long_window_chart_still_advertises_the_hour(renders):
    response = await utils.return_plot_response(
        go.Figure(), _request("/plots/mainnet/ccd_kraken_1d/image.png"), "t"
    )

    assert response.headers["cache-control"] == f"public, max-age={utils.PLOT_IMAGE_TTL}"


async def test_the_intraday_image_is_redrawn_after_its_minute(renders, monkeypatch):
    """The point of the whole change: it expires in a minute, not an hour."""
    clock = [1000.0]
    monkeypatch.setattr(og_cards, "monotonic", lambda: clock[0])
    request = _request("/plots/mainnet/ccd_kraken_1m/image.png")

    await utils.return_plot_response(go.Figure(), request, "t")
    assert len(renders) == 1

    clock[0] += 30  # still inside the minute
    await utils.return_plot_response(go.Figure(), request, "t")
    assert len(renders) == 1, "redrawn while still fresh"

    clock[0] += 31  # past it
    await utils.return_plot_response(go.Figure(), request, "t")
    assert len(renders) == 2, "not redrawn after its ttl"


async def test_an_hourly_chart_is_not_redrawn_after_a_minute(renders, monkeypatch):
    """The default must not have been shortened for everything."""
    clock = [1000.0]
    monkeypatch.setattr(og_cards, "monotonic", lambda: clock[0])
    request = _request("/plots/mainnet/ccd_kraken_1d/image.png")

    await utils.return_plot_response(go.Figure(), request, "t")
    clock[0] += 120
    await utils.return_plot_response(go.Figure(), request, "t")

    assert len(renders) == 1


async def test_a_busting_parameter_does_not_cost_an_extra_render(renders):
    """The bot's &t= must not multiply renders -- the cache key is the path."""
    a = _request("/plots/mainnet/ccd_kraken_1d/image.png", query=b"theme=light&t=1")
    b = _request("/plots/mainnet/ccd_kraken_1d/image.png", query=b"theme=light&t=2")

    await utils.return_plot_response(go.Figure(), a, "t")
    await utils.return_plot_response(go.Figure(), b, "t")

    assert len(renders) == 1
