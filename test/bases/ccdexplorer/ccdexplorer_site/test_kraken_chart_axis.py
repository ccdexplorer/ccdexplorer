"""The price scale belongs on the right, with the last price marked on it.

Every trading chart puts the price scale on the right, because the newest
candles are on the right and that is where the eye already is. Marking the last
price on the scale rather than inside the plot also settles a problem the old
layout could only paper over: the label sat on the canvas at the newest candle,
so the x range had to be padded by 11% to keep it off the data. Padding to make
room for a label meant every chart drew a stripe of empty time.

On the axis there is nothing to collide with, so the padding goes.
"""

from types import SimpleNamespace

import pytest

from ccdexplorer.ccdexplorer_site.app.routers import statistics

CANDLES = [
    {
        "at": f"2026-09-29T{h:02d}:00:00Z",
        "open": 0.0036 + h / 100000,
        "high": 0.0037 + h / 100000,
        "low": 0.0035 + h / 100000,
        "close": 0.00365 + h / 100000,
        "volume": 100.0 + h,
        "trades": 5,
    }
    for h in range(6)
]
LAST_CLOSE = CANDLES[-1]["close"]


async def _figure(monkeypatch, candles=CANDLES, change=1.5):
    captured = {}
    payload = {
        "candles": candles,
        "change_pct": change,
        "bars": len(candles),
        "bars_without_trades": 0,
    }

    async def fake_api(url, client):
        return SimpleNamespace(ok=True, return_value=payload)

    async def fake_theme(request):
        return "light"

    async def fake_response(fig, request, title):
        captured["fig"] = fig
        return "rendered"

    monkeypatch.setattr(statistics, "get_url_from_api", fake_api)
    monkeypatch.setattr(statistics, "get_theme_from_request", fake_theme)
    monkeypatch.setattr(statistics, "return_plot_response", fake_response)

    request = SimpleNamespace(app=SimpleNamespace(api_url="http://api", httpx_client=None))
    await statistics._ccd_kraken_plot(request, "mainnet", "1h")
    return captured["fig"]


def _price_label(fig):
    return next(a for a in fig.layout.annotations if "0.0" in (a.text or ""))


async def test_the_price_scale_is_on_the_right(monkeypatch):
    fig = await _figure(monkeypatch)

    assert fig.layout.yaxis.side == "right"


async def test_the_volume_scale_is_on_the_right_too(monkeypatch):
    """Two scales on opposite sides would read as two unrelated charts."""
    fig = await _figure(monkeypatch)

    assert fig.layout.yaxis2.side == "right"


async def test_the_last_price_is_marked_on_the_scale_not_in_the_plot(monkeypatch):
    fig = await _figure(monkeypatch)
    label = _price_label(fig)

    assert label.xref == "paper"
    assert label.x == 1
    # Anchored by its left edge at the plot's right edge, so it sits in the
    # margin over the scale rather than on top of the newest candles.
    assert label.xanchor == "left"


async def test_the_marker_sits_at_the_last_close(monkeypatch):
    fig = await _figure(monkeypatch)
    label = _price_label(fig)

    assert label.y == pytest.approx(LAST_CLOSE)
    assert f"{LAST_CLOSE:.8f}" in label.text


async def test_the_x_range_is_no_longer_padded_for_a_label(monkeypatch):
    """The whole reason the padding existed is gone."""
    fig = await _figure(monkeypatch)

    assert fig.layout.xaxis.range is None, "still reserving empty time for the label"
    assert fig.layout.xaxis2.range is None


def test_the_padding_constant_is_gone():
    assert not hasattr(statistics, "LAST_PRICE_LABEL_SHARE")


async def test_the_right_margin_has_room_for_the_scale_and_the_marker(monkeypatch):
    """An eight-decimal price is a wide label, and the template only leaves 24px."""
    fig = await _figure(monkeypatch)

    assert fig.layout.margin.r >= 80


async def test_the_dashed_line_still_runs_to_the_marker(monkeypatch):
    """The line is what ties the number on the scale to the candles."""
    fig = await _figure(monkeypatch)
    lines = [s for s in fig.layout.shapes if s.type == "line"]

    assert any(s.y0 == pytest.approx(LAST_CLOSE) for s in lines)


async def test_a_falling_market_still_gets_its_marker(monkeypatch):
    fig = await _figure(monkeypatch, change=-2.0)

    assert _price_label(fig).y == pytest.approx(LAST_CLOSE)
