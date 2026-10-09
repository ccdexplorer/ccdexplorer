"""The price scale belongs on the right, with the last price marked on it.

Every trading chart puts the price scale on the right, because the newest
candles are on the right and that is where the eye already is. Marking the last
price on the scale rather than inside the plot also settles a problem the old
layout could only paper over: the label sat on the canvas at the newest candle,
so the x range had to be padded by 11% to keep it off the data. Padding to make
room for a label meant every chart drew a stripe of empty time.

On the axis there is nothing to collide with, so the padding goes.
"""

import pytest

from ccdexplorer.ccdexplorer_site.app.routers.charts.providers import build_kraken_figure

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


def _figure(candles=CANDLES, change=1.5):
    """The figure itself, with nothing faked.

    This used to monkeypatch the api call, the theme lookup and the response
    helper to get at a figure buried inside a route handler. The builder is a
    function of its payload now, so none of that is needed.
    """
    return build_kraken_figure(
        {
            "candles": candles,
            "change_pct": change,
            "bars": len(candles),
            "bars_without_trades": 0,
        },
        "1h",
        "light",
    )


def _price_label(fig):
    return next(a for a in fig.layout.annotations if "0.0" in (a.text or ""))


def test_the_price_scale_is_on_the_right():
    fig = _figure()

    assert fig.layout.yaxis.side == "right"


def test_the_volume_scale_is_on_the_right_too():
    """Two scales on opposite sides would read as two unrelated charts."""
    fig = _figure()

    assert fig.layout.yaxis2.side == "right"


def test_the_last_price_is_marked_on_the_scale_not_in_the_plot():
    fig = _figure()
    label = _price_label(fig)

    assert label.xref == "paper"
    assert label.x == 1
    # Anchored by its left edge at the plot's right edge, so it sits in the
    # margin over the scale rather than on top of the newest candles.
    assert label.xanchor == "left"


def test_the_marker_sits_at_the_last_close():
    fig = _figure()
    label = _price_label(fig)

    assert label.y == pytest.approx(LAST_CLOSE)
    assert f"{LAST_CLOSE:.8f}" in label.text


def test_the_x_range_is_no_longer_padded_for_a_label():
    """The whole reason the padding existed is gone."""
    fig = _figure()

    assert fig.layout.xaxis.range is None, "still reserving empty time for the label"
    assert fig.layout.xaxis2.range is None


def test_the_padding_constant_is_gone():
    """It lived in statistics.py, which no longer draws this chart at all."""
    from ccdexplorer.ccdexplorer_site.app.routers import statistics
    from ccdexplorer.ccdexplorer_site.app.routers.charts import providers

    assert not hasattr(statistics, "LAST_PRICE_LABEL_SHARE")
    assert not hasattr(providers, "LAST_PRICE_LABEL_SHARE")


def test_the_right_margin_has_room_for_the_scale_and_the_marker():
    """An eight-decimal price is a wide label, and the template only leaves 24px."""
    fig = _figure()

    assert fig.layout.margin.r >= 80


def test_the_dashed_line_still_runs_to_the_marker():
    """The line is what ties the number on the scale to the candles."""
    fig = _figure()
    lines = [s for s in fig.layout.shapes if s.type == "line"]

    assert any(s.y0 == pytest.approx(LAST_CLOSE) for s in lines)


def test_a_falling_market_still_gets_its_marker():
    fig = _figure(change=-2.0)

    assert _price_label(fig).y == pytest.approx(LAST_CLOSE)
