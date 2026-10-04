"""The two exchange charts are not bar charts.

CCD on exchanges is a stacked area -- ten balances making up a total, where
the band heights are the point. Exchange wallets is ten lines. Drawn as bars
both became ten overlapping series nobody could read, which is what the
generic default gave them.

The colours are the originals', in the original order, because the band a
reader recognises as Kraken should stay the colour Kraken was.
"""

import pytest

from ccdexplorer.charts import ChartState, Kind
from ccdexplorer.charts.registry import BY_NAME
from ccdexplorer.ccdexplorer_site.app.routers.charts.generic import build_figure

EXCHANGES = [
    "bitfinex", "bitglobal", "mexc", "ascendex", "kucoin",
    "coinex", "lcx", "gate_io", "bitmart", "kraken",
]


def _rows():
    return [{"date": "2026-10-01", **{e: i + 1 for i, e in enumerate(EXCHANGES)}}]


def _state(name):
    return ChartState.from_query(BY_NAME[name], {})


def test_ccd_on_exchanges_is_a_stacked_area():
    spec = BY_NAME["ccd_on_exchanges"]
    assert spec.kind is Kind.AREA
    fig = build_figure(spec, _rows(), _state("ccd_on_exchanges"), theme="light")
    assert all(t.stackgroup for t in fig.data), "not stacked"
    assert all(type(t).__name__ == "Scatter" for t in fig.data)


def test_exchange_wallets_is_lines():
    spec = BY_NAME["exchange_wallets"]
    assert spec.kind is Kind.LINE
    fig = build_figure(spec, _rows(), _state("exchange_wallets"), theme="light")
    assert all(type(t).__name__ == "Scatter" for t in fig.data)
    assert not any(t.stackgroup for t in fig.data), "lines should not stack"


def test_the_area_keeps_the_original_colour_cycle():
    """Seven colours over ten exchanges, as px.area cycled them."""
    spec = BY_NAME["ccd_on_exchanges"]
    expected = ["#DC5050", "#33C364", "#2485DF", "#7939BA", "#E87E90", "#F6DB9A", "#8BE7AA"]
    got = [s.colour for s in spec.series]
    assert got == [expected[i % len(expected)] for i in range(len(got))]


def test_the_wallet_lines_keep_their_original_colour_cycle():
    """Eight colours over ten exchanges."""
    spec = BY_NAME["exchange_wallets"]
    expected = [
        "#EE9B54", "#F7D30A", "#6E97F7", "#F36F85",
        "#AE7CF7", "#508A86", "#005B58", "#0E2625",
    ]
    got = [s.colour for s in spec.series]
    assert got == [expected[i % len(expected)] for i in range(len(got))]


def test_both_keep_the_exchanges_in_their_original_order():
    for name in ("ccd_on_exchanges", "exchange_wallets"):
        assert [s.key for s in BY_NAME[name].series] == EXCHANGES, name


def test_an_area_chart_still_marks_nothing_it_should_not():
    """Balances are levels: a short period does not understate them."""
    spec = BY_NAME["ccd_on_exchanges"]
    assert not any(s.short_period_understates for s in spec.series)
