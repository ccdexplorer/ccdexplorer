"""Total PLT stablecoin TVL in USD, as one line.

The page draws this data as a stacked bar per token, filtered to the stablecoins
tracking one currency. At card size in a chat that is unreadable, and the
question a reader has there is how much is locked in total -- so this is a
different figure over the same data, not the page's figure reused.

TVL is a level, not a flow: a period's value is its last, never the sum of its
days. The page's own agg_map already says so for these columns.
"""

import datetime as dt
from types import SimpleNamespace

import plotly.graph_objects as go
import pytest
from fastapi import HTTPException

from ccdexplorer.ccdexplorer_site.app.routers.charts import sc_plt_transfers as mod

SAMPLE = [
    {
        "date": "2026-09-01",
        "tokens": {
            "EUROe": {"USD": {"transfer": 1, "burn": 0, "mint": 0, "total_supply": 100},
                      "count_txs": 2},
            "USDC": {"USD": {"transfer": 1, "burn": 0, "mint": 0, "total_supply": 200},
                     "count_txs": 3},
        },
    },
    {
        "date": "2026-09-02",
        "tokens": {
            "EUROe": {"USD": {"transfer": 1, "burn": 0, "mint": 0, "total_supply": 150},
                      "count_txs": 2},
            "USDC": {"USD": {"transfer": 1, "burn": 0, "mint": 0, "total_supply": 250},
                     "count_txs": 3},
        },
    },
]

WINDOWS = [30, 90, 180, 365]


def _request():
    return SimpleNamespace(app=SimpleNamespace(env={}))


def test_it_is_one_line_not_a_bar_per_token():
    fig = mod.build_plt_tvl_figure(SAMPLE, theme="light", freq="D")

    assert len(fig.data) == 1


def test_the_line_is_every_stablecoin_added_together():
    fig = mod.build_plt_tvl_figure(SAMPLE, theme="light", freq="D")

    assert list(fig.data[0].y) == [300, 400]


def test_a_period_takes_its_last_value_not_the_sum():
    """TVL is a level. Summing two days of it would invent money."""
    fig = mod.build_plt_tvl_figure(SAMPLE, theme="light", freq="W-MON")

    assert list(fig.data[0].y) == [400], "summed the days instead of taking the level"


def test_only_total_supply_is_counted():
    """transfer, mint, burn and count_txs are flows and must not land in TVL."""
    fig = mod.build_plt_tvl_figure(SAMPLE, theme="light", freq="D")

    assert max(fig.data[0].y) == 400


def test_an_empty_window_still_gives_a_figure():
    """Review Focus 4."""
    fig = mod.build_plt_tvl_figure([], theme="light", freq="D")

    assert isinstance(fig, go.Figure)


@pytest.mark.parametrize("days", WINDOWS)
async def test_the_image_route_asks_for_its_own_window(days, monkeypatch):
    seen = {}

    async def fake_fetch(analysis, app, start, end):
        seen.update(analysis=analysis, start=start, end=end)
        return SAMPLE

    async def fake_theme(request):
        return "light"

    async def fake_response(fig, request, title):
        return "rendered"

    monkeypatch.setattr(mod, "get_all_data_for_analysis_limited", fake_fetch)
    monkeypatch.setattr(mod, "get_theme_from_request", fake_theme)
    monkeypatch.setattr(mod, "return_plot_response", fake_response)

    await mod.plt_tvl_image(_request(), "mainnet", days)

    assert seen["analysis"] == "statistics_plt"
    span = dt.date.fromisoformat(seen["end"]) - dt.date.fromisoformat(seen["start"])
    assert span.days == days


async def test_a_non_mainnet_net_is_refused():
    """Review Focus 5."""
    with pytest.raises(HTTPException) as exc:
        await mod.plt_tvl_image(_request(), "testnet", 30)

    assert exc.value.status_code == 404


@pytest.mark.parametrize("days", WINDOWS)
def test_every_window_has_a_route(days):
    paths = {getattr(r, "path", "") for r in mod.router.routes}

    assert f"/plots/{{net}}/plt_tvl_{days}d/image.png" in paths
