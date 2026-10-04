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

from ccdexplorer.ccdexplorer_site.app.routers.charts import images
from ccdexplorer.charts import ChartState, Window
from ccdexplorer.charts.registry import BY_NAME
from fastapi import HTTPException

from ccdexplorer.ccdexplorer_site.app.routers.charts import sc_plt_transfers as mod

SAMPLE = [
    {
        "date": "2026-09-01",
        "tokens": {
            "EUROe": {
                "USD": {"transfer": 1, "burn": 0, "mint": 0, "total_supply": 100},
                "count_txs": 2,
            },
            "USDC": {
                "USD": {"transfer": 1, "burn": 0, "mint": 0, "total_supply": 200},
                "count_txs": 3,
            },
        },
    },
    {
        "date": "2026-09-02",
        "tokens": {
            "EUROe": {
                "USD": {"transfer": 1, "burn": 0, "mint": 0, "total_supply": 150},
                "count_txs": 2,
            },
            "USDC": {
                "USD": {"transfer": 1, "burn": 0, "mint": 0, "total_supply": 250},
                "count_txs": 3,
            },
        },
    },
]

WINDOWS = [30, 90, 180, 365]


def _state_for_days(days: int) -> ChartState:
    """The state an image request for roughly `days` would resolve to.

    The enum has no 180d, so the suite's 180 maps to the 90d window the
    retired route redirects to.
    """
    window = {30: Window.D30, 90: Window.D90, 180: Window.D90, 365: Window.Y1}[days]
    return ChartState.from_query(BY_NAME["plt_tvl"], {"window": window.value})


def _request():
    return SimpleNamespace(app=SimpleNamespace(env={}, api_url="http://api", httpx_client=None))


async def _overview_says_all_are_stablecoins(url, client):
    return SimpleNamespace(
        ok=True,
        return_value={
            "EUROe": {"_id": "EUROe", "stablecoin_tracks": "EUR"},
            "USDC": {"_id": "USDC", "stablecoin_tracks": "USD"},
        },
    )


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

    def fake_theme(request):
        return "light"

    async def fake_response(fig, request, title):
        return "rendered"

    monkeypatch.setattr(mod, "get_all_data_for_analysis_limited", fake_fetch)
    monkeypatch.setattr(mod, "get_url_from_api", _overview_says_all_are_stablecoins)
    monkeypatch.setattr(mod, "theme_from_query", fake_theme)
    monkeypatch.setattr(mod, "return_plot_response", fake_response)

    await mod.plt_tvl_image(_request(), "mainnet", _state_for_days(days))

    assert seen["analysis"] == "statistics_plt"
    # Against the state the request resolved to, not the raw `days`: the enum
    # has no 180d window, and a chart whose history is shorter than the window
    # is clamped to its own chain start rather than asking for data that
    # cannot exist.
    state = _state_for_days(days)
    assert dt.date.fromisoformat(seen["start"]) == state.start
    assert dt.date.fromisoformat(seen["end"]) == state.end


async def test_a_non_mainnet_net_is_refused():
    """Review Focus 5."""
    with pytest.raises(HTTPException) as exc:
        await mod.plt_tvl_image(_request(), "testnet", 30)

    assert exc.value.status_code == 404


def test_the_parameterised_image_route_exists():
    """One route per window became one route with parameters."""
    paths = {getattr(r, "path", "") for r in mod.router.routes}

    assert "/plots/{net}/plt_tvl/image.png" in paths


@pytest.mark.parametrize("days", WINDOWS)
def test_every_old_window_name_still_resolves(days):
    """The retired names are in Telegram's file cache and in shared links, so
    they redirect rather than 404."""
    paths = {getattr(r, "path", "") for r in mod.router.routes}

    assert "/plots/{net}/plt_tvl_{window}/image.png" in paths
    assert images.legacy_redirect_target("mainnet", "plt_tvl", f"{days}d") is not None


# --- review finding 3 ------------------------------------------------------


GAPPED = [
    {
        "date": "2026-09-01",
        "tokens": {
            "EUROe": {
                "USD": {"transfer": 0, "burn": 0, "mint": 0, "total_supply": 100},
                "count_txs": 0,
            }
        },
    },
    {
        "date": "2026-09-03",
        "tokens": {
            "EUROe": {
                "USD": {"transfer": 0, "burn": 0, "mint": 0, "total_supply": 120},
                "count_txs": 0,
            }
        },
    },
]


def test_a_missing_day_holds_the_level_instead_of_plotting_zero():
    """One skipped nightly run must not draw a cliff to zero and back.

    A daily bin with no rows takes NaN from .last(), and summing NaN across
    columns skips it to 0 -- so the chart claimed every stablecoin had been
    redeemed and reissued overnight.
    """
    fig = mod.build_plt_tvl_figure(GAPPED, theme="light", freq="D")

    assert 0 not in list(fig.data[0].y), f"plotted a zero: {list(fig.data[0].y)}"
    assert list(fig.data[0].y) == [100, 100, 120]


# --- review finding 4 ------------------------------------------------------


MIXED = [
    {
        "date": "2026-09-01",
        "tokens": {
            "EUROe": {
                "USD": {"transfer": 0, "burn": 0, "mint": 0, "total_supply": 100},
                "count_txs": 0,
            },
            "NOTACOIN": {
                "USD": {"transfer": 0, "burn": 0, "mint": 0, "total_supply": 900},
                "count_txs": 0,
            },
        },
    },
]


def test_only_stablecoins_count_towards_stablecoin_tvl():
    """The chart is titled "PLT stablecoin TVL" and named plt_tvl_*.

    Every PLT with a USD supply was being added, so a PLT that tracks nothing
    inflated a figure labelled as stablecoins.
    """
    fig = mod.build_plt_tvl_figure(MIXED, theme="light", freq="D", stablecoins={"EUROe"})

    assert list(fig.data[0].y) == [100], "counted a PLT that is not a stablecoin"


def test_no_known_stablecoins_draws_nothing_rather_than_everything():
    """If we cannot tell which are stablecoins we cannot honour the title.

    A blank chart is wrong-looking; a confident total that is 10x the truth is
    worse.
    """
    fig = mod.build_plt_tvl_figure(MIXED, theme="light", freq="D", stablecoins=set())

    assert not fig.data


async def test_the_route_asks_the_api_which_plts_are_stablecoins(monkeypatch):
    asked = {}

    async def fake_fetch(analysis, app, start, end):
        return MIXED

    async def fake_api(url, client):
        asked["url"] = url
        return SimpleNamespace(
            ok=True,
            return_value={
                "EUROe": {"_id": "EUROe", "stablecoin_tracks": "USD"},
                "NOTACOIN": {"_id": "NOTACOIN", "stablecoin_tracks": None},
            },
        )

    def fake_theme(request):
        return "light"

    captured = {}

    async def fake_response(fig, request, title):
        captured["y"] = list(fig.data[0].y) if fig.data else []
        return "rendered"

    monkeypatch.setattr(mod, "get_all_data_for_analysis_limited", fake_fetch)
    monkeypatch.setattr(mod, "get_url_from_api", fake_api)
    monkeypatch.setattr(mod, "theme_from_query", fake_theme)
    monkeypatch.setattr(mod, "return_plot_response", fake_response)

    request = SimpleNamespace(app=SimpleNamespace(env={}, api_url="http://api", httpx_client=None))
    await mod.plt_tvl_image(request, "mainnet", _state_for_days(30))

    assert "plts/overview" in asked["url"]
    assert captured["y"] == [100], "the non-stablecoin leaked into the total"
