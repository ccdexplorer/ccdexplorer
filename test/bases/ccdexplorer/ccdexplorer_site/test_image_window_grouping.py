"""How many bars a window draws.

A year of daily bars is slow to build and unreadable at card size, and the TVL
and agent-registry charts were slow enough to notice. Thirty days stays daily;
anything longer groups by week.

One file for all three families, because the rule is one rule -- if they ever
disagree about it, that is the bug this would catch.
"""

from types import SimpleNamespace

import plotly.graph_objects as go
import pytest

from ccdexplorer.ccdexplorer_site.app.routers.charts import sc_agent_registries
from ccdexplorer.ccdexplorer_site.app.routers.charts import sc_plt_transfers
from ccdexplorer.ccdexplorer_site.app.routers.charts import sc_transactions_count

WINDOWS = [(30, "D"), (90, "W-MON"), (180, "W-MON"), (365, "W-MON")]

FAMILIES = [
    (sc_agent_registries, "build_agent_registries_figure", "agent_registries_image"),
    (sc_plt_transfers, "build_plt_tvl_figure", "plt_tvl_image"),
    (sc_transactions_count, "build_transactions_count_figure", "transactions_count_image"),
]

IDS = [m.__name__.rsplit(".", 1)[-1] for m, _b, _r in FAMILIES]


def _request():
    return SimpleNamespace(
        app=SimpleNamespace(env={}, api_url="http://api", httpx_client=None)
    )


async def _overview(url, client):
    return SimpleNamespace(
        ok=True, return_value={"E": {"_id": "E", "stablecoin_tracks": "EUR"}}
    )


def _stub(mod, builder_name, monkeypatch, seen):
    async def fake_fetch(analysis, app, start, end):
        return [{"date": "2026-09-01"}]

    def fake_builder(all_data, **kw):
        seen["freq"] = kw["freq"]
        return go.Figure()

    async def fake_theme(request):
        return "light"

    async def fake_response(fig, request, title):
        return "rendered"

    monkeypatch.setattr(mod, "get_all_data_for_analysis_limited", fake_fetch)
    monkeypatch.setattr(mod, builder_name, fake_builder)
    monkeypatch.setattr(mod, "get_theme_from_request", fake_theme)
    monkeypatch.setattr(mod, "return_plot_response", fake_response)
    if hasattr(mod, "get_url_from_api"):
        monkeypatch.setattr(mod, "get_url_from_api", _overview)


@pytest.mark.parametrize(("days", "expected"), WINDOWS)
@pytest.mark.parametrize(("mod", "builder", "route"), FAMILIES, ids=IDS)
async def test_the_window_picks_its_grouping(mod, builder, route, days, expected, monkeypatch):
    seen: dict[str, str] = {}
    _stub(mod, builder, monkeypatch, seen)

    await getattr(mod, route)(_request(), "mainnet", days)

    assert seen["freq"] == expected


@pytest.mark.parametrize(("mod", "builder", "route"), FAMILIES, ids=IDS)
async def test_only_the_shortest_window_is_daily(mod, builder, route, monkeypatch):
    """Pinned as a rule rather than four separate facts: a fifth window added
    later should group weekly by default, not silently draw a year of days."""
    daily = []
    for days, _expected in WINDOWS:
        seen: dict[str, str] = {}
        _stub(mod, builder, monkeypatch, seen)
        await getattr(mod, route)(_request(), "mainnet", days)
        if seen["freq"] == "D":
            daily.append(days)

    assert daily == [30]


# --- and the title has to say which it is -----------------------------------
#
# The grouping changed but the label did not: a 365-day chart drew weekly bars
# under a title that said "per Day". A chart that misreports its own bar width
# is worse than one that is merely dense, because nothing looks wrong.


def _title_of(mod, route, days, monkeypatch):
    captured: dict[str, str] = {}

    async def fake_fetch(analysis, app, start, end):
        return [{"date": "2026-09-01", "agents_registered": 1, "registries": ["<1,0>"],
                 "account_creation": 1,
                 "tokens": {"E": {"USD": {"transfer": 0, "burn": 0, "mint": 0,
                                          "total_supply": 1}, "count_txs": 0}}}]

    async def fake_theme(request):
        return "light"

    async def fake_response(fig, request, title):
        captured["title"] = fig.layout.title.text or ""
        return "rendered"

    monkeypatch.setattr(mod, "get_all_data_for_analysis_limited", fake_fetch)
    monkeypatch.setattr(mod, "get_theme_from_request", fake_theme)
    monkeypatch.setattr(mod, "return_plot_response", fake_response)
    if hasattr(mod, "get_url_from_api"):
        monkeypatch.setattr(mod, "get_url_from_api", _overview)

    return captured, getattr(mod, route)


async def test_the_transactions_title_names_the_real_bar_width(monkeypatch):
    captured, route = _title_of(sc_transactions_count, "transactions_count_image", 30, monkeypatch)
    await route(_request(), "mainnet", 30)
    assert "per Day" in captured["title"]

    await route(_request(), "mainnet", 365)
    assert "per Week" in captured["title"]
    assert "per Day" not in captured["title"], "weekly bars labelled as daily"


@pytest.mark.parametrize(("mod", "route"), [
    (sc_agent_registries, "agent_registries_image"),
    (sc_plt_transfers, "plt_tvl_image"),
])
async def test_the_other_two_say_how_they_are_grouped(mod, route, monkeypatch):
    """They carried no period label at all, so weekly bars went unannounced."""
    captured, fn = _title_of(mod, route, 30, monkeypatch)

    await fn(_request(), "mainnet", 30)
    assert "day" in captured["title"].lower()

    await fn(_request(), "mainnet", 180)
    assert "week" in captured["title"].lower()
