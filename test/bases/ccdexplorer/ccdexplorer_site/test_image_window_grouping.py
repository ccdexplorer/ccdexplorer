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
