"""Agent registries, as an image the bot can fetch.

The figure building is extracted from the POST ajax handler so the website and
the image route draw the same chart from the same code. The characterisation
tests below are what make that move safe: they pin what the handler produced
before anything was touched.
"""

import datetime as dt
from types import SimpleNamespace

import plotly.graph_objects as go
import pytest

from ccdexplorer.ccdexplorer_site.app.routers.charts import images
from ccdexplorer.charts import ChartState, Window
from ccdexplorer.charts.registry import BY_NAME
from fastapi import HTTPException

from ccdexplorer.ccdexplorer_site.app.routers.charts import sc_agent_registries as mod

SAMPLE = [
    {"date": "2026-09-01", "agents_registered": 3, "registries": ["<9390,0>"]},
    {"date": "2026-09-02", "agents_registered": 5, "registries": ["<9390,0>"]},
]

WINDOWS = [30, 90, 180, 365]


def _state_for_days(days: int) -> ChartState:
    """The state an image request for roughly `days` would resolve to.

    The enum has no 180d, so the suite's 180 maps to the 90d window the
    retired route redirects to.
    """
    window = {30: Window.D30, 90: Window.D90, 180: Window.D90, 365: Window.Y1}[days]
    return ChartState.from_query(BY_NAME["agent_registries"], {"window": window.value})


def _request():
    return SimpleNamespace(app=SimpleNamespace(env={}, templates=None))


# --- the extracted builder -------------------------------------------------


def test_the_builder_draws_a_bar_per_period():
    fig = mod.build_agent_registries_figure(SAMPLE, theme="light", freq="D")

    assert isinstance(fig, go.Figure)
    assert len(fig.data) == 1
    assert list(fig.data[0].y) == [3, 5]


def test_the_title_names_a_single_registry_contract():
    fig = mod.build_agent_registries_figure(SAMPLE, theme="light", freq="D")

    assert "9390" in fig.layout.title.text


def test_several_registries_are_counted_not_named():
    data = [
        {"date": "2026-09-01", "agents_registered": 1, "registries": ["<1,0>", "<2,0>"]},
    ]

    fig = mod.build_agent_registries_figure(data, theme="light", freq="D")

    assert "2 CIS-8004 contracts" in fig.layout.title.text


def test_an_empty_window_still_gives_a_figure():
    """Review Focus 4: a quiet 30 days must not raise."""
    fig = mod.build_agent_registries_figure([], theme="light", freq="D")

    assert isinstance(fig, go.Figure)


def test_the_builder_writes_no_csv(tmp_path, monkeypatch):
    """The page's download button writes one; an image route has nowhere to."""
    monkeypatch.chdir(tmp_path)

    mod.build_agent_registries_figure(SAMPLE, theme="light", freq="D")

    assert not list(tmp_path.iterdir())


# --- the image routes ------------------------------------------------------


@pytest.mark.parametrize("days", WINDOWS)
async def test_the_image_route_asks_for_its_own_window(days, monkeypatch):
    seen = {}

    async def fake_fetch(analysis, app, start, end):
        seen.update(analysis=analysis, start=start, end=end)
        return SAMPLE

    async def fake_theme(request):
        return "light"

    captured = {}

    async def fake_response(fig, request, title):
        captured["fig"], captured["title"] = fig, title
        return "rendered"

    monkeypatch.setattr(mod, "get_all_data_for_analysis_limited", fake_fetch)
    monkeypatch.setattr(mod, "get_theme_from_request", fake_theme)
    monkeypatch.setattr(mod, "return_plot_response", fake_response)

    await mod.agent_registries_image(_request(), "mainnet", _state_for_days(days))

    assert seen["analysis"] == "statistics_agent_registry"
    # Against the state the request resolved to, not the raw `days`: the enum
    # has no 180d window, and a chart whose history is shorter than the window
    # is clamped to its own chain start rather than asking for data that
    # cannot exist.
    state = _state_for_days(days)
    assert dt.date.fromisoformat(seen["start"]) == state.start
    assert dt.date.fromisoformat(seen["end"]) == state.end
    assert captured["fig"] is not None


async def test_a_non_mainnet_net_is_refused_not_rendered_as_html():
    """Review Focus 5: Telegram fetching an HTML page shows a broken image."""
    with pytest.raises(HTTPException) as exc:
        await mod.agent_registries_image(_request(), "testnet", 30)

    assert exc.value.status_code == 404


def test_the_parameterised_image_route_is_served_by_the_generated_router():
    """One route per window became one route with parameters -- and then
    those routes moved to charts/generated.py, which registers the same
    shape for every spec. This module kept registering them too, and because
    its router is included first it won every request: the chart rendered
    through the old handler, whose theme falls back to dark, so it arrived
    in Telegram as a black rectangle. Only the legacy redirects stay here.
    """
    from ccdexplorer.ccdexplorer_site.app.routers.charts import generated

    mine = {getattr(r, "path", "") for r in mod.router.routes}
    theirs = {getattr(r, "path", "") for r in generated.router.routes}

    assert "/plots/{net}/agent_registries/image.png" not in mine
    assert "/plots/{net}/agent_registries/image.png" in theirs
    assert "/plots/{net}/agent_registries_{window}/image.png" in mine, (
        "the legacy redirect went too"
    )


@pytest.mark.parametrize("days", WINDOWS)
def test_every_old_window_name_still_resolves(days):
    """The retired names are in Telegram's file cache and in shared links, so
    they redirect rather than 404."""
    paths = {getattr(r, "path", "") for r in mod.router.routes}

    assert "/plots/{net}/agent_registries_{window}/image.png" in paths
    assert images.legacy_redirect_target("mainnet", "agent_registries", f"{days}d") is not None
