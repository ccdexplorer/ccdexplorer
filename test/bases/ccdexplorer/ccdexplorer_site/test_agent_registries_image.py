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
from fastapi import HTTPException

from ccdexplorer.ccdexplorer_site.app.routers.charts import sc_agent_registries as mod

SAMPLE = [
    {"date": "2026-09-01", "agents_registered": 3, "registries": ["<9390,0>"]},
    {"date": "2026-09-02", "agents_registered": 5, "registries": ["<9390,0>"]},
]

WINDOWS = [30, 90, 180, 365]


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

    await mod.agent_registries_image(_request(), "mainnet", days)

    assert seen["analysis"] == "statistics_agent_registry"
    span = dt.date.fromisoformat(seen["end"]) - dt.date.fromisoformat(seen["start"])
    assert span.days == days
    assert captured["fig"] is not None


async def test_a_non_mainnet_net_is_refused_not_rendered_as_html():
    """Review Focus 5: Telegram fetching an HTML page shows a broken image."""
    with pytest.raises(HTTPException) as exc:
        await mod.agent_registries_image(_request(), "testnet", 30)

    assert exc.value.status_code == 404


@pytest.mark.parametrize("days", WINDOWS)
def test_every_window_has_a_route(days):
    paths = {getattr(r, "path", "") for r in mod.router.routes}

    assert f"/plots/{{net}}/agent_registries_{days}d/image.png" in paths
