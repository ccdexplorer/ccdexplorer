"""Transaction counts, as an image the bot can fetch.

The figure building comes out of the POST handler, which gates every bar on
post_data.trace_selection. The builder takes that selection as a parameter, so
the page keeps choosing and the image route asks for all five.

The category rollups are the part most likely to break quietly -- a renamed
source column just stops contributing, and the chart still draws. So the
characterisation test asserts the trace names, their order, and their values.
"""

import datetime as dt
from types import SimpleNamespace

import plotly.graph_objects as go
import pytest
from fastapi import HTTPException

from ccdexplorer.ccdexplorer_site.app.routers.charts import sc_transactions_count as mod

#: Captured from the existing handler before anything moved, in the order it
#: adds them.
EXPECTED_TRACES = ["Account", "Transfer", "Smart Contracts", "Staking", "Data"]

ALL_TRACES = ["account", "transfer", "smart ctr", "staking", "register data"]

SAMPLE = [
    {
        "date": "2026-09-01",
        "account_creation": 1,
        "credentials_updated": 2,
        "account_transfer": 10,
        "contract_update_issued": 4,
        "baker_configured": 3,
        "data_registered": 5,
    },
    {
        "date": "2026-09-02",
        "account_creation": 2,
        "credentials_updated": 0,
        "account_transfer": 20,
        "contract_update_issued": 1,
        "baker_configured": 1,
        "data_registered": 0,
    },
]

WINDOWS = [30, 90, 180, 365]


def _request():
    return SimpleNamespace(app=SimpleNamespace(env={}))


def test_the_builder_keeps_its_categories_and_their_order():
    fig = mod.build_transactions_count_figure(
        SAMPLE, theme="light", freq="D", traces=ALL_TRACES
    )

    assert [t.name for t in fig.data] == EXPECTED_TRACES


def test_a_category_sums_its_source_columns():
    """Account is account_creation + credential_keys_updated + credentials_updated."""
    fig = mod.build_transactions_count_figure(
        SAMPLE, theme="light", freq="D", traces=ALL_TRACES
    )
    account = next(t for t in fig.data if t.name == "Account")

    assert list(account.y) == [3, 2]


def test_a_missing_source_column_does_not_break_the_category():
    """add_if_present skips columns the window has no rows for."""
    data = [{"date": "2026-09-01", "account_creation": 1}]

    fig = mod.build_transactions_count_figure(
        data, theme="light", freq="D", traces=ALL_TRACES
    )
    account = next(t for t in fig.data if t.name == "Account")

    assert list(account.y) == [1]


def test_the_page_can_still_choose_fewer_traces():
    fig = mod.build_transactions_count_figure(
        SAMPLE, theme="light", freq="D", traces=["transfer"]
    )

    assert [t.name for t in fig.data] == ["Transfer"]


def test_an_empty_window_still_gives_a_figure():
    """Review Focus 4."""
    fig = mod.build_transactions_count_figure([], theme="light", freq="D", traces=ALL_TRACES)

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
        seen["traces"] = [t.name for t in fig.data]
        return "rendered"

    monkeypatch.setattr(mod, "get_all_data_for_analysis_limited", fake_fetch)
    monkeypatch.setattr(mod, "get_theme_from_request", fake_theme)
    monkeypatch.setattr(mod, "return_plot_response", fake_response)

    await mod.transactions_count_image(_request(), "mainnet", days)

    assert seen["analysis"] == "statistics_mongo_transactions"
    span = dt.date.fromisoformat(seen["end"]) - dt.date.fromisoformat(seen["start"])
    assert span.days == days
    assert seen["traces"] == EXPECTED_TRACES, "the image route must show every category"


async def test_a_non_mainnet_net_is_refused():
    """Review Focus 5."""
    with pytest.raises(HTTPException) as exc:
        await mod.transactions_count_image(_request(), "testnet", 30)

    assert exc.value.status_code == 404


@pytest.mark.parametrize("days", WINDOWS)
def test_every_window_has_a_route(days):
    paths = {getattr(r, "path", "") for r in mod.router.routes}

    assert f"/plots/{{net}}/transactions_count_{days}d/image.png" in paths


def test_the_title_still_says_what_a_bar_covers():
    """The page's title said "per Day"/"per Week"/"per Month" and must keep to it.

    Extracting the figure dropped it, which left `tooltip` unused in the
    handler -- the lint finding was the only thing that noticed.
    """
    fig = mod.build_transactions_count_figure(
        SAMPLE, theme="light", freq="D", traces=ALL_TRACES, per="Day"
    )

    assert "per Day" in fig.layout.title.text
