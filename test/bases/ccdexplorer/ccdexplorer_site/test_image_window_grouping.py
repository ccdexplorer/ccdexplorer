"""What an image route draws, and whether its title admits it.

The window used to pick the grouping -- thirty days daily, longer weekly --
because there was one route per window and nothing else to go on. Window and
grouping are independent now and both come off the query string, so the rule
under test changed: the grouping the caller asked for is the frequency the
builder gets.

What has not changed is the title. The grouping once moved while the label did
not, and a 365-day chart drew weekly bars under a title saying "per Day". A
chart that misreports its own bar width is worse than a dense one, because
nothing about it looks wrong.

One file for all three families, because the rule is one rule -- if they ever
disagree about it, that is the bug this would catch.
"""

from types import SimpleNamespace

import plotly.graph_objects as go
import pytest

from ccdexplorer.charts import ChartState, Grouping, Window
from ccdexplorer.charts.registry import BY_NAME
from ccdexplorer.ccdexplorer_site.app.routers.charts import sc_agent_registries
from ccdexplorer.ccdexplorer_site.app.routers.charts import sc_plt_transfers
from ccdexplorer.ccdexplorer_site.app.routers.charts import sc_transactions_count

#: Each grouping and the pandas frequency it must reach the builder as.
GROUPINGS = [
    (Grouping.DAILY, "D"),
    (Grouping.WEEKLY, "W-MON"),
    (Grouping.MONTHLY, "MS"),
]

FAMILIES = [
    (
        sc_agent_registries,
        "build_agent_registries_figure",
        "agent_registries_image",
        "agent_registries",
    ),
    (sc_plt_transfers, "build_plt_tvl_figure", "plt_tvl_image", "plt_tvl"),
    (
        sc_transactions_count,
        "build_transactions_count_figure",
        "transactions_count_image",
        "transactions_count",
    ),
]

IDS = [spec for _m, _b, _r, spec in FAMILIES]


def _request():
    return SimpleNamespace(app=SimpleNamespace(env={}, api_url="http://api", httpx_client=None))


def _state(spec_name, grouping=Grouping.WEEKLY, window=Window.Y1):
    state = ChartState.from_query(
        BY_NAME[spec_name], {"grouping": grouping.value, "window": window.value}
    )
    # Forced past the automatic resolution. A chart of closing values -- PLT
    # TVL among them -- takes its grouping from the span now, so from_query
    # would hand back the same one for all three cases; what is under test
    # here is that whatever grouping the state carries reaches the builder.
    return state.model_copy(update={"grouping": grouping})


async def _overview(url, client):
    return SimpleNamespace(ok=True, return_value={"E": {"_id": "E", "stablecoin_tracks": "EUR"}})


def _stub(mod, builder_name, monkeypatch, seen):
    async def fake_fetch(analysis, app, start, end):
        seen["start"] = start
        seen["end"] = end
        return [{"date": "2026-09-01"}]

    def fake_builder(all_data, **kw):
        seen["freq"] = kw["freq"]
        return go.Figure()

    # Two resolvers in play: the POST handlers read the body with the async
    # get_theme_from_request, while an image route is a GET and uses the
    # sync theme_from_query, which falls back to light rather than dark.
    async def fake_theme(request):
        return "light"

    def fake_theme_sync(request):
        return "light"

    async def fake_response(fig, request, title):
        return "rendered"

    monkeypatch.setattr(mod, "get_all_data_for_analysis_limited", fake_fetch)
    monkeypatch.setattr(mod, builder_name, fake_builder)
    for _name, _fake in (
        ("get_theme_from_request", fake_theme),
        ("theme_from_query", fake_theme_sync),
    ):
        if hasattr(mod, _name):
            monkeypatch.setattr(mod, _name, _fake)
    monkeypatch.setattr(mod, "return_plot_response", fake_response)
    if hasattr(mod, "get_url_from_api"):
        monkeypatch.setattr(mod, "get_url_from_api", _overview)


@pytest.mark.parametrize(("grouping", "expected"), GROUPINGS)
@pytest.mark.parametrize(("mod", "builder", "route", "spec"), FAMILIES, ids=IDS)
async def test_the_requested_grouping_reaches_the_builder(
    mod, builder, route, spec, grouping, expected, monkeypatch
):
    seen: dict[str, str] = {}
    _stub(mod, builder, monkeypatch, seen)

    await getattr(mod, route)(_request(), "mainnet", _state(spec, grouping))

    assert seen["freq"] == expected


@pytest.mark.parametrize(("mod", "builder", "route", "spec"), FAMILIES, ids=IDS)
async def test_an_image_with_no_parameters_is_weekly(mod, builder, route, spec, monkeypatch):
    """Every chart opens weekly, including the one the bot fetches."""
    seen: dict[str, str] = {}
    _stub(mod, builder, monkeypatch, seen)

    await getattr(mod, route)(_request(), "mainnet", ChartState.from_query(BY_NAME[spec], {}))

    assert seen["freq"] == "W-MON"


@pytest.mark.parametrize(("mod", "builder", "route", "spec"), FAMILIES, ids=IDS)
async def test_the_window_sets_the_range_that_is_fetched(mod, builder, route, spec, monkeypatch):
    """A shorter window must actually ask for less, not just draw less."""
    ranges = {}
    for window in (Window.D30, Window.Y1):
        seen: dict[str, str] = {}
        _stub(mod, builder, monkeypatch, seen)
        await getattr(mod, route)(_request(), "mainnet", _state(spec, window=window))
        ranges[window] = seen["start"]

    assert ranges[Window.D30] > ranges[Window.Y1], "30d should start later than 1y"


# --- and the title has to say which it is -----------------------------------


def _title_of(mod, route, monkeypatch):
    captured: dict[str, str] = {}

    async def fake_fetch(analysis, app, start, end):
        return [
            {
                "date": "2026-09-01",
                "agents_registered": 1,
                "registries": ["<1,0>"],
                "account_creation": 1,
                "tokens": {
                    "E": {
                        "USD": {"transfer": 0, "burn": 0, "mint": 0, "total_supply": 1},
                        "count_txs": 0,
                    }
                },
            }
        ]

    # Two resolvers in play: the POST handlers read the body with the async
    # get_theme_from_request, while an image route is a GET and uses the
    # sync theme_from_query, which falls back to light rather than dark.
    async def fake_theme(request):
        return "light"

    def fake_theme_sync(request):
        return "light"

    async def fake_response(fig, request, title):
        captured["title"] = fig.layout.title.text or ""
        return "rendered"

    monkeypatch.setattr(mod, "get_all_data_for_analysis_limited", fake_fetch)
    for _name, _fake in (
        ("get_theme_from_request", fake_theme),
        ("theme_from_query", fake_theme_sync),
    ):
        if hasattr(mod, _name):
            monkeypatch.setattr(mod, _name, _fake)
    monkeypatch.setattr(mod, "return_plot_response", fake_response)
    if hasattr(mod, "get_url_from_api"):
        monkeypatch.setattr(mod, "get_url_from_api", _overview)

    return captured, getattr(mod, route)


async def test_the_transactions_title_names_the_real_bar_width(monkeypatch):
    captured, route = _title_of(sc_transactions_count, "transactions_count_image", monkeypatch)
    await route(_request(), "mainnet", _state("transactions_count", Grouping.DAILY))
    assert "per Day" in captured["title"]

    await route(_request(), "mainnet", _state("transactions_count", Grouping.WEEKLY))
    assert "per Week" in captured["title"]
    assert "per Day" not in captured["title"], "weekly bars labelled as daily"

    await route(_request(), "mainnet", _state("transactions_count", Grouping.MONTHLY))
    assert "per Month" in captured["title"]


@pytest.mark.parametrize(
    ("mod", "route", "spec"),
    [
        (sc_agent_registries, "agent_registries_image", "agent_registries"),
        (sc_plt_transfers, "plt_tvl_image", "plt_tvl"),
    ],
)
async def test_the_other_two_say_how_they_are_grouped(mod, route, spec, monkeypatch):
    """They carried no period label at all, so weekly bars went unannounced."""
    captured, fn = _title_of(mod, route, monkeypatch)

    await fn(_request(), "mainnet", _state(spec, Grouping.DAILY))
    assert "day" in captured["title"].lower()

    await fn(_request(), "mainnet", _state(spec, Grouping.WEEKLY))
    assert "week" in captured["title"].lower()
