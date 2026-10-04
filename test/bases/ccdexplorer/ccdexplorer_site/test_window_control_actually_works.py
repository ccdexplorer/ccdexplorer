"""Where a date range is chosen: the slider on the site, buttons in the bot.

Both existed on the page for a while and they set the same thing, so one had
to lose. A window radio is always checked, so sending it unconditionally
meant it always won and dragging the slider redrew the identical chart.

They are split by medium now. The site has the slider, which can express any
range; the bot cannot show a slider, so it has the windows. Nothing on the
page sends a window, so nothing overrides what the slider asked for.
"""

import datetime as dt
import re
from pathlib import Path

import pytest

from ccdexplorer.charts import ChartState, Window
from ccdexplorer.charts.registry import BY_NAME
from ccdexplorer.ccdexplorer_site.app.routers.charts import (
    sc_accounts_growth,
    sc_active_addresses,
    sc_agent_registries,
    sc_holders,
    sc_plt_transfers,
    sc_transactions_count,
)
from ccdexplorer.ccdexplorer_site.app.routers.charts.generated import ChartPostData

TEMPLATES = Path(__file__).resolve().parents[4] / "projects" / "ccdexplorer_site" / "templates"
SETTINGS = TEMPLATES / "charts" / "standalone_chart.html"
HTMX = TEMPLATES / "charts" / "graph_htmx_div.html"

MODULES = [
    sc_transactions_count,
    sc_accounts_growth,
    sc_active_addresses,
    sc_holders,
    sc_agent_registries,
    sc_plt_transfers,
]


# --- the site: the slider, and only the slider ----------------------------


def test_the_settings_panel_offers_no_range_buttons():
    """They duplicated the slider and beat it."""
    body = SETTINGS.read_text()
    assert 'name="window"' not in body
    assert "btnradio_window" not in body


def test_the_page_still_offers_grouping_and_traces():
    body = SETTINGS.read_text()
    assert 'name="group_by"' in body
    assert 'name="trace_selection"' in body


def test_the_page_sends_the_sliders_dates():
    body = HTMX.read_text()
    assert "event-start-pretty" in body
    assert "event-end-pretty" in body


def test_the_page_sends_no_window():
    """The one thing that would override the slider."""
    body = HTMX.read_text()
    assert not re.search(r"^\s*window:", body, re.M)


@pytest.mark.parametrize("mod", MODULES, ids=[m.__name__.rsplit(".", 1)[-1] for m in MODULES])
def test_no_chart_post_model_carries_a_window(mod):
    """A field nothing sends is a field that misleads the next reader."""
    from pydantic import BaseModel

    for obj in vars(mod).values():
        if (
            isinstance(obj, type)
            and issubclass(obj, BaseModel)
            and obj is not BaseModel
            and obj.__module__ == mod.__name__
        ):
            assert "window" not in obj.model_fields, obj.__name__


def test_the_generated_post_model_carries_no_window_either():
    assert "window" not in ChartPostData.model_fields


# --- the bot: windows, because it cannot show a slider --------------------


def test_a_window_still_resolves_to_a_range_for_the_bot():
    spec = BY_NAME["transactions_count"]
    state = ChartState.from_query(spec, {"window": "30d"})
    assert (state.end - state.start).days == 30


def test_the_all_window_clamps_to_the_charts_own_beginning():
    spec = BY_NAME["agent_registries"]
    state = ChartState.from_query(spec, {"window": Window.ALL.value})
    assert state.start == dt.date(2026, 5, 27)


def test_the_image_route_still_reads_a_window():
    """That is how the bot's buttons work."""
    from ccdexplorer.ccdexplorer_site.app.routers.charts.images import state_for

    assert callable(state_for)
