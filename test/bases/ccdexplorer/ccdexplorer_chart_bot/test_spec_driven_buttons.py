"""The bot's buttons for the charts that have specs.

A family used to be four route names for one chart -- transactions_count_90d
and its siblings -- and the only configurability was switching between them.
A spec-backed chart is one entry now, with a window row and a grouping row
built from what its spec actually offers.

The charts without a spec yet keep their existing entries untouched. Plan 2
migrates those; until then the catalogue holds both shapes, and these tests
pin that the two do not interfere.
"""

import pytest

from ccdexplorer.charts import Grouping, Window
from ccdexplorer.charts.state import resolution_for
from ccdexplorer.charts.registry import BY_NAME
from ccdexplorer.ccdexplorer_chart_bot.catalogue import (
    BY_NAME as CHART_BY_NAME,
    CHARTS,
    SPEC_BACKED,
    callback_data,
    image_url,
    page_url,
    parse_callback,
    state_for,
)
from ccdexplorer.ccdexplorer_chart_bot.direct import keyboard_for

SITE = "https://ccdexplorer.io"


def _chart(name):
    return CHART_BY_NAME[name]


# --- the spec-backed charts -------------------------------------------------


def test_the_three_migrated_families_are_one_entry_each():
    """Twelve catalogue entries became three."""
    assert SPEC_BACKED == ("transactions_count", "plt_tvl", "agent_registries")
    for name in SPEC_BACKED:
        assert name in CHART_BY_NAME


def test_the_retired_window_names_are_gone_from_the_catalogue():
    names = {c.name for c in CHARTS}
    for retired in ("transactions_count_90d", "plt_tvl_30d", "agent_registries_365d"):
        assert retired not in names


def test_image_url_carries_its_state():
    """In the path now, the same shape as the chart's page. The window is a
    way of choosing a range, not something the url has to repeat."""
    chart = _chart("transactions_count")
    state = state_for(chart, Window.D90, Grouping.DAILY)
    url = image_url(chart, SITE, state, now=0)
    assert "/plots/mainnet/transactions_count/daily/" in url
    assert url.endswith("/image.png?t=0")
    assert (state.end - state.start).days == 90


def test_image_url_still_busts_telegrams_cache():
    """Telegram serves its own stored copy for a url it has seen before."""
    chart = _chart("transactions_count")
    state = state_for(chart)
    assert image_url(chart, SITE, state, now=0) != image_url(
        chart, SITE, state, now=chart.refresh_seconds + 1
    )


def test_keyboard_has_a_window_row_and_a_grouping_row():
    chart = _chart("transactions_count")
    spec = BY_NAME["transactions_count"]
    rows = keyboard_for(chart, state_for(chart)).inline_keyboard
    assert {b.text.strip("· ") for b in rows[0]} == {w.value for w in spec.windows}
    assert {b.text.strip("· ") for b in rows[1]} == {g.value for g in spec.groupings}


def test_the_current_selection_is_marked():
    chart = _chart("transactions_count")
    rows = keyboard_for(chart, state_for(chart, Window.D90)).inline_keyboard
    assert [b.text for b in rows[0] if b.text.startswith("·")] == ["· 90d ·"]


def test_callback_data_fits_telegrams_limit():
    for name in SPEC_BACKED:
        spec = BY_NAME[name]
        for window in spec.windows:
            for grouping in spec.groupings:
                data = callback_data(_chart(name), window, grouping)
                assert len(data.encode()) <= 64, data


@pytest.mark.parametrize("name", list(SPEC_BACKED))
def test_every_button_round_trips(name):
    spec = BY_NAME[name]
    for window in spec.windows:
        for grouping in spec.groupings:
            chart, state = parse_callback(callback_data(_chart(name), window, grouping))
            assert chart.name == name
            assert state.window is window
            if spec.automatic_grouping:
                # No grouping buttons to round-trip: the chart picks its
                # resolution from the span, and a grouping in the payload
                # is a record of what was drawn rather than a request.
                assert state.grouping is resolution_for(state.start, state.end)
            else:
                assert state.grouping is grouping


def test_caption_links_to_the_site_in_the_same_state():
    """'Open this properly' should show the same picture."""
    chart = _chart("transactions_count")
    state = state_for(chart, Window.D90, Grouping.DAILY)
    url = page_url(chart, SITE, state)
    assert "/mainnet/charts/transactions-count/daily/" in url
    assert (state.end - state.start).days == 90


# --- the charts that have no spec yet --------------------------------------


def test_an_unmigrated_chart_keeps_its_interval_family():
    """The seven Kraken intervals are still seven entries with period buttons."""
    kraken = [c for c in CHARTS if c.group == "price"]
    assert len(kraken) == 7
    rows = keyboard_for(_chart("ccd_kraken_4h"), None).inline_keyboard
    # Four to a row: seven intervals in one row is unreadable at phone width.
    periods = {b.text.strip("· ") for row in rows for b in row}
    assert {c.period for c in kraken} <= periods


def test_an_unmigrated_standalone_chart_gets_no_configuration_row():
    """A chart with neither a spec nor siblings has nothing to offer but
    sending it onward.

    Validator stake is the last one: every other standalone chart in the
    catalogue now names a spec and gets period and grouping buttons.
    """
    rows = keyboard_for(_chart("staking_validator_staked_amounts"), None).inline_keyboard
    assert len(rows) == 1
    assert rows[0][0].switch_inline_query == "staking_validator_staked_amounts"


def test_a_legacy_callback_still_resolves():
    """Buttons already sitting in people's chats carry the old payload."""
    chart, state = parse_callback("c:ccd_kraken_1h")
    assert chart.name == "ccd_kraken_1h"
    assert state is None


def test_a_stale_callback_resolves_to_nothing_rather_than_raising():
    assert parse_callback("c:no_such_chart") is None
    assert parse_callback("c:no_such_chart:1y:weekly") is None
    assert parse_callback("garbage") is None


# --- buttons already sitting in people's chats ------------------------------


RETIRED = [
    ("transactions_count_30d", "transactions_count", Window.D30),
    ("transactions_count_90d", "transactions_count", Window.D90),
    ("transactions_count_180d", "transactions_count", Window.D90),
    ("transactions_count_365d", "transactions_count", Window.Y1),
    ("plt_tvl_30d", "plt_tvl", Window.D30),
    ("plt_tvl_365d", "plt_tvl", Window.Y1),
    ("agent_registries_30d", "agent_registries", Window.D30),
    ("agent_registries_365d", "agent_registries", Window.Y1),
]


@pytest.mark.parametrize("old,name,window", RETIRED, ids=[r[0] for r in RETIRED])
def test_a_retired_family_button_still_answers(old, name, window):
    """The twelve retired URLs redirect; their twelve callback payloads must
    resolve too. A button tapped in a months-old message otherwise answers
    'That chart is no longer available' about a chart that very much is.
    """
    resolved = parse_callback(f"c:{old}")
    assert resolved is not None, old
    chart, state = resolved
    assert chart.name == name
    assert state is not None
    assert state.window is window


def test_a_retired_name_that_was_never_a_window_is_still_refused():
    assert parse_callback("c:transactions_count_7d") is None


# --- the caption link for charts that are not yet button-driven ------------


def test_a_legacy_chart_links_to_its_configurable_page():
    """Its image route still takes no parameters, so it gets no buttons --
    but the page that configures it exists now, and the caption should lead
    there rather than to a raw png."""
    chart = _chart("daily_limits")
    url = page_url(chart, SITE, None)
    assert "/mainnet/charts/daily-limits" in url
    assert "/plots/" not in url


def test_a_chart_with_no_spec_still_gets_no_window_buttons():
    """Its /plots route ignores window and grouping, so a button row would
    redraw the identical image -- the dead control this avoids.

    Daily limits used to be the example here. It has a spec now, so it is
    the opposite case: it gets the buttons, and the test above holds the
    one chart that still cannot use them.
    """
    rows = keyboard_for(_chart("staking_validator_staked_amounts"), None).inline_keyboard
    assert len(rows) == 1

    configured = keyboard_for(_chart("daily_limits"), None).inline_keyboard
    assert len(configured) > 1


def test_an_intraday_chart_keeps_its_plots_link():
    """It has no configurable page: nothing to group, nothing to set."""
    url = page_url(_chart("ccd_kraken_4h"), SITE, None)
    assert "/plots/mainnet/ccd_kraken_4h" in url


def test_a_spec_backed_chart_still_carries_its_state_in_the_link():
    chart = _chart("transactions_count")
    state = state_for(chart, Window.D90, Grouping.DAILY)
    url = page_url(chart, SITE, state)
    assert "?" not in url
    assert "/daily/" in url


# --- the image url follows the same path shape as the page ----------------


def test_the_image_url_puts_its_state_in_the_path():
    chart = _chart("transactions_count")
    url = image_url(chart, SITE, state_for(chart, Window.D90, Grouping.DAILY), now=0)
    assert "/plots/mainnet/transactions_count/daily/" in url
    assert "grouping=" not in url
    assert "window=" not in url


def test_the_image_url_asks_for_no_theme_at_all():
    """Light is what a request carrying neither a parameter nor a cookie
    gets, and Telegram sends neither. The bucket is the only parameter left,
    and its whole job is to stop the url matching."""
    chart = _chart("transactions_count")
    url = image_url(chart, SITE, state_for(chart), now=0)
    assert "theme=" not in url
    assert url.endswith("?t=0")


def test_an_unmigrated_chart_keeps_its_plain_image_url():
    """Its route takes no state, so there is none to put in the path."""
    url = image_url(_chart("daily_limits"), SITE, None, now=0)
    assert url.startswith(f"{SITE}/plots/mainnet/daily_limits/image.png?")
    assert "/weekly/" not in url


def test_the_bucket_still_changes_so_telegram_refetches():
    chart = _chart("transactions_count")
    state = state_for(chart)
    assert image_url(chart, SITE, state, now=0) != image_url(
        chart, SITE, state, now=chart.refresh_seconds + 1
    )


def test_the_caption_link_is_a_path_too():
    """It is the one url a reader actually sees and might keep."""
    chart = _chart("transactions_count")
    url = page_url(chart, SITE, state_for(chart, Window.D90, Grouping.DAILY))
    assert "?" not in url
    assert "/mainnet/charts/transactions-count/daily/" in url


# --- charts that pick their own resolution ---------------------------------
#
# A grouping button on a closing value redraws the same measurement at a
# different point count. It is not a question about the data, and asked of
# a short window it answers badly: fee stabilization over thirty days,
# grouped monthly, is two points. Those charts choose from the span, so the
# row is absent rather than dead.


def _labels(rows):
    return [b.text.strip("· ") for row in rows for b in row]


@pytest.mark.parametrize(
    "name", ["fee_stabilization", "daily_limits", "ccd_on_exchanges", "realized_prices"]
)
def test_a_closing_value_gets_periods_but_no_grouping(name):
    rows = keyboard_for(_chart(name), None).inline_keyboard
    labels = _labels(rows)

    assert not ({"daily", "weekly", "monthly"} & set(labels)), labels
    assert {"30d", "90d", "1y"} <= set(labels), labels


@pytest.mark.parametrize("name", ["transaction_fees", "transactions_count", "accounts_growth"])
def test_a_chart_whose_grouping_means_something_keeps_its_row(name):
    labels = _labels(keyboard_for(_chart(name), None).inline_keyboard)
    assert {"daily", "weekly", "monthly"} <= set(labels), labels


def test_the_period_buttons_still_redraw_it():
    """Dropping the grouping row must not drop the range row with it: the
    range is the only control these charts have left."""
    chart = _chart("fee_stabilization")
    rows = keyboard_for(chart, None).inline_keyboard
    assert any(b.callback_data for row in rows for b in row if b.callback_data)
