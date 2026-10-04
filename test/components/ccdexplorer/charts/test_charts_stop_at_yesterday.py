"""No chart draws today.

Every one of these is built from the last block of a day, so today's figure
does not exist until the day does. Drawing it gave a final bar that was a
fraction of its neighbours and a line that dipped at the right-hand edge --
and when the old and new charts were compared, every value agreed except
that the new ones carried one extra day.
"""

import datetime as dt


from ccdexplorer.charts import ChartState, Window, resolve_window
from ccdexplorer.charts.paths import state_from_path
from ccdexplorer.charts.state import latest_complete_day
from ccdexplorer.charts.registry import BY_NAME

SPEC = BY_NAME["transaction_fees"]
TODAY = dt.date(2026, 10, 2)
YESTERDAY = dt.date(2026, 10, 1)


def test_the_latest_complete_day_is_yesterday():
    assert latest_complete_day(TODAY) == YESTERDAY


def test_a_window_ends_yesterday():
    _start, end = resolve_window(SPEC, Window.D30, today=TODAY)
    assert end == YESTERDAY


def test_a_window_still_spans_its_whole_length():
    start, end = resolve_window(SPEC, Window.D30, today=TODAY)
    assert (end - start).days == 30


def test_the_all_window_ends_yesterday_too():
    _start, end = resolve_window(SPEC, Window.ALL, today=TODAY)
    assert end == YESTERDAY


def test_a_default_state_ends_yesterday():
    state = ChartState.from_query(SPEC, {}, today=TODAY)
    assert state.end == YESTERDAY


def test_an_explicit_end_in_the_future_is_clamped():
    """A path naming this month runs to its last day, which has not happened."""
    state = ChartState.from_query(SPEC, {"from": "2026-09-01", "to": "2026-10-31"}, today=TODAY)
    assert state.end == YESTERDAY


def test_an_explicit_end_in_the_past_is_left_alone():
    state = ChartState.from_query(SPEC, {"from": "2023-06-01", "to": "2024-04-30"}, today=TODAY)
    assert state.end == dt.date(2024, 4, 30)


def test_a_path_naming_this_month_stops_at_yesterday():
    state = state_from_path(SPEC, "weekly", "202609", "202610", today=TODAY)
    assert state.end == YESTERDAY


def test_a_range_entirely_in_the_future_names_no_state():
    """There is nothing to draw, and a chart of nothing is not what the url
    says it is."""
    assert state_from_path(SPEC, "weekly", "202611", "202612", today=TODAY) is None
