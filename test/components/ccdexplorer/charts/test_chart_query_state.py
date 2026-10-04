"""A chart page's configuration, carried in its URL.

Until now the date range, grouping and trace selection lived only in the DOM,
so a configured chart could not be linked and lost its configuration on
reload.
"""

import datetime as dt

from ccdexplorer.charts import Grouping, Window
from ccdexplorer.charts.registry import BY_NAME
from ccdexplorer.charts.state import ChartState, resolve_window

TODAY = dt.date(2026, 10, 1)


def test_empty_query_gives_the_spec_defaults():
    state = ChartState.from_query(BY_NAME["transactions_count"], {})
    assert state.grouping is Grouping.WEEKLY
    assert state.window is Window.Y1


def test_query_overrides_the_defaults():
    state = ChartState.from_query(
        BY_NAME["transactions_count"], {"grouping": "daily", "window": "30d"}
    )
    assert state.grouping is Grouping.DAILY
    assert state.window is Window.D30


def test_a_nonsense_grouping_falls_back_rather_than_erroring():
    """A page is not an API: a stale link should still draw a chart."""
    state = ChartState.from_query(BY_NAME["transactions_count"], {"grouping": "sideways"})
    assert state.grouping is Grouping.WEEKLY


def test_a_grouping_the_spec_does_not_offer_falls_back():
    spec = BY_NAME["agent_registries"].model_copy(update={"groupings": (Grouping.WEEKLY,)})
    state = ChartState.from_query(spec, {"grouping": "daily"})
    assert state.grouping is Grouping.WEEKLY


def test_explicit_dates_win_over_the_window():
    state = ChartState.from_query(
        BY_NAME["transactions_count"],
        {"from": "2026-01-01", "to": "2026-03-01", "window": "30d"},
    )
    assert state.start == dt.date(2026, 1, 1)
    assert state.end == dt.date(2026, 3, 1)


def test_round_trips_through_the_query_string():
    spec = BY_NAME["transactions_count"]
    original = ChartState.from_query(spec, {"grouping": "monthly", "window": "90d"})
    assert ChartState.from_query(spec, original.to_query()) == original


def test_window_resolves_to_a_date_range():
    """Ending yesterday: today's figure is built from a day that has not
    finished, so it does not exist yet."""
    spec = BY_NAME["transactions_count"]
    start, end = resolve_window(spec, Window.D90, today=TODAY)
    assert end == TODAY - dt.timedelta(days=1)
    assert (end - start).days == 90


def test_all_window_starts_at_the_charts_own_beginning():
    """Not at chain start: the agent registry chart begins in 2026."""
    spec = BY_NAME["agent_registries"]
    start, _end = resolve_window(spec, Window.ALL, today=TODAY)
    assert start == dt.date(2026, 5, 27)


def test_traces_default_to_every_series():
    state = ChartState.from_query(BY_NAME["transactions_count"], {})
    assert state.traces == tuple(s.key for s in BY_NAME["transactions_count"].series)


def test_unknown_traces_are_dropped_not_trusted():
    state = ChartState.from_query(
        BY_NAME["transactions_count"], {"traces": "transfer,not_a_series"}
    )
    assert state.traces == ("transfer",)


def test_dropping_every_trace_falls_back_to_all():
    """An empty chart is never what the reader meant."""
    state = ChartState.from_query(BY_NAME["transactions_count"], {"traces": "nonsense"})
    assert len(state.traces) == len(BY_NAME["transactions_count"].series)
