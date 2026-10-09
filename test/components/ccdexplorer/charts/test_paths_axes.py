"""An off-calendar chart's state as a path.

/charts/ccd-kraken/1h, /charts/cooldown-schedule/30d

One segment, because one thing is configurable. Refused rather than
defaulted when it names something the chart does not offer: the path IS the
address, and drawing a different chart under it makes the url a lie.
"""

import datetime as dt

from ccdexplorer.charts.models import Interval, Window
from ccdexplorer.charts.paths import chart_path, state_from_axis_path
from ccdexplorer.charts.state import ChartState

from .test_state_axes import TODAY, _calendar_spec, _interval_spec, _lookback_spec


def test_an_interval_chart_lives_under_its_shared_page_slug():
    """Seven specs, one page. They cannot share a slug -- that keys BY_SLUG
    -- so page_slug carries it."""
    spec = _interval_spec()
    state = ChartState.from_query(spec, {"interval": "1h"}, today=TODAY)
    assert chart_path(spec, state, "mainnet") == "/mainnet/charts/ccd-kraken/1h"


def test_a_lookback_chart_names_its_window():
    spec = _lookback_spec()
    state = ChartState.from_query(spec, {"window": "90d"}, today=TODAY)
    assert chart_path(spec, state, "mainnet") == "/mainnet/charts/cooldown-schedule/90d"


def test_a_stateless_path_is_still_the_bare_slug():
    """The shortest thing to hand anyone: the chart as it opens."""
    assert chart_path(_lookback_spec(), None, "mainnet") == "/mainnet/charts/cooldown-schedule"
    assert chart_path(_interval_spec(), None, "mainnet") == "/mainnet/charts/ccd-kraken"


def test_a_path_without_a_net_is_still_rooted():
    assert chart_path(_interval_spec(), None) == "/charts/ccd-kraken"


def test_a_calendar_path_is_unchanged():
    spec = _calendar_spec()
    state = ChartState.from_query(spec, {"grouping": "weekly", "window": "1y"}, today=TODAY)
    assert chart_path(spec, state, "mainnet").startswith("/mainnet/charts/fees/weekly/")


def test_an_interval_segment_resolves():
    state = state_from_axis_path(_interval_spec(), "15m", today=TODAY)
    assert state is not None
    assert state.interval is Interval.M15


def test_an_interval_the_chart_does_not_offer_is_refused():
    spec = _interval_spec().model_copy(update={"intervals": (Interval.H1, Interval.H4)})
    assert state_from_axis_path(spec, "1m", today=TODAY) is None


def test_an_interval_that_is_not_one_at_all_is_refused():
    assert state_from_axis_path(_interval_spec(), "7s", today=TODAY) is None


def test_a_grouping_in_an_interval_slot_is_refused():
    """ "weekly" is a real Grouping and not a real interval. Accepted, it
    would have drawn the default while the url named something else."""
    assert state_from_axis_path(_interval_spec(), "weekly", today=TODAY) is None


def test_a_lookback_segment_resolves():
    state = state_from_axis_path(_lookback_spec(), "30d", today=TODAY)
    assert state is not None
    assert state.window is Window.D30
    # The dates are the span the lookback selects -- stored history behind
    # today -- and not the chart's x-axis, which comes from the data.
    assert state.start == dt.date(2026, 9, 8)
    assert state.end == dt.date(2026, 10, 8)


def test_a_window_the_chart_does_not_offer_is_refused():
    assert state_from_axis_path(_lookback_spec(), "5y", today=TODAY) is None


def test_an_empty_segment_is_refused():
    assert state_from_axis_path(_interval_spec(), "", today=TODAY) is None


def test_a_calendar_chart_has_no_axis_path():
    """Its state needs three segments, not one. Asked for one, it answers
    nothing rather than inventing a range."""
    assert state_from_axis_path(_calendar_spec(), "weekly", today=TODAY) is None


def test_every_interval_round_trips_through_its_path():
    spec = _interval_spec()
    for interval in spec.intervals:
        state = ChartState.from_query(spec, {"interval": interval.value}, today=TODAY)
        segment = chart_path(spec, state, "mainnet").rsplit("/", 1)[-1]
        assert state_from_axis_path(spec, segment, today=TODAY) == state


def test_every_lookback_round_trips_through_its_path():
    spec = _lookback_spec()
    for window in spec.windows:
        state = ChartState.from_query(spec, {"window": window.value}, today=TODAY)
        segment = chart_path(spec, state, "mainnet").rsplit("/", 1)[-1]
        assert state_from_axis_path(spec, segment, today=TODAY) == state


def test_a_shared_page_slug_does_not_leak_into_a_calendar_path():
    """Every other chart's page is at its own slug, and page_slug defaults to
    it -- pinned so the Kraken family's sharing cannot move anything else."""
    spec = _calendar_spec()
    assert spec.page_slug == spec.slug
    assert chart_path(spec, None, "mainnet") == "/mainnet/charts/fees"


def test_an_unknown_axis_date_is_not_needed_to_resolve_one():
    """state_from_axis_path takes no range, so it cannot be refused for
    naming a month that has not happened -- which state_from_path does."""
    assert state_from_axis_path(_interval_spec(), "1d") is not None
