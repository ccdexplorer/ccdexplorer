"""The state an off-calendar chart is in.

A LOOKBACK chart's start and end are its forward horizon, and its window
means something else entirely: the span some reference figure is measured
over. Reading the window as the x-range -- which is what from_query does for
every other chart -- would draw the cooldown schedule over the last thirty
days, which is a chart of a schedule that has already happened.

Built here rather than taken from the registry: these test the mechanism, and
a spec is the cheapest possible fixture for it.
"""

import datetime as dt

from ccdexplorer.charts.models import Agg, Axis, ChartSpec, Interval, Series, Window
from ccdexplorer.charts.state import ChartState, lookback_range

TODAY = dt.date(2026, 10, 9)
CHAIN_START = dt.date(2021, 6, 9)

ALL_INTERVALS = (
    Interval.M1,
    Interval.M5,
    Interval.M15,
    Interval.M30,
    Interval.H1,
    Interval.H4,
    Interval.D1,
)


def _interval_spec(default: Interval = Interval.H4) -> ChartSpec:
    return ChartSpec(
        name="ccd_kraken",
        slug="ccd-kraken",
        title="CCD on Kraken",
        description="Candles.",
        blurb="Candles",
        category="exchanges",
        source="",
        series=(),
        axis=Axis.INTERVAL,
        live_source="kraken_ohlc",
        intervals=ALL_INTERVALS,
        default_interval=default,
        groupings=(),
        windows=(),
        chain_start=CHAIN_START,
    )


def _lookback_spec() -> ChartSpec:
    return ChartSpec(
        name="cooldown_schedule",
        slug="cooldown-schedule",
        title="Cooldown schedule",
        description="Releases.",
        blurb="Releases",
        category="staking",
        source="",
        series=(),
        axis=Axis.LOOKBACK,
        live_source="cooldown_schedule",
        horizon_days=7,
        groupings=(),
        windows=(Window.D30, Window.D90, Window.Y1, Window.ALL),
        default_window=Window.ALL,
        chain_start=CHAIN_START,
    )


def _calendar_spec() -> ChartSpec:
    return ChartSpec(
        name="fees",
        slug="fees",
        title="Fees",
        description="Fees.",
        blurb="Fees",
        category="chain",
        source="statistics_transaction_fees",
        series=(Series(key="fee", label="Fee", colour="#fff", agg=Agg.SUM),),
        chain_start=CHAIN_START,
    )


def test_an_interval_chart_opens_at_its_default_interval():
    state = ChartState.from_query(_interval_spec(), {}, today=TODAY)
    assert state.interval is Interval.H4


def test_an_interval_chart_takes_the_interval_it_is_given():
    state = ChartState.from_query(_interval_spec(), {"interval": "1m"}, today=TODAY)
    assert state.interval is Interval.M1


def test_an_unoffered_interval_falls_back_to_the_default():
    """A page is not an API: a stale link draws the chart rather than an
    error. The path form refuses it instead -- see test_paths_axes."""
    state = ChartState.from_query(_interval_spec(), {"interval": "7s"}, today=TODAY)
    assert state.interval is Interval.H4


def test_a_calendar_chart_has_no_interval():
    assert ChartState.from_query(_calendar_spec(), {}, today=TODAY).interval is None


def test_a_lookback_chart_draws_its_horizon_not_its_window():
    """Seven days forward from today, inclusive, whatever the window says."""
    state = ChartState.from_query(_lookback_spec(), {"window": "30d"}, today=TODAY)
    assert state.start == TODAY
    assert state.end == dt.date(2026, 10, 15)


def test_a_lookback_horizon_ignores_an_explicit_range():
    """A from/to in the query would otherwise override the horizon, and a
    shared link carrying one would draw a schedule in the past."""
    state = ChartState.from_query(
        _lookback_spec(), {"from": "2026-01-01", "to": "2026-02-01"}, today=TODAY
    )
    assert state.start == TODAY
    assert state.end == dt.date(2026, 10, 15)
    assert not state.explicit_dates


def test_a_lookback_window_is_the_averaging_span():
    spec = _lookback_spec()
    state = ChartState.from_query(spec, {"window": "30d"}, today=TODAY)
    start, end = lookback_range(spec, state, today=TODAY)
    assert end == dt.date(2026, 10, 8), "history stops at the last complete day"
    assert start == dt.date(2026, 9, 8)


def test_an_all_lookback_reaches_the_chain_start():
    spec = _lookback_spec()
    state = ChartState.from_query(spec, {"window": "all"}, today=TODAY)
    start, _ = lookback_range(spec, state, today=TODAY)
    assert start == spec.chain_start


def test_a_lookback_chart_opens_at_its_default_window():
    """`all` is what average_daily_release was handed before the window was a
    control, so the line a reader sees does not move."""
    assert ChartState.from_query(_lookback_spec(), {}, today=TODAY).window is Window.ALL


def test_an_interval_state_round_trips_through_a_query():
    spec = _interval_spec()
    state = ChartState.from_query(spec, {"interval": "1h"}, today=TODAY)
    assert ChartState.from_query(spec, state.to_query(), today=TODAY) == state


def test_a_lookback_state_round_trips_through_a_query():
    spec = _lookback_spec()
    state = ChartState.from_query(spec, {"window": "90d"}, today=TODAY)
    assert ChartState.from_query(spec, state.to_query(), today=TODAY) == state


def test_a_lookback_chart_is_not_automatically_grouped():
    """automatic_grouping reads display_series, which is empty here, so it
    already answers False -- pinned because it reaching True would send the
    page down the resolution-from-span branch with no span to read."""
    spec = _lookback_spec()
    assert spec.axis is Axis.LOOKBACK
    assert not spec.automatic_grouping
