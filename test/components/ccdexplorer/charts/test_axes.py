"""What a chart lets the reader change.

Every chart used to be configured the same way -- a grouping and a date range
over a date-keyed collection -- and the two that are not had no page at all.
"""

import datetime as dt

import pytest
from pydantic import ValidationError

from ccdexplorer.charts.models import Agg, Axis, ChartSpec, Grouping, Interval, Series, Window


def _spec(**kwargs) -> ChartSpec:
    base = dict(
        name="x",
        slug="x",
        title="X",
        description="X.",
        blurb="X",
        category="chain",
        source="statistics_x",
        series=(Series(key="v", label="V", colour="#fff", agg=Agg.LAST),),
        chain_start=dt.date(2021, 6, 9),
    )
    return ChartSpec(**{**base, **kwargs})


def test_a_chart_is_calendar_configured_unless_it_says_otherwise():
    """Twenty-three charts predate the field and must not change meaning."""
    assert _spec().axis is Axis.CALENDAR


def test_the_page_slug_defaults_to_the_slug():
    """Only the Kraken family shares a page; everything else is its own."""
    assert _spec(slug="daily-limits").page_slug == "daily-limits"


def test_a_shared_page_slug_is_kept():
    assert _spec(page_slug="ccd-kraken").page_slug == "ccd-kraken"


def test_an_interval_chart_declares_its_intervals():
    spec = _spec(
        axis=Axis.INTERVAL,
        source="",
        series=(),
        groupings=(),
        windows=(),
        intervals=(Interval.H1, Interval.H4),
        default_interval=Interval.H4,
        live_source="kraken_ohlc",
    )
    assert spec.intervals == (Interval.H1, Interval.H4)
    assert spec.default_interval is Interval.H4


def test_an_interval_chart_without_intervals_is_refused():
    """A chart whose only control is the interval, offering none, is a page
    with nothing on it."""
    with pytest.raises(ValidationError):
        _spec(axis=Axis.INTERVAL, source="", series=(), intervals=(), live_source="kraken_ohlc")


def test_an_interval_chart_defaults_to_its_first_interval():
    spec = _spec(
        axis=Axis.INTERVAL,
        source="",
        series=(),
        groupings=(),
        windows=(),
        intervals=(Interval.H1, Interval.H4),
        live_source="kraken_ohlc",
    )
    assert spec.default_interval is Interval.H1


def test_an_interval_default_outside_the_offered_set_is_refused():
    with pytest.raises(ValidationError):
        _spec(
            axis=Axis.INTERVAL,
            source="",
            series=(),
            groupings=(),
            windows=(),
            intervals=(Interval.H1,),
            default_interval=Interval.D1,
            live_source="kraken_ohlc",
        )


def test_a_lookback_chart_has_a_horizon_and_windows():
    spec = _spec(
        axis=Axis.LOOKBACK,
        source="",
        series=(),
        groupings=(),
        horizon_days=7,
        windows=(Window.D30, Window.ALL),
        default_window=Window.ALL,
        live_source="cooldown_schedule",
    )
    assert spec.horizon_days == 7
    assert spec.windows == (Window.D30, Window.ALL)


def test_a_lookback_chart_without_a_horizon_is_refused():
    """The horizon is its x-axis. Without one there is nothing to draw."""
    with pytest.raises(ValidationError):
        _spec(
            axis=Axis.LOOKBACK,
            source="",
            series=(),
            groupings=(),
            horizon_days=0,
            live_source="cooldown_schedule",
        )


def test_a_lookback_chart_offers_no_grouping():
    """Seven days is seven days. A monthly button over it draws one bar."""
    with pytest.raises(ValidationError):
        _spec(
            axis=Axis.LOOKBACK,
            source="",
            series=(),
            groupings=(Grouping.DAILY, Grouping.MONTHLY),
            horizon_days=7,
            live_source="cooldown_schedule",
        )


def test_an_off_calendar_chart_names_a_provider():
    """Neither reads a Mongo collection, so something has to say where the
    figure comes from -- otherwise the page registers and 500s on every hit."""
    with pytest.raises(ValidationError):
        _spec(
            axis=Axis.INTERVAL,
            source="",
            series=(),
            groupings=(),
            windows=(),
            intervals=(Interval.H4,),
        )


def test_a_calendar_chart_needs_no_provider():
    assert _spec().live_source is None
