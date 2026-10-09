"""One figure builder for the charts that only ever needed one.

Fifteen handlers in statistics.py each fetched a series, drew it, and set a
title. They differ in colour, label and chart kind -- all of which the spec
already carries -- so they collapse into this.

What the builder must not do is quietly draw nothing: an empty result, a
field that is not in the data, and a series the reader deselected all look
alike once a figure is rendered, and each means something different.
"""

import plotly.graph_objects as go
import pytest

from ccdexplorer.charts import Axis
from ccdexplorer.charts import ChartState, Grouping
from ccdexplorer.charts.registry import BY_NAME
from ccdexplorer.ccdexplorer_site.app.routers.charts.generated import can_be_generated
from ccdexplorer.ccdexplorer_site.app.routers.charts.generic import build_figure

ROWS = [
    {"date": "2026-09-07", "open_pool_count": 60},
    {"date": "2026-09-14", "open_pool_count": 65},
]


def _state(name, **kw):
    return ChartState.from_query(BY_NAME[name], kw)


def test_it_draws_one_trace_per_selected_series():
    spec = BY_NAME["staking_open_pool_count"]
    fig = build_figure(spec, ROWS, _state("staking_open_pool_count"), theme="light")
    assert len(fig.data) == 1
    assert fig.data[0].name == "Open pools"


def test_it_uses_the_colour_the_spec_carries():
    spec = BY_NAME["staking_open_pool_count"]
    fig = build_figure(spec, ROWS, _state("staking_open_pool_count"), theme="light")
    assert fig.data[0].marker.color == "#80B589"


def test_a_line_chart_draws_lines_and_a_bar_chart_draws_bars():
    line = build_figure(
        BY_NAME["staking_open_pool_count"],
        ROWS,
        _state("staking_open_pool_count"),
        theme="light",
    )
    assert isinstance(line.data[0], go.Scatter)

    rows = [{"date": "2026-09-07", "fee_for_day": 1}]
    bar = build_figure(BY_NAME["transaction_fees"], rows, _state("transaction_fees"), theme="light")
    assert isinstance(bar.data[0], go.Bar)


def test_a_stacked_bar_chart_stacks():
    spec = BY_NAME["staking_distribution_of_rewards"]
    rows = [
        {
            "date": "2026-09-07",
            "total_rewards_validators": 1,
            "total_rewards_pool_delegators": 2,
            "total_rewards_passive_delegators": 3,
        }
    ]
    fig = build_figure(spec, rows, _state("staking_distribution_of_rewards"), theme="light")
    assert fig.layout.barmode == "stack"
    assert len(fig.data) == 3


def test_an_empty_result_says_so_rather_than_drawing_nothing():
    """A bare figure reads as a flat line at zero, which is a claim about the
    data rather than an absence of it."""
    spec = BY_NAME["staking_open_pool_count"]
    fig = build_figure(spec, [], _state("staking_open_pool_count"), theme="light")
    assert fig.data == ()
    assert any("No data" in a.text for a in fig.layout.annotations)


def test_a_series_missing_from_the_data_is_skipped_not_drawn_as_zero():
    """An exchange that reported nothing must not appear as a flat zero line
    alongside the ones that did."""
    spec = BY_NAME["exchange_wallets"]
    rows = [{"date": "2026-09-07", "kraken": 5}]
    fig = build_figure(spec, rows, _state("exchange_wallets"), theme="light")
    assert [t.name for t in fig.data] == ["Kraken"]


def test_the_title_says_what_one_bar_covers():
    """The grouping is the reader's now, so the figure has to state which one
    it drew."""
    spec = BY_NAME["staking_open_pool_count"]
    for grouping, word in (
        (Grouping.DAILY, "Day"),
        (Grouping.WEEKLY, "Week"),
        (Grouping.MONTHLY, "Month"),
    ):
        # Built directly rather than through from_query. Open pools is a
        # closing value and picks its own resolution from the span now, so
        # a grouping handed to from_query is recomputed and all three
        # cases collapse into one -- but what is under test here is
        # whether build_figure says which grouping it drew.
        fig = build_figure(
            spec,
            ROWS,
            _state("staking_open_pool_count").model_copy(update={"grouping": grouping}),
            theme="light",
        )
        assert f"per {word}" in fig.layout.title.text, grouping


def test_only_the_traces_the_reader_kept_are_drawn():
    spec = BY_NAME["daily_limits"]
    rows = [{"date": "2026-09-07", "amount_to_make_top_100": 1, "amount_to_make_top_250": 2}]
    fig = build_figure(
        spec, rows, _state("daily_limits", traces="amount_to_make_top_100"), theme="light"
    )
    assert [t.name for t in fig.data] == ["Top 100"]


#: The calendar charts, which are what the invariants below are about: a
#: grouping and a date range in the path, and series for build_figure to
#: draw. A live chart is generated too, and configured by one segment
#: instead -- see test_generated_live_charts.py.
GENERATED = [s.name for s in BY_NAME.values() if can_be_generated(s) and s.axis is Axis.CALENDAR]


@pytest.mark.parametrize("name", GENERATED, ids=GENERATED)
def test_every_generated_chart_can_be_drawn(name):
    """Whatever the spec, the builder returns a figure rather than raising."""
    spec = BY_NAME[name]
    rows = [{"date": "2026-09-07", **{s.key: 1 for s in spec.series}}]
    fig = build_figure(spec, rows, _state(name), theme="light")
    assert isinstance(fig, go.Figure)
    assert fig.layout.title.text


def test_an_intraday_spec_is_refused():
    """It has no series and no source; a generic figure of it would be an
    empty chart with a confident title."""
    with pytest.raises(ValueError, match="intraday"):
        build_figure(BY_NAME["ccd_kraken_1h"], [], _state("ccd_kraken_1h"), theme="light")


# --- partial buckets -------------------------------------------------------
#
# Seen in a real render: a 90-day window starts mid-week and ends mid-week, so
# the first and last weekly bars covered 2 and 4 days against a full 7. Drawn
# plainly they read as activity collapsing at both ends of the chart, and
# nothing about them looks wrong.


from ccdexplorer.ccdexplorer_site.app.routers.charts.generic import (  # noqa: E402
    expected_days,
    partial_flags,
)


def test_a_full_week_is_not_partial():
    rows = [{"date": "2026-09-07", "_days": 7}]
    assert partial_flags(rows, Grouping.WEEKLY) == [False]


def test_a_short_week_is_partial():
    rows = [{"date": "2026-09-07", "_days": 3}]
    assert partial_flags(rows, Grouping.WEEKLY) == [True]


def test_a_daily_bucket_is_never_partial():
    """One day is one day."""
    assert partial_flags([{"date": "2026-09-07", "_days": 1}], Grouping.DAILY) == [False]


def test_a_month_is_measured_against_its_own_length():
    """February is not short for having 28 days."""
    assert expected_days("2026-02-01", Grouping.MONTHLY) == 28
    assert expected_days("2026-01-01", Grouping.MONTHLY) == 31
    assert partial_flags([{"date": "2026-02-01", "_days": 28}], Grouping.MONTHLY) == [False]
    assert partial_flags([{"date": "2026-01-01", "_days": 28}], Grouping.MONTHLY) == [True]


def test_rows_without_a_day_count_are_not_guessed_at():
    """An older API response carries no _days; absent is not partial."""
    assert partial_flags([{"date": "2026-09-07"}], Grouping.WEEKLY) == [False]


def test_a_partial_bar_is_drawn_differently_from_a_full_one():
    spec = BY_NAME["staking_distribution_of_rewards"]
    rows = [
        {
            "date": "2026-09-07",
            "_days": 7,
            "total_rewards_validators": 10,
            "total_rewards_pool_delegators": 1,
            "total_rewards_passive_delegators": 1,
        },
        {
            "date": "2026-09-14",
            "_days": 2,
            "total_rewards_validators": 3,
            "total_rewards_pool_delegators": 1,
            "total_rewards_passive_delegators": 1,
        },
    ]
    fig = build_figure(spec, rows, _state("staking_distribution_of_rewards"), theme="light")
    opacities = list(fig.data[0].marker.opacity)
    assert opacities[0] > opacities[1], "the short week is not marked"


def test_a_partial_bar_says_so_on_hover():
    spec = BY_NAME["staking_distribution_of_rewards"]
    rows = [
        {
            "date": "2026-09-07",
            "_days": 7,
            "total_rewards_validators": 10,
            "total_rewards_pool_delegators": 1,
            "total_rewards_passive_delegators": 1,
        },
        {
            "date": "2026-09-14",
            "_days": 2,
            "total_rewards_validators": 3,
            "total_rewards_pool_delegators": 1,
            "total_rewards_passive_delegators": 1,
        },
    ]
    fig = build_figure(spec, rows, _state("staking_distribution_of_rewards"), theme="light")
    text = list(fig.data[0].customdata)
    assert "partial" in str(text[1]).lower()
    assert "partial" not in str(text[0]).lower()


def test_the_partial_note_is_actually_shown_on_hover():
    """customdata alone displays nothing: plotly needs a hovertemplate that
    refers to it. Without one the faded bar had no explanation anywhere, and
    the first thing anyone asked was why two bars were a different colour.
    """
    spec = BY_NAME["staking_distribution_of_rewards"]
    rows = [
        {
            "date": "2026-09-07",
            "_days": 7,
            "total_rewards_validators": 10,
            "total_rewards_pool_delegators": 1,
            "total_rewards_passive_delegators": 1,
        },
        {
            "date": "2026-09-14",
            "_days": 2,
            "total_rewards_validators": 3,
            "total_rewards_pool_delegators": 1,
            "total_rewards_passive_delegators": 1,
        },
    ]
    fig = build_figure(spec, rows, _state("staking_distribution_of_rewards"), theme="light")
    template = fig.data[0].hovertemplate
    assert template, "no hovertemplate, so customdata is never displayed"
    assert "customdata" in template
