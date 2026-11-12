"""Network activity draws two things from two collections.

CCD transferred comes from statistics_network_activity; transactions per
second is the account_transaction count in statistics_mongo_transactions,
divided by the seconds the bucket covers. A spec reads one collection, so the
second is named separately and merged on the date.

Without it the chart lost its TPS line -- the only trace missing anywhere
once the rest were matched up.
"""

import pytest

from ccdexplorer.charts import ChartState
from ccdexplorer.charts.registry import BY_NAME
from ccdexplorer.ccdexplorer_site.app.routers.charts.generic import build_figure

SECONDS_PER_DAY = 86_400


def _state():
    return ChartState.from_query(BY_NAME["network_activity"], {})


def test_the_spec_names_a_second_collection():
    spec = BY_NAME["network_activity"]
    assert spec.extra_source == "statistics_mongo_transactions"


def test_both_traces_are_drawn():
    spec = BY_NAME["network_activity"]
    rows = [
        {"date": "2026-10-01", "network_activity": 14045197.921003, "account_transaction": 54875}
    ]
    fig = build_figure(spec, rows, _state(), theme="light")
    assert {t.name for t in fig.data} == {"Activity", "TPS"}


def test_tps_is_the_transaction_count_over_a_day():
    spec = BY_NAME["network_activity"]
    rows = [{"date": "2026-10-01", "network_activity": 1.0, "account_transaction": 54875}]
    fig = build_figure(spec, rows, _state(), theme="light")
    tps = next(t for t in fig.data if t.name == "TPS")
    assert tps.y[0] == pytest.approx(54875 / SECONDS_PER_DAY)


def test_activity_is_unchanged_by_the_addition():
    spec = BY_NAME["network_activity"]
    rows = [{"date": "2026-10-01", "network_activity": 14045197.921003, "account_transaction": 1}]
    fig = build_figure(spec, rows, _state(), theme="light")
    activity = next(t for t in fig.data if t.name == "Activity")
    assert activity.y[0] == pytest.approx(14045197.921003)


def test_the_chart_still_draws_when_the_second_collection_is_missing():
    """One source failing should cost one line, not the whole chart."""
    spec = BY_NAME["network_activity"]
    fig = build_figure(
        spec, [{"date": "2026-10-01", "network_activity": 5.0}], _state(), theme="light"
    )
    assert [t.name for t in fig.data] == ["Activity"]


# --- the rate has to be over the period, not over a day -------------------
#
# The division was written when every row was a day, and survived the move to
# a grouped pipeline unchanged. A weekly row holds account_transaction summed
# over seven days (Agg.SUM in the spec), so dividing it by one day's seconds
# reported seven times the real figure -- 0.6 TPS daily became 4.2 weekly and
# roughly 15 monthly, three different answers to the same question about the
# same chain.
#
# The settings panel already said what it should have been doing: the derived
# TPS series is Agg.MEAN, which the panel renders as "average of the days".
# Sum over (days x 86400) is exactly that mean, since the days are equal:
# (1/n)SUM(c_i/86400) == SUM(c_i)/(n*86400).


def _state_for(grouping: str):
    return ChartState.from_query(BY_NAME["network_activity"], {"grouping": grouping})


def _tps(rows, grouping="daily"):
    fig = build_figure(BY_NAME["network_activity"], rows, _state_for(grouping), theme="light")
    return next(t for t in fig.data if t.name == "TPS").y


def test_a_week_is_not_divided_by_one_days_seconds():
    """Seven days at 54,875 a day is still the same rate, not seven times it."""
    rows = [
        {
            "date": "2026-09-28",
            "_days": 7,
            "network_activity": 1.0,
            "account_transaction": 54875 * 7,
        }
    ]
    assert _tps(rows, "weekly")[0] == pytest.approx(54875 / SECONDS_PER_DAY)


def test_a_month_is_divided_by_its_own_length():
    rows = [
        {
            "date": "2026-09-01",
            "_days": 30,
            "network_activity": 1.0,
            "account_transaction": 54875 * 30,
        }
    ]
    assert _tps(rows, "monthly")[0] == pytest.approx(54875 / SECONDS_PER_DAY)


def test_a_partial_bucket_averages_only_the_days_it_has():
    """The trailing week of a window is short; three days of data is a rate
    over three days, not over seven."""
    rows = [
        {
            "date": "2026-10-05",
            "_days": 3,
            "network_activity": 1.0,
            "account_transaction": 54875 * 3,
        }
    ]
    assert _tps(rows, "weekly")[0] == pytest.approx(54875 / SECONDS_PER_DAY)


def test_an_empty_bucket_is_not_a_division_by_zero():
    """$fill writes _days: 0 and account_transaction: 0 into a bucket no
    document landed in. 0/0 is NaN, which plotly draws as a gap in the line
    where the old code drew a zero."""
    rows = [
        {"date": "2026-09-28", "_days": 0, "network_activity": 0.0, "account_transaction": 0},
        {
            "date": "2026-10-05",
            "_days": 7,
            "network_activity": 1.0,
            "account_transaction": 54875 * 7,
        },
    ]
    drawn = _tps(rows, "weekly")
    assert drawn[0] == 0
    assert drawn[1] == pytest.approx(54875 / SECONDS_PER_DAY)


def test_a_row_with_no_day_count_is_one_day():
    """An API that predates ?grouping= answers with raw daily documents and no
    _days at all -- see rows_are_grouped. Absent is not zero days."""
    rows = [{"date": "2026-10-01", "network_activity": 1.0, "account_transaction": 54875}]
    assert _tps(rows)[0] == pytest.approx(54875 / SECONDS_PER_DAY)


def test_the_panel_and_the_computation_say_the_same_thing():
    """The settings panel reads the derived series' agg. It claimed an average
    of the days while the code summed them, and nothing on the page disagreed
    with anything else on the page."""
    from ccdexplorer.charts import Agg
    from ccdexplorer.ccdexplorer_site.app.routers.charts.generated import AGG_WORDING

    tps = next(s for s in BY_NAME["network_activity"].display_series if s.label == "TPS")
    assert tps.agg is Agg.MEAN
    assert AGG_WORDING[tps.agg] == "average of the days"
