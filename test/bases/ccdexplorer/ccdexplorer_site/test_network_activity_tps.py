"""Network activity draws two things from two collections.

CCD transferred comes from statistics_network_activity; transactions per
second is the account_transaction count in statistics_mongo_transactions,
divided by the seconds in a day. A spec reads one collection, so the second
is named separately and merged on the date.

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
    rows = [{"date": "2026-10-01", "network_activity": 14045197.921003,
             "account_transaction": 54875}]
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
    rows = [{"date": "2026-10-01", "network_activity": 14045197.921003,
             "account_transaction": 1}]
    fig = build_figure(spec, rows, _state(), theme="light")
    activity = next(t for t in fig.data if t.name == "Activity")
    assert activity.y[0] == pytest.approx(14045197.921003)


def test_the_chart_still_draws_when_the_second_collection_is_missing():
    """One source failing should cost one line, not the whole chart."""
    spec = BY_NAME["network_activity"]
    fig = build_figure(spec, [{"date": "2026-10-01", "network_activity": 5.0}],
                       _state(), theme="light")
    assert [t.name for t in fig.data] == ["Activity"]
