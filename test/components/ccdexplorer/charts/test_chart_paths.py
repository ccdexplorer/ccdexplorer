"""Chart state as a path, not a query string.

/charts/daily-limits/weekly/202306/202604 rather than
/charts/daily-limits?grouping=weekly&from=...&to=...

Months, not days, because that is the precision the slider offers -- it steps
by a month -- and what the handlers already round to. A `to` month means the
whole of it, so 202604 ends on 30 April.
"""

import datetime as dt

import pytest

from ccdexplorer.charts import ChartState, Grouping
from ccdexplorer.charts.paths import (
    chart_path,
    month_end,
    month_start,
    parse_month,
    state_from_path,
)
from ccdexplorer.charts.registry import BY_NAME

# A chart that still asks for its grouping, so the path round-trips what it
# was handed. daily_limits used to be this example and no longer is: every
# one of its series is a closing value, so it picks its own resolution from
# the span and a grouping in the url is a record rather than a request.
SPEC = BY_NAME["transaction_fees"]


def test_a_month_parses_to_its_first_day():
    assert month_start("202306") == dt.date(2023, 6, 1)


def test_a_to_month_means_the_whole_month():
    assert month_end("202604") == dt.date(2026, 4, 30)
    assert month_end("202602") == dt.date(2026, 2, 28)
    assert month_end("202412") == dt.date(2024, 12, 31)


def test_a_leap_february_is_twenty_nine_days():
    assert month_end("202402") == dt.date(2024, 2, 29)


@pytest.mark.parametrize("bad", ["", "2026", "20261", "2026133", "abcdef", "202600", "202613"])
def test_nonsense_months_are_refused(bad):
    assert parse_month(bad) is None


def test_a_path_round_trips_through_state():
    state = state_from_path(SPEC, "weekly", "202306", "202604")
    assert state is not None
    assert state.grouping is Grouping.WEEKLY
    assert state.start == dt.date(2023, 6, 1)
    assert state.end == dt.date(2026, 4, 30)
    assert chart_path(SPEC, state) == "/charts/transaction-fees/weekly/202306/202604"


def test_an_unknown_grouping_in_the_path_is_refused():
    """A page falls back on a bad query value; a path is the address itself,
    so a wrong one should 404 rather than quietly draw something else."""
    assert state_from_path(SPEC, "sideways", "202306", "202604") is None


def test_a_grouping_the_chart_does_not_offer_is_refused():
    spec = SPEC.model_copy(update={"groupings": (Grouping.WEEKLY,)})
    assert state_from_path(spec, "daily", "202306", "202604") is None


def test_a_backwards_range_is_refused():
    assert state_from_path(SPEC, "weekly", "202604", "202306") is None


def test_the_bare_slug_still_means_the_defaults():
    """No path segments is the chart as it opens."""
    assert chart_path(SPEC, None) == "/charts/transaction-fees"


def test_the_path_carries_the_net_when_asked():
    state = state_from_path(SPEC, "monthly", "202601", "202606")
    assert chart_path(SPEC, state, net="mainnet") == (
        "/mainnet/charts/transaction-fees/monthly/202601/202606"
    )


def test_a_state_built_from_a_window_still_has_a_path():
    """The bot thinks in windows; the link it hands over is still a path."""
    state = ChartState.from_query(SPEC, {"window": "90d"})
    path = chart_path(SPEC, state, net="mainnet")
    assert path.startswith("/mainnet/charts/transaction-fees/weekly/")
    assert "?" not in path
