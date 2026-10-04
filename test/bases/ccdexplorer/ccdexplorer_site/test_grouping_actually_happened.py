"""The site must not draw ungrouped rows under a grouped title.

?grouping= is new. An API that predates it ignores the parameter -- FastAPI
drops unknown query params silently -- and answers with the raw daily
documents. Drawn by a page that asked for weekly, those become seven bars
where one was meant, under a title that says "per Week".

Nothing about that looks wrong, which is the whole problem, and it is the
state the site is in for as long as it is deployed ahead of the API. So the
rows are checked rather than trusted: a grouped response carries the _days
count the pipeline emits, and an ungrouped one cannot.
"""

import pytest

from ccdexplorer.charts import ChartState, Grouping
from ccdexplorer.charts.registry import BY_NAME
from ccdexplorer.ccdexplorer_site.app.routers.charts.generated import (
    rows_are_grouped,
)

SPEC = BY_NAME["staking_open_pool_count"]


def _state(grouping=Grouping.WEEKLY):
    return ChartState.from_query(SPEC, {"grouping": grouping.value})


def test_grouped_rows_are_recognised():
    rows = [{"date": "2026-09-07", "_days": 7, "open_pool_count": 65}]
    assert rows_are_grouped(rows) is True


def test_ungrouped_daily_rows_are_caught():
    """What an API that ignored the parameter returns."""
    rows = [
        {"date": "2026-09-07", "open_pool_count": 60},
        {"date": "2026-09-08", "open_pool_count": 61},
    ]
    assert rows_are_grouped(rows) is False


def test_an_empty_result_is_not_called_ungrouped():
    """Nothing to draw is a different thing from drawn wrongly, and the
    empty state already says so."""
    assert rows_are_grouped([]) is True


@pytest.mark.parametrize("grouping", list(Grouping))
def test_the_check_holds_for_every_grouping(grouping):
    assert rows_are_grouped([{"date": "2026-09-07", "_days": 1}]) is True
    assert rows_are_grouped([{"date": "2026-09-07"}]) is False
