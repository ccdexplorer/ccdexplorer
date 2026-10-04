"""The grouping parameter on the statistics endpoint.

Omitting it has to behave exactly as before: the site's unmigrated charts are
still asking for raw daily documents while the migration runs.
"""

import pytest

from ccdexplorer.charts import Grouping
from ccdexplorer.ccdexplorer_api.app.routers.v2 import misc_v2


def test_unknown_grouping_is_rejected_not_crashed():
    """Review Focus 4: reachable from any shared link."""
    with pytest.raises(Exception) as caught:
        misc_v2.parse_grouping("sideways")
    assert getattr(caught.value, "status_code", None) == 422


def test_known_groupings_parse():
    assert misc_v2.parse_grouping("weekly") is Grouping.WEEKLY
    assert misc_v2.parse_grouping("daily") is Grouping.DAILY
    assert misc_v2.parse_grouping("monthly") is Grouping.MONTHLY


def test_absent_grouping_means_ungrouped():
    assert misc_v2.parse_grouping(None) is None


def test_grouping_an_unknown_analysis_is_rejected():
    """Without a spec there is no aggregation rule, and guessing one is how a
    level series gets summed."""
    with pytest.raises(Exception) as caught:
        misc_v2.spec_for_analysis("statistics_not_a_real_type")
    assert getattr(caught.value, "status_code", None) == 422


def test_grouping_a_registered_but_unimplemented_source_is_422_not_500():
    """statistics_plt is in BY_SOURCE but its pipeline refuses to be built.

    A documented parameter on a documented source must explain itself rather
    than crash: the NotImplementedError already carries the reason, so it
    reaches the caller as a 422 instead of an uncaught 500.
    """
    import pytest as _pytest

    with _pytest.raises(Exception) as caught:
        misc_v2.pipeline_for(
            misc_v2.spec_for_analysis("statistics_plt"),
            "2026-01-01",
            "2026-01-31",
            Grouping.WEEKLY,
        )
    assert getattr(caught.value, "status_code", None) == 422
    assert "statistics_plt" in str(getattr(caught.value, "detail", ""))


def test_an_implemented_source_still_builds_its_pipeline():
    pipeline = misc_v2.pipeline_for(
        misc_v2.spec_for_analysis("statistics_mongo_transactions"),
        "2026-01-01",
        "2026-01-31",
        Grouping.WEEKLY,
    )
    assert pipeline[0]["$match"]["type"] == "statistics_mongo_transactions"
