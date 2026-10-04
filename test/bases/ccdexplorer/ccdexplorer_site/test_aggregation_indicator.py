"""The settings panel says how a period's value is arrived at.

Group By offers day, week and month without saying what happens to the days
inside one. For a sum that is obvious; for a closing value or a mean it is
not, and the difference is the whole reason some of these charts are right.

Per trace only where the traces differ. Accounts growth draws a closing
total beside a difference and network activity a sum beside a mean, so
those have to be named; transactions by category draws five sums, and
saying "summed" five times beside five labels is a paragraph where one
word would do.
"""

import pytest

from ccdexplorer.charts import Agg
from ccdexplorer.charts.registry import BY_NAME
from ccdexplorer.ccdexplorer_site.app.routers.charts.generated import (
    aggregation_notes,
)


def _notes(name):
    return dict(aggregation_notes(BY_NAME[name]))


def test_a_sum_says_it_is_summed():
    assert aggregation_notes(BY_NAME["transaction_fees"]) == [("", "summed")]


def test_traces_that_agree_are_said_once():
    """Transactions by category draws five sums. It used to print
    "Account: summed / Transfer: summed / ..." -- five lines that differ
    only in a label the legend already carries."""
    spec = BY_NAME["transactions_count"]
    assert len(spec.display_series) > 1
    assert aggregation_notes(spec) == [("", "summed")]


def test_a_collapsed_note_carries_no_label():
    """So the template can print the wording on its own."""
    for label, _note in aggregation_notes(BY_NAME["transactions_count"]):
        assert label == ""


def test_a_level_says_it_is_the_closing_value():
    assert aggregation_notes(BY_NAME["staking_open_pool_count"]) == [("", "value on the last day")]


def test_an_average_says_it_is_a_mean():
    assert aggregation_notes(BY_NAME["staking_avg_delegator_stake"]) == [
        ("", "average of the days")
    ]


def test_a_delta_says_what_it_is_the_difference_of():
    notes = _notes("accounts_growth")
    assert notes["Account Growth"] == "change since the previous period"


def test_a_mixed_chart_reports_each_trace_separately():
    """The point of doing this per trace."""
    notes = _notes("accounts_growth")
    assert notes["Accounts On Chain"] == "value on the last day"
    assert notes["Account Growth"] == "change since the previous period"


def test_the_derived_traces_are_the_ones_described():
    """The reader picked Activity and TPS, not network_activity and
    account_transaction."""
    notes = _notes("network_activity")
    assert set(notes) == {"Activity", "TPS"}


def test_a_chart_that_does_not_aggregate_says_so():
    """Active addresses reads a differently-grouped collection instead."""
    assert aggregation_notes(BY_NAME["active_addresses"]) == [
        ("", "counted over the period itself")
    ]


def test_every_chart_with_a_page_has_a_note_for_every_trace():
    from ccdexplorer.charts.registry import ALL_SPECS
    from ccdexplorer.ccdexplorer_site.app.routers.charts.generated import (
        can_be_generated,
    )

    for spec in ALL_SPECS:
        if not can_be_generated(spec):
            continue
        notes = aggregation_notes(spec)
        assert all(note for _label, note in notes), spec.name
        labels = {label for label, _note in notes}
        if labels == {""}:
            assert len(notes) == 1, spec.name
        else:
            assert labels == {s.label for s in spec.display_series}, spec.name


def test_every_aggregation_has_wording():
    """A new Agg member must not render as a blank."""
    from ccdexplorer.ccdexplorer_site.app.routers.charts.generated import AGG_WORDING

    assert set(AGG_WORDING) == set(Agg)


# --- charts that do not ask ------------------------------------------------


@pytest.fixture(scope="module")
def client():
    from pathlib import Path

    from fastapi.testclient import TestClient

    from ccdexplorer.ccdexplorer_site.app.factory import AppSettings, create_app

    project = Path(__file__).resolve().parents[4] / "projects" / "ccdexplorer_site"
    app = create_app(
        AppSettings(
            static_dir=project / "static",
            templates_dir=project / "templates",
            node_modules_dir=project / "node_modules",
            addresses_dir=project / "addresses",
        )
    )
    return TestClient(app, follow_redirects=False)


def test_the_panel_hides_group_by_for_a_chart_that_picks_its_own(client):
    """Fee stabilization is a closing value: grouping changes nothing about
    the number, only how many points there are. The slider decides the
    range, the span decides the resolution, and there is no third question
    to put to the reader.

    Asked of the real page, not of a fragment this test wraps in the
    condition it is checking for -- that version passed before the template
    had the condition at all.
    """
    assert "Group By" not in client.get("/mainnet/charts/fee-stabilization").text


def test_the_panel_still_asks_where_grouping_means_something(client):
    assert "Group By" in client.get("/mainnet/charts/transaction-fees").text


def test_a_chart_that_picks_its_own_still_explains_the_value():
    """No control does not mean no explanation: a weekly point is still
    the last day's value and the reader should be told which."""
    from ccdexplorer.charts.registry import BY_NAME

    assert aggregation_notes(BY_NAME["fee_stabilization"]) == [("", "value on the last day")]


def test_the_page_carries_the_rule_the_rewriter_needs(client):
    """The address-bar rewriter has to reach the same answer as the server
    with no radios to read, so the thresholds are rendered into the page
    rather than written twice."""
    from ccdexplorer.charts.state import DAILY_UP_TO_DAYS, WEEKLY_UP_TO_DAYS

    body = client.get("/mainnet/charts/fee-stabilization").text
    assert f'"daily_up_to": {DAILY_UP_TO_DAYS}' in body
    assert f'"weekly_up_to": {WEEKLY_UP_TO_DAYS}' in body


def test_a_chart_that_asks_carries_no_rule(client):
    body = client.get("/mainnet/charts/transaction-fees").text
    assert "window.CHART_AUTO_GROUPING = null" in body
