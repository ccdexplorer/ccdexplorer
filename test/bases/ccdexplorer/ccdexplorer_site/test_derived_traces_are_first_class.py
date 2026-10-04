"""A derived chart's traces are traces like any other.

The reader sees "Activity" and "TPS", not network_activity and
account_transaction. Those names have to reach the settings panel so they
can be turned off, and the url so a selection can be shared -- the raw
fields are an implementation detail they never chose.

And they are not all the same shape: TPS is a line against a bar, on its own
axis, because 0.6 transactions a second next to fourteen million CCD is an
invisible bar.
"""

import pytest

from ccdexplorer.charts import ChartState
from ccdexplorer.charts.registry import BY_NAME
from ccdexplorer.ccdexplorer_site.app.routers.charts.generic import build_figure


def _state(name, **kw):
    return ChartState.from_query(BY_NAME[name], kw)


ROWS = [{"date": "2026-10-01", "network_activity": 14045197.9, "account_transaction": 54875}]


def test_a_derived_chart_exposes_its_drawn_traces():
    spec = BY_NAME["network_activity"]
    assert [s.label for s in spec.display_series] == ["Activity", "TPS"]


def test_a_plain_chart_exposes_its_own_series():
    spec = BY_NAME["daily_limits"]
    assert spec.display_series == spec.series


def test_the_drawn_traces_have_url_names():
    for name in ("network_activity", "accounts_growth", "staking_validator_count"):
        for series in BY_NAME[name].display_series:
            assert series.url_name, f"{name}.{series.label}"
            assert "-" not in series.url_name


def test_tps_is_a_line_on_its_own_axis():
    """A bar of 0.6 beside a bar of fourteen million is not visible."""
    spec = BY_NAME["network_activity"]
    tps = next(s for s in spec.display_series if s.label == "TPS")
    assert tps.secondary_y is True
    fig = build_figure(spec, ROWS, _state("network_activity"), theme="light")
    drawn = {t.name: type(t).__name__ for t in fig.data}
    assert drawn["TPS"] == "Scatter"
    assert drawn["Activity"] == "Bar"


def test_a_derived_trace_can_be_deselected():
    spec = BY_NAME["network_activity"]
    fig = build_figure(spec, ROWS, _state("network_activity", traces="activity"), theme="light")
    assert [t.name for t in fig.data] == ["Activity"]


def test_deselecting_everything_falls_back_to_all():
    spec = BY_NAME["network_activity"]
    fig = build_figure(spec, ROWS, _state("network_activity", traces="nonsense"), theme="light")
    assert len(fig.data) == 2


def test_suspended_validators_are_bars_against_the_active_line():
    """As the handwritten chart draws it: a line for the running count, bars
    for the suspended ones. A second line would read as two comparable
    series rather than a count and a subset of it."""
    spec = BY_NAME["staking_validator_count"]
    rows = [{"date": "2026-10-01", "validator_count": 122, "suspended_count": 45}]
    fig = build_figure(spec, rows, _state("staking_validator_count"), theme="light")
    drawn = {t.name: type(t).__name__ for t in fig.data}
    assert drawn["Active Validators"] == "Scatter"
    assert drawn["Suspended Validators"] == "Bar"


def test_the_validator_counts_group_by_their_closing_day():
    """A count of validators is a snapshot; a week of them summed would
    report seven times the chain."""
    from ccdexplorer.charts import Agg

    spec = BY_NAME["staking_validator_count"]
    for series in spec.series:
        assert series.agg is Agg.LAST, series.key
