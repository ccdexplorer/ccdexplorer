"""Three charts plot a computation of their fields, not the fields.

Drawn generically they came out as several raw lines -- fee stabilization
showed GTU_numerator at 1.2e19 against NRG_numerator at 1, which is not a
chart of anything. Each names a derivation instead, and the builder runs it
before drawing.

The numbers here are the ones the handwritten charts produce for the same
day, so a drift in either shows up as a failure.
"""

import pandas as pd
import pytest

from ccdexplorer.charts import ChartState
from ccdexplorer.charts.registry import BY_NAME
from ccdexplorer.ccdexplorer_site.app.routers.charts.generic import build_figure


def _state(name):
    return ChartState.from_query(BY_NAME[name], {})


def test_fee_stabilization_draws_one_cost_line():
    spec = BY_NAME["fee_stabilization"]
    rows = [{
        "date": "2026-09-30",
        "GTU_numerator": 1.2259715872310272e19,
        "GTU_denominator": 39688763837.0,
        "NRG_numerator": 1.0,
        "NRG_denominator": 50000.0,
    }]
    fig = build_figure(spec, rows, _state("fee_stabilization"), theme="light")
    assert len(fig.data) == 1
    assert fig.data[0].y[0] == pytest.approx(3.095142, rel=1e-5)


def test_fee_stabilization_is_drawn_on_a_log_axis():
    """The handwritten one is, and the shape is meaningless without it."""
    spec = BY_NAME["fee_stabilization"]
    rows = [{
        "date": "2026-09-30", "GTU_numerator": 1.2e19, "GTU_denominator": 4e10,
        "NRG_numerator": 1.0, "NRG_denominator": 50000.0,
    }]
    fig = build_figure(spec, rows, _state("fee_stabilization"), theme="light")
    assert fig.layout.yaxis.type == "log"


def test_percentage_staked_draws_a_percentage():
    spec = BY_NAME["staking_percentage_staked"]
    rows = [{"date": "2026-09-30", "staked": 6699344155.036469,
             "total_supply": 14681990677.777601}]
    fig = build_figure(spec, rows, _state("staking_percentage_staked"), theme="light")
    assert len(fig.data) == 1
    assert fig.data[0].y[0] == pytest.approx(45.629672, rel=1e-5)


def test_percentage_staked_does_not_draw_the_raw_amounts():
    """Six billion against fourteen billion is not what the chart is for."""
    spec = BY_NAME["staking_percentage_staked"]
    rows = [{"date": "2026-09-30", "staked": 1.0, "total_supply": 4.0}]
    fig = build_figure(spec, rows, _state("staking_percentage_staked"), theme="light")
    assert [t.name for t in fig.data] == ["Percentage staked"]


def test_validator_count_draws_active_not_registered():
    """Active is registered minus suspended: 122 - 45 = 77."""
    spec = BY_NAME["staking_validator_count"]
    rows = [{"date": "2026-09-30", "validator_count": 122, "suspended_count": 45}]
    fig = build_figure(spec, rows, _state("staking_validator_count"), theme="light")
    by_name = {t.name: list(t.y) for t in fig.data}
    assert by_name["Active Validators"] == [77]
    assert by_name["Suspended Validators"] == [45]


def test_accounts_growth_draws_the_level_as_well_as_the_growth():
    """The handwritten chart shows both; only the growth survived."""
    spec = BY_NAME["accounts_growth"]
    rows = [
        {"date": "2026-09-29", "account_level": 104779, "account_count": 3},
        {"date": "2026-09-30", "account_level": 104781, "account_count": 2},
    ]
    fig = build_figure(spec, rows, _state("accounts_growth"), theme="light")
    by_name = {t.name: list(t.y) for t in fig.data}
    assert by_name["Accounts On Chain"][-1] == 104781
    assert by_name["Account Growth"][-1] == 2


def test_a_derivation_with_a_missing_field_draws_nothing_rather_than_guessing():
    spec = BY_NAME["staking_percentage_staked"]
    fig = build_figure(spec, [{"date": "2026-09-30", "staked": 1.0}], _state("staking_percentage_staked"), theme="light")
    assert fig.data == ()


def test_a_zero_denominator_does_not_blow_up():
    spec = BY_NAME["staking_percentage_staked"]
    rows = [{"date": "2026-09-30", "staked": 1.0, "total_supply": 0.0}]
    fig = build_figure(spec, rows, _state("staking_percentage_staked"), theme="light")
    assert isinstance(fig, type(build_figure(spec, rows, _state("staking_percentage_staked"), theme="light")))


def test_active_validators_are_drawn_before_suspension_existed():
    """suspended_count was added in March 2025; the first four years of
    documents have no such field.

    Subtracting a missing column gives NaN, so the active line simply
    stopped existing before 2025 while the chart claimed to show all of
    history. The handwritten one filled with zero first.
    """
    spec = BY_NAME["staking_validator_count"]
    rows = [
        {"date": "2023-01-01", "validator_count": 200},           # no suspended_count
        {"date": "2026-10-01", "validator_count": 122, "suspended_count": 45},
    ]
    fig = build_figure(spec, rows, _state("staking_validator_count"), theme="light")
    active = next(t for t in fig.data if t.name == "Active Validators")
    assert list(active.y) == [200, 77], "the early years went missing"


def test_suspended_is_zero_rather_than_absent_before_it_existed():
    spec = BY_NAME["staking_validator_count"]
    rows = [{"date": "2023-01-01", "validator_count": 200}]
    fig = build_figure(spec, rows, _state("staking_validator_count"), theme="light")
    suspended = next(t for t in fig.data if t.name == "Suspended Validators")
    assert list(suspended.y) == [0]
