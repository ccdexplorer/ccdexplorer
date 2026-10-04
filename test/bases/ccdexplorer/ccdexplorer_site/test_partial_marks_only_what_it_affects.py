"""A short period understates a total. It does not spoil a snapshot.

One day is missing from five years of statistics -- 2025-09-15 -- and that
week was drawn faded on the validator count, which reads as something being
wrong with it. Nothing is: the week's closing count is Sunday's number
whether or not Monday was recorded.

A sum is different. Six days of fees in a seven-day bar really is less than
a week's worth, and that is worth saying.
"""

import pytest

from ccdexplorer.charts import Agg, ChartState
from ccdexplorer.charts.registry import BY_NAME
from ccdexplorer.ccdexplorer_site.app.routers.charts.generic import build_figure


def _state(name):
    return ChartState.from_query(BY_NAME[name], {})


def _opacities(fig, name):
    trace = next(t for t in fig.data if t.name == name)
    marker = trace.marker
    return list(marker.opacity) if marker.opacity is not None else None


def test_a_short_week_does_not_fade_a_level():
    """Validator count: the closing value is right either way."""
    spec = BY_NAME["staking_validator_count"]
    rows = [
        {"date": "2025-09-08", "_days": 7, "validator_count": 120, "suspended_count": 40},
        {"date": "2025-09-15", "_days": 6, "validator_count": 121, "suspended_count": 41},
    ]
    fig = build_figure(spec, rows, _state("staking_validator_count"), theme="light")
    assert _opacities(fig, "Suspended Validators") in (None, [1.0, 1.0])


def test_a_short_week_still_fades_a_sum():
    """Six days of fees in a seven-day bar really is short."""
    spec = BY_NAME["transaction_fees"]
    rows = [
        {"date": "2026-09-07", "_days": 7, "fee_for_day": 7_000_000},
        {"date": "2026-09-14", "_days": 4, "fee_for_day": 4_000_000},
    ]
    fig = build_figure(spec, rows, _state("transaction_fees"), theme="light")
    assert _opacities(fig, "Fees (CCD)") == [1.0, pytest.approx(0.45)]


def test_a_per_period_count_is_treated_as_a_sum():
    """Active addresses collapse with LAST for a mechanical reason but are a
    count of the period: fewer days means fewer addresses seen."""
    series = BY_NAME["active_addresses"].series[0]
    assert series.short_period_understates is True


def test_an_average_is_not_understated_by_a_short_period():
    series = BY_NAME["staking_avg_delegator_stake"].series[0]
    assert series.agg is Agg.MEAN
    assert series.short_period_understates is False


def test_growth_is_understated_by_a_short_period():
    """Fewer days is less growth, which is worth saying."""
    series = next(
        s for s in BY_NAME["accounts_growth"].series if s.agg is Agg.DELTA_OF_LAST
    )
    assert series.short_period_understates is True
