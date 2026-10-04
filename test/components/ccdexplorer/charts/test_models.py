"""The shape of a chart spec.

`agg` has no default on purpose: a series whose aggregation rule was forgotten
is the one bug in this system that produces a plausible-looking chart that is
seven times wrong, so it must fail at import rather than at render.
"""

import datetime as dt

import pytest
from pydantic import ValidationError

from ccdexplorer.charts import Agg, ChartSpec, Grouping, Kind, Series, Window


def _series(**kw):
    base = {"key": "fee_for_day", "label": "Fees", "colour": "#549FF2", "agg": Agg.SUM}
    return Series(**{**base, **kw})


def test_series_requires_an_aggregation_rule():
    with pytest.raises(ValidationError):
        Series(key="fee_for_day", label="Fees", colour="#549FF2")


def test_series_is_frozen():
    series = _series()
    with pytest.raises(ValidationError):
        series.agg = Agg.LAST


def test_spec_defaults_to_weekly_over_one_year():
    spec = ChartSpec(
        name="transaction_fees",
        slug="transaction-fees",
        title="Transaction fees",
        description="Fees paid on the chain over time.",
        blurb="Fees paid on the chain over time",
        category="chain",
        source="statistics_transaction_fees",
        series=(_series(),),
        chain_start=dt.date(2021, 6, 9),
    )
    assert spec.default_grouping is Grouping.WEEKLY
    assert spec.default_window is Window.Y1
    assert spec.kind is Kind.BAR
    assert Grouping.DAILY in spec.groupings


def test_intraday_spec_offers_no_calendar_grouping():
    spec = ChartSpec(
        name="ccd_kraken_1m",
        slug="ccd-kraken-1m",
        title="CCD on Kraken, 1m",
        description="1m candles and volume from the order book.",
        blurb="1m candles and volume from the order book",
        category="exchanges",
        source="",
        series=(),
        groupings=(),
        kind=Kind.CANDLE,
        chain_start=dt.date(2021, 6, 9),
    )
    assert spec.groupings == ()
