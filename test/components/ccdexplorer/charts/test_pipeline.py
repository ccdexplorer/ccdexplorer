"""Grouping days into buckets, in Mongo rather than pandas.

These are the tests that catch a level series being summed. The numbers are
hand-computed from the fixture below and written out literally -- a test that
recomputes the thing it is checking checks nothing.
"""

import datetime as dt
import os

import pandas as pd
import pytest

from ccdexplorer.charts import Agg, ChartSpec, Grouping, Series
from ccdexplorer.charts.pipeline import build_grouping_pipeline, mongo_unit


def _spec(*series, **kw):
    return ChartSpec(
        name="t",
        slug="t",
        title="T",
        description="d",
        blurb="b",
        category="chain",
        source="statistics_test",
        series=series,
        chain_start=dt.date(2021, 6, 9),
        **kw,
    )


def test_mongo_unit_maps_each_grouping():
    assert mongo_unit(Grouping.DAILY) == "day"
    assert mongo_unit(Grouping.WEEKLY) == "week"
    assert mongo_unit(Grouping.MONTHLY) == "month"


def test_match_is_a_range_not_a_list_of_every_date():
    """The old endpoint built an $in of ~1900 date strings for an all-time query."""
    spec = _spec(Series(key="f", label="F", colour="#fff", agg=Agg.SUM))
    pipeline = build_grouping_pipeline(spec, "2026-01-01", "2026-12-31", Grouping.WEEKLY)
    match = pipeline[0]["$match"]
    assert match["date"] == {"$gte": "2026-01-01", "$lte": "2026-12-31"}
    assert match["type"] == "statistics_test"


def test_weeks_start_on_monday():
    spec = _spec(Series(key="f", label="F", colour="#fff", agg=Agg.SUM))
    pipeline = build_grouping_pipeline(spec, "2026-01-01", "2026-01-31", Grouping.WEEKLY)
    group_id = _stage(pipeline, "$group")["_id"]
    assert group_id["$dateTrunc"]["startOfWeek"] == "monday"
    assert group_id["$dateTrunc"]["unit"] == "week"


def test_sort_precedes_group_so_last_is_meaningful():
    """$last takes the final document in the group's input order."""
    spec = _spec(Series(key="f", label="F", colour="#fff", agg=Agg.LAST))
    pipeline = build_grouping_pipeline(spec, "2026-01-01", "2026-01-31", Grouping.WEEKLY)
    stages = [next(iter(s)) for s in pipeline]
    assert stages.index("$sort") < stages.index("$group")


@pytest.mark.parametrize(
    "agg,expected",
    [
        (Agg.SUM, {"$sum": "$f"}),
        (Agg.LAST, {"$last": "$f"}),
        (Agg.MEAN, {"$avg": "$f"}),
    ],
)
def test_each_rule_emits_its_operator(agg, expected):
    spec = _spec(Series(key="f", label="F", colour="#fff", agg=agg))
    pipeline = build_grouping_pipeline(spec, "2026-01-01", "2026-01-31", Grouping.WEEKLY)
    assert _stage(pipeline, "$group")["f"] == expected


def test_string_fields_are_cast_before_summing():
    """statistics_microccd stores its four fields as strings."""
    spec = _spec(
        Series(key="GTU_numerator", label="N", colour="#fff", agg=Agg.LAST, cast_to_double=True)
    )
    pipeline = build_grouping_pipeline(spec, "2026-01-01", "2026-01-31", Grouping.WEEKLY)
    assert _stage(pipeline, "$group")["GTU_numerator"] == {"$last": {"$toDouble": "$GTU_numerator"}}


def test_rollup_series_sums_its_source_fields():
    spec = _spec(
        Series(
            key="transfer",
            label="Transfer",
            colour="#fff",
            agg=Agg.SUM,
            source_fields=("account_transfer", "transferred_to_public"),
        )
    )
    pipeline = build_grouping_pipeline(spec, "2026-01-01", "2026-01-31", Grouping.WEEKLY)
    assert _stage(pipeline, "$group")["transfer"] == {
        "$sum": {
            "$add": [
                {"$ifNull": ["$account_transfer", 0]},
                {"$ifNull": ["$transferred_to_public", 0]},
            ]
        }
    }


def test_delta_of_last_adds_a_window_stage_after_the_group():
    """accounts_per_day: account_count is cumulative, the chart plots growth."""
    spec = _spec(
        Series(key="account_count", label="Accounts", colour="#fff", agg=Agg.DELTA_OF_LAST)
    )
    pipeline = build_grouping_pipeline(spec, "2026-01-01", "2026-01-31", Grouping.WEEKLY)
    stages = [next(iter(s)) for s in pipeline]
    assert "$setWindowFields" in stages
    assert stages.index("$group") < stages.index("$setWindowFields")
    window = _stage(pipeline, "$setWindowFields")
    assert window["sortBy"] == {"_id": 1}
    assert window["output"]["_prev_account_count"]["$shift"]["by"] == -1


def test_empty_buckets_are_filled_so_bars_stay_contiguous():
    """Review Focus 1.

    Pandas Grouper emitted a zero row for a day with no data. Mongo $group
    omits the bucket entirely, which would draw two non-adjacent weeks side by
    side as though nothing had happened between them.
    """
    spec = _spec(Series(key="f", label="F", colour="#fff", agg=Agg.SUM))
    pipeline = build_grouping_pipeline(
        spec, "2026-01-01", "2026-01-31", Grouping.WEEKLY, fill_gaps=True
    )
    densify = _stage(pipeline, "$densify")
    assert densify["field"] == "_id"
    assert densify["range"]["unit"] == "week"
    assert densify["range"]["bounds"] == "full"
    fill = _stage(pipeline, "$fill")
    assert fill["output"]["f"] == {"value": 0}


def test_partial_trailing_bucket_is_flagged_not_hidden():
    """Review Focus 2.

    On a Wednesday the current week holds three days, so a flow series ends on
    a cliff that is an artefact of the calendar. The pipeline reports each
    bucket's day count so the renderer can mark the last bar as partial.
    """
    spec = _spec(Series(key="f", label="F", colour="#fff", agg=Agg.SUM))
    pipeline = build_grouping_pipeline(spec, "2026-01-01", "2026-01-31", Grouping.WEEKLY)
    assert _stage(pipeline, "$group")["_days"] == {"$sum": 1}


def test_range_shorter_than_one_bucket_still_groups():
    """Review Focus 5: three days, grouped monthly, is one honest partial bar."""
    spec = _spec(Series(key="f", label="F", colour="#fff", agg=Agg.SUM))
    pipeline = build_grouping_pipeline(spec, "2026-03-01", "2026-03-03", Grouping.MONTHLY)
    assert _stage(pipeline, "$group")["_id"]["$dateTrunc"]["unit"] == "month"
    assert pipeline[0]["$match"]["date"] == {"$gte": "2026-03-01", "$lte": "2026-03-03"}


def _stage(pipeline, name):
    for stage in pipeline:
        if name in stage:
            return stage[name]
    raise AssertionError(f"no {name} stage in {[next(iter(s)) for s in pipeline]}")


# Week boundaries must land where pandas put them.
#
# Run against a real Mongo because $dateTrunc's week boundaries are the thing
# being checked, and a hand-written expectation of them would just be the same
# assumption twice.
#
# Fed through $documents rather than a collection: TEST_MONGO_URI points at a
# secondary, and this needs no writes -- $dateTrunc is a pure function of the
# date, so the fixture can be handed to the server inline.


# Scoped to this one test, not the module: the thirteen above are pure dict
# construction and must run everywhere, including the pre-commit hook.
@pytest.mark.skipif(not os.environ.get("TEST_MONGO_URI"), reason="needs TEST_MONGO_URI")
def test_week_buckets_match_pandas_w_mon():
    from pymongo import MongoClient

    # Fourteen consecutive days, one unit each, starting mid-week so a
    # boundary error cannot hide.
    days = pd.date_range("2026-01-07", periods=14, freq="D")
    rows = [{"date": d.strftime("%Y-%m-%d"), "f": 1} for d in days]

    spec = _spec(Series(key="f", label="F", colour="#fff", agg=Agg.SUM))
    pipeline = build_grouping_pipeline(
        spec, "2026-01-07", "2026-01-20", Grouping.WEEKLY, fill_gaps=False
    )
    # The $match selected the documents; $documents supplies them instead.
    assert "$match" in pipeline[0]
    inline = [{"$documents": rows}] + pipeline[1:]

    client = MongoClient(os.environ["TEST_MONGO_URI"], serverSelectionTimeoutMS=10000)
    mongo_buckets = {r["_id"].date(): r["f"] for r in client["admin"].aggregate(inline)}

    frame = pd.DataFrame({"date": days, "f": 1})
    pandas_buckets = (
        frame.groupby(pd.Grouper(key="date", freq="W-MON", label="left", closed="left"))
        .sum()
        .reset_index()
    )
    expected = {row.date.date(): int(row.f) for row in pandas_buckets.itertuples()}

    assert mongo_buckets == expected


def test_plt_refuses_the_generic_pipeline_rather_than_inventing_a_number():
    """statistics_plt stores a dict keyed by token symbol, not flat fields.

    Its published figure takes each token's last value in the bucket, carries
    a token that stopped reporting forward, and only sums what actually tracks
    a currency. The generic pipeline does none of that, and the number it
    would produce looks entirely reasonable -- so it refuses instead. Wiring
    this up belongs with the task that can check it against the live page.
    """
    from ccdexplorer.charts.registry import BY_NAME

    with pytest.raises(NotImplementedError, match="statistics_plt"):
        build_grouping_pipeline(BY_NAME["plt_tvl"], "2026-01-01", "2026-01-31", Grouping.WEEKLY)


def test_a_flow_collapsed_with_last_still_fills_empty_buckets_with_zero():
    """Review follow-up: the fill rule must key on what the series MEANS, not
    on how it collapses.

    active_addresses uses LAST for a mechanical reason -- its source is
    already grouped by period, so a bucket holds exactly one document -- but
    it is a per-period count, a flow. Carrying it forward would draw a week
    the nightly job missed at the previous week's value: indistinguishable
    from real activity, which is the same silent plausibility this component
    exists to prevent.
    """
    spec = _spec(Series(key="n", label="N", colour="#fff", agg=Agg.LAST, fills_with_zero=True))
    pipeline = build_grouping_pipeline(spec, "2026-01-01", "2026-01-31", Grouping.WEEKLY)
    assert _stage(pipeline, "$fill")["output"]["n"] == {"value": 0}


def test_a_level_collapsed_with_last_is_still_carried_forward():
    spec = _spec(Series(key="n", label="N", colour="#fff", agg=Agg.LAST))
    pipeline = build_grouping_pipeline(spec, "2026-01-01", "2026-01-31", Grouping.WEEKLY)
    assert _stage(pipeline, "$fill")["output"]["n"] == {"method": "locf"}


def test_active_addresses_does_not_repeat_a_missing_week():
    from ccdexplorer.charts.registry import BY_NAME

    spec = BY_NAME["active_addresses"]
    pipeline = build_grouping_pipeline(spec, "2026-01-01", "2026-03-31", Grouping.WEEKLY)
    for series in spec.series:
        assert _stage(pipeline, "$fill")["output"][series.key] == {"value": 0}, series.key


def test_a_field_whose_name_contains_a_dot_is_read_literally():
    """`$gate.io` means the `io` subfield of `gate`, not the top-level field
    named "gate.io" -- so $ifNull quietly returned 0 for an exchange that had
    a wallet. $getField addresses the name itself.

    The distinction cannot be read off the string: active_addresses' dotted
    source_fields ARE genuine nesting, so the series has to say which it is.
    """
    spec = _spec(
        Series(
            key="gate_io",
            label="Gate.io",
            colour="#fff",
            agg=Agg.LAST,
            source_fields=("gate.io",),
            literal_field=True,
        )
    )
    pipeline = build_grouping_pipeline(spec, "2026-01-01", "2026-01-31", Grouping.WEEKLY)
    expr = _stage(pipeline, "$group")["gate_io"]["$last"]
    assert "$getField" in str(expr), expr
    assert "gate.io" in str(expr)


def test_a_dotted_path_that_is_real_nesting_still_uses_a_path():
    spec = _spec(
        Series(
            key="address",
            label="A",
            colour="#fff",
            agg=Agg.LAST,
            source_fields=("unique_impacted_address_count.address",),
        )
    )
    pipeline = build_grouping_pipeline(spec, "2026-01-01", "2026-01-31", Grouping.WEEKLY)
    expr = str(_stage(pipeline, "$group")["address"]["$last"])
    assert "$getField" not in expr
    assert "$unique_impacted_address_count.address" in expr


@pytest.mark.skipif(not os.environ.get("TEST_MONGO_URI"), reason="needs TEST_MONGO_URI")
def test_the_dotted_field_really_comes_back_from_mongo():
    """The regression that prompted this: a real value of 1 read as 0.

    Against the real collection rather than a $documents fixture, because
    $documents validates its keys as field paths and refuses to hold one
    containing a dot -- while a stored document may, which is the whole
    problem. Read-only, and asserted against the raw documents for the same
    week so it cannot drift with the data.
    """
    from pymongo import MongoClient

    from ccdexplorer.charts.registry import BY_SOURCE

    client = MongoClient(os.environ["TEST_MONGO_URI"], serverSelectionTimeoutMS=10000)
    col = client["concordium_mainnet"]["statistics"]
    start, end = "2026-09-07", "2026-09-13"

    raw = list(
        col.find(
            {"type": "statistics_exchange_wallets", "date": {"$gte": start, "$lte": end}}
        ).sort("date", 1)
    )
    if not raw:
        pytest.skip("no exchange-wallet documents for that week")
    expected = raw[-1].get("gate.io")
    assert expected is not None, "fixture week has no gate.io field to check"

    spec = BY_SOURCE["statistics_exchange_wallets"]
    grouped = list(col.aggregate(build_grouping_pipeline(spec, start, end, Grouping.WEEKLY)))

    assert grouped[0]["gate_io"] == expected, (
        "the dotted field read as something else -- $gate.io means the io "
        "subfield of gate, and $ifNull turns that miss into a zero"
    )
    assert grouped[0]["kraken"] == raw[-1]["kraken"]
