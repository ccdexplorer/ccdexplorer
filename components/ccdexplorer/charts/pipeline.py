"""A chart spec, as a Mongo aggregation pipeline.

Pure dict construction -- no pymongo here, so this is testable without a
database and the component stays importable by the bot.

Grouping used to happen in pandas, after fetching every daily document over
HTTP. Two things made that worth moving: an all-time query built a $in of
roughly 1900 date strings, and every reader paid the grouping again.
"""

from .models import Agg, ChartSpec, Grouping, Series

#: $dateTrunc's vocabulary for each grouping.
_UNITS = {Grouping.DAILY: "day", Grouping.WEEKLY: "week", Grouping.MONTHLY: "month"}

#: Monday, matching the pandas "W-MON" the image routes have always used. A
#: chart that silently shifted its week boundaries would disagree with its own
#: published history by up to six days.
START_OF_WEEK = "monday"


def mongo_unit(grouping: Grouping) -> str:
    """The $dateTrunc unit for this grouping."""
    return _UNITS[grouping]


def _read(field: str, literal: bool) -> dict | str:
    """One field, as an expression that actually reaches it.

    A dot in a path normally means nesting, so "$a.b" is b inside a. For a
    top-level field whose own name contains a dot -- statistics_exchange_wallets
    has one called "gate.io" -- that reads a document that is not there, and
    the $ifNull below turns the miss into a zero. $getField addresses the name.
    """
    if literal:
        return {"$getField": {"field": {"$literal": field}, "input": "$$ROOT"}}
    return f"${field}"


def _field_expr(series: Series) -> dict | str:
    """The value being aggregated, before the accumulator wraps it."""
    if series.source_fields:
        # A rollup. $ifNull because a source column absent from an early
        # document would otherwise make the whole sum null -- these columns
        # appear as the chain gains transaction types.
        return {
            "$add": [{"$ifNull": [_read(f, series.literal_field), 0]} for f in series.source_fields]
        }
    if series.cast_to_double:
        return {"$toDouble": f"${series.key}"}
    return f"${series.key}"


def _accumulator(series: Series) -> dict:
    expr = _field_expr(series)
    if series.agg is Agg.SUM:
        return {"$sum": expr}
    if series.agg is Agg.MEAN:
        return {"$avg": expr}
    # LAST and DELTA_OF_LAST both close the bucket on its final day; the delta
    # is taken afterwards, once neighbouring buckets exist to subtract.
    return {"$last": expr}


#: statistics_plt stores one nested document per token symbol rather than flat
#: fields, and its published figure does three things the generic pipeline does
#: not: it takes each token's last value within the bucket, carries a token
#: that stopped reporting forward rather than letting the total drop, and sums
#: only the tokens that actually track a currency. A generic $group over it
#: produces a number that looks entirely reasonable and is not the TVL.
PLT_SOURCE = "statistics_plt"


def build_grouping_pipeline(
    spec: ChartSpec,
    start: str,
    end: str,
    grouping: Grouping,
    *,
    fill_gaps: bool = True,
) -> list[dict]:
    """Daily documents in [start, end], collapsed into `grouping` buckets.

    Dates are ISO strings and sort lexicographically, so the range match uses
    the existing type_1_date_1 index directly.
    """
    if spec.source == PLT_SOURCE:
        raise NotImplementedError(
            f"{PLT_SOURCE} needs per-token last, forward-fill and a "
            "stablecoin_tracks filter; it still renders through pandas. "
            "Wire it up with the task that can check it against the live page."
        )
    unit = mongo_unit(grouping)
    pipeline: list[dict] = [
        {"$match": {"type": spec.source_for(grouping), "date": {"$gte": start, "$lte": end}}},
        # Before $group, because $last means "the last document this group
        # saw" and nothing else orders them.
        {"$sort": {"date": 1}},
        {"$addFields": {"_d": {"$dateFromString": {"dateString": "$date"}}}},
        {
            "$group": {
                "_id": {"$dateTrunc": {"date": "$_d", "unit": unit, "startOfWeek": START_OF_WEEK}},
                # How many days actually landed in this bucket. The renderer
                # marks a short trailing bucket as partial rather than letting
                # an incomplete week read as a collapse in activity.
                "_days": {"$sum": 1},
                **{s.key: _accumulator(s) for s in spec.series},
            }
        },
        {"$sort": {"_id": 1}},
    ]

    if fill_gaps:
        # $group drops a bucket with no documents entirely, so two
        # non-adjacent weeks would be drawn as neighbours. Pandas zero-filled;
        # this restores that.
        pipeline.append(
            {"$densify": {"field": "_id", "range": {"step": 1, "unit": unit, "bounds": "full"}}}
        )
        pipeline.append(
            {
                "$fill": {
                    "sortBy": {"_id": 1},
                    "output": {
                        "_days": {"value": 0},
                        **{s.key: _fill_rule(s) for s in spec.series},
                    },
                }
            }
        )

    deltas = [s for s in spec.series if s.agg is Agg.DELTA_OF_LAST]
    if deltas:
        pipeline.append(
            {
                "$setWindowFields": {
                    "sortBy": {"_id": 1},
                    "output": {
                        f"_prev_{s.key}": {"$shift": {"output": f"${s.key}", "by": -1}}
                        for s in deltas
                    },
                }
            }
        )
        pipeline.append(
            {
                "$addFields": {
                    s.key: {
                        "$cond": [
                            {"$eq": [f"$_prev_{s.key}", None]},
                            None,  # the first bucket has nothing to subtract from
                            {"$subtract": [f"${s.key}", f"$_prev_{s.key}"]},
                        ]
                    }
                    for s in deltas
                }
            }
        )

    return pipeline


def _fill_rule(series: Series) -> dict:
    """What an empty bucket holds.

    Zero for a flow: nothing happened that week. Carried forward for a level:
    the validator count did not drop to zero, the job simply did not write.

    Asked of the series rather than inferred from `agg` here, because the two
    can disagree -- a per-period count whose source is pre-grouped collapses
    with LAST and is still a flow.
    """
    return {"value": 0} if series.empty_bucket_is_zero else {"method": "locf"}
