from enum import Enum
import datetime as dt
from ccdexplorer.mongodb import Collections, MongoDB
from pymongo import ReplaceOne
import calendar
from ..nightrunner.utils import (
    AnalysisType,
    get_start_end_block_from_date,
    write_queue_to_collection,
)


class Grouping(Enum):
    daily = "daily"
    weekly = "weekly"
    monthly = "monthly"


def get_month_bounds(d_date) -> tuple[str, str]:
    """
    Given a date string "YYYY-%m-%d", return a tuple (month_start, month_end),
    where month_start is the first day of that month and month_end is the last day
    of that month, both formatted as "YYYY-%m-%d".
    """
    # Parse the input into a date object
    date = dt.datetime.strptime(d_date, "%Y-%m-%d").date()

    # The first day of this month
    month_start = date.replace(day=1)

    # Find how many days are in this month
    last_day = calendar.monthrange(date.year, date.month)[1]
    month_end = date.replace(day=last_day)

    return f"{month_start:%Y-%m-%d}", f"{month_end:%Y-%m-%d}"


def find_week_bounds(d_date: str) -> tuple[str, str]:
    """
    Given date_str in "YYYY-%m-%d" format, return (week_start, week_end)
    where week_start is the Monday of that week and week_end is the following Sunday,
    both formatted as "YYYY-%m-%d".
    """
    # parse the input
    date = dt.datetime.strptime(d_date, "%Y-%m-%d").date()

    # weekday(): Monday=0, …, Sunday=6
    # so subtracting date.weekday() gives us that week’s Monday
    week_start = date - dt.timedelta(days=date.weekday())
    week_end = week_start + dt.timedelta(days=6)

    return f"{week_start:%Y-%m-%d}", f"{week_end:%Y-%m-%d}"


def perform_data_for_unique_addresses(
    context, d_date: str, grouping: str, mongodb: MongoDB
) -> dict:
    """
    Calculate transaction fees per day.
    """

    if grouping == "daily":
        analysis = AnalysisType.statistics_unique_addresses_v2_daily
        height_for_first_block, height_for_last_block = get_start_end_block_from_date(
            mongodb, d_date
        )
        context.log.info(
            f"Grouping: {grouping}, date: {d_date}, first block: {height_for_first_block:,.0f}, last block: {height_for_last_block:,.0f}"
        )
    elif grouping == "weekly":
        analysis = AnalysisType.statistics_unique_addresses_v2_weekly
        week_start_date, week_end_date = find_week_bounds(d_date)

        height_for_first_block, _ = get_start_end_block_from_date(mongodb, week_start_date)
        _, height_for_last_block = get_start_end_block_from_date(mongodb, week_end_date)
        context.log.info(
            f"Grouping: {grouping}, date: {d_date}, week_start: {week_start_date}, week_end: {week_end_date}, first block: {height_for_first_block:,.0f}, last block: {height_for_last_block:,.0f}"
        )
        d_date = week_start_date
    elif grouping == "monthly":
        analysis = AnalysisType.statistics_unique_addresses_v2_monthly
        month_start_date, month_end_date = get_month_bounds(d_date)

        height_for_first_block, _ = get_start_end_block_from_date(mongodb, month_start_date)
        _, height_for_last_block = get_start_end_block_from_date(mongodb, month_end_date)
        context.log.info(
            f"Grouping: {grouping}, date: {d_date}, month_start: {month_start_date}, month_end: {month_end_date}, first block: {height_for_first_block:,.0f}, last block: {height_for_last_block:,.0f}"
        )
        d_date = month_start_date
    else:
        Exception(f"Unknown grouping: {grouping}")
        return {"message": "Unknown grouping"}

    dct = calculate_unique_address_stats(
        context,
        analysis,
        d_date,
        height_for_first_block,
        height_for_last_block,
        mongodb,
    )
    context.log.info(dct)
    return {"message": dct}


def calculate_unique_address_stats(
    context,
    analysis: AnalysisType,
    d_date,
    height_for_first_block,
    height_for_last_block,
    mongodb: MongoDB,
    grouping=Grouping.daily,
    complete: bool = True,
) -> dict:
    _id = f"{d_date}-{analysis.value}"
    context.log.info(_id)
    pipeline = [
        {
            "$match": {
                "block_height": {
                    "$gte": height_for_first_block,
                    "$lte": height_for_last_block,
                },
                "effect_type": {"$ne": "Account Reward"},
            }
        },
        {
            "$project": {
                "address_length": {"$strLenCP": "$impacted_address_canonical"},
                "impacted_address_canonical": 1,
            }
        },
        {
            "$group": {
                "_id": {
                    "category": {
                        "$switch": {
                            "branches": [
                                {
                                    "case": {"$lt": ["$address_length", 29]},
                                    "then": "contract",
                                },
                                {
                                    "case": {
                                        "$and": [
                                            {"$gte": ["$address_length", 29]},
                                            {"$lt": ["$address_length", 64]},
                                        ]
                                    },
                                    "then": "address",
                                },
                                {
                                    "case": {"$eq": ["$address_length", 64]},
                                    "then": "public_key",
                                },
                            ],
                            "default": "other",
                        }
                    }
                },
                "unique_addresses": {"$addToSet": "$impacted_address_canonical"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "category": "$_id.category",
                "count": {"$size": "$unique_addresses"},
            }
        },
    ]
    result = mongodb.mainnet[Collections.impacted_addresses].aggregate(pipeline)
    ll = list(result)

    if len(ll) > 0:
        counts_by_category = {doc["category"]: doc["count"] for doc in ll}

    else:
        counts_by_category = {
            "address": 0,
            "contract": 0,
            "public_key": 0,
        }
    dct = {
        "_id": _id,
        "date": d_date,
        "type": analysis.value,
        "unique_impacted_address_count": counts_by_category,
    }

    if grouping.weekly or grouping.monthly:
        dct.update({"complete": complete})

    queue = [
        ReplaceOne(
            {"_id": _id},
            replacement=dct,
            upsert=True,
        )
    ]
    write_queue_to_collection(mongodb, queue, analysis)
    return dct
