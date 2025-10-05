import datetime as dt
import dagster as dg
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.mongodb import MongoDB, Collections
from pymongo import ReplaceOne

from dateutil import parser

from ccdexplorer.env import *  # type: ignore # noqa: E402, F403
from .utils import calc_apy_for_period


def fill_daily_apy_averages_for_date(
    context: dg.AssetExecutionContext,
    mongodb: MongoDB,
    grpcclient: GRPCClient,
    # payday_info: PaydayInfo,
    date: str,
    period: int,
):
    """
    Calculating averages for daily APY for bakers and delegators for a given payday.
    """
    # all accounts that need APY calculations
    entries = list(
        mongodb.mainnet[Collections.paydays_v2_apy].aggregate(
            [{"$match": {"date": date, "days_in_average": 1}}]
        )
    )
    if len(entries) == 0:
        context.log.info(f"(Payday: {date}) | Not started / not fully processed yet...")
        return

    context.log.info(
        f"(Payday: {date}) | Found {len(entries):,.0f} entries to process for average APY for {period} days..."
    )  # type: ignore
    start_date = parser.parse(date) - dt.timedelta(days=period)
    start_str = start_date.strftime("%Y-%m-%d")

    queue = []
    for entry in entries:
        entry_id = entry["_id"]
        entry_type = entry["type"]
        # if index_in_list < period:
        #     continue
        if entry_type == "account":
            reward_receiver = entry["account_id"]
            reward_receiver_label = "account_id"
        elif entry_type in ["validator", "delegator", "passive_delegation"]:
            reward_receiver = entry["validator_id"]
            reward_receiver_label = "validator_id"
        else:
            context.log.error(f"Unknown entry type: {entry_type} for entry: {entry}")
            continue

        # Compute start of period-day window

        pipeline = [
            {
                "$match": {
                    reward_receiver_label: reward_receiver,
                    "days_in_average": 1,
                    "type": entry_type,
                    "date": {"$gt": start_str, "$lte": date},
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "apy": 1,
                    "date": 1,
                    "reward": 1,
                }
            },
            {"$sort": {"date": -1}},
        ]

        # Pull matching docs using aggregation pipeline
        docs = list(mongodb.mainnet[Collections.paydays_v2_apy].aggregate(pipeline))
        apis = [x["apy"] for x in docs]
        count_of_days = len(docs)
        sum_of_rewards = sum(doc["reward"] for doc in docs)
        _id = f"{entry_id}-{period} days"
        period_apy_dct = {
            "_id": _id,  # type: ignore
            reward_receiver_label: reward_receiver,
            "date": date,
            "apy": calc_apy_for_period(apis),
            "days_in_average": period,
            "type": entry_type,
            "sum_of_rewards": sum_of_rewards,
            "count_of_days": count_of_days,
        }
        queue.append(ReplaceOne({"_id": _id}, period_apy_dct, upsert=True))

    # BULK_WRITE
    if len(queue) > 0:
        _ = mongodb.mainnet[Collections.paydays_v2_apy].bulk_write(queue)

    context.log.info(
        f"(Payday: {date}) | Average APY for {period} days...done.| Processed {len(entries):,.0f} entries."
    )  # type: ignore
