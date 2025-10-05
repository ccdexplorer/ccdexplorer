from ccdexplorer.grpc_client import GRPCClient

from ccdexplorer.mongodb import MongoDB
from pymongo import ReplaceOne
import pandas as pd
from ..nightrunner.utils import (
    AnalysisType,
    get_df_from_git,
    write_queue_to_collection,
    get_total_amount_from_summary_for_date,
)


def calculate_activity(
    d_date_today: str,
    df_accounts_for_day: pd.DataFrame,
    df_accounts_for_yesterday: pd.DataFrame,
    d_date_yesterday: str,
    mongodb: MongoDB,
):
    df_accounts_for_day.set_index("account", inplace=True)
    d_today = df_accounts_for_day.to_dict("index")

    df_accounts_for_yesterday.set_index("account", inplace=True)
    d_yesterday = df_accounts_for_yesterday.to_dict("index")

    movement = 0
    for acc, row in d_today.items():
        today_value = row["total_balance"]
        yesterday_value = d_yesterday.get(acc, {"total_balance": 0.0})["total_balance"]
        movement += abs(today_value - yesterday_value)

    total_balance_yesterday = get_total_amount_from_summary_for_date(d_date_yesterday, mongodb)
    total_balance_today = get_total_amount_from_summary_for_date(d_date_today, mongodb)

    inflation = total_balance_today - total_balance_yesterday

    network_activity = max(0, (movement - inflation) / 2)

    return network_activity


def perform_data_for_network_activity(
    context, d_date: str, commits_by_day: dict, mongodb: MongoDB, grpcclient: GRPCClient
) -> dict:
    analysis = AnalysisType.statistics_network_activity
    _id = f"{d_date}-{analysis.value}"
    queue = []
    d_date_yesterday = pd.to_datetime(d_date) - pd.Timedelta(days=1)
    d_date_yesterday = d_date_yesterday.strftime("%Y-%m-%d")

    df_accounts_for_day = get_df_from_git(commits_by_day[d_date])
    if df_accounts_for_day is None:
        context.log.error(f"No data found for {d_date}.")
        return {}

    df_accounts_for_yesterday = get_df_from_git(commits_by_day[d_date_yesterday])
    if df_accounts_for_day is None:
        context.log.error(f"No data found for {d_date_yesterday}.")
        return {}

    _id = f"{d_date}-{analysis.value}"

    network_activity = calculate_activity(
        d_date,
        df_accounts_for_day,
        df_accounts_for_yesterday,  # type: ignore
        d_date_yesterday,
        mongodb,
    )

    dct = {
        "_id": _id,
        "date": d_date,
        "type": analysis.value,
        "network_activity": network_activity,
    }

    queue.append(
        ReplaceOne(
            {"_id": _id},
            replacement=dct,
            upsert=True,
        )
    )

    write_queue_to_collection(mongodb, queue, analysis)
    context.log.info(f"info: {dct}")
    return {"dct": dct}
