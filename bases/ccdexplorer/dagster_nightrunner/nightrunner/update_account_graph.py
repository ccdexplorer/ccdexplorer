import io
from io import StringIO

import pandas as pd
from ccdexplorer.mongodb import Collections, CollectionsUtilities, MongoDB
from dateutil import parser, relativedelta
from git import Commit
from pandas import DataFrame
from pymongo import ReplaceOne

from ..nightrunner.utils import (
    AnalysisType,
    write_queue_to_collection,
)


def get_all_account_addresses(mongodb: MongoDB):
    result = mongodb.mainnet[Collections.stable_address_info].find({})
    return {x["_id"]: x for x in result}


def get_exchange_rates_historical(mongodb: MongoDB):
    collection = mongodb.utilities

    coll = collection[CollectionsUtilities.exchange_rates_historical]
    exchange_rates_historical = {}
    result = coll.find({})
    for x in result:
        if x["token"] not in exchange_rates_historical:
            exchange_rates_historical[x["token"]] = {}
        token_dict = exchange_rates_historical[x["token"]]
        token_dict[x["date"]] = x["rate"]
        exchange_rates_historical[x["token"]] = token_dict

    return exchange_rates_historical


def get_df_from_git(commit: Commit | None) -> DataFrame | None:
    if not commit:
        return None
    targetfile = commit.tree / "accounts.csv"
    with io.BytesIO(targetfile.data_stream.read()) as f:
        my_file = f.read().decode("utf-8")
    data = StringIO(my_file)
    df = pd.read_csv(data, low_memory=False)

    return df


def get_date_from_git(commit: Commit) -> str:
    timestamp = parser.parse(commit.message)
    d_date = f"{timestamp:%Y-%m-%d}"
    return d_date


def perform_data_for_account_graph(
    context, d_date: str, commits_by_day: dict, mongodb: MongoDB
) -> dict:
    all_account_addresses = get_all_account_addresses(mongodb)
    exchange_rates_historical = get_exchange_rates_historical(mongodb)
    exchange_rates_ccd = exchange_rates_historical.get("CCD", {})
    analysis = AnalysisType.account_graph
    # commits_by_day = get_commits_by_day()
    queue = []
    df_today = get_df_from_git(commits_by_day[d_date])
    if df_today is None:
        context.log.error(f"No data found for {d_date}.")
        return {}

    today = {x["account"][:29]: x for x in df_today.to_dict(orient="records")}
    previous_day = (parser.parse(d_date) - relativedelta.relativedelta(days=1)).strftime("%Y-%m-%d")
    try:
        df_yesterday = get_df_from_git(commits_by_day.get(previous_day))
        yesterday = {
            x["account"][:29]: x
            for x in df_yesterday.to_dict(orient="records")  # type: ignore
        }
    except Exception as _:
        yesterday = None

    _id = f"{d_date}-{analysis.value}"

    fx_rate_for_day = exchange_rates_ccd.get(d_date, None)
    if fx_rate_for_day is None:
        context.log.warning(
            f"Missing exchange rate for {d_date}, skipping USD balance calculation."
        )
        raise Exception(f"Missing exchange rate for {d_date}, skipping USD balance calculation.")

    context.log.info(
        f"USD balance for {len(today)} accounts on {d_date} with rate {fx_rate_for_day}"
    )
    for account_address, values in today.items():
        # If the account address is not in the all_account_addresses, skip it
        if account_address not in all_account_addresses:
            context.log.info(
                f"Skipping account address {account_address} not in all_account_addresses."
            )
            continue
        _id = f"{d_date}-{analysis.value}-{account_address}"
        total_balance = values["total_balance"]

        # We only want to store balances that have changed, or are new.
        if not yesterday:
            include_address = True
        else:
            if account_address in yesterday:
                include_address = (
                    values["total_balance"] != yesterday[account_address]["total_balance"]
                )
            else:
                include_address = True

        if not include_address:
            continue

        if fx_rate_for_day is not None:
            ccd_balance_in_USD = total_balance * fx_rate_for_day
        else:
            ccd_balance_in_USD = None
        dct = {
            "_id": _id,
            "type": analysis.value,
            "account_address_canonical": account_address[:29],
            "account_index": all_account_addresses[account_address[:29]].get("account_index", None),
            "date": d_date,
            "ccd_balance": total_balance,
            "rate": fx_rate_for_day,
            "ccd_balance_in_USD": ccd_balance_in_USD,
        }

        queue.append(
            ReplaceOne(
                {"_id": _id},
                replacement=dct,
                upsert=True,
            )
        )

    # update the date helper
    _ = mongodb.mainnet[Collections.statistics].bulk_write(
        [
            ReplaceOne(
                {"_id": f"account_graph_dates-{d_date}"},
                replacement={
                    "_id": f"account_graph_dates-{d_date}",
                    "date": d_date,
                    "type": "account_graph_dates",
                    "complete": True,
                },
                upsert=True,
            )
        ]
    )
    write_queue_to_collection(mongodb, queue, analysis)
    context.log.info(
        f"USD balance for {len(today)} accounts on {d_date} with rate {fx_rate_for_day}"
    )
    return {
        "message": f"USD balance for {len(today)} accounts on {d_date} with rate {fx_rate_for_day}"
    }
