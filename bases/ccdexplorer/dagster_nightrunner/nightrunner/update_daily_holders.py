import io
from io import StringIO

import pandas as pd
from ccdexplorer.mongodb import MongoDB
from dateutil import parser
from git import Commit
from pandas import DataFrame
from pymongo import ReplaceOne

from ..nightrunner.utils import (
    AnalysisType,
    write_queue_to_collection,
)


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


def perform_data_for_daily_holders(
    context, d_date: str, commits_by_day: dict, mongodb: MongoDB
) -> dict:
    analysis = AnalysisType.statistics_daily_holders

    queue = []
    df_today = get_df_from_git(commits_by_day[d_date])

    if df_today is None:
        context.log.error(f"No data found for {d_date}.")
        return {}

    _id = f"{d_date}-{analysis.value}"
    context.log.info(_id)

    dct: dict = {
        "_id": _id,
        "type": analysis.value,
        "date": d_date,
    }
    # the actual analysis
    for limit in [
        0,
        100,
        10_000,
        20_000,
        50_000,
        100_000,
        500_000,
        1_000_000,
        2_500_000,
        5_000_000,
        10_000_000,
        50_000_000,
        100_000_000,
    ]:
        f = df_today["total_balance"] >= limit
        count_accounts = int(df_today[f].count()["account"])
        sum_amount = int(sum(df_today[f]["total_balance"]))
        dct.update(
            {
                f"count_>={limit}": count_accounts,
                f"amount_>={limit}": sum_amount,
            }
        )
    # type: ignore
    queue.append(
        ReplaceOne(
            {"_id": _id},
            replacement=dct,
            upsert=True,
        )
    )
    context.log.info(f"{dct}")
    write_queue_to_collection(mongodb, queue, analysis)
    return dct
