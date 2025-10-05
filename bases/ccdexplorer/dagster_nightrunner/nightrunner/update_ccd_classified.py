from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.mongodb import MongoDB
from pymongo import ReplaceOne

from ..nightrunner.utils import (
    AnalysisType,
    get_df_from_git,
    get_exchanges,
    write_queue_to_collection,
)


def perform_data_for_ccd_classified(
    context, d_date: str, commits_by_day: dict, mongodb: MongoDB, grpcclient: GRPCClient
) -> dict:
    exchanges = get_exchanges(mongodb)
    analysis = AnalysisType.statistics_ccd_classified

    queue = []
    df = get_df_from_git(commits_by_day[d_date])
    if df is None:
        context.log.error(f"No data found for {d_date}.")
        return {}

    df["account_29"] = df["account"].str[:29]
    f_staked = df["staked_amount"] > 0.0
    # this is only available with the Sirius protocol and later
    if "delegation_target" in df.columns:
        f_no_delegators = df["delegation_target"].isna()
        pool_total_delegated = df[~f_no_delegators]["staked_amount"].sum()
    else:
        pool_total_delegated = 0

    filters = {}
    for key, address_list in exchanges.items():
        filters[key] = df["account_29"].isin(address_list)

    tradeable = {}
    for key, address_list in exchanges.items():
        tradeable[key] = df[filters[key]]["total_balance"].sum()
    tradeable_sum = sum(tradeable.values())

    staked = df[f_staked]["staked_amount"].sum() - pool_total_delegated
    total_supply = df["total_balance"].sum()

    unstaked = total_supply - staked - tradeable_sum - pool_total_delegated
    _id = f"{d_date}-{analysis.value}"
    dct = {
        "_id": _id,
        "type": analysis.value,
        "date": d_date,
        "total_supply": float(total_supply),
        "staked": float(staked),
        "unstaked": float(unstaked),
        "delegated": float(pool_total_delegated),
    }

    for key in exchanges:
        dct.update({key: float(tradeable[key])})

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
