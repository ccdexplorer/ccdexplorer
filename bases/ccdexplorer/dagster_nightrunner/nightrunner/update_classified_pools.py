from ccdexplorer.mongodb import MongoDB
from pymongo import ReplaceOne

from ..nightrunner.utils import AnalysisType, get_df_from_git, write_queue_to_collection


def perform_data_for_classified_pools(
    context, d_date: str, commits_by_day: dict, mongodb: MongoDB
) -> dict:
    analysis = AnalysisType.statistics_classified_pools

    queue = []
    df = get_df_from_git(commits_by_day[d_date])

    if df is None:
        context.log.error(f"No data found for {d_date}.")
        return {}

    _id = f"{d_date}-{analysis.value}"
    context.log.info(_id)

    if len(df[df["pool_status"] == "openForAll"]) > 0:
        open_pool_count = len(df[df["pool_status"] == "openForAll"])
        closed_pool_count = len(df[df["pool_status"] == "closedForAll"])
        closed_new_pool_count = len(df[df["pool_status"] == "closedForNew"])
    else:
        open_pool_count = len(df[df["pool_status"] == "open_for_all"])
        closed_pool_count = len(df[df["pool_status"] == "closed_for_all"])
        closed_new_pool_count = len(df[df["pool_status"] == "closed_for_new"])

    f_no_delegators = df["delegation_target"].isna()
    delegator_count = len(df[~f_no_delegators])
    delegator_avg_stake = df[~f_no_delegators]["staked_amount"].mean()
    if open_pool_count > 0:
        delegator_avg_count_per_pool = delegator_count / open_pool_count
    else:
        delegator_avg_count_per_pool = 0
    pool_total_delegated = df[~f_no_delegators]["staked_amount"].sum()
    pass
    dct = {
        "_id": _id,
        "type": analysis.value,
        "date": d_date,
        "open_pool_count": open_pool_count,
        "closed_pool_count": closed_pool_count,
        "closed_new_pool_count": closed_new_pool_count,
        "delegator_count": delegator_count,
        "delegator_avg_stake": delegator_avg_stake,
        "delegator_avg_count_per_pool": delegator_avg_count_per_pool,
        "pool_total_delegated": pool_total_delegated,
    }
    summary = (
        f"date: {d_date}, open: {open_pool_count}, closed: {closed_pool_count}, "
        f"closed_new: {closed_new_pool_count}, delegators: {delegator_count}, "
        f"avg_stake: {delegator_avg_stake:.2f}, avg_per_pool: {delegator_avg_count_per_pool:.2f}, "
        f"total_delegated: {pool_total_delegated:.2f}"
    )
    queue.append(
        ReplaceOne(
            {"_id": _id},
            replacement=dct,
            upsert=True,
        )
    )
    write_queue_to_collection(mongodb, queue, analysis)
    context.log.info(summary)
    return {"message": summary}
