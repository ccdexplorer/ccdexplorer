from ccdexplorer.mongodb import Collections, MongoDB
from pymongo import ReplaceOne

from ..nightrunner.utils import AnalysisType, get_df_from_git, write_queue_to_collection


def passive_delegator_count(d_date: str, mongodb: MongoDB, grpcclient, net) -> int | None:
    """How many accounts delegate to the passive pool, or None if unknown.

    Not in the accounts snapshot this asset otherwise reads: it carries a
    delegation_target per account, and `delegator_count` is everyone who
    has one. Measured against mainnet for 2026-10-06, that is 1803 --
    1194 passive plus 609 across the 122 pools -- so the count the chart
    has always drawn includes passive delegators without saying so.

    Read at the day's final block, which is what lets it be backfilled.
    None rather than 0 when the day has no recorded block: zero is a
    reading, and a gap is not.
    """
    day = mongodb.mainnet[Collections.blocks_per_day].find_one({"date": d_date})
    if not day:
        return None
    block_hash = day["hash_for_last_block"]
    return len(list(grpcclient.get_delegators_for_passive_delegation(block_hash, net)))


def average_delegators_per_pool(
    delegator_count: int, passive: int | None, open_pools: int, closed_new_pools: int
) -> float | None:
    """Delegators per pool that can hold one, or None if unknowable.

    Both halves of this used to be wrong and it read about twice what it
    should.

    delegator_count is everyone with a delegation target, and passive
    delegation is a target: on 2025-03-18 it was 1718, of which 900 were
    passive. Those 900 are in no pool, so spreading them across pools
    counted people who are not there.

    And it divided by the open pools alone. A pool closedForNew is closed
    to new delegators, not emptied of the ones it has, so the denominator
    is the pools a delegator can be in: open plus closedForNew.
    closedForAll pools hold nobody and are left out.

    On that date: 20.70 before, 9.74 after.
    """
    pools = open_pools + closed_new_pools
    if passive is None or pools <= 0:
        return None
    return (delegator_count - passive) / pools


def perform_data_for_classified_pools(
    context, d_date: str, commits_by_day: dict, mongodb: MongoDB, grpcclient=None, net=None
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
    pool_total_delegated = df[~f_no_delegators]["staked_amount"].sum()

    # No grpc client means no passive count, and the average is then left
    # unknown rather than computed the old, wrong way.
    passive = passive_delegator_count(d_date, mongodb, grpcclient, net) if grpcclient else None
    delegator_avg_count_per_pool = average_delegators_per_pool(
        delegator_count, passive, open_pool_count, closed_new_pool_count
    )
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
    if passive is not None:
        dct["passive_delegator_count"] = passive
    avg_per_pool = (
        f"{delegator_avg_count_per_pool:.2f}"
        if delegator_avg_count_per_pool is not None
        else "unknown"
    )
    summary = (
        f"date: {d_date}, open: {open_pool_count}, closed: {closed_pool_count}, "
        f"closed_new: {closed_new_pool_count}, delegators: {delegator_count}, "
        f"avg_stake: {delegator_avg_stake:.2f}, avg_per_pool: {avg_per_pool}, "
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
