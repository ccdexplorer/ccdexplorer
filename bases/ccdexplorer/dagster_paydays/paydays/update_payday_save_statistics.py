import dagster as dg
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.mongodb import MongoDB
from pymongo import ReplaceOne


from ccdexplorer.env import *  # type: ignore # noqa: E402, F403
from .utils import PaydayInfo, calc_apy


def save_statistics_for_date(
    context: dg.AssetExecutionContext,
    mongodb: MongoDB,
    grpcclient: GRPCClient,
    payday_info: PaydayInfo,
) -> PaydayInfo:
    """
    This method saves the statistics for a given payday date.
    """
    queue = []
    validator_staked_amount = payday_info.metadata["validator_staked_amount"]
    delegator_staked_amount = payday_info.metadata["delegator_staked_amount"]
    passive_staked_amount = payday_info.metadata["passive_staked_amount"]
    total_staked_amount = validator_staked_amount + delegator_staked_amount + passive_staked_amount
    payday_info.metadata["daily_apy_validators"] = calc_apy(
        payday_info.metadata["total_rewards_validators"],
        validator_staked_amount,
        payday_info,
    )

    payday_info.metadata["daily_apy_all"] = calc_apy(
        payday_info.metadata["total_rewards"],
        total_staked_amount,
        payday_info,
    )

    _id = f"{payday_info.date}-statistics_daily_payday"
    dct = {
        "_id": _id,
        "date": payday_info.date,
        "type": "statistics_daily_payday",
        "total_rewards": payday_info.metadata["total_rewards"],
        "total_rewards_pools": payday_info.metadata["total_rewards_pools"],
        "total_rewards_validators": payday_info.metadata["total_rewards_validators"],
        "total_rewards_pool_delegators": payday_info.metadata["total_rewards_pool_delegators"],
        "total_rewards_passive_delegators": payday_info.metadata["total_rewards_passive"],
        "restaked_rewards": payday_info.metadata["restaked_rewards"],
        "paidout_rewards": payday_info.metadata["paidout_rewards"],
        "restaked_rewards_perc": payday_info.metadata["restaked_rewards_perc"],
        "daily_apy_all": payday_info.metadata["daily_apy_all"],
        "daily_apy_validators": payday_info.metadata["daily_apy_validators"],
        "daily_apy_pool_delegators": payday_info.metadata["average_daily_apy_pool_delegators"],
        "daily_apy_passive_delegators": payday_info.metadata["daily_apy_passive"],
        "validator_staked_amount": payday_info.metadata["validator_staked_amount"],
        "delegator_staked_amount": payday_info.metadata["delegator_staked_amount"],
        "passive_staked_amount": payday_info.metadata["passive_staked_amount"],
        "count_validators": payday_info.metadata["count_validators"],
        "count_pool_delegators": payday_info.metadata["count_pool_delegators"],
        "count_passive_delegators": payday_info.metadata["count_passive_delegators"],
    }
    queue.append(ReplaceOne({"_id": _id}, dct, upsert=True))

    _ = mongodb.mainnet_db["statistics"].bulk_write(queue)

    context.log.info(f"(Payday: {payday_info.date}) | Step 5.5: Saved statistics.")  # type: ignore
    return payday_info
