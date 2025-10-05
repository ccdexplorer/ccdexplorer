import math

import dagster as dg
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.mongodb import MongoDB, Collections
from pymongo import ReplaceOne

from ccdexplorer.env import *  # type: ignore # noqa: E402, F403
from .utils import PaydayInfo, reverse_search_from_dictionary


def fill_daily_apy_for_accounts_for_date(
    context: dg.AssetExecutionContext,
    mongodb: MongoDB,
    grpcclient: GRPCClient,
    payday_info: PaydayInfo,
) -> PaydayInfo:
    """
    We only get into this method if it's a account that is either a baker or a delegator.

    This method fills the paydays_apy_intermediate collection, for a given payday.
    This contains the daily apy (reward/relevant_stake). There are documents for every account,
    with a property daily_apy, which is a dictionary, keyed by date, valued is daily apy.
    """

    queue = []
    total_rewards_accounts = 0
    total_rewards_passive = 0
    average_daily_apy_accounts = 0
    count_pool_delegators = 0
    count_passive_delegators = 0
    count_account_is_validator = 0

    daily_apy_passive = 0
    for account_id in payday_info.accounts_that_need_APY:  # type: ignore
        pool_id = reverse_search_from_dictionary(
            payday_info.bakers_with_delegation_information,  # type: ignore
            account_id,
        )
        if account_id in payday_info.account_rewards.keys():  # type: ignore
            reward_for_account = payday_info.account_rewards[str(account_id)]  # type: ignore

            sum_reward = (
                reward_for_account.baker_reward
                + reward_for_account.finalization_reward
                + reward_for_account.transaction_fees
            )
            if pool_id != "passive_delegation":
                total_rewards_accounts += sum_reward
            else:
                total_rewards_passive += sum_reward

            staked_amount_for_account = payday_info.account_with_stake_by_account_id[  # type: ignore
                str(account_id)
            ]

            daily_apy = (
                math.pow(
                    1 + (sum_reward / staked_amount_for_account),
                    payday_info.seconds_per_year / payday_info.payday_duration_in_seconds,  # type: ignore
                )
                - 1
            )
        else:
            daily_apy = 0
            reward_for_account = {}
            sum_reward = 0
            staked_amount_for_account = 0

        apy_to_insert = {}

        if pool_id is None:
            count_account_is_validator += 1
        elif pool_id != "passive_delegation":
            average_daily_apy_accounts += daily_apy
            count_pool_delegators += 1
        else:
            daily_apy_passive = daily_apy
            count_passive_delegators += 1

        apy_to_insert.update(
            {
                "_id": f"{payday_info.date}-{account_id}-account",  # type: ignore
                "account_id": account_id,
                "date": payday_info.date,
                "apy": daily_apy,
                "days_in_average": 1,
                "type": "account",
                "reward": sum_reward / 1_000_000,
                "staked_amount": staked_amount_for_account / 1_000_000,
                "pool_id": pool_id,
            }
        )

        queue.append(
            ReplaceOne(
                {"_id": f"{payday_info.date}-{account_id}-account"},
                apy_to_insert,
                upsert=True,
            )
        )

    # For passive delegation, we need to insert the passive delegation APY
    _id = f"{payday_info.date}-passive_delegation"
    daily_passive = {
        "_id": _id,
        "validator_id": "passive_delegation",
        "date": payday_info.date,
        "apy": daily_apy_passive,
        "days_in_average": 1,
        "type": "passive_delegation",
        "reward": total_rewards_passive / 1_000_000,
        "staked_amount": payday_info.passive_delegation_info.current_payday_delegated_capital  # type: ignore
        / 1_000_000,  # type: ignore
    }
    queue.append(
        ReplaceOne(
            {"_id": _id},
            daily_passive,
            upsert=True,
        )
    )
    _ = mongodb.mainnet[Collections.paydays_v2_apy].bulk_write(queue)

    context.log.info(
        f"(Payday: {payday_info.date}) | Step 4: APY for accounts...done.| Processed {len(payday_info.accounts_that_need_APY)} accounts. Total reward accounts: {(total_rewards_accounts / 1000000):,.0f} CCD."  # type: ignore
    )  # type: ignore
    payday_info.metadata["average_daily_apy_pool_delegators"] = (
        average_daily_apy_accounts / count_pool_delegators if count_pool_delegators > 0 else 0.0
    )  # type: ignore
    payday_info.metadata["daily_apy_passive"] = (
        daily_apy_passive if count_passive_delegators > 0 else 0.0
    )  # type: ignore
    payday_info.metadata["count_pool_delegators"] = count_pool_delegators
    payday_info.metadata["count_account_is_validator"] = count_account_is_validator
    payday_info.metadata["count_passive_delegators"] = count_passive_delegators
    payday_info.metadata["total_rewards_accounts"] = total_rewards_accounts / 1_000_000
    payday_info.metadata["total_rewards_passive"] = total_rewards_passive / 1_000_000
    payday_info.metadata["passive_staked_amount"] = (
        payday_info.passive_delegation_info.current_payday_delegated_capital / 1_000_000  # type: ignore
    )  # type: ignore
    return payday_info
