import math

import dagster as dg
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.mongodb import MongoDB, Collections
from pymongo import ReplaceOne

from ccdexplorer.env import *  # type: ignore # noqa: E402, F403
from .utils import PaydayInfo


def fill_daily_apy_for_validators_for_date(
    context: dg.AssetExecutionContext,
    mongodb: MongoDB,
    grpcclient: GRPCClient,
    payday_info: PaydayInfo,
) -> PaydayInfo:
    """
    We only get into this method if it's a baker.

    This method fills the paydays_apy_intermediate collection, for a given payday.
    This contains the daily apy (reward/relevant_stake). There are documents for every account,
    with a property daily_apy, which is a dictionary, keyed by date, valued is daily apy.
    For bakers
    """
    queue = []
    total_reward_all_pools = 0
    total_reward_all_validators = 0
    total_reward_all_delegators = 0
    average_daily_apy_validator = 0
    # average_daily_apy_delegator = 0
    count_validator = 0
    count_delegator = 0
    # daily_apy_passive = 0
    validator_staked_amount = 0
    delegator_staked_amount = 0
    # passive_staked_amount = 0
    restaked_rewards = payday_info.metadata["restaked_rewards"]
    paidout_rewards = payday_info.metadata["paidout_rewards"]

    for baker_id in payday_info.bakers_that_need_APY:  # type: ignore
        # daily_total = None
        daily_baker = None
        daily_delegator = None
        # daily_passive = None

        if baker_id in payday_info.pool_rewards.keys():  # type: ignore
            if baker_id == "passive_delegation":
                continue  # passive delegation is handled in a separate method

            reward_for_baker = payday_info.pool_rewards[baker_id]  # type: ignore
            total_reward = (
                reward_for_baker.baker_reward
                + reward_for_baker.finalization_reward
                + reward_for_baker.transaction_fees
            )
            total_reward_all_pools += total_reward

            pool_info_for_baker = payday_info.pool_info_by_baker_id[baker_id]  # type: ignore
            delegation_info_for_baker = payday_info.bakers_with_delegation_information[  # type: ignore
                baker_id
            ]

            delegator_ratio = (
                pool_info_for_baker.current_payday_info.delegated_capital  # type: ignore
                / pool_info_for_baker.current_payday_info.effective_stake  # type: ignore
            )

            delegators_baking_reward = (
                1 - pool_info_for_baker.pool_info.commission_rates.baking  # type: ignore
            ) * (delegator_ratio * reward_for_baker.baker_reward)

            delegators_transaction_reward = (
                1 - pool_info_for_baker.pool_info.commission_rates.transaction  # type: ignore
            ) * (delegator_ratio * reward_for_baker.transaction_fees)

            delegators_finalization_reward = (
                1 - pool_info_for_baker.pool_info.commission_rates.finalization  # type: ignore
            ) * (delegator_ratio * reward_for_baker.finalization_reward)

            delegator_reward = (
                delegators_baking_reward
                + delegators_transaction_reward
                + delegators_finalization_reward
            )

            total_reward_all_delegators += delegator_reward

            baker_reward = total_reward - delegator_reward

            total_reward_all_validators += baker_reward

            restaked_rewards += (
                baker_reward
                if payday_info.restake_info_validators[int(baker_id)]  # type: ignore
                else 0
            )
            paidout_rewards += (
                baker_reward
                if not payday_info.restake_info_validators[int(baker_id)]  # type: ignore
                else 0
            )

            if pool_info_for_baker.current_payday_info.effective_stake > 0:  # type: ignore
                daily_apy = (
                    math.pow(
                        1
                        + (
                            total_reward / pool_info_for_baker.current_payday_info.effective_stake  # type: ignore
                        ),
                        payday_info.seconds_per_year / payday_info.payday_duration_in_seconds,  # type: ignore
                    )
                    - 1
                )
            else:
                daily_apy = 0
            # sum_rewards = total_reward / 1_000_000
            # daily_total = {"apy": daily_apy, "reward": sum_rewards}

            if pool_info_for_baker.current_payday_info.baker_equity_capital > 0:  # type: ignore
                daily_apy = (
                    math.pow(
                        1
                        + (
                            baker_reward
                            / pool_info_for_baker.current_payday_info.baker_equity_capital  # type: ignore
                        ),
                        payday_info.seconds_per_year / payday_info.payday_duration_in_seconds,  # type: ignore
                    )
                    - 1
                )
            else:
                daily_apy = 0
            # sum_rewards = baker_reward / 1_000_000
            average_daily_apy_validator += daily_apy
            count_validator += 1
            validator_staked_amount += (
                pool_info_for_baker.current_payday_info.baker_equity_capital / 1_000_000  # type: ignore
            )  # type: ignore

            _id = f"{payday_info.date}-{baker_id}-validator"
            daily_baker = {
                "_id": _id,  # type: ignore
                "validator_id": baker_id,
                "date": payday_info.date,
                "apy": daily_apy,
                "days_in_average": 1,
                "type": "validator",
                "reward": baker_reward / 1_000_000,
                "staked_amount": pool_info_for_baker.current_payday_info.baker_equity_capital  # type: ignore
                / 1_000_000,  # type: ignore
            }
            queue.append(ReplaceOne({"_id": _id}, daily_baker, upsert=True))

            if len(delegation_info_for_baker) > 0:
                if (
                    pool_info_for_baker.current_payday_info.delegated_capital  # type: ignore
                    > 0
                ):
                    daily_apy = (
                        math.pow(
                            1
                            + (
                                delegator_reward
                                / pool_info_for_baker.current_payday_info.delegated_capital  # type: ignore
                            ),
                            payday_info.seconds_per_year / payday_info.payday_duration_in_seconds,  # type: ignore
                        )
                        - 1
                    )
                else:
                    daily_apy = 0
                # sum_rewards = delegator_reward / 1_000_000
                # average_daily_apy_delegator += daily_apy
                delegator_staked_amount += (
                    pool_info_for_baker.current_payday_info.delegated_capital / 1_000_000  # type: ignore
                )  # type: ignore
                count_delegator += 1
                _id = f"{payday_info.date}-{baker_id}-delegator"
                daily_delegator = {
                    "_id": _id,  # type: ignore
                    "validator_id": baker_id,
                    "date": payday_info.date,
                    "apy": daily_apy,
                    "days_in_average": 1,
                    "type": "delegator",
                    "reward": delegator_reward / 1_000_000,
                    "staked_amount": pool_info_for_baker.current_payday_info.delegated_capital  # type: ignore
                    / 1_000_000,  # type: ignore
                }
                queue.append(ReplaceOne({"_id": _id}, daily_delegator, upsert=True))

    _ = mongodb.mainnet[Collections.paydays_v2_apy].bulk_write(queue)

    context.log.info(
        f"(Payday: {payday_info.date}) | Step 5: APY for validators...done.| Processed {len(payday_info.bakers_that_need_APY)} validators. Total reward all validators: {(total_reward_all_pools / 1000000):,.0f} CCD."  # type: ignore
    )  # type: ignore
    payday_info.metadata["average_daily_apy_validator"] = (
        average_daily_apy_validator / count_validator if count_validator > 0 else 0
    )
    payday_info.metadata["validator_staked_amount"] = (
        validator_staked_amount if count_validator > 0 else 0.0
    )
    payday_info.metadata["delegator_staked_amount"] = (
        delegator_staked_amount if count_delegator > 0 else 0.0
    )
    payday_info.metadata["total_rewards_validators"] = total_reward_all_validators / 1_000_000
    payday_info.metadata["total_rewards_pool_delegators"] = total_reward_all_delegators / 1_000_000
    payday_info.metadata["total_rewards_pools"] = total_reward_all_pools / 1_000_000
    payday_info.metadata["total_rewards"] = (
        total_reward_all_pools / 1_000_000
    ) + payday_info.metadata["total_rewards_passive"]  # type: ignore
    payday_info.metadata["count_validators"] = (
        len(payday_info.bakers_that_need_APY) - 1  # type: ignore
    )  # -1 for passive delegation
    payday_info.metadata["restaked_rewards"] = restaked_rewards / 1_000_000
    payday_info.metadata["paidout_rewards"] = paidout_rewards / 1_000_000
    payday_info.metadata["restaked_rewards_perc"] = restaked_rewards / (
        restaked_rewards + paidout_rewards
    )
    return payday_info
