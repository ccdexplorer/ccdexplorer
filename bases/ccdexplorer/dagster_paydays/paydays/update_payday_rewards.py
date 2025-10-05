import dagster as dg
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import (
    CCD_BlockSpecialEvent_PaydayAccountReward,
    CCD_BlockSpecialEvent_PaydayPoolReward,
)
from ccdexplorer.mongodb import Collections, MongoDB
from pymongo import ReplaceOne
from ccdexplorer.domain.mongo import MongoImpactedAddress, AccountStatementEntryType


from ccdexplorer.env import *  # type: ignore # noqa: E402, F403
from .utils import PaydayInfo, reverse_search_from_dictionary


def file_a_balance_movement(
    block_height: int,
    impacted_addresses_in_tx: dict[str, MongoImpactedAddress],
    impacted_address: str,
    balance_movement_to_add: AccountStatementEntryType,
    payday_info_date: str,
):
    if impacted_addresses_in_tx.get(impacted_address):
        impacted_address_as_class: MongoImpactedAddress = impacted_addresses_in_tx[impacted_address]
        bm = impacted_address_as_class.balance_movement
        field_set = list(balance_movement_to_add.model_fields_set)[0]
        if field_set == "transfer_in":
            if not bm.transfer_in:  # type: ignore
                bm.transfer_in = []  # type: ignore
            bm.transfer_in.extend(balance_movement_to_add.transfer_in)  # type: ignore
        elif field_set == "transfer_out":
            if not bm.transfer_out:  # type: ignore
                bm.transfer_out = []  # type: ignore
            bm.transfer_out.extend(balance_movement_to_add.transfer_out)  # type: ignore
        elif field_set == "amount_encrypted":
            bm.amount_encrypted = balance_movement_to_add.amount_encrypted  # type: ignore
        elif field_set == "amount_decrypted":
            bm.amount_decrypted = balance_movement_to_add.amount_decrypted  # type: ignore
        elif field_set == "baker_reward":
            bm.baker_reward = balance_movement_to_add.baker_reward  # type: ignore
        elif field_set == "finalization_reward":
            bm.finalization_reward = balance_movement_to_add.finalization_reward  # type: ignore
        elif field_set == "foundation_reward":
            bm.foundation_reward = balance_movement_to_add.foundation_reward  # type: ignore
        elif field_set == "transaction_fee_reward":
            bm.transaction_fee_reward = (  # type: ignore
                balance_movement_to_add.transaction_fee_reward
            )

        impacted_address_as_class.balance_movement = bm
    else:
        impacted_address_as_class = MongoImpactedAddress(
            **{
                "_id": f"{block_height}-{impacted_address[:29]}",
                "impacted_address": impacted_address,
                "impacted_address_canonical": impacted_address[:29],
                "effect_type": "Account Reward",
                "balance_movement": balance_movement_to_add,
                "block_height": block_height,
                "date": payday_info_date,
            }
        )
        impacted_addresses_in_tx[impacted_address] = impacted_address_as_class


def add_reward_to_impacted_accounts(
    context: dg.AssetExecutionContext,
    account_rewards: dict[str, CCD_BlockSpecialEvent_PaydayAccountReward],
    payday_info: PaydayInfo,
    mongodb: MongoDB,
):
    impacted_addresses_queue = []
    for ar in account_rewards.values():
        impacted_addresses_in_tx: dict = {}
        balance_movement = AccountStatementEntryType(
            transaction_fee_reward=ar.transaction_fees,
            baker_reward=ar.baker_reward,
            finalization_reward=ar.finalization_reward,
        )
        file_a_balance_movement(
            payday_info.height_for_last_block + 1,  # type: ignore
            impacted_addresses_in_tx,
            ar.account,
            balance_movement,
            payday_info.date,
        )

        # now this tx is done, so add impacted_addresses to queue
        for ia in impacted_addresses_in_tx.values():
            ia: MongoImpactedAddress
            repl_dict = ia.model_dump(exclude_none=True)
            if "id" in repl_dict:
                del repl_dict["id"]

            impacted_addresses_queue.append(
                ReplaceOne(
                    {"_id": ia.id},
                    repl_dict,
                    upsert=True,
                )
            )
    _ = mongodb.mainnet[Collections.impacted_addresses].bulk_write(impacted_addresses_queue)
    context.log.info(
        f"(Payday: {payday_info.date}) | Step 3.5: add_reward_to_impacted_accounts...done."
    )


def perform_payday_rewards(
    context: dg.AssetExecutionContext,
    mongodb: MongoDB,
    grpcclient: GRPCClient,
    payday_info: PaydayInfo,
):
    """
    This method runs through all rewards for the payday and stores an entry for each in collection paydays_v2_rewards.
    """
    queue = []
    account_rewards: dict[str, CCD_BlockSpecialEvent_PaydayAccountReward] = {}
    pool_rewards: dict[str, CCD_BlockSpecialEvent_PaydayPoolReward] = {}
    total_rewards = 0
    account_count = 0
    pool_count = 0
    restaked_rewards = 0
    paidout_rewards = 0

    assert payday_info.special_events_with_rewards is not None
    pop_log = payday_info.special_events_with_rewards.copy()
    for e in payday_info.special_events_with_rewards.values() or ():  #
        if (e.payday_pool_reward) or e.payday_account_reward:
            d = {}

            if e.payday_account_reward:
                pop_log.pop(e.payday_account_reward.account, None)  # type: ignore
                account_count += 1
                total_reward_for_account = (
                    e.payday_account_reward.baker_reward
                    + e.payday_account_reward.finalization_reward
                    + e.payday_account_reward.transaction_fees
                )
                total_rewards += total_reward_for_account
                account_rewards[e.payday_account_reward.account] = e.payday_account_reward

                if e.payday_account_reward.account in payday_info.restake_info_delegators:  # type: ignore
                    restaked_rewards += (
                        total_reward_for_account
                        if payday_info.restake_info_delegators[  # type: ignore
                            e.payday_account_reward.account
                        ]
                        else 0
                    )
                    paidout_rewards += (
                        total_reward_for_account
                        if not payday_info.restake_info_delegators[  # type: ignore
                            e.payday_account_reward.account
                        ]
                        else 0
                    )

                _tag = "payday_account_reward"
                d["account_id"] = e.payday_account_reward.account
                d["reward"] = e.payday_account_reward.model_dump()
                receiver = e.payday_account_reward.account
                if e.payday_account_reward.account in payday_info.list_of_delegators:  # type: ignore
                    d["account_is_delegator"] = True
                    d["delegation_target"] = reverse_search_from_dictionary(
                        payday_info.bakers_with_delegation_information,  # type: ignore
                        e.payday_account_reward.account,
                    )
                    d["staked_amount"] = payday_info.account_with_stake_by_account_id[  # type: ignore
                        e.payday_account_reward.account
                    ]

                if (
                    e.payday_account_reward.account in payday_info.baker_account_ids.values()  # type: ignore
                ):
                    # request poolstatus to get a stable stakedAmount for an account from the baker it
                    d["staked_amount"] = payday_info.pool_info_by_account_id[  # type: ignore
                        e.payday_account_reward.account
                    ].current_payday_info.baker_equity_capital  # type: ignore
                    d["account_is_baker"] = True
                    d["baker_id"] = payday_info.baker_account_ids_by_account_id[  # type: ignore
                        e.payday_account_reward.account
                    ]

            elif e.payday_pool_reward:
                pop_log.pop(e.payday_pool_reward.pool_owner, None)  # type: ignore
                pool_count += 1
                total_rewards += (
                    e.payday_pool_reward.baker_reward
                    + e.payday_pool_reward.finalization_reward
                    + e.payday_pool_reward.transaction_fees
                )
                d["pool_owner"] = (
                    e.payday_pool_reward.pool_owner
                    if e.payday_pool_reward.pool_owner
                    else "passive_delegation"
                )
                receiver = (
                    payday_info.baker_account_ids[str(e.payday_pool_reward.pool_owner)]  # type: ignore
                    if e.payday_pool_reward.pool_owner
                    else "passive_delegation"
                )
                pool_rewards[str(d["pool_owner"])] = e.payday_pool_reward
                _tag = "payday_pool_reward"
                d["pool_status"] = (
                    payday_info.pool_info_by_baker_id[  # type: ignore
                        str(e.payday_pool_reward.pool_owner)
                    ].model_dump(exclude_none=True)
                    if e.payday_pool_reward.pool_owner
                    else payday_info.passive_delegation_info.model_dump(exclude_none=True)  # type: ignore
                )
                d["reward"] = e.payday_pool_reward.model_dump(exclude_none=True)

            # receiver = "passive_delegation" if not receiver else receiver #type: ignore
            d["_id"] = f"{payday_info.date}-{_tag}-{receiver}"  # type: ignore
            d["date"] = payday_info.date
            # d["slot_time"] = payday_info.block_info.slot_time
            d["block_info_hash"] = payday_info.block_info.hash
            d["block_info_height"] = payday_info.block_info.height
            queue.append(ReplaceOne({"_id": d["_id"]}, d, upsert=True))

    # BULK_WRITE
    _ = mongodb.mainnet[Collections.paydays_v2_rewards].bulk_write(queue)

    payday_info.account_rewards = account_rewards
    payday_info.pool_rewards = pool_rewards

    add_reward_to_impacted_accounts(context, account_rewards, payday_info, mongodb)
    context.log.info(
        f"(Payday: {payday_info.date}) | Step 3: rewards...done.| Processed {account_count:,.0f} account rewards, {pool_count:,.0f} pool rewards. Processed {len(payday_info.baker_account_ids.keys())} validators. Total reward: {(total_rewards / 1000000):,.0f} CCD"  # type: ignore
    )  # type: ignore

    sum_account_rewards = sum(
        [
            x.baker_reward + x.finalization_reward + x.transaction_fees
            for x in account_rewards.values()
        ]
    )
    sum_pool_rewards = sum(
        [x.baker_reward + x.finalization_reward + x.transaction_fees for x in pool_rewards.values()]
    )
    assert total_rewards == sum_account_rewards + sum_pool_rewards, (
        f"Total rewards do not match the sum of account and pool rewards. Difference: total - (account + pool) = {(total_rewards - (sum_account_rewards + sum_pool_rewards)):,.0f} CCD"
    )
    context.log.info(
        f"(Payday: {payday_info.date}) | Total account rewards: {(sum_account_rewards / 1000000):,.0f} CCD, Total pool rewards: {(sum_pool_rewards / 1000000):,.0f} CCD"
    )
    payday_info.metadata["total_payday_rewards"] = total_rewards / 1000000
    payday_info.metadata["total_account_rewards"] = sum_account_rewards / 1000000
    payday_info.metadata["total_pool_rewards"] = sum_pool_rewards / 1000000
    payday_info.metadata["restaked_rewards"] = restaked_rewards
    payday_info.metadata["paidout_rewards"] = paidout_rewards
    # payday_info.metadata["restaked_rewards_perc"] = restaked_rewards / total_rewards
    return payday_info
