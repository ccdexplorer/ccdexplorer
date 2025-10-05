import dagster as dg
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import (
    CCD_AccountAddress,
    CCD_BakerId,
    CCD_DelegatorRewardPeriodInfo,
    CCD_AccountInfo,
)
from ccdexplorer.mongodb import MongoDB, Collections
from ccdexplorer.env import *  # type: ignore # noqa: E402, F403
from .utils import PaydayInfo


def perform_payday_state_information_for_current_payday(
    context: dg.AssetExecutionContext,
    mongodb: MongoDB,
    grpcclient: GRPCClient,
    payday_info: PaydayInfo,
) -> PaydayInfo:
    """
    State information for the current payday from the last block in the payday.
    """
    #
    # bakers for this payday
    last_hash = payday_info.hash_for_last_block
    # first_hash = payday_info.hash_for_first_block

    bakers_in_block = grpcclient.get_election_info(
        last_hash  # type: ignore
    ).baker_election_info

    # needed for current payday information to show pools at /staking
    bakers_in_block_current_payday = grpcclient.get_election_info(
        payday_info.hash  # type: ignore
    ).baker_election_info

    baker_account_ids_by_baker_id: dict[str, CCD_AccountAddress] = {}
    baker_account_ids_by_account_id: dict[str, CCD_BakerId] = {}
    bakers_with_delegation_information: dict[str, list[CCD_DelegatorRewardPeriodInfo]] = {}
    bakers_with_delegation_information_current_payday: dict[
        str, list[CCD_DelegatorRewardPeriodInfo]
    ] = {}
    payday_info.pool_info_by_baker_id = {}
    payday_info.pool_info_by_baker_id_current_payday = {}
    payday_info.pool_info_by_account_id = {}
    payday_info.restake_info_validators = {}
    payday_info.restake_info_delegators = {}
    account_info_by_baker_id: dict[str, CCD_AccountInfo] = {}
    account_info_by_account_id: dict[str, CCD_AccountInfo] = {}

    pool_status_dict: dict[str, list] = {}
    pool_status_dict_current_payday: dict[str, list] = {}
    for election_info_baker in bakers_in_block:
        baker_id = election_info_baker.baker
        account_info = grpcclient.get_account_info(last_hash, account_index=baker_id)  # type: ignore
        account_info_by_baker_id[str(baker_id)] = account_info
        account_info_by_account_id[account_info.address] = account_info

        # future me: this needs to be collected from the last_hash,
        # as we are using this to collect the actually baked blocks
        # in a payday (in baker-tally).
        pool_info_for_baker = grpcclient.get_pool_info_for_pool(baker_id, last_hash)  # type: ignore

        payday_info.pool_info_by_baker_id[str(baker_id)] = pool_info_for_baker
        payday_info.pool_info_by_account_id[account_info.address] = pool_info_for_baker

        # lookup mappings from acount_id <---> baker_id
        baker_account_ids_by_baker_id[str(baker_id)] = pool_info_for_baker.address
        baker_account_ids_by_account_id[pool_info_for_baker.address] = baker_id

        # contains delegators with info
        bakers_with_delegation_information[str(baker_id)] = (
            grpcclient.get_delegators_for_pool_in_reward_period(
                baker_id,
                last_hash,  # type: ignore
            )
        )

        # restake info
        validator_account_info = grpcclient.get_account_info(last_hash, account_index=baker_id)  # type: ignore

        payday_info.restake_info_validators[baker_id] = False  # type: ignore
        if validator_account_info.stake:
            if validator_account_info.stake.baker:
                payday_info.restake_info_validators[baker_id] = (  # type: ignore
                    validator_account_info.stake.baker.restake_earnings
                )  # type: ignore

        for delegator in bakers_with_delegation_information[str(baker_id)]:
            delegator_account_info = grpcclient.get_account_info(
                last_hash,  # type: ignore
                hex_address=delegator.account,  # type: ignore
            )

            payday_info.restake_info_delegators[  # type: ignore
                delegator.account
            ] = False
            if delegator_account_info.stake:
                if delegator_account_info.stake.delegator:
                    # restake info for delegators
                    payday_info.restake_info_delegators[  # type: ignore
                        delegator.account
                    ] = delegator_account_info.stake.delegator.restake_earnings  # type: ignore

        # add dictionary with payday pool status for each baker/pool
        current_baker_pool_status = pool_info_for_baker.pool_info.open_status  # type: ignore

        if current_baker_pool_status in pool_status_dict.keys():
            pool_status_dict[current_baker_pool_status].append(baker_id)
        else:
            pool_status_dict[current_baker_pool_status] = [baker_id]

    # needed for current payday information to show pools at /staking
    for election_info_baker in bakers_in_block_current_payday:
        baker_id = election_info_baker.baker

        # future me: this needs to be collected from the payday_block_hash,
        # as we are using this to display the current payday information
        pool_info_for_baker_current_payday = grpcclient.get_pool_info_for_pool(
            baker_id, payday_info.hash
        )

        payday_info.pool_info_by_baker_id_current_payday[str(baker_id)] = (
            pool_info_for_baker_current_payday
        )

        # contains delegators with info
        bakers_with_delegation_information_current_payday[str(baker_id)] = (
            grpcclient.get_delegators_for_pool_in_reward_period(baker_id, payday_info.hash)
        )

        # add dictionary with payday pool status for each baker/pool
        current_baker_pool_status = (
            pool_info_for_baker_current_payday.pool_info.open_status  # type: ignore
        )

        if current_baker_pool_status in pool_status_dict_current_payday.keys():
            pool_status_dict_current_payday[current_baker_pool_status].append(baker_id)
        else:
            pool_status_dict_current_payday[current_baker_pool_status] = [baker_id]

    # add passive delegators
    bakers_with_delegation_information["passive_delegation"] = (
        grpcclient.get_delegators_for_passive_delegation_in_reward_period(last_hash)  # type: ignore
    )

    payday_info.passive_delegation_info = grpcclient.get_passive_delegation_info(last_hash)  # type: ignore

    for passive_delegator in bakers_with_delegation_information["passive_delegation"]:
        delegator_account_info = grpcclient.get_account_info(
            last_hash,  # type: ignore
            hex_address=passive_delegator.account,  # type: ignore
        )
        payday_info.restake_info_delegators[  # type: ignore
            passive_delegator.account
        ] = False
        if delegator_account_info.stake:
            if delegator_account_info.stake.delegator:
                payday_info.restake_info_delegators[  # type: ignore
                    passive_delegator.account
                ] = delegator_account_info.stake.delegator.restake_earnings  # type: ignore

    # for saving to payday collection
    bakers_with_delegation_information_mongo: dict[str, list[CCD_DelegatorRewardPeriodInfo]] = {}
    for k, v in bakers_with_delegation_information.items():
        save_list = []
        for info in v:
            save_list.append(info.model_dump(exclude_none=True))

        bakers_with_delegation_information_mongo[k] = save_list

    bakers_with_delegation_information_mongo_current_payday = {}
    for k, v in bakers_with_delegation_information_current_payday.items():
        save_list = []
        for info in v:
            save_list.append(info.model_dump(exclude_none=True))

        bakers_with_delegation_information_mongo_current_payday[k] = save_list

    # fill payday_info with the collected information
    payday_info.pool_status_for_bakers = pool_status_dict
    payday_info.baker_account_ids = baker_account_ids_by_baker_id
    payday_info.bakers_with_delegation_information = bakers_with_delegation_information_mongo
    payday_info.bakers_with_delegation_information_current_payday = (
        bakers_with_delegation_information_current_payday
    )
    payday_info.baker_account_ids_by_account_id = baker_account_ids_by_account_id

    payday_info.payday_block_slot_time = payday_info.block_info.slot_time
    payday_information_entry = {
        "_id": payday_info.hash,
        "date": payday_info.date,
        "height_for_first_block": payday_info.height_for_first_block,
        "height_for_last_block": payday_info.height_for_last_block,
        "hash_for_first_block": payday_info.hash_for_first_block,
        "hash_for_last_block": payday_info.hash_for_last_block,
        "payday_duration_in_seconds": payday_info.payday_duration_in_seconds,
        "payday_block_slot_time": payday_info.payday_block_slot_time,
    }
    query = {"_id": payday_info.hash}

    _ = mongodb.mainnet[Collections.paydays_v2].replace_one(
        query, payday_information_entry, upsert=True
    )

    return payday_info
