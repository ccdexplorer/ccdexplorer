import dagster as dg
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import (
    CCD_BlockHash,
)
from ccdexplorer.mongodb import (
    MongoDB,
)

from ccdexplorer.env import *  # type: ignore # noqa: E402, F403
from .utils import (
    PaydayInfo,
    get_previous_payday_information_entry,
    get_historical_payday_information_entry,
)


def perform_payday_init(
    context: dg.AssetExecutionContext,
    mongodb: MongoDB,
    grpcclient: GRPCClient,
    payday_date_string: str,
    payday_block_hash: CCD_BlockHash | None,
) -> PaydayInfo:
    """
    This method initializes the payday process by creating a Payday object.
    It creates the payday information entry and retrieves the state information for the current payday.
    """
    if not payday_block_hash:
        historical_payday_info = get_historical_payday_information_entry(
            payday_date_string, mongodb
        )
        payday_block_hash = historical_payday_info["_id"] if historical_payday_info else None
    if payday_block_hash is None:
        raise ValueError(
            f"Payday block hash for {payday_date_string} is not provided and no historical data found."
        )

    block_info = grpcclient.get_block_info(payday_block_hash)

    payday_info = PaydayInfo(date=payday_date_string, hash=payday_block_hash, block_info=block_info)
    # current payday information

    special_events_with_rewards = grpcclient.get_block_special_events(payday_info.block_info.hash)
    payday_info.special_events_with_rewards = {}
    for e in special_events_with_rewards:
        if e.payday_account_reward:
            payday_info.special_events_with_rewards[e.payday_account_reward.account] = e
        elif e.payday_pool_reward:
            if not e.payday_pool_reward.pool_owner:
                e.payday_pool_reward.pool_owner = 0  # -1 == passive delegation
            payday_info.special_events_with_rewards[e.payday_pool_reward.pool_owner] = e  # type: ignore

    # payday_info.special_events_with_rewards
    # current payday information first block
    previous_payday = get_previous_payday_information_entry(payday_date_string, mongodb)
    payday_info.height_for_first_block = (
        previous_payday["height_for_last_block"] + 1 if previous_payday else 3_232_445
    )
    _hash: CCD_BlockHash = grpcclient.get_blocks_at_height(payday_info.height_for_first_block)[0]
    payday_info.hash_for_first_block = _hash
    # payday_block_info_first_block = grpcclient.get_block_info(_hash)

    # current payday information last block
    height_for_pool_status = payday_info.block_info.height - 1
    _hash: CCD_BlockHash = grpcclient.get_blocks_at_height(height_for_pool_status)[0]
    payday_block_info_last_block = grpcclient.get_block_info(_hash)
    payday_info.height_for_last_block = payday_block_info_last_block.height
    payday_info.hash_for_last_block = payday_block_info_last_block.hash
    # duration is measured from the slot_time of the last block from
    # previous Reward period until slot_time from the last block in this
    # Reward period
    _height_start_duration = (
        previous_payday["height_for_last_block"] if previous_payday else 3_232_444
    )
    _hash_start_duration = grpcclient.get_blocks_at_height(_height_start_duration)[0]  # type:ignore
    block_start_duration = grpcclient.get_block_info(_hash_start_duration)
    payday_info.payday_duration_in_seconds = (
        payday_block_info_last_block.slot_time - block_start_duration.slot_time
    ).total_seconds()

    context.log.info(f"(Payday: {payday_info.date}) | Step 1: Init done.")
    return payday_info
