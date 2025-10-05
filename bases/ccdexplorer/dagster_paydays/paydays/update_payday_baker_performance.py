import dagster as dg
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.mongodb import MongoDB, Collections

from ccdexplorer.env import *  # type: ignore # noqa: E402, F403
from .utils import PaydayInfo
from pymongo import ReplaceOne
import math


def perform_payday_performance_for_bakers(
    context: dg.AssetExecutionContext,
    mongodb: MongoDB,
    grpcclient: GRPCClient,
    payday_info: PaydayInfo,
):
    """
    Performance calculations for bakers.
    """
    estimated_blocks_per_day = (
        payday_info.height_for_last_block
        - payday_info.height_for_first_block  # type: ignore
        + 1
    )

    queue = []
    for baker_id in payday_info.bakers_with_delegation_information or ():
        _id = f"{payday_info.date}-{baker_id}"
        d = {}
        if baker_id == "passive_delegation":
            d["pool_status"] = payday_info.passive_delegation_info.model_dump(  # type: ignore
                exclude_none=True
            )
        else:
            d["pool_status"] = payday_info.pool_info_by_baker_id[str(baker_id)].model_dump(  # type: ignore
                exclude_none=True
            )
            if payday_info.pool_info_by_baker_id[str(baker_id)].current_payday_info:  # type: ignore
                d["expectation"] = (
                    payday_info.pool_info_by_baker_id[  # type: ignore
                        str(baker_id)
                    ].current_payday_info.lottery_power  # type: ignore
                    * estimated_blocks_per_day
                )

            else:
                d["expectation"] = 0

        pool_owner = baker_id
        d["_id"] = _id
        d["date"] = payday_info.date
        d["payday_block_slot_time"] = payday_info.block_info.slot_time
        d["baker_id"] = pool_owner
        if baker_id != "passive_delegation":
            expp = (
                payday_info.pool_info_by_baker_id[  # type: ignore
                    str(baker_id)
                ].current_payday_info.lottery_power  # type: ignore
                * estimated_blocks_per_day
            )
            assert math.isclose(
                d["expectation"],
                expp,
                rel_tol=0.1,
            ), (
                f"{_id}: Expectation should be close to estimated blocks per day! {d['expectation']=}, {expp=}"
            )

        queue.append(ReplaceOne({"_id": _id}, d, upsert=True))

    _ = mongodb.mainnet[Collections.paydays_v2_performance].bulk_write(queue)

    # for current payday...
    queue = []
    for baker_id in payday_info.bakers_with_delegation_information_current_payday or ():
        _id = f"{payday_info.date}-{baker_id}"
        d = {}
        d["pool_status"] = payday_info.pool_info_by_baker_id_current_payday[  # type: ignore
            str(baker_id)
        ].model_dump(exclude_none=True)
        if payday_info.pool_info_by_baker_id_current_payday[str(baker_id)].current_payday_info:  # type: ignore
            d["expectation"] = (
                payday_info.pool_info_by_baker_id_current_payday[  # type: ignore
                    str(baker_id)
                ].current_payday_info.lottery_power  # type: ignore
                * estimated_blocks_per_day
            )

        else:
            d["expectation"] = 0

        pool_owner = baker_id
        d["_id"] = _id
        d["date"] = payday_info.date
        d["payday_block_slot_time"] = payday_info.block_info.slot_time
        d["baker_id"] = pool_owner

        queue.append(ReplaceOne({"_id": _id}, d, upsert=True))

    _ = mongodb.mainnet[Collections.paydays_v2_current_payday].delete_many({})
    _ = mongodb.mainnet[Collections.paydays_v2_current_payday].bulk_write(queue)

    context.log.info(
        f"(Payday: {payday_info.date}) | Step 2: Validator performance...done.| Processed {len(payday_info.bakers_with_delegation_information.keys())} validators."  # type: ignore
    )  # type: ignore
