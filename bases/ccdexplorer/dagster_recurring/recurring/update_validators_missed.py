from collections import Counter

from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import (
    CCD_BlockInfo,
    CCD_WinningBaker,
)
from ccdexplorer.mongodb import MongoDB, Collections
from pymongo import ReplaceOne
import time


def perform_validators_missed_update(
    context,
    grpcclient: GRPCClient,
    mongodb: MongoDB,
) -> dict:
    latest_block = grpcclient.get_block_info("last_final")
    context.log.info(
        f"height: {latest_block.height}, genesis: {latest_block.genesis_index}, epoch: {latest_block.epoch}"
    )
    doc = (mongodb.mainnet[Collections.helpers].find_one({"_id": "last_known_payday"})) or {}
    latest_payday_block: CCD_BlockInfo = grpcclient.get_block_info(block_input=doc["hash"])

    db = mongodb.mainnet_db
    context.log.info(
        f"latest payday - height: {latest_payday_block.height}, genesis: {latest_payday_block.genesis_index}, epoch: {latest_payday_block.epoch}"
    )
    if latest_payday_block.epoch > latest_block.epoch:  # type: ignore
        start = 1
        end = latest_block.epoch
    else:
        start = latest_payday_block.epoch
        end = latest_block.epoch
    for epoch in range(start, end):  # type: ignore
        local_queue = []
        winning_bakers: list[CCD_WinningBaker] = grpcclient.get_winning_bakers_epoch(
            latest_block.genesis_index,
            epoch - 1,  # type: ignore
        )

        filtered = [x for x in winning_bakers if not x.present]

        winner_counts = Counter(str(x.winner) for x in filtered)

        sorted_counts = dict(sorted(winner_counts.items(), key=lambda item: item[1], reverse=True))
        _id = f"genesis-{latest_block.genesis_index}-epoch-{epoch}"  # type: ignore
        dct = {
            "_id": _id,
            "genesis_index": latest_block.genesis_index,
            "epoch": epoch,  # type: ignore
        }
        context.log.info(
            f"genesis: {latest_block.genesis_index}, epoch: {epoch}, missed_rounds_count: {sorted_counts}"
        )
        dct.update({"missed_rounds_count": sorted_counts})
        time.sleep(0.1)
        local_queue.append(
            ReplaceOne(
                {"_id": _id},
                replacement=dct,
                upsert=True,
            )
        )

        if len(local_queue) > 0:
            _ = db["paydays_v2_validators_missed"].bulk_write(local_queue)
    return {}
