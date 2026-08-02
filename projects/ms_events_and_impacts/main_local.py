#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import sys
from typing import List

from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.mongodb import Collections, MongoDB, MongoMotor, net_db
from ccdexplorer.tooter import Tooter

from ccdexplorer.ms_events_and_impacts.subscriber import Subscriber

net = "devnet"

# Same discovery pipeline as ms_plt/main_local.py, so a devnet rerun here processes the
# exact same block set that ms_plt already rebuilds plts_locks/plts_locks_links for -
# needed to backfill impacted_addresses (with lock_id tags) after a change to how PLT
# lock events are handled.
_BLOCK_DISCOVERY_PIPELINE = [
    {"$match": {"block_info.height": {"$gte": 0}}},
    {
        "$match": {
            "$or": [
                {"account_transaction.effects.token_update_effect": {"$exists": True}},
                {"account_transaction.effects.meta_update_effect": {"$exists": True}},
                {"token_creation": {"$exists": True}},
            ]
        }
    },
    {"$sort": {"block_info.height": 1}},
    {"$project": {"_id": 0, "block_info.height": 1}},
]


async def main() -> None:
    print(f"[local] Running on {net} as events_and_impacts")

    grpcclient = GRPCClient()
    tooter = Tooter()
    motormongo = MongoMotor(tooter, nearest=True, caller_name="ms_events_and_impacts")
    mongodb = MongoDB(tooter, caller_name="ms_events_and_impacts")

    subscriber = Subscriber(grpcclient, tooter, motormongo, mongodb)

    net_enum = NET(net)
    db = net_db(mongodb, net)

    block_heights: List[int] = [
        x["block_info"]["height"]
        for x in db[Collections.transactions].aggregate(_BLOCK_DISCOVERY_PIPELINE)
    ]

    for height in block_heights:
        block_doc = db[Collections.blocks].find_one({"height": height}, {"hash": 1})
        if block_doc is None:
            print(f"[local] block height={height} not found in db, skipping")
            continue
        block_hash: str = block_doc["hash"]
        print(f"[local] processing height={height} hash={block_hash}")
        # store_progress=False: this is a backfill over historical heights, not the live
        # consumer - it must not overwrite helpers["event_creation_last_processed_block"],
        # which tracks the live indexer's actual position.
        await subscriber.process_new_logged_events_from_block(
            net_enum, height, block_hash, store_progress=False
        )
        await asyncio.sleep(0)

    print("[local] done")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[local] interrupted", file=sys.stderr)
