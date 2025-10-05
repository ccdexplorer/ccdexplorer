#!/usr/bin/env python3
from __future__ import annotations

import asyncio
from datetime import timedelta
import sys

from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.mongodb import MongoDB, MongoMotor, Collections
from ccdexplorer.tooter import Tooter
from ccdexplorer.grpc_client.CCD_Types import CCD_BlockInfo
from ccdexplorer.ms_indexers.subscriber import Subscriber
from rich.progress import track

grpcclient = GRPCClient()
tooter = Tooter()
motormongo = MongoMotor(tooter, nearest=True)
mongodb = MongoDB(tooter)
net = "mainnet"
blocks = [37211558]
db = mongodb.mainnet if net == "mainnet" else mongodb.testnet

pipeline = [
    {"$match": {"date": {"$gt": "2025-09-01"}}},
    {"$project": {"_id": 1, "height_for_last_block": 1}},
]
primed_blocks = [
    x["height_for_last_block"] + 1 for x in db[Collections.paydays_v2].aggregate(pipeline)
]
all_blocks = primed_blocks
possible_sus_blocks = []
for block in track(primed_blocks):
    block_info = CCD_BlockInfo(**db[Collections.blocks].find_one({"height": block}))  # type: ignore
    end = block_info.slot_time - timedelta(hours=1) + timedelta(seconds=6)
    start = block_info.slot_time - timedelta(hours=1) - timedelta(seconds=6)
    epoch = block_info.epoch
    pipeline = [
        {"$match": {"slot_time": {"$gte": start, "$lt": end}}},
        {"$match": {"epoch": epoch - 1}},  # type: ignore
        {"$sort": {"slot_time": 1}},
        {"$limit": 1},
        {"$project": {"_id": 1, "height": 1, "epoch": 1}},
    ]
    possible_sus_blocks.extend([x["height"] for x in db[Collections.blocks].aggregate(pipeline)])
all_blocks.extend(possible_sus_blocks)
blocks = sorted(set(all_blocks))
# pipeline = [
#     {"$match": {"block_info.height": {"$gte": 37211558}}},
#     {"$match": {"account_transaction.effects.account_transfer": {"$exists": True}}},
#     {"$project": {"_id": 0, "block_info.height": 1}},
# ]
# blocks = [x["block_info"]["height"] for x in db[Collections.transactions].aggregate(pipeline)]


async def main() -> None:
    print(f"[local] Running on {net} as indexers")

    subscriber = Subscriber(grpcclient, tooter, motormongo, mongodb)
    subscriber.net = net

    for height in blocks:
        print(f"[local] processing height={height}")
        subscriber.generate_indices_based_on_transactions(height)
        subscriber.do_specials(height)
        await asyncio.sleep(0)

    print("[local] done")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[local] interrupted", file=sys.stderr)
