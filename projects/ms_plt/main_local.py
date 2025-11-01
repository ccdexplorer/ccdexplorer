#!/usr/bin/env python3
from __future__ import annotations

import asyncio
from datetime import timedelta
import sys

from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.mongodb import MongoDB, MongoMotor, Collections
from ccdexplorer.tooter import Tooter
from ccdexplorer.grpc_client.CCD_Types import CCD_BlockInfo
from ccdexplorer.ms_plt.update_plts_from_txs import update_plts
from rich.progress import track

grpcclient = GRPCClient()
tooter = Tooter()
motormongo = MongoMotor(tooter, nearest=True)
mongodb = MongoDB(tooter)
net = "mainnet"
blocks = [37211558]
db = mongodb.mainnet if net == "mainnet" else mongodb.testnet

pipeline = [
    {"$match": {"block_info.height": {"$gte": 37600000}}},
    {"$match": {"account_transaction.effects.token_update_effect": {"$exists": True}}},
    {"$project": {"_id": 0, "block_info.height": 1}},
]
blocks = [x["block_info"]["height"] for x in db[Collections.transactions].aggregate(pipeline)]


async def main() -> None:
    print(f"[local] Running on {net} as indexers")

    for height in blocks:
        print(f"[local] processing height={height}")
        update_plts(mongodb, grpcclient, net, height)
        await asyncio.sleep(0)

    print("[local] done")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[local] interrupted", file=sys.stderr)
