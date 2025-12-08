#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import sys
from typing import List

from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import CCD_BlockItemSummary
from ccdexplorer.mongodb import MongoDB, MongoMotor, Collections
from ccdexplorer.tooter import Tooter
from ccdexplorer.ms_modules.subscriber import Subscriber
from ccdexplorer.concordium_client import ConcordiumClient
from ccdexplorer.env import RUN_ON_NET

grpcclient = GRPCClient()
tooter = Tooter()
mongodb = MongoDB(tooter, nearest=True)
block_heights: List[int] = [39566930]
db = mongodb.mainnet if RUN_ON_NET == "mainnet" else mongodb.testnet
# pipeline = [
#     {"$match": {"block_info.height": {"$gt": 30_000_000}}},
#     {"$match": {"account_transaction.effects.module_deployed": {"$exists": True}}},
#     {"$project": {"block_info.height": 1, "_id": 0}},
# ]
# block_heights = [
#     x["block_info"]["height"] for x in db[Collections.transactions].aggregate(pipeline)
# ]


async def main() -> None:
    print(f"[local] Running on {RUN_ON_NET} as module_deployed")

    # Build dependencies (same as in main.py)
    grpcclient = GRPCClient()
    tooter = Tooter()
    motormongo = MongoMotor(tooter, nearest=True)
    mongodb = MongoDB(tooter)
    concordium_client = ConcordiumClient(tooter=tooter)
    subscriber = Subscriber(grpcclient, tooter, motormongo, mongodb, concordium_client)

    net = NET(RUN_ON_NET)
    db_to_use = mongodb.testnet if RUN_ON_NET == "testnet" else mongodb.mainnet

    for h in block_heights:
        print(f"[local] processing block_height={h}")
        pipeline = [
            {"$match": {"block_info.height": h}},
            {"$match": {"account_transaction.effects.module_deployed": {"$exists": True}}},
        ]
        txs = [
            CCD_BlockItemSummary(**x)
            for x in db_to_use[Collections.transactions].aggregate(pipeline)
        ]
        for tx in txs:
            if tx.account_transaction:
                if tx.account_transaction.effects.module_deployed:
                    module_ref = tx.account_transaction.effects.module_deployed
                    await subscriber.process_new_module(net, module_ref)
                    await subscriber.verify_module(net, subscriber.concordium_client, module_ref)
                    if net == NET.MAINNET:
                        await subscriber.save_smart_contracts_overview(net)

        await asyncio.sleep(0)

    print("[local] done")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[local] interrupted", file=sys.stderr)
