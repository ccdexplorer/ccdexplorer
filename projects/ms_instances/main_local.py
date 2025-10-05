#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import sys
from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.mongodb import MongoDB, MongoMotor
from ccdexplorer.tooter import Tooter
from ccdexplorer.ms_instances.subscriber import Subscriber
from ccdexplorer.env import RUN_ON_NET

grpcclient = GRPCClient()
tooter = Tooter()
mongodb = MongoDB(tooter)
motormongo = MongoMotor(tooter, nearest=True)
subscriber = Subscriber(grpcclient, tooter, motormongo, mongodb)
db = mongodb.mainnet if RUN_ON_NET == "mainnet" else mongodb.testnet

# pipeline = [
#     {"$match": {"block_info.height": {"$gt": 36_560_000}}},
#     {
#         "$match": {
#             "$or": [
#                 {"account_transaction.effects.contract_initialized": {"$exists": True}},
#                 {
#                     "account_transaction.effects.contract_update_issued": {
#                         "$exists": True
#                     }
#                 },
#             ]
#         }
#     },
#     {"$sort": {"block_info.height": 1}},
#     {"$project": {"block_info.height": 1, "_id": 0}},
# ]
# blocks = [
#     x["block_info"]["height"] for x in db[Collections.transactions].aggregate(pipeline)
# ]
blocks = [30000000]


async def main() -> None:
    print(f"[local] Running on {RUN_ON_NET} as instances")
    for block_height in blocks:
        await subscriber.process_block_for_instances({"height": block_height}, NET(RUN_ON_NET))
        await asyncio.sleep(0)

    print("[local] done")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[local] interrupted", file=sys.stderr)
