#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import sys
from typing import List

from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.mongodb import MongoDB, MongoMotor, Collections
from ccdexplorer.tooter import Tooter

from subscriber import Subscriber
from env import RUN_ON_NET

grpcclient = GRPCClient()
tooter = Tooter()
motormongo = MongoMotor(tooter, nearest=True)
mongodb = MongoDB(tooter)

token_addresses: List[str] = ["<9484,0>-7e2efe07"]


pipeline = [
    {"$match": {"failed_attempt": {"$exists": True}}},
    {"$match": {"contract": "<9378,0>"}},
    # {
    #     "$match": {
    #         "failed_attempt.last_error": "'Subscriber' object has no attribute 'slow_session'"
    #     }
    # },
    # {"$sort": {"contract": 1}},
    # {"$sort": {"token_id": 1}},
    {"$project": {"_id": 1}},
]
token_addresses = [
    x["_id"] for x in mongodb.mainnet[Collections.tokens_token_addresses_v2].aggregate(pipeline)
]


async def main() -> None:
    print(f"[local] Running on {RUN_ON_NET} as metadata")

    # Same dependencies as the worker
    grpcclient = GRPCClient()
    tooter = Tooter()
    motormongo = MongoMotor(tooter, nearest=True)
    mongodb = MongoDB(tooter)

    subscriber = Subscriber(grpcclient, tooter, motormongo, mongodb)
    await subscriber.init_sessions()
    net = NET(RUN_ON_NET)

    for addr in token_addresses:
        await subscriber.fetch_token_metadata(net, addr)
        await asyncio.sleep(0)

    print("[local] done")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[local] interrupted", file=sys.stderr)
