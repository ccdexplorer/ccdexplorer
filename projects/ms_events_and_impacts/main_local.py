#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import sys
from typing import List

from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.mongodb import Collections, MongoDB, MongoMotor
from ccdexplorer.tooter import Tooter
from ccdexplorer.env import RUN_ON_NET

from ccdexplorer.ms_events_and_impacts.subscriber import Subscriber

block_heights: List[int] = [46983492]


async def main() -> None:
    print(f"[local] Running on {RUN_ON_NET} as events_and_impacts")

    grpcclient = GRPCClient()
    tooter = Tooter()
    motormongo = MongoMotor(tooter, nearest=True, caller_name="ms_events_and_impacts")
    mongodb = MongoDB(tooter, caller_name="ms_events_and_impacts")

    subscriber = Subscriber(grpcclient, tooter, motormongo, mongodb)

    net = NET(RUN_ON_NET)
    db = mongodb.mainnet if RUN_ON_NET == "mainnet" else mongodb.testnet

    for height in block_heights:
        block_doc = db[Collections.blocks].find_one({"height": height}, {"hash": 1})
        if block_doc is None:
            print(f"[local] block height={height} not found in db, skipping")
            continue
        block_hash: str = block_doc["hash"]
        print(f"[local] processing height={height} hash={block_hash}")
        await subscriber.process_new_logged_events_from_block(net, height, block_hash)
        await asyncio.sleep(0)

    print("[local] done")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[local] interrupted", file=sys.stderr)
