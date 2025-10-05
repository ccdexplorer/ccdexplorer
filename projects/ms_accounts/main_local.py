#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import sys
from typing import List

from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.mongodb import MongoDB, MongoMotor
from ccdexplorer.tooter import Tooter

from ccdexplorer.ms_accounts.subscriber import Subscriber
from ccdexplorer.env import RUN_ON_NET

block_heights: List[int] = [35778146]


async def main() -> None:
    print(f"[local] Running on {RUN_ON_NET}")

    # Reuse the same dependencies your worker uses
    grpcclient = GRPCClient()
    tooter = Tooter()
    motormongo = MongoMotor(tooter, nearest=True)
    mongodb = MongoDB(tooter)

    subscriber = Subscriber(grpcclient, tooter, motormongo, mongodb)

    net = NET(RUN_ON_NET)
    for h in block_heights:
        print(f"[local] processing block_height={h}")
        await subscriber.process_new_address(net, h)

        await asyncio.sleep(0)

    print("[local] done")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[local] interrupted", file=sys.stderr)
