#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import os
import sys

# Only change this value to switch networks.
NET = "devnet"

os.environ["RUN_ON_NET"] = NET

# Import after RUN_ON_NET is set, since ms_block_analyser.core reads it at
# import time to build its GRPCClient and Celery queue names.
from ccdexplorer.mongodb import Collections, MongoDB, net_db  # noqa: E402
from ccdexplorer.ms_block_analyser.core import extract_processors, publish_to_celery  # noqa: E402
from ccdexplorer.tooter import Tooter  # noqa: E402
from rich.progress import track  # noqa: E402

tooter = Tooter()
mongodb = MongoDB(tooter, caller_name="ms_block_analyser_backfill")
db = net_db(mongodb, NET)

pipeline = [
    {"$sort": {"height": 1}},
    {"$project": {"_id": 1, "height": 1}},
]
blocks = list(db[Collections.blocks].aggregate(pipeline))


async def main() -> None:
    print(f"[local] backfilling {len(blocks):,.0f} blocks on {NET}")

    for block in track(blocks, description="processing"):
        block_hash = block["_id"]
        height = block["height"]
        processors = extract_processors(block_hash)
        print(f"{height:,.0f} - {processors}")
        for proc in processors:
            await publish_to_celery(proc, {"height": height, "block_hash": block_hash})

    print("[local] done")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[local] interrupted", file=sys.stderr)
