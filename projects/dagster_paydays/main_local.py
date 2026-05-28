#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import sys
from typing import List
from ccdexplorer.mongodb import MongoDB, Collections, MongoMotor
from ccdexplorer.domain.generic import NET
from ccdexplorer.grpc_client import GRPCClient

from ccdexplorer.tooter import Tooter
from unittest.mock import Mock

import dagster as dg
from ccdexplorer.dagster_paydays.paydays.update_payday_init import perform_payday_init
from ccdexplorer.dagster_paydays.paydays.update_payday_state_information import (
    perform_payday_state_information_for_current_payday,
)

from ccdexplorer.env import RUN_ON_NET

block_heights: List[int] = [35778146]


async def main() -> None:
    print(f"[local] Running on {RUN_ON_NET}")
    context = Mock(spec=dg.AssetExecutionContext)
    context.log = Mock()
    context.log.info = Mock()

    # Reuse the same dependencies your worker uses
    grpcclient = GRPCClient()
    tooter = Tooter()
    motormongo = MongoMotor(tooter, nearest=True, caller_name="paydays_local")
    mongodb = MongoDB(tooter, caller_name="ms_accounts")

    db_to_use = mongodb.mainnet
    blocks_to_retry = set()
    pipeline = [
        {"$sort": {"date": -1}},
        # {"$project": {"_id": 1, "height_for_last_block": 1}},
    ]
    result = list(db_to_use[Collections.paydays_v2].aggregate(pipeline))
    # blocks_to_retry = [(x["height_for_last_block"] + 1) for x in result][100:]
    net = NET("mainnet")

    for r in reversed(result[:100]):
        print(r["date"])
        payday_info = perform_payday_init(
            context=context,
            mongodb=mongodb,
            grpcclient=grpcclient,
            payday_date_string=r["date"],
            payday_block_hash=None,  # type: ignore
        )
        payday_info = perform_payday_state_information_for_current_payday(
            context=context,
            mongodb=mongodb,
            grpcclient=grpcclient,
            payday_info=payday_info,
        )
        print(r["date"], "...done")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[local] interrupted", file=sys.stderr)
