#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import datetime as dt
import os
import sys

from scheduler.asyncio import Scheduler

# Only change this value to switch networks.
NET = "testnet"  # "mainnet" | "testnet"

# If you want heartbeat debug logs, set DEBUG_MODE = True or run with DEBUG=True.
DEBUG_MODE = False


def _validate_net(net: str) -> None:
    if net not in {"mainnet", "testnet"}:
        raise ValueError(f"Invalid NET={net!r}. Expected 'mainnet' or 'testnet'.")


def _configure_env(net: str) -> None:
    os.environ["RUN_ON_NET"] = net
    os.environ["DEBUG"] = "True" if DEBUG_MODE else os.environ.get("DEBUG", "False")


def _build_heartbeat(net: str):
    _configure_env(net)

    # Import after env setup so heartbeat picks up RUN_ON_NET/DEBUG correctly.
    from ccdexplorer.grpc_client import GRPCClient
    from ccdexplorer.heartbeat.heartbeat_sub import Heartbeat
    from ccdexplorer.mongodb import MongoDB, MongoMotor
    from ccdexplorer.tooter import Tooter

    grpcclient = GRPCClient()
    tooter = Tooter()
    mongodb = MongoDB(tooter)
    motormongo = MongoMotor(tooter)
    return Heartbeat(grpcclient, tooter, mongodb, motormongo, net)


async def main() -> None:
    _validate_net(NET)
    heartbeat = _build_heartbeat(NET)

    print(f"[local] heartbeat starting on net={NET} debug={os.environ.get('DEBUG')}")

    schedule = Scheduler()
    schedule.cyclic(dt.timedelta(microseconds=100), heartbeat.get_finalized_blocks)
    schedule.cyclic(dt.timedelta(microseconds=100), heartbeat.process_blocks)
    schedule.cyclic(dt.timedelta(microseconds=100), heartbeat.send_to_mongo)
    schedule.cyclic(dt.timedelta(seconds=3), heartbeat.get_special_purpose_blocks)
    schedule.cyclic(dt.timedelta(seconds=3), heartbeat.process_special_purpose_blocks)
    if NET == "mainnet":
        schedule.cyclic(dt.timedelta(seconds=15), heartbeat.get_project_addresses)

    while True:
        await asyncio.sleep(0.01)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[local] interrupted", file=sys.stderr)
