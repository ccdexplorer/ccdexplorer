from .heartbeat_sub import Heartbeat
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.tooter import Tooter
from ccdexplorer.mongodb import (
    MongoDB,
    MongoMotor,
)

import asyncio
import datetime as dt


# from apscheduler.triggers.interval import IntervalTrigger
from scheduler.asyncio import Scheduler
from ccdexplorer.env import RUN_ON_NET
from rich.console import Console
import urllib3


urllib3.disable_warnings()

console = Console()
grpcclient = GRPCClient()
tooter = Tooter()

mongodb = MongoDB(tooter)
motormongo = MongoMotor(tooter)

heartbeat = Heartbeat(grpcclient, tooter, mongodb, motormongo, RUN_ON_NET)


async def main():
    """
    The Hearbeat repo is an endless async loop of three methods:
    1. `get_finalized_blocks`: this method looks up the last processed block
    in a mongoDB helper collection, and determines how many finalized
    blocks it needs to request from the node. These blocks are then added
    to the queue `finalized_block_infos_to_process`.
    2. `process_blocks` picks up this queue of blocks to process, and
    continues processing until the queue is empty again. For every block,
    we store the block_info (including tx hashes) into the collection `blocks`.
    Furthermore, we inspect all transactions for a block to determine whether we need
    to create any indices for them.
    3. `send_to_mongo`: this method takes all queues and sends them to the respective
    MongoDB collections.
    """
    console.log(f"{RUN_ON_NET=}")

    heartbeat = Heartbeat(grpcclient, tooter, mongodb, motormongo, RUN_ON_NET)

    schedule = Scheduler()

    schedule.cyclic(dt.timedelta(seconds=1), heartbeat.get_finalized_blocks)
    schedule.cyclic(dt.timedelta(seconds=1), heartbeat.process_blocks)
    schedule.cyclic(dt.timedelta(seconds=1), heartbeat.send_to_mongo)
    schedule.cyclic(dt.timedelta(seconds=1), heartbeat.get_special_purpose_blocks)
    schedule.cyclic(dt.timedelta(seconds=1), heartbeat.process_special_purpose_blocks)
    if RUN_ON_NET == "mainnet":
        schedule.cyclic(dt.timedelta(seconds=15), heartbeat.get_project_addresses)
    while True:
        await asyncio.sleep(0.01)
