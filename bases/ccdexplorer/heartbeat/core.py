from .heartbeat_sub import Heartbeat
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.tooter import Tooter, TooterChannel, TooterType
from ccdexplorer.mongodb import (
    Collections,
    MongoDB,
    MongoMotor,
    net_db,
)
from ccdexplorer.single_instance import SingleInstanceLock

import asyncio
import datetime as dt
import os


# from apscheduler.triggers.interval import IntervalTrigger
from scheduler.asyncio import Scheduler
from ccdexplorer.env import ADMIN_CHAT_ID, RUN_ON_NET
from rich.console import Console
import urllib3


urllib3.disable_warnings()

console = Console()
grpcclient = GRPCClient()
tooter = Tooter()

mongodb = MongoDB(tooter, caller_name=f"heartbeat:{RUN_ON_NET}")
motormongo = MongoMotor(tooter, caller_name=f"heartbeat:{RUN_ON_NET}")

heartbeat = Heartbeat(grpcclient, tooter, mongodb, motormongo, RUN_ON_NET)

# Only one heartbeat per net may run. It keeps its position in a single mongo
# document -- read the last height, request the next run, write the new height
# back -- so a second copy reads the same height, fetches the same blocks and
# races on the write. Nothing enforced that before; being the only deployed
# copy did, until a host is failed over and the original returns with
# `restart: always`.
STANDBY_POLL_SECONDS = 15


async def wait_for_our_turn(lock: SingleInstanceLock) -> None:
    """Stand by until the lease is ours, rather than exiting.

    Waiting makes a second deployment a warm standby: it sits here while the
    other copy works, and takes over by itself within a lease once that copy
    stops renewing. Exiting instead would leave the restart policy to churn,
    and would mean a failover still needed somebody to press something.
    """
    announced = False
    while not lock.acquire():
        if not announced:
            console.log(
                f"another heartbeat holds the {RUN_ON_NET} lock; standing by "
                f"and checking every {STANDBY_POLL_SECONDS}s"
            )
            announced = True
        await asyncio.sleep(STANDBY_POLL_SECONDS)


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

    lock = SingleInstanceLock(
        collection=net_db(mongodb, RUN_ON_NET)[Collections.helpers],
        service=f"heartbeat:{RUN_ON_NET}",
    )
    await wait_for_our_turn(lock)
    console.log(f"holding the heartbeat lock for {RUN_ON_NET} as {lock.holder}")

    def lease_lost() -> None:
        # Another instance took the lease, which means this one has been
        # writing while it no longer had the right to. Exit rather than carry
        # on; the restart policy brings us back as a standby.
        tooter.relay(
            channel=TooterChannel.NOTIFIER,
            title="",
            chat_id=int(ADMIN_CHAT_ID),  # type: ignore
            body=f"Heartbeat on {RUN_ON_NET} lost its single-instance lease. Exiting.",
            notifier_type=TooterType.REQUESTS_ERROR,
        )
        os._exit(1)

    lock.start_renewing(on_lost=lease_lost)

    heartbeat = Heartbeat(grpcclient, tooter, mongodb, motormongo, RUN_ON_NET)

    schedule = Scheduler()

    schedule.cyclic(dt.timedelta(seconds=0.5), heartbeat.get_finalized_blocks)
    schedule.cyclic(dt.timedelta(seconds=0.5), heartbeat.process_blocks)
    schedule.cyclic(dt.timedelta(seconds=0.5), heartbeat.send_to_mongo)
    schedule.cyclic(dt.timedelta(seconds=3), heartbeat.get_special_purpose_blocks)
    schedule.cyclic(dt.timedelta(seconds=3), heartbeat.process_special_purpose_blocks)
    if RUN_ON_NET == "mainnet":
        schedule.cyclic(dt.timedelta(seconds=15), heartbeat.get_project_addresses)
    while True:
        await asyncio.sleep(0.01)
