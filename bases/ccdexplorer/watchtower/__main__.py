"""Wiring for the watchtower.

Everything testable lives in core.py; this module only builds the clients and
starts the schedule, following heartbeat's shape -- including
`scheduler.asyncio`, so the port drops the old apscheduler 4.0.0a4 alpha.
"""

import asyncio
import datetime as dt
import logging
import os
import time

from rich.console import Console

from ccdexplorer.env import ADMIN_CHAT_ID
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.mongodb import Collections, MongoDB, MongoMotor, net_db
from ccdexplorer.single_instance import SingleInstanceLock
from ccdexplorer.tooter import Tooter, TooterChannel, TooterType
from scheduler.asyncio import Scheduler

from .core import WatchTower

console = Console()
log = logging.getLogger(__name__)

SERVICE = "watchtower"
LOCK_NET = "mainnet"
STANDBY_POLL_SECONDS = 15
CHECK_INTERVAL = dt.timedelta(seconds=60)


def wait_for_our_turn(lock: SingleInstanceLock) -> None:
    announced = False
    while not lock.acquire():
        if not announced:
            log.warning(
                "another watchtower holds the lock; standing by, checking every %ss",
                STANDBY_POLL_SECONDS,
            )
            announced = True
        time.sleep(STANDBY_POLL_SECONDS)


async def run(watchtower: WatchTower) -> None:
    schedule = Scheduler()
    schedule.cyclic(CHECK_INTERVAL, watchtower.check_bot)
    schedule.cyclic(CHECK_INTERVAL, watchtower.check_mongo)
    schedule.cyclic(CHECK_INTERVAL, watchtower.check_heartbeat_running)
    while True:
        await asyncio.sleep(3600)


if __name__ == "__main__":
    grpcclient = GRPCClient()
    tooter = Tooter()
    mongodb = MongoDB(tooter, caller_name=SERVICE)
    motormongo = MongoMotor(tooter, caller_name=SERVICE)

    lock = SingleInstanceLock(
        collection=net_db(mongodb, LOCK_NET)[Collections.helpers],
        service=SERVICE,
    )
    wait_for_our_turn(lock)

    def lease_lost() -> None:
        tooter.relay(
            channel=TooterChannel.NOTIFIER,
            title="",
            chat_id=ADMIN_CHAT_ID,
            body="Watchtower lost its single-instance lease. Exiting.",
            notifier_type=TooterType.REQUESTS_ERROR,
        )
        os._exit(1)

    lock.start_renewing(on_lost=lease_lost)

    watchtower = WatchTower(
        grpcclient, tooter, mongodb, motormongo, admin_chat_id=ADMIN_CHAT_ID
    )
    console.log("Watchtower starting")
    try:
        asyncio.run(run(watchtower))
    except Exception as e:
        console.log("main error: ", e)
