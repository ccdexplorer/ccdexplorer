import datetime as dt

import urllib3
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import CCD_BlockInfo
from ccdexplorer.mongodb import (
    Collections,
    CollectionsUtilities,
    MongoDB,
    MongoMotor,
)
from pymongo.asynchronous.collection import AsyncCollection

from ccdexplorer.tooter import Tooter
from pymongo.collection import Collection
from rich.console import Console

from .block_loop import BlockLoop as _block_loop
from .block_processing import BlockProcessing as _block_processing
from .send_to_mongo import SendToMongo as _send_to_mongo

# bump
from .start_over import StartOver as _start_over
from .utils import Queue

urllib3.disable_warnings()
console = Console()


class Heartbeat(
    _block_loop,
    _block_processing,
    _start_over,
    _send_to_mongo,
):
    def __init__(
        self,
        grpcclient: GRPCClient,
        tooter: Tooter,
        mongodb: MongoDB,
        motormongo: MongoMotor,
        net: str,
    ):
        self.grpc_client = grpcclient
        self.tooter = tooter
        self.mongodb = mongodb
        self.motormongo = motormongo

        self.net = net
        self.namespace: str = "concordium_mainnet" if net == "mainnet" else "concordium_testnet"
        self.utilities: dict[CollectionsUtilities, Collection] = self.mongodb.utilities
        self.db: dict[Collections, Collection] = (
            self.mongodb.mainnet if self.net == "mainnet" else self.mongodb.testnet
        )
        self.motordb: dict[Collections, AsyncCollection] = (
            self.motormongo.testnet if net == "testnet" else self.motormongo.mainnet
        )
        self.finalized_block_infos_to_process: list[CCD_BlockInfo] = []
        self.special_purpose_block_infos_to_process: list[CCD_BlockInfo] = []

        self.queues: dict[Queue, list] = {}
        self.project_addresses = {}
        for q in Queue:
            self.queues[q] = []

        # this gets set every time the log heartbeat last processed helper gets set
        # in block_loop we check if this value is < x min from now.
        # If so, we restart, as there's probably something wrong that a restart
        # can fix.
        self.internal_freqency_timer: dt.datetime = dt.datetime.now().astimezone(tz=dt.timezone.utc)
