import datetime as dt
from concurrent.futures import ThreadPoolExecutor

import urllib3
from ccdexplorer.env import HEARTBEAT_FETCH_CONCURRENCY
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
        self.namespace: str = {
            "mainnet": "concordium_mainnet",
            "testnet": "concordium_testnet",
            "devnet": "concordium_devnet",
        }[net]
        self.utilities: dict[CollectionsUtilities, Collection] = self.mongodb.utilities
        self.db: dict[Collections, Collection] = {
            "mainnet": self.mongodb.mainnet,
            "testnet": self.mongodb.testnet,
            "devnet": self.mongodb.devnet,
        }[net]
        self.motordb: dict[Collections, AsyncCollection] = {
            "mainnet": self.motormongo.mainnet,
            "testnet": self.motormongo.testnet,
            "devnet": self.motormongo.devnet,
        }[net]
        self.finalized_block_infos_to_process: list[CCD_BlockInfo] = []
        self.special_purpose_block_infos_to_process: list[CCD_BlockInfo] = []

        # Shared pool for fanning out independent, blocking gRPC calls
        # (per-block GetBlockInfo / GetBlockSpecialEvents) concurrently
        # instead of one round trip at a time.
        self._grpc_executor = ThreadPoolExecutor(
            max_workers=HEARTBEAT_FETCH_CONCURRENCY, thread_name_prefix="heartbeat-grpc"
        )

        self.queues: dict[Queue, list] = {}
        self.project_addresses = {}
        for q in Queue:
            self.queues[q] = []

        # this gets set every time the log heartbeat last processed helper gets set
        # in block_loop we check if this value is < x min from now.
        # If so, we restart, as there's probably something wrong that a restart
        # can fix.
        self.internal_freqency_timer: dt.datetime = dt.datetime.now().astimezone(tz=dt.timezone.utc)
