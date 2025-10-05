import urllib3
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.grpc_client.CCD_Types import CCD_BlockInfo, CCD_ModuleRef
from ccdexplorer.mongodb import (
    Collections,
    CollectionsUtilities,
    MongoDB,
    MongoMotor,
)
from redis.asyncio import Redis
from ccdexplorer.tooter import Tooter
from pymongo.collection import Collection
from rich.console import Console


# from .token_accounting import TokenAccounting as _token_accounting
from .token_accounting_v2 import TokenAccountingV2 as _token_accounting_v2
from .utils import Queue
from pymongo.asynchronous.collection import AsyncCollection

urllib3.disable_warnings()
console = Console()
# bump


class Heartbeat(_token_accounting_v2):
    def __init__(
        self,
        grpcclient: GRPCClient,
        tooter: Tooter,
        mongodb: MongoDB,
        motormongo: MongoMotor,
        net: str,
    ):
        self.grpc_client: GRPCClient = grpcclient
        self.tooter = tooter
        self.mongodb = mongodb
        self.motormongo = motormongo
        self.net = net
        self.redis: Redis | None = None
        self.address_to_follow = None
        self.sending = False
        self.entrypoint_cache = {}
        self.utilities: dict[CollectionsUtilities, Collection] = self.mongodb.utilities
        self.db: dict[Collections, Collection] = (
            self.mongodb.mainnet if self.net == "mainnet" else self.mongodb.testnet
        )
        self.motordb: dict[Collections, AsyncCollection] = (
            self.motormongo.testnet if net == "testnet" else self.motormongo.mainnet
        )
        self.finalized_block_infos_to_process: list[CCD_BlockInfo] = []
        self.special_purpose_block_infos_to_process: list[CCD_BlockInfo] = []

        self.existing_source_modules: dict[CCD_ModuleRef, set] = {}
        self.queues: dict[Queue, list] = {}
        for q in Queue:
            self.queues[q] = []
