from __future__ import annotations

import aiohttp
import urllib3
from ccdexplorer.grpc_client import GRPCClient
from pymongo.asynchronous.collection import AsyncCollection
from ccdexplorer.mongodb import (
    Collections,
    CollectionsUtilities,
    MongoDB,
    MongoMotor,
)
from ccdexplorer.tooter import Tooter
from pymongo.collection import Collection
from rich.console import Console
from ccdexplorer.env import RUN_ON_NET
from .indexers import Indexers as _indexers
from .utils import Utils as _utils
from enum import Enum

urllib3.disable_warnings()
console = Console()


class Subscriber(_indexers, _utils):
    def __init__(
        self,
        grpcclient: GRPCClient,
        tooter: Tooter,
        motormongo: MongoMotor,
        mongodb: MongoDB,
    ):
        self.grpc_client = grpcclient
        self.tooter = tooter
        self.motormongo = motormongo
        self.mongodb = mongodb
        self.net = RUN_ON_NET  # type: ignore
        self.queues: dict[Enum, list] = {}
        self.mainnet: dict[Collections, Collection] = self.mongodb.mainnet
        self.testnet: dict[Collections, Collection] = self.mongodb.testnet
        self.motor_mainnet: dict[Collections, AsyncCollection] = self.motormongo.mainnet
        self.motor_testnet: dict[Collections, AsyncCollection] = self.motormongo.testnet
        self.motor_utilities: dict[CollectionsUtilities, AsyncCollection] = (
            self.motormongo.utilities
        )
        self.namespace: str = (
            "concordium_mainnet" if self.net == "mainnet" else "concordium_testnet"
        )

    async def init_sessions(self) -> None:
        quick_timeout = aiohttp.ClientTimeout(total=1, connect=1, sock_read=1)
        slow_timeout = aiohttp.ClientTimeout(total=10, connect=5, sock_read=10)

        self.quick_session = aiohttp.ClientSession(timeout=quick_timeout)
        self.slow_session = aiohttp.ClientSession(timeout=slow_timeout)
