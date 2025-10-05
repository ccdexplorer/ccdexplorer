from __future__ import annotations

import aiohttp
import urllib3
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.mongodb import (
    Collections,
    CollectionsUtilities,
    MongoDB,
    MongoMotor,
)
from ccdexplorer.tooter import Tooter
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.collection import Collection
from rich.console import Console

from .metadata import MetaData as _metadata
from .utils import Utils as _utils

urllib3.disable_warnings()
console = Console()


class Subscriber(_metadata, _utils):
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
        self.mainnet: dict[Collections, Collection] = self.mongodb.mainnet
        self.testnet: dict[Collections, Collection] = self.mongodb.testnet
        self.motor_mainnet: dict[Collections, AsyncCollection] = self.motormongo.mainnet
        self.motor_testnet: dict[Collections, AsyncCollection] = self.motormongo.testnet
        self.motor_utilities: dict[CollectionsUtilities, AsyncCollection] = (
            self.motormongo.utilities
        )

    async def init_sessions(self) -> None:
        quick_timeout = aiohttp.ClientTimeout(total=1, connect=1, sock_read=1)
        slow_timeout = aiohttp.ClientTimeout(total=10, connect=5, sock_read=10)

        self.quick_session = aiohttp.ClientSession(timeout=quick_timeout)
        self.slow_session = aiohttp.ClientSession(timeout=slow_timeout)
