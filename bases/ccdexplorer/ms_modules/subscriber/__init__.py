from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.mongodb import (
    Collections,
    CollectionsUtilities,
    MongoMotor,
    MongoDB,
)
from ccdexplorer.tooter import Tooter
from ccdexplorer.concordium_client import ConcordiumClient
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.collection import Collection
from rich.console import Console

from .module import Module as _module

console = Console()


class Subscriber(_module):
    def __init__(
        self,
        grpcclient: GRPCClient,
        tooter: Tooter,
        motormongo: MongoMotor,
        mongodb: MongoDB,
        concordium_client: ConcordiumClient,
    ):
        self.grpc_client = grpcclient
        self.tooter = tooter
        self.motormongo = motormongo
        self.concordium_client = concordium_client
        self.mongodb = mongodb
        self.mainnet: dict[Collections, Collection] = self.mongodb.mainnet
        self.testnet: dict[Collections, Collection] = self.mongodb.testnet
        self.motor_mainnet: dict[Collections, AsyncCollection] = self.motormongo.mainnet
        self.motor_testnet: dict[Collections, AsyncCollection] = self.motormongo.testnet
        self.motor_utilities: dict[CollectionsUtilities, AsyncCollection] = (
            self.motormongo.utilities
        )

    def exit(self):
        pass
