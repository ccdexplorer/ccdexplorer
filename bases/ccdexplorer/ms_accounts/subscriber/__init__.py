# ruff: noqa: F403, F405, E402, E501, E722
import urllib3
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.mongodb import (
    Collections,
    MongoDB,
    MongoMotor,
)
from ccdexplorer.tooter import Tooter
from pymongo.collection import Collection
from rich.console import Console

from ccdexplorer.env import *

from .address import Address as _address


urllib3.disable_warnings()
console = Console()


class Subscriber(_address):
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

    def exit(self):
        pass
