# ruff: noqa: F403, F405, E402, E501, E722
import urllib3
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.mongodb import (
    Collections,
    CollectionsUtilities,
    MongoDB,
    MongoMotor,
)
from ccdexplorer.tooter import Tooter
from pymongo.collection import Collection
from pymongo.asynchronous.collection import AsyncCollection
from redis.asyncio import Redis
from rich.console import Console
import datetime as dt

from .impacted_addresses_from_tx import ImpactedAddresses as _impacted_addresses_from_tx
from .logged_events import LoggedEvent as _logged_event
from .utils import Utils as _utils

urllib3.disable_warnings()
console = Console()


class Subscriber(_logged_event, _impacted_addresses_from_tx, _utils):
    def __init__(
        self,
        grpcclient: GRPCClient,
        tooter: Tooter | None,
        motormongo: MongoMotor,
        mongodb: MongoDB,
    ):
        self.grpc_client = grpcclient
        self.tooter = tooter  # type: ignore
        self.motormongo = motormongo
        self.mongodb = mongodb
        self.redis: Redis | None = None
        # caches
        self.contract_supports = {}
        self.source_module_refs = {}
        self.source_module_ref_to_schema_cache = {}
        self.instances_cache = {}
        self.block_dict = {}

        self.mainnet: dict[Collections, Collection] = self.mongodb.mainnet
        self.testnet: dict[Collections, Collection] = self.mongodb.testnet
        self.motor_mainnet: dict[Collections, AsyncCollection] = (  # type: ignore
            self.motormongo.mainnet
        )
        self.motor_testnet: dict[Collections, AsyncCollection] = (  # type: ignore
            self.motormongo.testnet
        )
        self.motor_utilities: dict[CollectionsUtilities, AsyncCollection] = (
            self.motormongo.utilities
        )
        self.cis5_keys_update_last_requested = dt.datetime.now().astimezone(dt.UTC) - dt.timedelta(
            seconds=10
        )
        self.cis5_keys_cache = None

    def exit(self):
        pass
