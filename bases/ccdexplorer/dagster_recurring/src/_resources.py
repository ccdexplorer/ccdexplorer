import functools
import dagster as dg
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.mongodb import MongoDB
from ccdexplorer.tooter import Tooter
from redis import Redis
from ccdexplorer.env import REDIS_URL


class GRPCResource(dg.ConfigurableResource):
    """Resource to access the GRPCClient"""

    def get_client(self) -> GRPCClient:
        grpc = GRPCClient()
        return grpc


@functools.cache
def shared_mongodb() -> MongoDB:
    """One MongoDB client per process, built on first use.

    Each MongoDB() opens a fresh MongoClient: three server connections on the
    replica set plus a server_info() round trip before any query runs. Built
    per get_client() call, as this used to be, that was a new client for every
    asset execution, never closed. Caching per process keeps one for the life
    of a run worker, and one for the long-lived code server where schedules
    evaluate. Lazy rather than at import, so importing the definitions opens
    nothing.
    """
    return MongoDB(Tooter(), nearest=True, caller_name="dagster_recurring")


class MongoDBResource(dg.ConfigurableResource):
    """Resource to access the shared MongoDB database"""

    def get_client(self) -> MongoDB:
        return shared_mongodb()


class RedisResource(dg.ConfigurableResource):
    """Resource to access the shared Redis instance"""

    def get_client(self) -> Redis:
        return Redis.from_url(REDIS_URL, decode_responses=False)  # type: ignore


class TooterResource(dg.ConfigurableResource):
    """Resource to access the shared Tooter instance"""

    def get_client(self) -> Tooter:
        return Tooter()


# Create single instances
mongodb_resource_instance = MongoDBResource()
grpc_resource_instance = GRPCResource()
redis_resource_instance = RedisResource()
tooter_resource_instance = TooterResource()
