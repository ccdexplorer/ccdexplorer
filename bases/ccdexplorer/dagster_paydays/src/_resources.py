import functools

import dagster as dg
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.mongodb import MongoDB
from ccdexplorer.tooter import Tooter


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
    return MongoDB(Tooter(), nearest=True, caller_name="dagster_paydays")


class MongoDBResource(dg.ConfigurableResource):
    """Resource to access the shared MongoDB database"""

    def get_client(self) -> MongoDB:
        return shared_mongodb()


# Create single instances
mongodb_resource_instance = MongoDBResource()
grpc_resource_instance = GRPCResource()
