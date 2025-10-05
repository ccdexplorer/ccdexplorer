import dagster as dg
from ccdexplorer.grpc_client import GRPCClient
from ccdexplorer.mongodb import MongoDB
from ccdexplorer.tooter import Tooter


class GRPCResource(dg.ConfigurableResource):
    """Resource to access the GRPCClient"""

    def get_client(self) -> GRPCClient:
        grpc = GRPCClient()
        return grpc


class MongoDBResource(dg.ConfigurableResource):
    """Resource to access the shared MongoDB database"""

    def get_client(self) -> MongoDB:
        tooter: Tooter = Tooter()
        mongodb: MongoDB = MongoDB(tooter, nearest=True)
        return mongodb


# Create single instances
mongodb_resource_instance = MongoDBResource()
grpc_resource_instance = GRPCResource()
