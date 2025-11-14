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


class MongoDBResource(dg.ConfigurableResource):
    """Resource to access the shared MongoDB database"""

    def get_client(self) -> MongoDB:
        tooter: Tooter = Tooter()
        mongodb: MongoDB = MongoDB(tooter, nearest=True)
        return mongodb


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
