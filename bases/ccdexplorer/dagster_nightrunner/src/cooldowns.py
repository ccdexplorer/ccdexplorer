import dagster as dg

from ..nightrunner.update_cooldowns import perform_data_for_cooldowns
from ._partitions import partitions_def_from_cooldowns
from ._resources import (
    GRPCResource,
    MongoDBResource,
    grpc_resource_instance,
    mongodb_resource_instance,
)

asset_name: str = "cooldowns"


@dg.asset(
    partitions_def=partitions_def_from_cooldowns,
    name=asset_name,
    group_name="source_grpc",
)
def cooldowns(
    context: dg.AssetExecutionContext,
    mongo_resource: dg.ResourceParam[MongoDBResource],
    grpc_resource: dg.ResourceParam[GRPCResource],
) -> dict:
    """How much stake is locked in cooldown, per day.

    Read at the day's final block rather than at last_final, so the series
    can be backfilled: the node still answers for blocks back to P7.
    """
    mongodb = mongo_resource.get_client()
    grpcclient = grpc_resource.get_client()
    partition_date = context.partition_key
    context.log.info(f"Processing data for {partition_date}")
    return perform_data_for_cooldowns(context, partition_date, mongodb, grpcclient)


defs = dg.Definitions(
    assets=[cooldowns],
    resources={
        "mongo_resource": mongodb_resource_instance,
        "grpc_resource": grpc_resource_instance,
    },
)
