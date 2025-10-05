import dagster as dg

from ..nightrunner.update_microccd import perform_data_for_microccd
from ._resources import (
    MongoDBResource,
    GRPCResource,
    mongodb_resource_instance,
    grpc_resource_instance,
)
from ._partitions import partitions_def_from_genesis

asset_name: str = "microccd"


@dg.asset(
    partitions_def=partitions_def_from_genesis,
    name=asset_name,
    group_name="source_grpc",
)
def microccd(
    context: dg.AssetExecutionContext,
    mongo_resource: dg.ResourceParam[MongoDBResource],
    grpc_resource: dg.ResourceParam[GRPCResource],
) -> dict:
    """
    Ratios for NRG and GTU per day.
    """
    mongodb = mongo_resource.get_client()
    grpcclient = grpc_resource.get_client()
    partition_date = context.partition_key
    context.log.info(f"Processing data for {partition_date}")
    dct = {}
    dct: dict = perform_data_for_microccd(context, partition_date, mongodb, grpcclient)
    return dct


defs = dg.Definitions(
    assets=[microccd],
    resources={
        "mongo_resource": mongodb_resource_instance,
        "grpc_resource": grpc_resource_instance,
    },
)
