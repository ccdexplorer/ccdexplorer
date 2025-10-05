import dagster as dg

from ..nightrunner.update_plt_statistics import perform_plt_statistics_update
from ._partitions import partitions_def_from_plts
from ._resources import (
    MongoDBResource,
    mongodb_resource_instance,
    GRPCResource,
    grpc_resource_instance,
)
from .forex import forex


asset_name = "plt_statistics"


@dg.asset(
    partitions_def=partitions_def_from_plts,
    deps=[forex],
    name=asset_name,
    group_name="source_mongo",
    output_required=False,
    automation_condition=dg.AutomationCondition.eager(),
)
def plt_statistics(
    context: dg.AssetExecutionContext,
    mongo_resource: dg.ResourceParam[MongoDBResource],
    grpc_resource: dg.ResourceParam[GRPCResource],
) -> bool | None:
    """PLT statistics for graphs."""
    mongodb = mongo_resource.get_client()
    grpcclient = grpc_resource.get_client()
    partition_key = context.partition_key
    materialized = perform_plt_statistics_update(context, partition_key, mongodb, grpcclient)

    if materialized:
        return True
    else:
        raise Exception(f"Failed to materialize PLT statistics: {partition_key}")


defs = dg.Definitions(
    assets=[plt_statistics],
    resources={
        "mongo_resource": mongodb_resource_instance,
        "grpc_resource": grpc_resource_instance,
    },
)
