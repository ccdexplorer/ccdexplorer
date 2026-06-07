import dagster as dg

from ..nightrunner.update_agent_registry_statistics import perform_agent_registry_statistics_update
from ._partitions import partitions_def_from_agent_registry
from ._resources import (
    MongoDBResource,
    mongodb_resource_instance,
    GRPCResource,
    grpc_resource_instance,
)
from .forex import forex


asset_name = "agent_registry_statistics"


@dg.asset(
    partitions_def=partitions_def_from_agent_registry,
    name=asset_name,
    group_name="source_mongo",
    output_required=False,
    automation_condition=dg.AutomationCondition.eager(),
)
def agent_registry_statistics(
    context: dg.AssetExecutionContext,
    mongo_resource: dg.ResourceParam[MongoDBResource],
) -> bool | None:
    """Agent Registry statistics for graphs."""
    mongodb = mongo_resource.get_client()
    partition_key = context.partition_key
    materialized = perform_agent_registry_statistics_update(context, partition_key, mongodb)

    if materialized:
        return True
    else:
        raise Exception(f"Failed to materialize Agent Registry statistics: {partition_key}")


defs = dg.Definitions(
    assets=[agent_registry_statistics],
    resources={
        "mongo_resource": mongodb_resource_instance,
    },
)
