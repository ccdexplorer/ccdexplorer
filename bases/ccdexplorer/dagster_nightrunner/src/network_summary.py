import dagster as dg

from ..nightrunner.update_network_summary import perform_data_for_network_summary
from .accounts_repo import accounts_repo
from ._resources import (
    GRPCResource,
    MongoDBResource,
    RepoResource,
    grpc_resource_instance,
    mongodb_resource_instance,
    repo_resource_instance,
)
from ._partitions import partitions_def_from_genesis

asset_name: str = "network_summary"


@dg.asset(
    deps=[accounts_repo],
    partitions_def=partitions_def_from_genesis,
    name=asset_name,
    group_name="source_grpc_repo",
    automation_condition=dg.AutomationCondition.eager(),
)
def network_summary(
    context: dg.AssetExecutionContext,
    mongo_resource: dg.ResourceParam[MongoDBResource],
    grpc_resource: dg.ResourceParam[GRPCResource],
    repo_resource: dg.ResourceParam[RepoResource],
) -> dict:
    """
    Calculate network summary statistics per day.
    """
    mongodb = mongo_resource.get_client()
    grpcclient = grpc_resource.get_client()
    commits_by_day = repo_resource.get_commits_by_day()
    partition_date = context.partition_key
    context.log.info(f"Processing data for {partition_date}")
    dct = {}
    dct: dict = perform_data_for_network_summary(
        context, partition_date, commits_by_day, mongodb, grpcclient
    )
    return dct


defs = dg.Definitions(
    assets=[network_summary],
    resources={
        "mongo_resource": mongodb_resource_instance,
        "grpc_resource": grpc_resource_instance,
        "repo_resource": repo_resource_instance,
    },
)
