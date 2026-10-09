import dagster as dg
from ccdexplorer.domain.generic import NET

from ..nightrunner.update_classified_pools import perform_data_for_classified_pools
from .accounts_repo import accounts_repo
from ._resources import (
    GRPCResource,
    MongoDBResource,
    RepoResource,
    grpc_resource_instance,
    mongodb_resource_instance,
    repo_resource_instance,
)
from ._partitions import partitions_def_from_staking

asset_name: str = "classified_pools"


@dg.asset(
    deps=[accounts_repo],
    partitions_def=partitions_def_from_staking,
    name=asset_name,
    group_name="source_repo",
    automation_condition=dg.AutomationCondition.eager(),
)
def classified_pools(
    context: dg.AssetExecutionContext,
    repo_resource: dg.ResourceParam[RepoResource],
    mongo_resource: dg.ResourceParam[MongoDBResource],
    grpc_resource: dg.ResourceParam[GRPCResource],
) -> dict:
    """
    Statistics on staking pools.

    """
    mongodb = mongo_resource.get_client()
    commits_by_day = repo_resource.get_commits_by_day()
    partition_date = context.partition_key
    context.log.info(f"Processing data for {partition_date}")
    dct = {}
    # grpc as well as the repo snapshot: the passive delegator count is not
    # in the accounts csv, and it is two thirds of the total.
    dct: dict = perform_data_for_classified_pools(
        context,
        partition_date,
        commits_by_day,
        mongodb,
        grpc_resource.get_client(),
        NET.MAINNET,
    )
    return dct


defs = dg.Definitions(
    assets=[classified_pools],
    resources={
        "repo_resource": repo_resource_instance,
        "mongo_resource": mongodb_resource_instance,
        "grpc_resource": grpc_resource_instance,
    },
)
