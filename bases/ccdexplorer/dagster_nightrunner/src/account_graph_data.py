import dagster as dg

from ..nightrunner.update_account_graph import perform_data_for_account_graph
from .accounts_repo import accounts_repo
from ._resources import (
    MongoDBResource,
    RepoResource,
    mongodb_resource_instance,
    repo_resource_instance,
)
from ._partitions import partitions_def_from_genesis

asset_name: str = "account_graph"


@dg.asset(
    deps=[accounts_repo],
    partitions_def=partitions_def_from_genesis,
    name=asset_name,
    group_name="source_repo",
    retry_policy=dg.RetryPolicy(
        max_retries=3,
        delay=5 * 60,  # 5 minutes
        backoff=dg.Backoff.EXPONENTIAL,
        jitter=dg.Jitter.PLUS_MINUS,
    ),
    automation_condition=dg.AutomationCondition.eager(),
)
def account_graph(
    context: dg.AssetExecutionContext,
    repo_resource: dg.ResourceParam[RepoResource],
    mongo_resource: dg.ResourceParam[MongoDBResource],
) -> dict:
    """
    Balance and USD value of accounts per day.
    """
    mongodb = mongo_resource.get_client()
    commits_by_day = repo_resource.get_commits_by_day()
    partition_date = context.partition_key
    context.log.info(f"Processing data for {partition_date}")
    dct = {}
    dct: dict = perform_data_for_account_graph(context, partition_date, commits_by_day, mongodb)
    return dct


defs = dg.Definitions(
    assets=[account_graph],
    resources={
        "repo_resource": repo_resource_instance,
        "mongo_resource": mongodb_resource_instance,
    },
)
