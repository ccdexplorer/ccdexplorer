import dagster as dg

from ..nightrunner.update_realized_prices import perform_realized_prices
from .accounts_repo import accounts_repo
from ._resources import (
    MongoDBResource,
    RepoResource,
    mongodb_resource_instance,
    repo_resource_instance,
)

asset_name: str = "realized_prices"


@dg.asset(
    deps=[accounts_repo],
    name=asset_name,
    group_name="source_repo",
    automation_condition=dg.AutomationCondition.eager(),
)
def realized_prices(
    context: dg.AssetExecutionContext,
    repo_resource: dg.ResourceParam[RepoResource],
    mongo_resource: dg.ResourceParam[MongoDBResource],
) -> dict:
    """"""
    mongodb = mongo_resource.get_client()
    commits_by_day = repo_resource.get_commits_by_day()
    dct = {}
    dct: dict = perform_realized_prices(context, commits_by_day, mongodb)
    return dct


defs = dg.Definitions(
    assets=[realized_prices],
    resources={
        "repo_resource": repo_resource_instance,
        "mongo_resource": mongodb_resource_instance,
    },
)
