import dagster as dg

from ..nightrunner.update_forex import perform_forex_update
from ._partitions import partitions_def_from_plts
from ._resources import MongoDBResource, mongodb_resource_instance

asset_name = "forex"


@dg.asset(
    partitions_def=partitions_def_from_plts,
    name=asset_name,
    group_name="source_coingecko",
    output_required=False,
    automation_condition=dg.AutomationCondition.eager(),
)
async def forex(
    context: dg.AssetExecutionContext, mongo_resource: dg.ResourceParam[MongoDBResource]
) -> bool | None:
    """Historical rates for tokens, fetched from CoinGecko API."""
    mongodb = mongo_resource.get_client()
    partition_key = context.partition_key
    materialized = await perform_forex_update(context, partition_key, mongodb)

    if materialized:
        return True
    else:
        raise Exception(f"Failed to materialize historical rates asset for token: {partition_key}")


defs = dg.Definitions(
    assets=[forex],
    resources={"mongo_resource": mongodb_resource_instance},
)
