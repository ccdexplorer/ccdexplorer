import dagster as dg

from ..nightrunner.update_exchange_volume import perform_data_for_exchange_volume
from ._resources import MongoDBResource, mongodb_resource_instance
from ._partitions import partitions_def_from_trading

asset_name: str = "exchange_volume"


@dg.asset(
    partitions_def=partitions_def_from_trading,
    name=asset_name,
    group_name="source_coingecko",
)
def exchange_volume(
    context: dg.AssetExecutionContext, mongo_resource: dg.ResourceParam[MongoDBResource]
) -> dict:
    """
    CCD Exchange volume as requested from CoinGecko API.
    """
    mongodb = mongo_resource.get_client()
    partition_date = context.partition_key
    context.log.info(f"Processing data for {partition_date}")
    dct = {}
    dct: dict = perform_data_for_exchange_volume(context, partition_date, mongodb)
    return dct


defs = dg.Definitions(
    assets=[exchange_volume],
    resources={"mongo_resource": mongodb_resource_instance},
)
