import dagster as dg

from ..nightrunner.update_exchange_wallets import perform_data_for_exchange_wallets
from ._resources import MongoDBResource, mongodb_resource_instance
from ._partitions import partitions_def_from_genesis

asset_name: str = "exchange_wallets"


@dg.asset(
    partitions_def=partitions_def_from_genesis,
    name=asset_name,
    group_name="source_mongo",
)
def exchange_wallets(
    context: dg.AssetExecutionContext, mongo_resource: dg.ResourceParam[MongoDBResource]
) -> dict:
    """
    Statistics on counts of exchange wallets (via alias).
    """
    mongodb = mongo_resource.get_client()
    partition_date = context.partition_key
    context.log.info(f"Processing data for {partition_date}")
    dct = {}
    dct: dict = perform_data_for_exchange_wallets(context, partition_date, mongodb)
    return dct


defs = dg.Definitions(
    assets=[exchange_wallets],
    resources={"mongo_resource": mongodb_resource_instance},
)
