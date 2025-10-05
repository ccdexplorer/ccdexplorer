import dagster as dg

from ..nightrunner.update_tx_types import perform_data_for_tx_types
from ._resources import MongoDBResource, mongodb_resource_instance
from ._partitions import partitions_def_from_genesis

asset_name: str = "tx_types"


@dg.asset(
    partitions_def=partitions_def_from_genesis,
    name=asset_name,
    group_name="source_mongo",
)
def tx_types(
    context: dg.AssetExecutionContext, mongo_resource: dg.ResourceParam[MongoDBResource]
) -> dict:
    """
    Transaction types from all transactions per day.
    """
    mongodb = mongo_resource.get_client()
    partition_date = context.partition_key
    context.log.info(f"Processing data for {partition_date}")
    dct = {}
    dct: dict = perform_data_for_tx_types(context, partition_date, mongodb)
    return dct


defs = dg.Definitions(
    assets=[tx_types],
    resources={"mongo_resource": mongodb_resource_instance},
)
