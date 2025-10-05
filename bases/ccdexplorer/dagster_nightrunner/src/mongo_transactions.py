import dagster as dg

from ..nightrunner.update_mongo_transactions import perform_data_for_mongo_transactions
from ._partitions import partitions_def_from_genesis
from ._resources import MongoDBResource, mongodb_resource_instance

asset_name: str = "mongo_transactions"


@dg.asset(
    partitions_def=partitions_def_from_genesis,
    name=asset_name,
    group_name="source_mongo",
)
def mongo_transactions(
    context: dg.AssetExecutionContext, mongo_resource: dg.ResourceParam[MongoDBResource]
) -> dict:
    """
    Calculate network summary statistics per day.
    """
    mongodb = mongo_resource.get_client()
    partition_date = context.partition_key
    context.log.info(f"Processing data for {partition_date}")
    dct = {}
    dct: dict = perform_data_for_mongo_transactions(context, partition_date, mongodb)
    return dct


defs = dg.Definitions(
    assets=[mongo_transactions],
    resources={"mongo_resource": mongodb_resource_instance},
)
