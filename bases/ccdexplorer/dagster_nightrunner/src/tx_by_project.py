import dagster as dg

from ..nightrunner.update_tx_by_project import perform_data_for_tx_by_project
from ._partitions import partitions_def_from_genesis
from ._resources import MongoDBResource, mongodb_resource_instance

asset_name: str = "tx_by_project"


@dg.asset(
    partitions_def=partitions_def_from_genesis,
    name=asset_name,
    group_name="source_mongo",
)
def tx_by_project(
    context: dg.AssetExecutionContext,
    mongo_resource: dg.ResourceParam[MongoDBResource],
) -> dict:
    """"""
    mongodb = mongo_resource.get_client()
    partition_date = context.partition_key
    context.log.info(f"Processing data for {partition_date}")
    dct = {}
    dct: dict = perform_data_for_tx_by_project(context, partition_date, mongodb)
    return dct


defs = dg.Definitions(
    assets=[tx_by_project],
    resources={
        "mongo_resource": mongodb_resource_instance,
    },
)
