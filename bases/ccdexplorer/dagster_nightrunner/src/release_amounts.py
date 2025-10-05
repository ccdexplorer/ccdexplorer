import dagster as dg

from ..nightrunner.update_release_amounts import perform_data_for_release_amounts
from ._resources import (
    MongoDBResource,
    CCDScanResource,
    mongodb_resource_instance,
    ccdscan_resource_instance,
)
from ._partitions import partitions_def_from_genesis

asset_name: str = "release_amounts"


@dg.asset(
    partitions_def=partitions_def_from_genesis,
    name=asset_name,
    group_name="source_ccdscan",
)
def release_amounts(
    context: dg.AssetExecutionContext,
    mongo_resource: dg.ResourceParam[MongoDBResource],
    ccdscan_resource: dg.ResourceParam[CCDScanResource],
) -> dict:
    """
    Total amount of released CCD per day.
    """
    mongodb = mongo_resource.get_client()
    ccdscan = ccdscan_resource.get_client()
    partition_date = context.partition_key
    context.log.info(f"Processing data for {partition_date}")
    dct = {}
    dct: dict = perform_data_for_release_amounts(context, partition_date, mongodb, ccdscan)
    return dct


defs = dg.Definitions(
    assets=[release_amounts],
    resources={
        "mongo_resource": mongodb_resource_instance,
        "ccdscan_resource": ccdscan_resource_instance,
    },
)
