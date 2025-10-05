from typing import cast

import dagster as dg
from dagster import MultiPartitionKey

from ..nightrunner.update_unique_addresses import perform_data_for_unique_addresses
from ._resources import MongoDBResource, mongodb_resource_instance
from ._partitions import partitions_def_grouping_from_genesis

asset_name: str = "unique_addresses"


@dg.asset(
    partitions_def=partitions_def_grouping_from_genesis,
    name=asset_name,
    group_name="source_mongo",
)
def unique_addresses(
    context: dg.AssetExecutionContext, mongo_resource: dg.ResourceParam[MongoDBResource]
) -> dict:
    """
    Calculate unique addresses per day and grouping (daily, weekly, monthly). Note that for weekly
    and monthly groupings, it will find the week and/or month bounds for the given date, and
    will always execute a query on the bounds. For weekly it's always Monday-Sunday
    and for monthly it's always the first to the last day of the month.
    """
    mongodb = mongo_resource.get_client()
    multi_partition_key = cast(MultiPartitionKey, context.partition_key)

    date = multi_partition_key.split("|")[0]
    grouping = multi_partition_key.split("|")[1]

    context.log.info(f"Processing data for {date} with grouping {grouping}")
    dct = {}
    dct: dict = perform_data_for_unique_addresses(context, date, grouping, mongodb)
    return dct


defs = dg.Definitions(
    assets=[unique_addresses],
    resources={"mongo_resource": mongodb_resource_instance},
)
