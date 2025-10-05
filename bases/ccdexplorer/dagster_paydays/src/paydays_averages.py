from typing import cast

import dagster as dg
from dagster import MultiPartitionKey
from ._partitions import partitions_def_daily_apy_averages
from .paydays import paydays_calculation_daily
from ._resources import (
    GRPCResource,
    MongoDBResource,
    grpc_resource_instance,
    mongodb_resource_instance,
)
from .paydays_day_info import paydays_day_info
from ..paydays.update_payday_daily_apy_averages import fill_daily_apy_averages_for_date

asset_name: str = "paydays_calculation_averages"


@dg.asset(
    deps=[paydays_day_info, paydays_calculation_daily],
    name=asset_name,
    partitions_def=partitions_def_daily_apy_averages,
    group_name="source_mongo",
)
def paydays_calculation_averages(
    context: dg.AssetExecutionContext,
    mongo_resource: dg.ResourceParam[MongoDBResource],
    grpc_resource: dg.ResourceParam[GRPCResource],
) -> bool:
    """Asset that updates the paydays average APY collection from the partitioned payday."""
    mongodb = mongo_resource.get_client()
    grpc_client = grpc_resource.get_client()
    multi_partition_key = cast(MultiPartitionKey, context.partition_key)

    date = multi_partition_key.split("|")[0]
    averaging_over_days = int(multi_partition_key.split("|")[1])
    _ = fill_daily_apy_averages_for_date(context, mongodb, grpc_client, date, averaging_over_days)
    return True


defs = dg.Definitions(
    assets=[paydays_calculation_averages],
    resources={
        "mongo_resource": mongodb_resource_instance,
        "grpc_resource": grpc_resource_instance,
    },
)
