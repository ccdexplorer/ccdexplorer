import dagster as dg
import datetime as dt
from typing import cast
from ..recurring.update_tx_types_count_v2 import update_tx_types_count_hourly
from ._partitions import partitions_def_hourly_net
from ._resources import MongoDBResource, mongodb_resource_instance
from dagster import MultiPartitionKey
from datetime import datetime, timedelta, timezone


def partition_key_to_window(partition_key: str):
    start = datetime.strptime(partition_key, "%Y-%m-%d-%H").replace(tzinfo=timezone.utc)
    end = start + timedelta(hours=1)
    return start, end


asset_name = "tx_types_count_hourly"


@dg.asset(
    partitions_def=partitions_def_hourly_net,
    group_name="source_mongo",
    tags={"reserved": "critical_job"},
)
def tx_types_count_hourly(
    context: dg.AssetExecutionContext,
    mongo_resource: dg.ResourceParam[MongoDBResource],
) -> dict:
    """
    Store tx type counts per (hour, net). Safe to rerun per partition.
    """
    mongodb = mongo_resource.get_client()

    mpk = cast(MultiPartitionKey, context.partition_key)
    datetimestamp = mpk.keys_by_dimension["datetime"]  # "YYYY-MM-DD-HH"
    net = mpk.keys_by_dimension["net"]  # "mainnet" | "testnet"

    context.log.info(f"Processing data for {datetimestamp} (net={net})")

    start, end = partition_key_to_window(datetimestamp)

    # Make sure the aggregation matches only this net as well.
    result = update_tx_types_count_hourly(context, mongodb, net, start, end)
    return result


job = dg.define_asset_job(f"j_{asset_name}", selection=[tx_types_count_hourly])


def _hour_start(dt: dt.datetime):
    dt = dt.astimezone(timezone.utc)
    return dt.replace(minute=0, second=0, microsecond=0)


@dg.schedule(
    job=job,
    cron_schedule="1 * * * *",
    name=f"s_{asset_name}",
)
def schedule(context: dg.ScheduleEvaluationContext):
    # scheduled_execution_time is timezone-aware
    t = context.scheduled_execution_time

    # Convert to UTC to match your HourlyPartitionsDefinition(timezone="UTC")
    t_utc = t.astimezone(dt.timezone.utc)

    # Run the previous fully completed hour partition
    hour_start = t_utc.replace(minute=0, second=0, microsecond=0) - dt.timedelta(hours=1)
    datetime_key = hour_start.strftime("%Y-%m-%d-%H")

    for net in ["mainnet", "testnet"]:
        yield dg.RunRequest(
            run_key=f"{hour_start.isoformat()}|{net}",
            partition_key=f"{datetime_key}|{net}",
        )


defs = dg.Definitions(
    assets=[tx_types_count_hourly],
    jobs=[job],
    schedules=[schedule],
    resources={"mongo_resource": mongodb_resource_instance},
)
