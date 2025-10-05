import datetime as dt

import dagster as dg
from ccdexplorer.mongodb import Collections, MongoDB

from ._jobs import job_payday_daily
from ._partitions import partitions_def_from_staking
from ._resources import (
    MongoDBResource,
    grpc_resource_instance,
    mongodb_resource_instance,
)


def parse_mongo_date(raw) -> dt.date | None:
    """
    Turn whatever comes back from Mongo into a date, or None.
    Accepts datetime, date, or YYYY-MM-DD strings.
    """
    if isinstance(raw, dt.datetime):
        return raw.date()
    if isinstance(raw, dt.date):
        return raw
    if isinstance(raw, str):
        try:
            return dt.date.fromisoformat(raw)
        except ValueError:
            return None
    return None


paydays_day_info = dg.AssetSpec(
    key="paydays_day_info",
    partitions_def=partitions_def_from_staking,
    group_name="source_mongo",
)


@dg.sensor(
    minimum_interval_seconds=60,
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def paydays_day_info_sensor(
    context: dg.SensorEvaluationContext,
    mongo_resource: dg.ResourceParam[MongoDBResource],
) -> dg.SensorResult:
    mongodb: MongoDB = mongo_resource.get_client()

    today = dt.datetime.now(dt.UTC).date()

    doc = (mongodb.mainnet[Collections.helpers].find_one({"_id": "last_known_payday"})) or {}

    last_known_payday = parse_mongo_date(doc.get("date"))

    if last_known_payday == today:
        # Check if the asset has already been materialized today
        latest_materialization = context.instance.get_latest_materialization_event(
            asset_key=paydays_day_info.key
        )

        if latest_materialization:
            # Get the timestamp of the latest materialization
            latest_materialization_date = dt.datetime.fromtimestamp(
                latest_materialization.timestamp, tz=dt.UTC
            ).date()

            # If already materialized today, skip
            if latest_materialization_date == today:
                return dg.SensorResult(skip_reason=f"Asset already materialized today ({today})")

        # Materialize the asset if not already done today
        return dg.SensorResult(
            asset_events=[
                dg.AssetMaterialization(
                    asset_key=paydays_day_info.key,
                    metadata={"date": doc.get("date"), "hash": doc.get("hash")},  # type: ignore
                )
            ],  # type: ignore
            cursor=dt.datetime.now(dt.UTC).strftime("%Y-%m-%d %H:%M:%S"),
        )
    else:
        return dg.SensorResult(
            skip_reason=f"Condition not met: last_known_payday={last_known_payday}, expected={today}"
        )


@dg.asset_sensor(
    asset_key=paydays_day_info.key,
    default_status=dg.DefaultSensorStatus.RUNNING,
    jobs=[job_payday_daily],
)
def trigger_payday_calculation(context: dg.SensorEvaluationContext, asset_event):
    today = dt.datetime.now(dt.UTC).date()
    partition_key = f"{(today):%Y-%m-%d}"
    time_partitioned_jobs = [
        dg.RunRequest(
            run_key=f"{context.cursor}_{job.name}",
            job_name=f"{job.name}",
            partition_key=partition_key,
        )
        for job in [job_payday_daily]
    ]
    return time_partitioned_jobs


defs = dg.Definitions(
    assets=[paydays_day_info],
    sensors=[paydays_day_info_sensor, trigger_payday_calculation],
    resources={
        "mongo_resource": mongodb_resource_instance,
        "grpc_resource": grpc_resource_instance,
    },
)
