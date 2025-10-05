import datetime as dt

import dagster as dg
from ccdexplorer.mongodb import Collections, MongoDB
from ._jobs import (
    job_from_staking,
    job_from_genesis,
    job_from_trading,
    job_historical_rates,
    job_forex,
    job_unique_addresses,
)
from ._resources import MongoDBResource, mongodb_resource_instance
from ._partitions import partitions_def_tokens, partitions_def_from_genesis


accounts_repo = dg.AssetSpec(
    key="accounts_repo",
    partitions_def=partitions_def_from_genesis,
    group_name="source_node",
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


@dg.sensor(
    minimum_interval_seconds=60,
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def accounts_repo_sensor(
    context: dg.SensorEvaluationContext,
    mongo_resource: dg.ResourceParam[MongoDBResource],
) -> dg.SensorResult:
    mongodb: MongoDB = mongo_resource.get_client()

    ONE_DAY = dt.timedelta(days=1)
    today = dt.datetime.now(dt.UTC).date()

    # Check MongoDB for the trigger condition
    doc = (
        mongodb.mainnet[Collections.helpers].find_one({"_id": "last_known_nightly_accounts"})
    ) or {}

    last_known = parse_mongo_date(doc.get("date"))

    if last_known == today - ONE_DAY:
        # Check if the asset has already been materialized today
        latest_materialization = context.instance.get_latest_materialization_event(
            asset_key=accounts_repo.key
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
            asset_events=[dg.AssetMaterialization(asset_key=accounts_repo.key)],
            cursor=dt.datetime.now(dt.UTC).strftime("%Y-%m-%d %H:%M:%S"),
        )
    else:
        return dg.SensorResult(
            skip_reason=f"Condition not met: last_known={last_known}, expected={today - ONE_DAY}"
        )


@dg.asset_sensor(
    asset_key=accounts_repo.key,
    default_status=dg.DefaultSensorStatus.RUNNING,
    jobs=[
        job_from_staking,
        job_from_genesis,
        job_from_trading,
        job_historical_rates,
        job_unique_addresses,
        job_forex,
    ],
)
def trigger_downstream_jobs(context: dg.SensorEvaluationContext, asset_event):
    ONE_DAY = dt.timedelta(days=1)
    today = dt.datetime.now(dt.UTC).date()
    partition_key = f"{(today - ONE_DAY):%Y-%m-%d}"
    time_partitioned_jobs = [
        dg.RunRequest(
            run_key=f"{context.cursor}_{job.name}",
            job_name=f"{job.name}",
            partition_key=partition_key,
        )
        for job in [
            job_from_staking,
            job_from_genesis,
            job_from_trading,
            job_forex,
        ]
    ]

    token_partitioned_jobs = [
        dg.RunRequest(
            run_key=f"{context.cursor}_{job_historical_rates.name}_{token}",
            job_name=job_historical_rates.name,
            partition_key=token,
        )
        for token in partitions_def_tokens.get_partition_keys()
    ]

    multi_partitioned_jobs = []
    for job in [job_unique_addresses]:
        for grouping in ["daily", "weekly", "monthly"]:
            multi_partitioned_jobs.append(
                dg.RunRequest(
                    run_key=f"{context.cursor}_{job.name}_{grouping}",
                    job_name=job.name,
                    partition_key=f"{partition_key}|{grouping}",
                )
            )
    return time_partitioned_jobs + multi_partitioned_jobs + token_partitioned_jobs


# Define the Definitions object
defs = dg.Definitions(
    assets=[accounts_repo],
    sensors=[accounts_repo_sensor, trigger_downstream_jobs],
    resources={"mongo_resource": mongodb_resource_instance},
)
