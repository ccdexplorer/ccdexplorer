import dagster as dg

from ..recurring.update_metadata import update_metadata
from ._partitions import net_partition
from ._resources import (
    MongoDBResource,
    mongodb_resource_instance,
    RedisResource,
    redis_resource_instance,
)

asset_name = "metadata"


@dg.asset(
    partitions_def=net_partition,
    group_name="source_mongo",
)
def metadata(
    context: dg.AssetExecutionContext,
    mongo_resource: dg.ResourceParam[MongoDBResource],
    redis_resource: dg.ResourceParam[RedisResource],
) -> dict:
    """
    A collection that is used to search for memo transfers.
    """
    mongodb = mongo_resource.get_client()
    redis = redis_resource.get_client()
    partition_key = context.partition_key
    result = update_metadata(context, mongodb, redis, partition_key)
    return result


job = dg.define_asset_job(f"j_{asset_name}", selection=[metadata])


@dg.schedule(
    job=job,
    cron_schedule="0 * * * *",
    name=f"s_{asset_name}",
)
def schedule(context):
    """Schedule that runs all partitions every 15 minutes"""
    # Get all partition keys and create a run request for each
    run_records = context.instance.get_run_records(
        dg.RunsFilter(
            job_name=job.name,
            statuses=[
                dg.DagsterRunStatus.QUEUED,
                dg.DagsterRunStatus.NOT_STARTED,
                dg.DagsterRunStatus.STARTING,
                dg.DagsterRunStatus.STARTED,
            ],
        )
    )
    if len(run_records) > 0:
        return dg.SkipReason(
            f"Skipping this run because another run of job '{job.name}' is already running"
        )

    return [
        dg.RunRequest(
            run_key=f"{context.scheduled_execution_time.isoformat()}_{partition_key}",
            partition_key=partition_key,
        )
        for partition_key in ["mainnet"]
    ]


defs = dg.Definitions(
    assets=[metadata],
    jobs=[job],
    schedules=[schedule],
    resources={
        "mongo_resource": mongodb_resource_instance,
        "redis_resource": redis_resource_instance,
    },
)
