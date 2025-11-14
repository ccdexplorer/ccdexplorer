import dagster as dg

from ..recurring.update_redis_failures import update_redis_failures
from ._partitions import net_partition
from ._resources import (
    MongoDBResource,
    mongodb_resource_instance,
    tooter_resource_instance,
    TooterResource,
)

asset_name = "redis_failures"


@dg.asset(
    partitions_def=net_partition,
    group_name="source_mongo",
)
def redis_failures(
    context: dg.AssetExecutionContext,
    mongo_resource: dg.ResourceParam[MongoDBResource],
    tooter_resource: dg.ResourceParam[TooterResource],
) -> dict:
    """
    Search for redis failures to retry them. .
    """
    mongodb = mongo_resource.get_client()
    tooter = tooter_resource.get_client()
    partition_key = context.partition_key
    result = update_redis_failures(context, mongodb, tooter, partition_key)
    return result


job = dg.define_asset_job(f"j_{asset_name}", selection=[redis_failures])


@dg.schedule(
    job=job,
    cron_schedule="*/5 * * * *",
    name=f"s_{asset_name}",
)
def schedule(context):
    """Schedule that runs all partitions every 5 minutes"""
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
    partition_keys = net_partition.get_partition_keys()

    return [
        dg.RunRequest(
            run_key=f"{context.scheduled_execution_time.isoformat()}_{partition_key}",
            partition_key=partition_key,
        )
        for partition_key in partition_keys
    ]


defs = dg.Definitions(
    assets=[redis_failures],
    jobs=[job],
    schedules=[schedule],
    resources={
        "mongo_resource": mongodb_resource_instance,
        "tooter_resource": tooter_resource_instance,
    },
)
